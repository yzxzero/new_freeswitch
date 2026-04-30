/*
 * provider_aliyun.c -- Aliyun ASR Provider implementation
 *
 * Supports two modes:
 * - WebSocket: Real-time streaming transcription (Aliyun NLS SpeechTranscriber)
 * - REST: One-sentence recognition (HTTP POST)
 *
 * WebSocket protocol (matching official JS demo):
 *   1. Connect to wss://nls-gateway-cn-shanghai.aliyuncs.com/ws/v1?token=xxx
 *   2. IMMEDIATELY send StartTranscription (in open callback)
 *   3. Wait for TranscriptionStarted event
 *   4. Then send audio data (Binary frames)
 *   5. Receive TranscriptionResultChanged / SentenceEnd events
 *   6. Send StopTranscription to end
 *   7. Receive TranscriptionCompleted event
 */

#include "mod_asr.h"
#include "provider_aliyun.h"
#include <switch_curl.h>
#include <switch_cJSON.h>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>

/* Global aliyun config (loaded once at module init) */
static struct {
	char *access_key_id;
	char *access_key_secret;
	char *app_key;
	char *region;
	char *format;
	int sample_rate;
	switch_bool_t enable_intermediate_result;
	switch_bool_t enable_punctuation;
	int rest_timeout;
	switch_memory_pool_t *pool;
} aliyun_globals;

/* Simple buffer for curl response */
typedef struct {
	char *data;
	size_t size;
} aliyun_curl_buf_t;

static size_t aliyun_curl_write_cb(void *ptr, size_t size, size_t nmemb, void *data)
{
	aliyun_curl_buf_t *buf = (aliyun_curl_buf_t *) data;
	size_t realsize = size * nmemb;
	char *tmp = realloc(buf->data, buf->size + realsize + 1);
	if (!tmp) return 0;
	buf->data = tmp;
	memcpy(buf->data + buf->size, ptr, realsize);
	buf->size += realsize;
	buf->data[buf->size] = '\0';
	return realsize;
}

/* Generate a 32-char hex UUID-like string for task_id / message_id */
static void aliyun_generate_id(char *buf, size_t buflen)
{
	int i;
	snprintf(buf, buflen, "%08x%08x%08x%08x",
		(unsigned int) (switch_micro_time_now() & 0xFFFFFFFF),
		(unsigned int) ((switch_micro_time_now() >> 16) & 0xFFFFFFFF),
		(unsigned int) ((uintptr_t) buf & 0xFFFFFFFF),
		(unsigned int) ((switch_micro_time_now() * 6364136223846793005LL + 1442695040888963407LL) & 0xFFFFFFFF));
	for (i = 0; i < 32 && i < (int)(buflen - 1); i++) {
		if (!isxdigit(buf[i])) buf[i] = '0';
	}
	buf[32 < buflen - 1 ? 32 : buflen - 1] = '\0';
}

/* ---- Token management ---- */

/* URL-encode per Aliyun POP API spec (RFC 3986 unreserved chars) */
static size_t aliyun_url_encode(const char *src, char *dst, size_t dst_len)
{
	static const char *unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";
	size_t i, j = 0;
	for (i = 0; src[i] && j < dst_len - 4; i++) {
		if (strchr(unreserved, src[i])) {
			dst[j++] = src[i];
		} else {
			j += snprintf(dst + j, dst_len - j, "%%%02X", (unsigned char) src[i]);
		}
	}
	dst[j] = '\0';
	return j;
}

/* HMAC-SHA1 + Base64 for Aliyun POP API signature */
static void aliyun_hmac_sha1_base64(const char *key, const char *data, char *out, size_t out_len)
{
	unsigned char hmac_result[EVP_MAX_MD_SIZE];
	unsigned int hmac_len = 0;
	char key_buf[512];
	size_t key_len;
	BIO *bio, *b64;
	BUF_MEM *bptr;
	long b64_len;

	key_len = snprintf(key_buf, sizeof(key_buf), "%s&", key);

	HMAC(EVP_sha1(), key_buf, (int) key_len,
		 (const unsigned char *) data, (int) strlen(data),
		 hmac_result, &hmac_len);

	b64 = BIO_new(BIO_f_base64());
	bio = BIO_new(BIO_s_mem());
	b64 = BIO_push(b64, bio);
	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
	BIO_write(b64, hmac_result, (int) hmac_len);
	BIO_flush(b64);
	BIO_get_mem_ptr(b64, &bptr);
	b64_len = bptr->length < (long)(out_len - 1) ? bptr->length : (long)(out_len - 1);
	memcpy(out, bptr->data, b64_len);
	out[b64_len] = '\0';
	BIO_free_all(b64);
}

static char *aliyun_get_token(switch_memory_pool_t *pool)
{
	switch_CURL *curl;
	aliyun_curl_buf_t buf = { NULL, 0 };
	CURLcode res;
	char *response = NULL;
	size_t resp_len = 0;
	char *token = NULL;
	cJSON *json, *token_obj;

	char timestamp[64];
	char nonce[37];
	switch_time_exp_t tm;
	char enc_val[512];
	char canonical_query[4096];
	char string_to_sign[8192];
	char signature[256];
	char url[8192];
	int offset = 0;

	switch_time_exp_gmt(&tm, switch_micro_time_now());
	snprintf(timestamp, sizeof(timestamp), "%04d-%02d-%02dT%02d:%02d:%02dZ",
			 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
			 tm.tm_hour, tm.tm_min, tm.tm_sec);

	snprintf(nonce, sizeof(nonce), "%08x%08x%08x%08x",
			 (unsigned int) (switch_micro_time_now() & 0xFFFFFFFF),
			 (unsigned int) ((switch_micro_time_now() >> 16) & 0xFFFFFFFF),
			 (unsigned int) ((uintptr_t) &offset & 0xFFFFFFFF),
			 (unsigned int) ((switch_micro_time_now() * 6364136223846793005LL) & 0xFFFFFFFF));

	/* Build canonical query string: URL-encode each param value,
	 * join with literal = and & separators, alphabetical by name */
	offset = 0;
	aliyun_url_encode(aliyun_globals.access_key_id, enc_val, sizeof(enc_val));
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"AccessKeyId=%s", enc_val);
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&Action=CreateToken");
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&Format=JSON");
	aliyun_url_encode(aliyun_globals.region, enc_val, sizeof(enc_val));
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&RegionId=%s", enc_val);
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&SignatureMethod=HMAC-SHA1");
	aliyun_url_encode(nonce, enc_val, sizeof(enc_val));
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&SignatureNonce=%s", enc_val);
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&SignatureVersion=1.0");
	aliyun_url_encode(timestamp, enc_val, sizeof(enc_val));
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&Timestamp=%s", enc_val);
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&Version=2019-02-28");

	/* String to sign: "GET&" + URL-encode("/") + "&" + URL-encode(canonical_query) */
	aliyun_url_encode(canonical_query, enc_val, sizeof(enc_val));
	snprintf(string_to_sign, sizeof(string_to_sign), "GET&%%2F&%s", enc_val);

	aliyun_hmac_sha1_base64(aliyun_globals.access_key_secret, string_to_sign, signature, sizeof(signature));

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
		"Aliyun token string_to_sign: %s\n", string_to_sign);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
		"Aliyun token signature: %s\n", signature);

	/* Build final URL using canonical_query + URL-encoded Signature */
	aliyun_url_encode(signature, enc_val, sizeof(enc_val));
	snprintf(url, sizeof(url), "%s?%s&Signature=%s",
			 ALIYUN_TOKEN_URL, canonical_query, enc_val);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun token request URL: %s\n", url);

	curl = switch_curl_easy_init();
	if (!curl) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "curl init failed\n");
		return NULL;
	}

	switch_curl_easy_setopt(curl, CURLOPT_URL, url);
	switch_curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
	switch_curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);
	switch_curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10);
	switch_curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5);
	switch_curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
	switch_curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);

	switch_curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, aliyun_curl_write_cb);
	switch_curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &buf);

	res = switch_curl_easy_perform(curl);
	switch_curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Aliyun token request failed: %s\n",
						  switch_curl_easy_strerror(res));
		switch_safe_free(buf.data);
		return NULL;
	}

	response = buf.data;
	resp_len = buf.size;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun token response: %s\n", response);

	if (!response || resp_len == 0) {
		return NULL;
	}

	json = cJSON_Parse(response);
	if (!json) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Aliyun token JSON parse failed\n");
		switch_safe_free(response);
		return NULL;
	}

	token_obj = cJSON_GetObjectItem(json, "Token");
	if (token_obj) {
		cJSON *id_obj = cJSON_GetObjectItem(token_obj, "Id");
		if (id_obj && cJSON_IsString(id_obj)) {
			token = switch_core_strdup(pool, id_obj->valuestring);
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun token obtained OK\n");
		}
	} else {
		cJSON *err_obj = cJSON_GetObjectItem(json, "Code");
		if (err_obj) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
				"Aliyun token error: %s - %s\n",
				err_obj->valuestring,
				cJSON_GetObjectItem(json, "Message") ? cJSON_GetObjectItem(json, "Message")->valuestring : "unknown");
		} else {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Aliyun token response missing Token field\n");
		}
	}

	cJSON_Delete(json);
	switch_safe_free(response);

	return token;
}

/* Forward declaration */
static switch_status_t aliyun_rest_recognize(asr_session_t *session);

/* ---- Process WS result messages ---- */

static void aliyun_process_ws_result(asr_session_t *session, const char *msg)
{
	cJSON *json, *header, *payload, *name, *status_obj, *result, *conf_obj;
	int confidence;
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!msg) return;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun WS recv: %s\n", msg);

	json = cJSON_Parse(msg);
	if (!json) return;

	header = cJSON_GetObjectItem(json, "header");
	payload = cJSON_GetObjectItem(json, "payload");

	if (header) {
		name = cJSON_GetObjectItem(header, "name");
		if (name && cJSON_IsString(name)) {
			if (!strcmp(name->valuestring, "TranscriptionStarted")) {
				if (ctx) ctx->transcription_started = SWITCH_TRUE;
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun TranscriptionStarted: ready for audio\n");
			} else if (!strcmp(name->valuestring, "SentenceBegin")) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Aliyun SentenceBegin\n");
				switch_set_flag(session, ASR_SESSION_FLAG_START_OF_SPEECH);
			} else if (!strcmp(name->valuestring, "TranscriptionResultChanged")) {
				if (payload) {
					result = cJSON_GetObjectItem(payload, "result");
					if (result && cJSON_IsString(result)) {
						switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
										  "Aliyun intermediate: %s\n", result->valuestring);
					}
				}
			} else if (!strcmp(name->valuestring, "SentenceEnd")) {
				if (payload) {
					result = cJSON_GetObjectItem(payload, "result");
					if (result && cJSON_IsString(result)) {
						confidence = 100;
						conf_obj = cJSON_GetObjectItem(payload, "confidence");
						if (conf_obj && cJSON_IsNumber(conf_obj)) {
							confidence = (int) (conf_obj->valuedouble * 100);
						}
						switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
										  "Aliyun SentenceEnd: %s (confidence=%d)\n",
										  result->valuestring, confidence);
						asr_session_set_result(session, result->valuestring, confidence);
					}
				}
			} else if (!strcmp(name->valuestring, "TranscriptionCompleted")) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun TranscriptionCompleted\n");
			} else if (!strcmp(name->valuestring, "TaskFailed")) {
				status_obj = cJSON_GetObjectItem(header, "status_text");
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
								  "Aliyun ASR task failed: %s (msg=%s)\n",
								  status_obj ? status_obj->valuestring : "unknown", msg);
			} else {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  "Aliyun unknown event: %s\n", name->valuestring);
			}
		}
	}

	cJSON_Delete(json);
}

/* ---- Aliyun provider callbacks ---- */

static switch_status_t aliyun_asr_open(asr_session_t *session, switch_asr_handle_t *ah)
{
	aliyun_asr_ctx_t *ctx;
	switch_status_t status;
	char ws_url[512];
	const char *ws_headers[2];
	char task_id[33];
	char message_id[33];
	char *start_cmd;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: START\n");

	ctx = switch_core_alloc(session->pool, sizeof(*ctx));
	if (!ctx) return SWITCH_STATUS_MEMERR;

	memset(ctx, 0, sizeof(*ctx));

	ctx->access_key_id = switch_core_strdup(session->pool, aliyun_globals.access_key_id);
	ctx->access_key_secret = switch_core_strdup(session->pool, aliyun_globals.access_key_secret);
	ctx->app_key = switch_core_strdup(session->pool, aliyun_globals.app_key);
	ctx->region = switch_core_strdup(session->pool, aliyun_globals.region);
	ctx->format = switch_core_strdup(session->pool, aliyun_globals.format);
	ctx->sample_rate = aliyun_globals.sample_rate;
	ctx->enable_intermediate_result = aliyun_globals.enable_intermediate_result;
	ctx->enable_punctuation = aliyun_globals.enable_punctuation;
	ctx->rest_timeout = aliyun_globals.rest_timeout;
	ctx->ws_connected = SWITCH_FALSE;
	ctx->recognition_started = SWITCH_FALSE;
	ctx->transcription_started = SWITCH_FALSE;
	ctx->task_id = NULL;
	ctx->rest_submitted = SWITCH_FALSE;

	if (session->native_rate > 0) {
		ctx->sample_rate = session->native_rate;
	}

	session->provider_private = ctx;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: getting token...\n");

	ctx->token = aliyun_get_token(session->pool);
	if (zstr(ctx->token)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to get Aliyun token\n");
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: token obtained (len=%zu)\n", strlen(ctx->token));

	if (session->mode == ASR_MODE_WEBSOCKET) {
		snprintf(ws_url, sizeof(ws_url), "%s?token=%s", ALIYUN_ASR_WS_URL, ctx->token);
		ws_headers[0] = switch_core_sprintf(session->pool, "X-NLS-Token: %s", ctx->token);
		ws_headers[1] = NULL;

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: connecting WS to %s\n", ALIYUN_ASR_WS_URL);

		status = asr_ws_connect(session, ws_url, ws_headers, 1);
		if (status != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to connect Aliyun WS: %s\n", ws_url);
			return SWITCH_STATUS_FALSE;
		}
		ctx->ws_connected = SWITCH_TRUE;

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: WS connected, sending StartTranscription\n");

		/* IMMEDIATELY send StartTranscription (matching JS demo) */
		aliyun_generate_id(task_id, sizeof(task_id));
		aliyun_generate_id(message_id, sizeof(message_id));
		ctx->task_id = switch_core_strdup(session->pool, task_id);

		start_cmd = switch_mprintf(
			"{"
			"\"header\":{"
			"\"appkey\":\"%s\","
			"\"namespace\":\"SpeechTranscriber\","
			"\"name\":\"StartTranscription\","
			"\"task_id\":\"%s\","
			"\"message_id\":\"%s\""
			"},"
			"\"payload\":{"
			"\"format\":\"%s\","
			"\"sample_rate\":%d,"
			"\"enable_intermediate_result\":%s,"
			"\"enable_punctuation_prediction\":%s,"
			"\"enable_inverse_text_normalization\":true"
			"}"
			"}",
			ctx->app_key, task_id, message_id,
			ctx->format, ctx->sample_rate,
			ctx->enable_intermediate_result ? "true" : "false",
			ctx->enable_punctuation ? "true" : "false");

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
			"Sending StartTranscription: appkey=%s, task_id=%s, format=%s, sample_rate=%d\n",
			ctx->app_key, task_id, ctx->format, ctx->sample_rate);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
			"StartTranscription JSON: %s\n", start_cmd);

		status = asr_ws_send_text(session, start_cmd);
		switch_safe_free(start_cmd);

		if (status != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to send StartTranscription\n");
			return SWITCH_STATUS_FALSE;
		}
		ctx->recognition_started = SWITCH_TRUE;

		/* Wait for TranscriptionStarted event (up to 5 seconds) */
		{
			int wait_ms;
			for (wait_ms = 0; wait_ms < 5000; wait_ms += 100) {
				if (ctx->transcription_started) break;
				if (asr_ws_has_data(session)) {
					char *msg = asr_ws_recv_text(session, session->pool);
					if (msg) {
						aliyun_process_ws_result(session, msg);
					}
				} else {
					switch_yield(100000);
				}
			}

			if (!ctx->transcription_started) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
					"Timed out waiting for TranscriptionStarted after %dms\n", wait_ms);
				return SWITCH_STATUS_FALSE;
			}
		}

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
			"Aliyun ASR ready: mode=websocket, app_key=%s, sample_rate=%d\n",
			ctx->app_key, ctx->sample_rate);
	} else {
		switch_buffer_create_dynamic(&ctx->rest_audio_buffer, 4096, 65536, 0);
		ctx->rest_max_audio_len = 1024 * 1024;

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
			"Aliyun ASR ready: mode=rest, app_key=%s, sample_rate=%d\n",
			ctx->app_key, ctx->sample_rate);
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t aliyun_asr_close(asr_session_t *session)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!ctx) return SWITCH_STATUS_FALSE;

	if (session->mode == ASR_MODE_WEBSOCKET && asr_ws_is_connected(session)) {
		if (ctx->task_id) {
			char stop_msg_id[33];
			char *stop_cmd;
			aliyun_generate_id(stop_msg_id, sizeof(stop_msg_id));
			stop_cmd = switch_mprintf(
				"{\"header\":{\"appkey\":\"%s\",\"namespace\":\"SpeechTranscriber\",\"name\":\"StopTranscription\",\"task_id\":\"%s\",\"message_id\":\"%s\"},\"payload\":{}}",
				ctx->app_key, ctx->task_id, stop_msg_id);
			asr_ws_send_text(session, stop_cmd);
			switch_safe_free(stop_cmd);
		}
		asr_ws_disconnect(session);
		ctx->ws_connected = SWITCH_FALSE;
	}

	if (ctx->rest_audio_buffer) {
		switch_buffer_destroy(&ctx->rest_audio_buffer);
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR closed\n");

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t aliyun_asr_feed(asr_session_t *session, void *data, unsigned int len)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;
	switch_size_t total;
	static uint32_t ws_feed_count = 0;
	static uint32_t ws_total_bytes = 0;
	switch_status_t send_status;

	if (!ctx) return SWITCH_STATUS_FALSE;

	if (session->mode == ASR_MODE_WEBSOCKET) {
		if (!asr_ws_is_connected(session)) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
				"Aliyun feed: WS not connected, dropping %u bytes\n", len);
			return SWITCH_STATUS_FALSE;
		}

		if (!ctx->transcription_started) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
				"Audio dropped: TranscriptionStarted not received yet (%u bytes)\n", len);
			return SWITCH_STATUS_SUCCESS;
		}

		ws_feed_count++;
		ws_total_bytes += len;

		send_status = asr_ws_send_binary(session, data, len);

		if (ws_feed_count <= 5 || ws_feed_count % 50 == 0) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
				"Aliyun WS feed: #%u, bytes=%u, total=%u, send_status=%d, sample_rate=%d\n",
				ws_feed_count, len, ws_total_bytes, send_status, ctx->sample_rate);
		}

		if (send_status != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
				"Aliyun WS send_binary FAILED: feed #%u, bytes=%u, status=%d\n",
				ws_feed_count, len, send_status);
		}

		return send_status;
	} else {
		if (ctx->rest_audio_buffer) {
			total = switch_buffer_inuse(ctx->rest_audio_buffer) + len;
			if (total <= ctx->rest_max_audio_len) {
				switch_buffer_write(ctx->rest_audio_buffer, data, len);
			}
		}
		return SWITCH_STATUS_SUCCESS;
	}
}

static switch_status_t aliyun_asr_resume(asr_session_t *session)
{
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t aliyun_asr_pause(asr_session_t *session)
{
	return SWITCH_STATUS_SUCCESS;
}

/* ---- Result polling (called from worker thread) ---- */

static switch_status_t aliyun_asr_poll_results(asr_session_t *session)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;
	char *msg;
	static uint32_t poll_count = 0;

	if (!ctx) return SWITCH_STATUS_FALSE;

	poll_count++;

	if (session->mode == ASR_MODE_WEBSOCKET) {
		while (asr_ws_has_data(session)) {
			msg = asr_ws_recv_text(session, session->pool);
			if (msg) {
				aliyun_process_ws_result(session, msg);
			} else {
				break;
			}
		}
		if (poll_count % 100 == 0) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
				"Aliyun poll_results: #%u, ws_connected=%d, transcription_started=%d, has_data=%d\n",
				poll_count, ctx->ws_connected, ctx->transcription_started, asr_ws_has_data(session));
		}
	} else {
		if (!ctx->rest_submitted && session->idle_count >= ASR_REST_IDLE_THRESHOLD) {
			if (switch_buffer_inuse(ctx->rest_audio_buffer) > 0) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
								  "Aliyun REST auto-submit: idle_count=%d, audio=%zu\n",
								  session->idle_count, switch_buffer_inuse(ctx->rest_audio_buffer));
				ctx->rest_submitted = SWITCH_TRUE;
				aliyun_rest_recognize(session);
			}
		}
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t aliyun_rest_recognize(asr_session_t *session)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;
	char url[512];
	char *response = NULL;
	switch_size_t resp_len = 0;
	switch_size_t audio_len;
	uint8_t *audio_data;
	switch_status_t status;
	const char *headers[3];
	cJSON *json, *result_obj, *conf_obj;
	int confidence;

	if (!ctx || !ctx->rest_audio_buffer) return SWITCH_STATUS_FALSE;

	audio_len = switch_buffer_inuse(ctx->rest_audio_buffer);
	if (audio_len == 0) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "No audio data for REST recognition\n");
		return SWITCH_STATUS_FALSE;
	}

	audio_data = malloc(audio_len);
	if (!audio_data) return SWITCH_STATUS_MEMERR;

	switch_buffer_read(ctx->rest_audio_buffer, audio_data, audio_len);

	snprintf(url, sizeof(url),
			 "%s?appkey=%s&format=%s&sample_rate=%d&enable_punctuation_prediction=%s",
			 ALIYUN_ASR_REST_URL,
			 ctx->app_key,
			 ctx->format,
			 ctx->sample_rate,
			 ctx->enable_punctuation ? "true" : "false");

	headers[0] = "Content-Type: application/octet-stream";
	headers[1] = switch_core_sprintf(session->pool, "X-NLS-Token: %s", ctx->token);
	headers[2] = NULL;

	status = asr_rest_request(session, url, "POST", headers, 2,
							  audio_data, audio_len, &response, &resp_len);
	free(audio_data);

	if (status != SWITCH_STATUS_SUCCESS || !response) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Aliyun REST recognition failed\n");
		return SWITCH_STATUS_FALSE;
	}

	json = cJSON_Parse(response);
	if (json) {
		result_obj = cJSON_GetObjectItem(json, "result");
		if (result_obj && cJSON_IsString(result_obj)) {
			confidence = 100;
			conf_obj = cJSON_GetObjectItem(json, "confidence");
			if (conf_obj && cJSON_IsNumber(conf_obj)) {
				confidence = (int) (conf_obj->valuedouble);
			}
			asr_session_set_result(session, result_obj->valuestring, confidence);
		}
		cJSON_Delete(json);
	}

	switch_safe_free(response);

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t aliyun_asr_check_results(asr_session_t *session)
{
	switch_status_t status;

	if (!session) return SWITCH_STATUS_FALSE;

	switch_mutex_lock(session->mutex);
	status = (session->state == ASR_SESSION_STATE_RESULT_READY) ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
	switch_mutex_unlock(session->mutex);

	if (status == SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
			"Aliyun check_results: RESULT READY for session %s\n", session->id);
	}

	return status;
}

static switch_status_t aliyun_asr_get_results(asr_session_t *session, char **result_xml)
{
	return asr_session_get_result(session, result_xml);
}

static switch_status_t aliyun_asr_start_input_timers(asr_session_t *session)
{
	return SWITCH_STATUS_SUCCESS;
}

static void aliyun_asr_text_param(asr_session_t *session, const char *param, const char *val)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!param || zstr(val)) return;

	if (!strcasecmp(param, "app-key") && ctx) {
		ctx->app_key = switch_core_strdup(session->pool, val);
	} else if (!strcasecmp(param, "format") && ctx) {
		ctx->format = switch_core_strdup(session->pool, val);
	} else if (!strcasecmp(param, "sample-rate") && ctx) {
		ctx->sample_rate = atoi(val);
	} else if (!strcasecmp(param, "enable-intermediate-result") && ctx) {
		ctx->enable_intermediate_result = switch_true(val);
	} else if (!strcasecmp(param, "enable-punctuation") && ctx) {
		ctx->enable_punctuation = switch_true(val);
	} else if (!strcasecmp(param, "language") && session) {
		session->language = switch_core_strdup(session->pool, val);
	}
}

static void aliyun_asr_numeric_param(asr_session_t *session, const char *param, int val)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!param) return;

	if (!strcasecmp(param, "sample-rate") && ctx) {
		ctx->sample_rate = val;
	} else if (!strcasecmp(param, "rest-timeout") && ctx) {
		ctx->rest_timeout = val;
	}
}

static void aliyun_asr_float_param(asr_session_t *session, const char *param, double val)
{
}

/* Aliyun provider interface definition */
static asr_provider_interface_t aliyun_provider = {
	.name = "aliyun",
	.open = aliyun_asr_open,
	.close = aliyun_asr_close,
	.feed = aliyun_asr_feed,
	.resume = aliyun_asr_resume,
	.pause = aliyun_asr_pause,
	.check_results = aliyun_asr_check_results,
	.get_results = aliyun_asr_get_results,
	.start_input_timers = aliyun_asr_start_input_timers,
	.text_param = aliyun_asr_text_param,
	.numeric_param = aliyun_asr_numeric_param,
	.float_param = aliyun_asr_float_param,
	.poll_results = aliyun_asr_poll_results,
	.next = NULL
};

/* Load aliyun config from XML */
static switch_status_t aliyun_load_config(switch_memory_pool_t *pool)
{
	switch_xml_t cfg, xml, settings, xprovider, param;

	if (!(xml = switch_xml_open_cfg("asr.conf", &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Failed to open asr.conf\n");
		return SWITCH_STATUS_FALSE;
	}

	if ((settings = switch_xml_child(cfg, "providers"))) {
		for (xprovider = switch_xml_child(settings, "provider"); xprovider; xprovider = switch_xml_next(xprovider)) {
			const char *name = switch_xml_attr(xprovider, "name");
			if (!name || strcasecmp(name, "aliyun")) continue;

			for (param = switch_xml_child(xprovider, "param"); param; param = switch_xml_next(param)) {
				const char *pname = switch_xml_attr(param, "name");
				const char *pval = switch_xml_attr(param, "value");

				if (zstr(pname) || zstr(pval)) continue;

				if (!strcasecmp(pname, "access-key-id")) {
					aliyun_globals.access_key_id = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "access-key-secret")) {
					aliyun_globals.access_key_secret = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "app-key")) {
					aliyun_globals.app_key = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "region")) {
					aliyun_globals.region = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "format")) {
					aliyun_globals.format = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "sample-rate")) {
					aliyun_globals.sample_rate = atoi(pval);
				} else if (!strcasecmp(pname, "enable-intermediate-result")) {
					aliyun_globals.enable_intermediate_result = switch_true(pval);
				} else if (!strcasecmp(pname, "enable-punctuation")) {
					aliyun_globals.enable_punctuation = switch_true(pval);
				} else if (!strcasecmp(pname, "rest-timeout")) {
					aliyun_globals.rest_timeout = atoi(pval);
				}
			}
			break;
		}
	}

	switch_xml_free(xml);

	if (zstr(aliyun_globals.region)) aliyun_globals.region = "cn-shanghai";
	if (zstr(aliyun_globals.format)) aliyun_globals.format = "pcm";
	if (aliyun_globals.sample_rate == 0) aliyun_globals.sample_rate = 16000;
	if (aliyun_globals.rest_timeout == 0) aliyun_globals.rest_timeout = 10000;

	return SWITCH_STATUS_SUCCESS;
}

switch_status_t asr_provider_aliyun_load(switch_memory_pool_t *pool)
{
	aliyun_globals.pool = pool;

	if (aliyun_load_config(pool) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Aliyun provider config not found, using defaults\n");
	}

	if (zstr(aliyun_globals.access_key_id) || zstr(aliyun_globals.access_key_secret) || zstr(aliyun_globals.app_key)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
						  "Aliyun ASR provider requires access-key-id, access-key-secret, and app-key in config\n");
		return SWITCH_STATUS_FALSE;
	}

	return asr_provider_register(&aliyun_provider);
}
