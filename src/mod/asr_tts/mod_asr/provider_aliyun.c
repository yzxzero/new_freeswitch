/*
 * provider_aliyun.c -- Aliyun ASR Provider implementation
 *
 * Supports two modes:
 * - WebSocket: Real-time streaming recognition (Aliyun NLS)
 * - REST: One-sentence recognition (HTTP POST)
 *
 * Result polling is done by the worker thread via poll_results():
 * - WS: non-blocking read of WebSocket frames for async results
 * - REST: auto-submit after idle period (no new audio for ~600ms)
 */

#include "mod_asr.h"
#include "provider_aliyun.h"
#include <switch_curl.h>
#include <switch_cJSON.h>

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

/* Write callback for curl responses */
static size_t aliyun_curl_write_cb(void *ptr, size_t size, size_t nmemb, void *data)
{
	switch_stream_handle_t *stream = (switch_stream_handle_t *) data;
	size_t realsize = size * nmemb;
	stream->write_function(stream, "%b", ptr, realsize);
	return realsize;
}

/* ---- Token management ---- */

static char *aliyun_get_token(switch_memory_pool_t *pool)
{
	switch_CURL *curl;
	char url[256];
	switch_stream_handle_t stream = { 0 };
	CURLcode res;
	char *response = NULL;
	size_t resp_len = 0;
	char *token = NULL;
	cJSON *json, *token_obj;

	curl = switch_curl_easy_init();
	if (!curl) return NULL;

	snprintf(url, sizeof(url), "%s?AccessKeyId=%s&Action=CreateToken",
			 ALIYUN_TOKEN_URL, aliyun_globals.access_key_id);

	switch_curl_easy_setopt(curl, CURLOPT_URL, url);
	switch_curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
	switch_curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);
	switch_curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10);
	switch_curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5);

	SWITCH_STANDARD_STREAM(stream);
	switch_curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, aliyun_curl_write_cb);
	switch_curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &stream);

	res = switch_curl_easy_perform(curl);
	switch_curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Aliyun token request failed: %s\n",
						  switch_curl_easy_strerror(res));
		switch_safe_free(stream.data);
		return NULL;
	}

	response = (char *) stream.data;
	resp_len = strlen(response);

	if (!response || resp_len == 0) {
		return NULL;
	}

	json = cJSON_Parse(response);
	if (!json) {
		switch_safe_free(response);
		return NULL;
	}

	token_obj = cJSON_GetObjectItem(json, "Token");
	if (token_obj) {
		cJSON *id_obj = cJSON_GetObjectItem(token_obj, "Id");
		if (id_obj && cJSON_IsString(id_obj)) {
			token = switch_core_strdup(pool, id_obj->valuestring);
		}
	}

	cJSON_Delete(json);
	switch_safe_free(response);

	return token;
}

/* ---- Aliyun provider callbacks ---- */

static switch_status_t aliyun_asr_open(asr_session_t *session, switch_asr_handle_t *ah)
{
	aliyun_asr_ctx_t *ctx;
	switch_status_t status;
	char ws_url[512];
	const char *ws_headers[2];

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
	ctx->task_id = NULL;
	ctx->rest_submitted = SWITCH_FALSE;

	session->provider_private = ctx;

	/* Get authentication token */
	ctx->token = aliyun_get_token(session->pool);
	if (zstr(ctx->token)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to get Aliyun token\n");
		return SWITCH_STATUS_FALSE;
	}

	if (session->mode == ASR_MODE_WEBSOCKET) {
		/* Connect WebSocket with token in URL */
		snprintf(ws_url, sizeof(ws_url), "%s?token=%s", ALIYUN_ASR_WS_URL, ctx->token);
		ws_headers[0] = switch_core_sprintf(session->pool, "X-NLS-Token: %s", ctx->token);
		ws_headers[1] = NULL;

		status = asr_ws_connect(session, ws_url, ws_headers, 1);
		if (status != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to connect Aliyun WS: %s\n", ws_url);
			return SWITCH_STATUS_FALSE;
		}
		ctx->ws_connected = SWITCH_TRUE;
	} else {
		/* REST mode: create audio buffer for accumulation */
		switch_buffer_create_dynamic(&ctx->rest_audio_buffer, 4096, 65536, 0);
		ctx->rest_max_audio_len = 1024 * 1024;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR opened: mode=%s, app_key=%s\n",
					  session->mode == ASR_MODE_WEBSOCKET ? "websocket" : "rest", ctx->app_key);

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t aliyun_asr_close(asr_session_t *session)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!ctx) return SWITCH_STATUS_FALSE;

	if (session->mode == ASR_MODE_WEBSOCKET && asr_ws_is_connected(session)) {
		if (ctx->task_id) {
			char *stop_cmd = switch_mprintf(
				"{\"header\":{\"message_id\":\"%s\",\"task_id\":\"%s\",\"namespace\":\"SpeechRecognizer\",\"name\":\"StopRecognition\",\"appkey\":\"%s\"},\"payload\":{}}",
				switch_core_sprintf(session->pool, "%lld", (long long) switch_micro_time_now()),
				ctx->task_id, ctx->app_key);
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

static switch_status_t aliyun_ws_start_recognition(asr_session_t *session)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;
	char *start_cmd;
	char message_id[64];
	switch_status_t status;

	if (!ctx) return SWITCH_STATUS_FALSE;

	snprintf(message_id, sizeof(message_id), "%lld", (long long) switch_micro_time_now());
	ctx->task_id = switch_core_sprintf(session->pool, "%s", message_id);

	start_cmd = switch_mprintf(
		"{"
		"\"header\":{"
		"\"message_id\":\"%s\","
		"\"task_id\":\"%s\","
		"\"namespace\":\"SpeechRecognizer\","
		"\"name\":\"StartRecognition\","
		"\"appkey\":\"%s\""
		"},"
		"\"payload\":{"
		"\"format\":\"%s\","
		"\"sample_rate\":%d,"
		"\"enable_intermediate_result\":%s,"
		"\"enable_punctuation_prediction\":%s"
		"}"
		"}",
		message_id, ctx->task_id, ctx->app_key,
		ctx->format, ctx->sample_rate,
		ctx->enable_intermediate_result ? "true" : "false",
		ctx->enable_punctuation ? "true" : "false");

	status = asr_ws_send_text(session, start_cmd);
	switch_safe_free(start_cmd);

	if (status == SWITCH_STATUS_SUCCESS) {
		ctx->recognition_started = SWITCH_TRUE;
	}

	return status;
}

static switch_status_t aliyun_asr_feed(asr_session_t *session, void *data, unsigned int len)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;
	switch_size_t total;

	if (!ctx) return SWITCH_STATUS_FALSE;

	if (session->mode == ASR_MODE_WEBSOCKET) {
		if (!asr_ws_is_connected(session)) {
			return SWITCH_STATUS_FALSE;
		}

		if (!ctx->recognition_started) {
			aliyun_ws_start_recognition(session);
		}

		if (ctx->recognition_started) {
			return asr_ws_send_binary(session, data, len);
		}

		return SWITCH_STATUS_SUCCESS;
	} else {
		/* REST mode: accumulate audio in provider buffer */
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
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!ctx) return SWITCH_STATUS_FALSE;

	if (session->mode == ASR_MODE_WEBSOCKET && !ctx->recognition_started) {
		aliyun_ws_start_recognition(session);
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t aliyun_asr_pause(asr_session_t *session)
{
	return SWITCH_STATUS_SUCCESS;
}

/* ---- Result polling (called from worker thread) ---- */

static void aliyun_process_ws_result(asr_session_t *session, const char *msg)
{
	cJSON *json, *header, *payload, *name, *result, *conf_obj, *status_text;
	int confidence;

	if (!msg) return;

	json = cJSON_Parse(msg);
	if (!json) return;

	header = cJSON_GetObjectItem(json, "header");
	payload = cJSON_GetObjectItem(json, "payload");

	if (header) {
		name = cJSON_GetObjectItem(header, "name");
		if (name && cJSON_IsString(name)) {
			if (!strcmp(name->valuestring, "SpeechRecognizerResultChanged")) {
				if (payload) {
					result = cJSON_GetObjectItem(payload, "result");
					if (result && cJSON_IsString(result)) {
						switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
										  "Aliyun intermediate: %s\n", result->valuestring);
					}
				}
			} else if (!strcmp(name->valuestring, "SpeechRecognizerCompleted") ||
					   !strcmp(name->valuestring, "RecognitionCompleted")) {
				if (payload) {
					result = cJSON_GetObjectItem(payload, "result");
					if (result && cJSON_IsString(result)) {
						confidence = 100;
						conf_obj = cJSON_GetObjectItem(payload, "confidence");
						if (conf_obj && cJSON_IsNumber(conf_obj)) {
							confidence = (int) (conf_obj->valuedouble);
						}
						asr_session_set_result(session, result->valuestring, confidence);
					}
				}
			} else if (!strcmp(name->valuestring, "TaskFailed")) {
				status_text = cJSON_GetObjectItem(header, "status_text");
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
								  "Aliyun ASR task failed: %s\n",
								  status_text ? status_text->valuestring : "unknown");
			}
		}
	}

	cJSON_Delete(json);
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

static switch_status_t aliyun_asr_poll_results(asr_session_t *session)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;
	char *msg;

	if (!ctx) return SWITCH_STATUS_FALSE;

	if (session->mode == ASR_MODE_WEBSOCKET) {
		/* Non-blocking WS read: only read if data is available */
		if (asr_ws_has_data(session)) {
			msg = asr_ws_recv_text(session, session->pool);
			if (msg) {
				aliyun_process_ws_result(session, msg);
			}
		}
	} else {
		/* REST mode: auto-submit after idle threshold */
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

/* check_results is now lightweight - just checks session state (called from media thread) */
static switch_status_t aliyun_asr_check_results(asr_session_t *session)
{
	switch_status_t status;

	if (!session) return SWITCH_STATUS_FALSE;

	switch_mutex_lock(session->mutex);
	status = (session->state == ASR_SESSION_STATE_RESULT_READY) ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
	switch_mutex_unlock(session->mutex);

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
	/* Reserved for future float params */
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
