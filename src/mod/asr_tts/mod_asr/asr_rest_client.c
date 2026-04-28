/*
 * asr_rest_client.c -- REST HTTP client for one-shot ASR recognition
 *
 * Uses FreeSWITCH's switch_curl wrapper (src/include/switch_curl.h)
 */

#include "mod_asr.h"
#include <switch_curl.h>

typedef struct {
	char *body;
	size_t size;
	switch_memory_pool_t *pool;
} rest_response_t;

static size_t rest_write_callback(void *ptr, size_t size, size_t nmemb, void *data)
{
	rest_response_t *resp = (rest_response_t *) data;
	size_t realsize = size * nmemb;

	if (!resp->body) {
		resp->body = malloc(resp->size + realsize + 1);
	} else {
		char *tmp = realloc(resp->body, resp->size + realsize + 1);
		if (!tmp) return 0;
		resp->body = tmp;
	}

	if (!resp->body) return 0;

	memcpy(&(resp->body[resp->size]), ptr, realsize);
	resp->size += realsize;
	resp->body[resp->size] = '\0';

	return realsize;
}

switch_status_t asr_rest_request(asr_session_t *session, const char *url, const char *method,
								 const char **headers, int header_count,
								 const void *body, size_t body_len,
								 char **response, switch_size_t *response_len)
{
	switch_CURL *curl_handle = NULL;
	switch_curl_slist_t *curl_headers = NULL;
	rest_response_t resp = { 0 };
	long http_code = 0;
	CURLcode curl_res;
	int i;

	if (!session || zstr(url) || !response) {
		return SWITCH_STATUS_FALSE;
	}

	curl_handle = switch_curl_easy_init();
	if (!curl_handle) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to initialize curl\n");
		return SWITCH_STATUS_FALSE;
	}

	resp.pool = session->pool;

	/* Set URL */
	switch_curl_easy_setopt(curl_handle, CURLOPT_URL, url);

	/* SSL options */
	switch_curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYPEER, 0);
	switch_curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYHOST, 0);

	/* Timeout */
	switch_curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, 10);
	switch_curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 30);

	/* Follow redirects */
	switch_curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);
	switch_curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 5);

	/* Response callback */
	switch_curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, rest_write_callback);
	switch_curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *) &resp);

	/* Method and body */
	if (!strcasecmp(method, "POST")) {
		switch_curl_easy_setopt(curl_handle, CURLOPT_POST, 1);
		if (body && body_len > 0) {
			switch_curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, body_len);
			switch_curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, (void *) body);
		}
	} else if (!strcasecmp(method, "PUT")) {
		switch_curl_easy_setopt(curl_handle, CURLOPT_CUSTOMREQUEST, "PUT");
		if (body && body_len > 0) {
			switch_curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, body_len);
			switch_curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, (void *) body);
		}
	} else if (!strcasecmp(method, "DELETE")) {
		switch_curl_easy_setopt(curl_handle, CURLOPT_CUSTOMREQUEST, "DELETE");
	} else {
		/* GET */
		switch_curl_easy_setopt(curl_handle, CURLOPT_HTTPGET, 1);
	}

	/* Custom headers */
	for (i = 0; i < header_count && headers[i]; i++) {
		curl_headers = switch_curl_slist_append(curl_headers, headers[i]);
	}

	if (curl_headers) {
		switch_curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, curl_headers);
	}

	/* Execute */
	curl_res = switch_curl_easy_perform(curl_handle);

	if (curl_res != CURLE_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "REST request failed: %s\n", switch_curl_easy_strerror(curl_res));
		if (curl_headers) switch_curl_slist_free_all(curl_headers);
		switch_curl_easy_cleanup(curl_handle);
		switch_safe_free(resp.body);
		return SWITCH_STATUS_FALSE;
	}

	switch_curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_code);

	if (curl_headers) switch_curl_slist_free_all(curl_headers);
	switch_curl_easy_cleanup(curl_handle);

	if (http_code < 200 || http_code >= 300) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "REST request returned HTTP %ld\n", http_code);
		switch_safe_free(resp.body);
		return SWITCH_STATUS_FALSE;
	}

	if (resp.body && resp.size > 0) {
		*response = resp.body;
		if (response_len) *response_len = (switch_size_t) resp.size;
	} else {
		*response = NULL;
		if (response_len) *response_len = 0;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "REST request completed: HTTP %ld, %zu bytes\n", http_code, resp.size);

	return SWITCH_STATUS_SUCCESS;
}
