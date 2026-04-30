/*
 * asr_session.c -- ASR session management and worker thread lifecycle
 *
 * Worker thread handles audio feeding and result polling.
 * Uses timed cond_wait (200ms) so poll_results is called regularly
 * even when no new audio arrives. This is critical for:
 * - WebSocket: receiving async results from the ASR server
 * - REST: detecting idle periods and auto-submitting recognition
 */

#include "mod_asr.h"

static void *SWITCH_THREAD_FUNC asr_session_worker_thread(switch_thread_t *thread, void *obj)
{
	asr_session_t *session = (asr_session_t *) obj;
	switch_size_t avail;
	uint8_t *data;
	switch_size_t read_len;
	uint32_t feed_count = 0;
	uint32_t total_bytes_fed = 0;
	uint32_t last_report_bytes = 0;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "ASR session worker started: %s (provider=%s, mode=%s, native_rate=%d)\n",
		session->id, session->provider ? session->provider->name : "none",
		session->mode == ASR_MODE_WEBSOCKET ? "ws" : "rest", session->native_rate);

	/* Note: provider->open() is now called from mod_asr_asr_open() before
	   the worker thread starts, so we just set state to LISTENING here */

	switch_mutex_lock(session->mutex);
	session->state = ASR_SESSION_STATE_LISTENING;
	switch_set_flag(session, ASR_SESSION_FLAG_READY);
	switch_mutex_unlock(session->mutex);

	while (session->running) {
		switch_mutex_lock(session->mutex);

		/* Wait for audio with 200ms timeout so we can poll results regularly */
		if (switch_buffer_inuse(session->audio_buffer) == 0 && session->running) {
			switch_thread_cond_timedwait(session->cond, session->mutex, 200000);
		}

		if (!session->running) {
			switch_mutex_unlock(session->mutex);
			break;
		}

		avail = switch_buffer_inuse(session->audio_buffer);
		if (avail > 0) {
			data = malloc(avail);
			if (!data) {
				switch_mutex_unlock(session->mutex);
				break;
			}
			read_len = switch_buffer_read(session->audio_buffer, data, avail);
			session->idle_count = 0;
			switch_mutex_unlock(session->mutex);

			if (read_len > 0 && session->provider && session->provider->feed) {
				session->provider->feed(session, data, (unsigned int) read_len);
				feed_count++;
				total_bytes_fed += (uint32_t) read_len;
				/* Log every ~50 feeds or ~160KB of audio */
				if (total_bytes_fed - last_report_bytes >= 160000) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
						"ASR worker: session %s, feed #%u, total_bytes=%u, last_chunk=%zu\n",
						session->id, feed_count, total_bytes_fed, read_len);
					last_report_bytes = total_bytes_fed;
				}
			}

			free(data);
		} else {
			session->idle_count++;
			switch_mutex_unlock(session->mutex);
		}

		/* Poll for results from provider (non-blocking) */
		if (session->provider && session->provider->poll_results) {
			session->provider->poll_results(session);
		}
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
		"ASR worker ending: session %s, feeds=%u, total_bytes=%u\n",
		session->id, feed_count, total_bytes_fed);

	switch_mutex_lock(session->mutex);
	session->state = ASR_SESSION_STATE_CLOSED;
	switch_set_flag(session, ASR_SESSION_FLAG_CLOSED);
	switch_mutex_unlock(session->mutex);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "ASR session worker ended: %s\n", session->id);

	return NULL;
}

asr_session_t *asr_session_create(switch_memory_pool_t *pool, const char *provider_name, asr_mode_t mode)
{
	asr_session_t *session;
	asr_provider_interface_t *provider;

	provider = asr_provider_find(provider_name);
	if (!provider) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ASR provider not found: %s\n", provider_name);
		return NULL;
	}

	session = switch_core_alloc(pool, sizeof(*session));
	if (!session) {
		return NULL;
	}

	session->pool = pool;
	session->provider = provider;
	session->mode = mode;
	session->state = ASR_SESSION_STATE_IDLE;
	session->running = SWITCH_TRUE;
	session->result_text = NULL;
	session->result_xml = NULL;
	session->result_confidence = 0;
	session->flags = 0;
	session->provider_private = NULL;
	session->grammar = NULL;
	session->no_input_timeout = 0;
	session->speech_timeout = 0;
	session->start_input_timers = SWITCH_TRUE;
	session->language = NULL;
	session->provider_name = switch_core_strdup(pool, provider_name);
	session->ws_handle = NULL;
	session->idle_count = 0;

	switch_mutex_init(&session->mutex, SWITCH_MUTEX_NESTED, pool);
	switch_thread_cond_create(&session->cond, pool);
	switch_buffer_create_dynamic(&session->audio_buffer, 1024, 8192, 0);

	session->id = switch_core_sprintf(pool, "%p", (void *) session);

	switch_mutex_lock(asr_globals.mutex);
	asr_globals.active_sessions++;
	switch_core_hash_insert(asr_globals.sessions, session->id, session);
	switch_mutex_unlock(asr_globals.mutex);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "ASR session created: %s (provider=%s, mode=%s)\n",
					  session->id, provider_name, mode == ASR_MODE_WEBSOCKET ? "websocket" : "rest");

	return session;
}

switch_status_t asr_session_destroy(asr_session_t *session)
{
	if (!session) {
		return SWITCH_STATUS_FALSE;
	}

	asr_session_stop_worker(session);

	switch_mutex_lock(asr_globals.mutex);
	if (asr_globals.active_sessions > 0) {
		asr_globals.active_sessions--;
	}
	switch_core_hash_delete(asr_globals.sessions, session->id);
	switch_mutex_unlock(asr_globals.mutex);

	if (session->audio_buffer) {
		switch_buffer_destroy(&session->audio_buffer);
	}

	switch_safe_free(session->result_text);
	switch_safe_free(session->result_xml);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "ASR session destroyed: %s\n", session->id);

	return SWITCH_STATUS_SUCCESS;
}

switch_status_t asr_session_start_worker(asr_session_t *session)
{
	switch_threadattr_t *thd_attr;

	if (!session) {
		return SWITCH_STATUS_FALSE;
	}

	switch_threadattr_create(&thd_attr, session->pool);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	switch_threadattr_detach_set(thd_attr, 0);

	return switch_thread_create(&session->worker_thread, thd_attr, asr_session_worker_thread, session, session->pool);
}

void asr_session_stop_worker(asr_session_t *session)
{
	switch_status_t retval;

	if (!session) {
		return;
	}

	switch_mutex_lock(session->mutex);
	session->running = SWITCH_FALSE;
	switch_thread_cond_signal(session->cond);
	switch_mutex_unlock(session->mutex);

	/* Wait for worker thread to actually exit before destroying resources */
	if (session->worker_thread) {
		switch_thread_join(&retval, session->worker_thread);
		session->worker_thread = NULL;
	}
}

switch_status_t asr_session_feed(asr_session_t *session, void *data, unsigned int len)
{
	static uint32_t global_feed_count = 0;

	if (!session || !data || len == 0) {
		return SWITCH_STATUS_FALSE;
	}

	if (switch_test_flag(session, ASR_SESSION_FLAG_CLOSED)) {
		return SWITCH_STATUS_BREAK;
	}

	global_feed_count++;

	switch_mutex_lock(session->mutex);
	switch_buffer_write(session->audio_buffer, data, len);
	switch_thread_cond_signal(session->cond);
	switch_mutex_unlock(session->mutex);

	/* Log first 5 feeds, then every 500th */
	if (global_feed_count <= 5 || global_feed_count % 500 == 0) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
			"asr_session_feed: session=%s, len=%u, feed_count=%u\n",
			session->id, len, global_feed_count);
	}

	return SWITCH_STATUS_SUCCESS;
}

switch_status_t asr_session_set_result(asr_session_t *session, const char *text, int confidence)
{
	if (!session || zstr(text)) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(session->mutex);
	switch_safe_free(session->result_text);
	switch_safe_free(session->result_xml);

	session->result_text = strdup(text);
	session->result_confidence = confidence;
	session->result_xml = switch_mprintf(
		"<?xml version=\"1.0\"?>\n"
		"<result grammar=\"%s\">\n"
		"  <interpretation grammar=\"%s\" confidence=\"%d\">\n"
		"    <input mode=\"speech\">%s</input>\n"
		"  </interpretation>\n"
		"</result>\n",
		session->grammar ? session->grammar : "default",
		session->grammar ? session->grammar : "default",
		confidence, text);

	session->state = ASR_SESSION_STATE_RESULT_READY;
	switch_set_flag(session, ASR_SESSION_FLAG_HAS_TEXT);
	switch_mutex_unlock(session->mutex);

	return SWITCH_STATUS_SUCCESS;
}

switch_status_t asr_session_get_result(asr_session_t *session, char **xmlstr)
{
	if (!session || !xmlstr) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(session->mutex);
	if (session->result_xml) {
		*xmlstr = strdup(session->result_xml);
		switch_clear_flag(session, ASR_SESSION_FLAG_HAS_TEXT);
		switch_mutex_unlock(session->mutex);
		return SWITCH_STATUS_SUCCESS;
	}
	switch_mutex_unlock(session->mutex);

	return SWITCH_STATUS_FALSE;
}
