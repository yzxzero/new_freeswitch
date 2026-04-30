/*
 * mod_asr.c -- Multi-cloud ASR Module
 *
 * Implements switch_asr_interface_t for dialplan integration
 * and provides an API command for fs_cli usage.
 *
 * Usage in dialplan:
 *   <action application="play_and_detect_speech" data="silence_stream://2000 asr:aliyun {mode=websocket}"/>
 *   <action application="play_and_detect_speech" data="silence_stream://2000 asr:aliyun {mode=rest}"/>
 *
 * Usage in fs_cli:
 *   asr status
 *   asr providers
 *   asr list
 */

#include "mod_asr.h"

SWITCH_MODULE_LOAD_FUNCTION(mod_asr_load);
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_asr_shutdown);
SWITCH_MODULE_DEFINITION(mod_asr, mod_asr_load, mod_asr_shutdown, NULL);

asr_globals_t asr_globals = { 0 };

/* ---- ASR Interface Callbacks ---- */

static switch_status_t mod_asr_asr_open(switch_asr_handle_t *ah, const char *codec, int rate, const char *dest, switch_asr_flag_t *flags)
{
	asr_session_t *session = NULL;
	char *provider_name = NULL;
	asr_mode_t mode = asr_globals.default_mode;
	char *dup_dest = NULL;
	char *p;
	switch_status_t status;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
		"mod_asr_asr_open: codec=%s, rate=%d, dest=%s\n", codec, rate, dest ? dest : "(null)");

	if (!zstr(dest)) {
		dup_dest = strdup(dest);
		if ((p = strchr(dup_dest, ':'))) {
			*p++ = '\0';
			provider_name = dup_dest;
			if (!strcasecmp(p, "rest")) {
				mode = ASR_MODE_REST;
			} else {
				mode = ASR_MODE_WEBSOCKET;
			}
		} else {
			provider_name = dup_dest;
		}
	}

	if (zstr(provider_name)) {
		provider_name = asr_globals.default_provider;
	}

	if (zstr(provider_name)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "No ASR provider specified\n");
		switch_safe_free(dup_dest);
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
		"mod_asr_asr_open: provider=%s, mode=%s, rate=%d\n",
		provider_name, mode == ASR_MODE_WEBSOCKET ? "websocket" : "rest", rate);

	session = asr_session_create(ah->memory_pool, provider_name, mode);
	switch_safe_free(dup_dest);

	if (!session) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create ASR session\n");
		return SWITCH_STATUS_FALSE;
	}

	ah->private_info = session;
	ah->codec = switch_core_strdup(ah->memory_pool, "L16");
	ah->rate = rate;
	session->native_rate = rate;

	session->grammar = switch_core_strdup(ah->memory_pool, "default");

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
		"mod_asr_asr_open: session created, native_rate=%d, calling provider->open()\n", session->native_rate);

	/* Call provider->open() in the main thread so network errors are caught properly */
	if (session->provider && session->provider->open) {
		status = session->provider->open(session, ah);
		if (status != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Provider open failed for %s\n", provider_name);
			return SWITCH_STATUS_FALSE;
		}
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Provider open succeeded, starting worker\n");
	}

	status = asr_session_start_worker(session);
	if (status != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to start ASR session worker\n");
		return SWITCH_STATUS_FALSE;
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mod_asr_asr_load_grammar(switch_asr_handle_t *ah, const char *grammar, const char *name)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session) return SWITCH_STATUS_FALSE;

	/* grammar is pool-allocated, do NOT free it with switch_safe_free */
	session->grammar = switch_core_strdup(ah->memory_pool, grammar ? grammar : "default");

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mod_asr_asr_unload_grammar(switch_asr_handle_t *ah, const char *name)
{
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mod_asr_asr_close(switch_asr_handle_t *ah, switch_asr_flag_t *flags)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session) return SWITCH_STATUS_FALSE;

	asr_session_destroy(session);
	switch_set_flag(ah, SWITCH_ASR_FLAG_CLOSED);

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mod_asr_asr_feed(switch_asr_handle_t *ah, void *data, unsigned int len, switch_asr_flag_t *flags)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session) return SWITCH_STATUS_FALSE;

	if (switch_test_flag(ah, SWITCH_ASR_FLAG_CLOSED)) {
		return SWITCH_STATUS_BREAK;
	}

	return asr_session_feed(session, data, len);
}

static switch_status_t mod_asr_asr_resume(switch_asr_handle_t *ah)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !session->provider || !session->provider->resume) return SWITCH_STATUS_FALSE;

	return session->provider->resume(session);
}

static switch_status_t mod_asr_asr_pause(switch_asr_handle_t *ah)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !session->provider || !session->provider->pause) return SWITCH_STATUS_FALSE;

	return session->provider->pause(session);
}

static switch_status_t mod_asr_asr_check_results(switch_asr_handle_t *ah, switch_asr_flag_t *flags)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;
	switch_status_t status;

	if (!session) return SWITCH_STATUS_FALSE;

	if (switch_test_flag(session, ASR_SESSION_FLAG_NOINPUT) ||
		switch_test_flag(session, ASR_SESSION_FLAG_NOMATCH) ||
		switch_test_flag(session, ASR_SESSION_FLAG_HAS_TEXT) ||
		switch_test_flag(session, ASR_SESSION_FLAG_BARGE)) {
		return SWITCH_STATUS_SUCCESS;
	}

	/* Result polling is done by the worker thread via provider->poll_results().
	   Here we just check the session state (non-blocking). */
	switch_mutex_lock(session->mutex);
	status = (session->state == ASR_SESSION_STATE_RESULT_READY) ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
	switch_mutex_unlock(session->mutex);

	return status;
}

static switch_status_t mod_asr_asr_get_results(switch_asr_handle_t *ah, char **xmlstr, switch_asr_flag_t *flags)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;
	switch_status_t pstatus;

	if (!session) return SWITCH_STATUS_FALSE;

	if (switch_test_flag(session, ASR_SESSION_FLAG_BARGE)) {
		switch_clear_flag(session, ASR_SESSION_FLAG_BARGE);
		return SWITCH_STATUS_BREAK;
	}

	if (session->provider && session->provider->get_results) {
		pstatus = session->provider->get_results(session, xmlstr);
		if (pstatus == SWITCH_STATUS_SUCCESS) {
			return SWITCH_STATUS_SUCCESS;
		}
	}

	if (session->result_xml) {
		*xmlstr = strdup(session->result_xml);
		switch_clear_flag(session, ASR_SESSION_FLAG_HAS_TEXT);
		return SWITCH_STATUS_SUCCESS;
	}

	if (switch_test_flag(session, ASR_SESSION_FLAG_NOINPUT)) {
		switch_clear_flag(session, ASR_SESSION_FLAG_NOINPUT);
		*xmlstr = switch_mprintf("<?xml version=\"1.0\"?>\n"
								 "<result grammar=\"%s\">\n"
								 "  <interpretation>\n"
								 "    <input mode=\"speech\"><noinput/></input>\n"
								 "  </interpretation>\n"
								 "</result>\n", session->grammar);
		return SWITCH_STATUS_SUCCESS;
	}

	if (switch_test_flag(session, ASR_SESSION_FLAG_NOMATCH)) {
		switch_clear_flag(session, ASR_SESSION_FLAG_NOMATCH);
		*xmlstr = switch_mprintf("<?xml version=\"1.0\"?>\n"
								 "<result grammar=\"%s\">\n"
								 "  <interpretation>\n"
								 "    <input mode=\"speech\"><nomatch/></input>\n"
								 "  </interpretation>\n"
								 "</result>\n", session->grammar);
		return SWITCH_STATUS_SUCCESS;
	}

	return SWITCH_STATUS_FALSE;
}

static switch_status_t mod_asr_asr_get_result_headers(switch_asr_handle_t *ah, switch_event_t **headers, switch_asr_flag_t *flags)
{
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mod_asr_asr_start_input_timers(switch_asr_handle_t *ah)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session) return SWITCH_STATUS_FALSE;

	switch_set_flag(session, ASR_SESSION_FLAG_INPUT_TIMERS);

	if (session->provider && session->provider->start_input_timers) {
		return session->provider->start_input_timers(session);
	}

	return SWITCH_STATUS_SUCCESS;
}

static void mod_asr_asr_text_param(switch_asr_handle_t *ah, char *param, const char *val)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !param || zstr(val)) return;

	if (!strcasecmp(param, "provider")) {
		session->provider_name = switch_core_strdup(ah->memory_pool, val);
	} else if (!strcasecmp(param, "mode")) {
		if (!strcasecmp(val, "rest")) {
			session->mode = ASR_MODE_REST;
		} else {
			session->mode = ASR_MODE_WEBSOCKET;
		}
	} else if (!strcasecmp(param, "language")) {
		session->language = switch_core_strdup(ah->memory_pool, val);
	} else if (session->provider && session->provider->text_param) {
		session->provider->text_param(session, param, val);
	}
}

static void mod_asr_asr_numeric_param(switch_asr_handle_t *ah, char *param, int val)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !param) return;

	if (!strcasecmp(param, "no-input-timeout")) {
		session->no_input_timeout = val;
	} else if (!strcasecmp(param, "speech-timeout")) {
		session->speech_timeout = val;
	} else if (session->provider && session->provider->numeric_param) {
		session->provider->numeric_param(session, param, val);
	}
}

static void mod_asr_asr_float_param(switch_asr_handle_t *ah, char *param, double val)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !param) return;

	if (session->provider && session->provider->float_param) {
		session->provider->float_param(session, param, val);
	}
}

/* ---- API Command ---- */

SWITCH_STANDARD_API(mod_asr_api)
{
	char *mycmd = NULL, *argv[5] = { 0 };

	if (zstr(cmd)) {
		stream->write_function(stream, "Usage: asr <status|providers|list>\n");
		return SWITCH_STATUS_SUCCESS;
	}

	mycmd = strdup(cmd);
	switch_separate_string(mycmd, ' ', argv, (sizeof(argv) / sizeof(argv[0])));

	if (!strcasecmp(argv[0], "status")) {
		stream->write_function(stream, "mod_asr Status:\n");
		stream->write_function(stream, "  Default Provider: %s\n", asr_globals.default_provider ? asr_globals.default_provider : "none");
		stream->write_function(stream, "  Default Mode: %s\n", asr_globals.default_mode == ASR_MODE_WEBSOCKET ? "websocket" : "rest");
		stream->write_function(stream, "  Max Sessions: %d\n", asr_globals.max_sessions);
		stream->write_function(stream, "  Active Sessions: %d\n", asr_globals.active_sessions);
	} else if (!strcasecmp(argv[0], "providers")) {
		stream->write_function(stream, "ASR Providers:\n");
		asr_provider_list(stream);
	} else if (!strcasecmp(argv[0], "list")) {
		switch_hash_index_t *hi;
		asr_session_t *session;

		stream->write_function(stream, "Active ASR Sessions:\n");
		switch_mutex_lock(asr_globals.mutex);
		for (hi = switch_core_hash_first(asr_globals.sessions); hi; hi = switch_core_hash_next(&hi)) {
			switch_core_hash_this(hi, NULL, NULL, (void **) &session);
			stream->write_function(stream, "  ID: %s  Provider: %s  Mode: %s  State: %d\n",
								   session->id,
								   session->provider ? session->provider->name : "none",
								   session->mode == ASR_MODE_WEBSOCKET ? "ws" : "rest",
								   session->state);
		}
		switch_mutex_unlock(asr_globals.mutex);
	} else {
		stream->write_function(stream, "Unknown command: %s\n", argv[0]);
		stream->write_function(stream, "Usage: asr <status|providers|list>\n");
	}

	switch_safe_free(mycmd);
	return SWITCH_STATUS_SUCCESS;
}

/* ---- Config Loading ---- */

static switch_status_t mod_asr_do_config(switch_memory_pool_t *pool)
{
	switch_xml_t cfg, xml, settings, param;

	if (!(xml = switch_xml_open_cfg("asr.conf", &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Failed to open asr.conf, using defaults\n");
		return SWITCH_STATUS_FALSE;
	}

	if ((settings = switch_xml_child(cfg, "settings"))) {
		for (param = switch_xml_child(settings, "param"); param; param = switch_xml_next(param)) {
			const char *pname = switch_xml_attr(param, "name");
			const char *pval = switch_xml_attr(param, "value");

			if (zstr(pname) || zstr(pval)) continue;

			if (!strcasecmp(pname, "default-provider")) {
				asr_globals.default_provider = switch_core_strdup(pool, pval);
			} else if (!strcasecmp(pname, "default-mode")) {
				if (!strcasecmp(pval, "rest")) {
					asr_globals.default_mode = ASR_MODE_REST;
				} else {
					asr_globals.default_mode = ASR_MODE_WEBSOCKET;
				}
			} else if (!strcasecmp(pname, "max-sessions")) {
				asr_globals.max_sessions = atoi(pval);
			}
		}
	}

	switch_xml_free(xml);

	if (zstr(asr_globals.default_provider)) {
		asr_globals.default_provider = "aliyun";
	}
	if (asr_globals.max_sessions == 0) {
		asr_globals.max_sessions = 100;
	}

	return SWITCH_STATUS_SUCCESS;
}

/* ---- Module Load/Shutdown ---- */

SWITCH_MODULE_LOAD_FUNCTION(mod_asr_load)
{
	switch_asr_interface_t *asr_interface;
	switch_api_interface_t *api_interface;

	asr_globals.pool = pool;
	switch_mutex_init(&asr_globals.mutex, SWITCH_MUTEX_NESTED, pool);
	switch_core_hash_init(&asr_globals.providers);
	switch_core_hash_init(&asr_globals.sessions);
	asr_globals.active_sessions = 0;

	mod_asr_do_config(pool);

	asr_provider_aliyun_load(pool);

	*module_interface = switch_loadable_module_create_module_interface(pool, modname);

	asr_interface = switch_loadable_module_create_interface(*module_interface, SWITCH_ASR_INTERFACE);
	asr_interface->interface_name = "mod_asr";
	asr_interface->asr_open = mod_asr_asr_open;
	asr_interface->asr_load_grammar = mod_asr_asr_load_grammar;
	asr_interface->asr_unload_grammar = mod_asr_asr_unload_grammar;
	asr_interface->asr_close = mod_asr_asr_close;
	asr_interface->asr_feed = mod_asr_asr_feed;
	asr_interface->asr_resume = mod_asr_asr_resume;
	asr_interface->asr_pause = mod_asr_asr_pause;
	asr_interface->asr_check_results = mod_asr_asr_check_results;
	asr_interface->asr_get_results = mod_asr_asr_get_results;
	asr_interface->asr_get_result_headers = mod_asr_asr_get_result_headers;
	asr_interface->asr_start_input_timers = mod_asr_asr_start_input_timers;
	asr_interface->asr_text_param = mod_asr_asr_text_param;
	asr_interface->asr_numeric_param = mod_asr_asr_numeric_param;
	asr_interface->asr_float_param = mod_asr_asr_float_param;

	api_interface = switch_loadable_module_create_interface(*module_interface, SWITCH_API_INTERFACE);
	api_interface->interface_name = "asr";
	api_interface->function = mod_asr_api;
	api_interface->syntax = "<status|providers|list>";

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "mod_asr loaded successfully\n");

	return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_asr_shutdown)
{
	switch_hash_index_t *hi;
	asr_session_t *session;

	switch_mutex_lock(asr_globals.mutex);
	for (hi = switch_core_hash_first(asr_globals.sessions); hi; hi = switch_core_hash_next(&hi)) {
		switch_core_hash_this(hi, NULL, NULL, (void **) &session);
		if (session) {
			asr_session_stop_worker(session);
		}
	}
	switch_mutex_unlock(asr_globals.mutex);

	switch_core_hash_destroy(&asr_globals.sessions);
	switch_core_hash_destroy(&asr_globals.providers);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "mod_asr shutdown\n");

	return SWITCH_STATUS_SUCCESS;
}
