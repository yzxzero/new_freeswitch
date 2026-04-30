/*
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 * mod_asr.h -- Multi-cloud ASR Module Header
 */

#ifndef MOD_ASR_H
#define MOD_ASR_H

#include <switch.h>

SWITCH_BEGIN_EXTERN_C

/* Session flags */
typedef enum {
	ASR_SESSION_FLAG_HAS_TEXT = (1 << 0),
	ASR_SESSION_FLAG_READY = (1 << 1),
	ASR_SESSION_FLAG_BARGE = (1 << 2),
	ASR_SESSION_FLAG_INPUT_TIMERS = (1 << 3),
	ASR_SESSION_FLAG_START_OF_SPEECH = (1 << 4),
	ASR_SESSION_FLAG_NOINPUT_TIMEOUT = (1 << 5),
	ASR_SESSION_FLAG_SPEECH_TIMEOUT = (1 << 6),
	ASR_SESSION_FLAG_NOINPUT = (1 << 7),
	ASR_SESSION_FLAG_NOMATCH = (1 << 8),
	ASR_SESSION_FLAG_CLOSED = (1 << 9)
} asr_session_flag_t;

/* Session state */
typedef enum {
	ASR_SESSION_STATE_IDLE = 0,
	ASR_SESSION_STATE_LISTENING,
	ASR_SESSION_STATE_PROCESSING,
	ASR_SESSION_STATE_RESULT_READY,
	ASR_SESSION_STATE_ERROR,
	ASR_SESSION_STATE_CLOSED
} asr_session_state_t;

/* Recognition mode */
typedef enum {
	ASR_MODE_WEBSOCKET = 0,
	ASR_MODE_REST
} asr_mode_t;

/* Forward declarations */
typedef struct asr_session asr_session_t;
typedef struct asr_provider_interface asr_provider_interface_t;

/* Provider interface - each ASR service must implement this */
struct asr_provider_interface {
	const char *name;
	switch_status_t (*open)(asr_session_t *session, switch_asr_handle_t *ah);
	switch_status_t (*close)(asr_session_t *session);
	switch_status_t (*feed)(asr_session_t *session, void *data, unsigned int len);
	switch_status_t (*resume)(asr_session_t *session);
	switch_status_t (*pause)(asr_session_t *session);
	switch_status_t (*check_results)(asr_session_t *session);
	switch_status_t (*get_results)(asr_session_t *session, char **result_xml);
	switch_status_t (*start_input_timers)(asr_session_t *session);
	void (*text_param)(asr_session_t *session, const char *param, const char *val);
	void (*numeric_param)(asr_session_t *session, const char *param, int val);
	void (*float_param)(asr_session_t *session, const char *param, double val);
	switch_status_t (*poll_results)(asr_session_t *session);
	struct asr_provider_interface *next;
};

/* ASR session structure */
struct asr_session {
	char *id;
	asr_session_state_t state;
	asr_provider_interface_t *provider;
	asr_mode_t mode;
	switch_mutex_t *mutex;
	switch_thread_cond_t *cond;
	switch_memory_pool_t *pool;
	switch_thread_t *worker_thread;
	switch_bool_t running;
	switch_buffer_t *audio_buffer;
	char *result_text;
	char *result_xml;
	int result_confidence;
	uint32_t flags;
	void *provider_private;
	char *grammar;
	int no_input_timeout;
	int speech_timeout;
	switch_bool_t start_input_timers;
	char *language;
	/* Provider-specific params set via text_param */
	char *provider_name;
	/* Native sample rate from FreeSWITCH (e.g. 8000 for phone calls) */
	int native_rate;
	/* WebSocket connection handle (separate from provider_private) */
	void *ws_handle;
	/* Idle counter for REST mode auto-submit */
	int idle_count;
};

/* Global configuration */
typedef struct {
	char *default_provider;
	asr_mode_t default_mode;
	int max_sessions;
	switch_memory_pool_t *pool;
	switch_mutex_t *mutex;
	switch_hash_t *providers;
	switch_hash_t *sessions;
	int active_sessions;
	switch_event_node_t *reload_node;
} asr_globals_t;

extern asr_globals_t asr_globals;

/* Provider framework */
switch_status_t asr_provider_register(asr_provider_interface_t *provider);
asr_provider_interface_t *asr_provider_find(const char *name);
void asr_provider_list(switch_stream_handle_t *stream);

/* Session management */
asr_session_t *asr_session_create(switch_memory_pool_t *pool, const char *provider_name, asr_mode_t mode);
switch_status_t asr_session_destroy(asr_session_t *session);
switch_status_t asr_session_start_worker(asr_session_t *session);
void asr_session_stop_worker(asr_session_t *session);
switch_status_t asr_session_feed(asr_session_t *session, void *data, unsigned int len);
switch_status_t asr_session_set_result(asr_session_t *session, const char *text, int confidence);
switch_status_t asr_session_get_result(asr_session_t *session, char **xmlstr);

/* WebSocket client */
switch_status_t asr_ws_connect(asr_session_t *session, const char *url, const char **headers, int header_count);
switch_status_t asr_ws_send_binary(asr_session_t *session, const void *data, size_t len);
switch_status_t asr_ws_send_text(asr_session_t *session, const char *text);
switch_status_t asr_ws_disconnect(asr_session_t *session);
switch_bool_t asr_ws_is_connected(asr_session_t *session);
switch_bool_t asr_ws_has_data(asr_session_t *session);
char *asr_ws_recv_text(asr_session_t *session, switch_memory_pool_t *pool);

/* REST idle threshold: number of consecutive idle periods before REST submit */
#define ASR_REST_IDLE_THRESHOLD 3

/* REST client */
switch_status_t asr_rest_request(asr_session_t *session, const char *url, const char *method,
								 const char **headers, int header_count,
								 const void *body, size_t body_len,
								 char **response, switch_size_t *response_len);

/* Provider registration functions (called from mod_asr_load) */
switch_status_t asr_provider_aliyun_load(switch_memory_pool_t *pool);

SWITCH_END_EXTERN_C

#endif /* MOD_ASR_H */
