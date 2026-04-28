/*
 * provider_aliyun.h -- Aliyun ASR Provider constants and config
 */

#ifndef PROVIDER_ALIYUN_H
#define PROVIDER_ALIYUN_H

#include "mod_asr.h"

#define ALIYUN_ASR_WS_URL     "wss://nls-gateway-cn-shanghai.aliyuncs.com/ws/v1"
#define ALIYUN_ASR_REST_URL   "https://nls-gateway-cn-shanghai.aliyuncs.com/stream/v1/asr"
#define ALIYUN_TOKEN_URL      "https://nls-meta.cn-shanghai.aliyuncs.com/pop/2019-02-28/tokens"

/* Aliyun provider-specific session data */
typedef struct {
	char *access_key_id;
	char *access_key_secret;
	char *app_key;
	char *token;
	char *region;
	char *format;
	int sample_rate;
	switch_bool_t enable_intermediate_result;
	switch_bool_t enable_punctuation;
	int rest_timeout;
	switch_bool_t ws_connected;
	switch_bool_t recognition_started;
	char *task_id;
	/* Audio accumulation for REST mode */
	switch_buffer_t *rest_audio_buffer;
	switch_size_t rest_audio_len;
	switch_size_t rest_max_audio_len;
	switch_bool_t rest_submitted;
} aliyun_asr_ctx_t;

switch_status_t asr_provider_aliyun_load(switch_memory_pool_t *pool);

#endif /* PROVIDER_ALIYUN_H */
