/*
 * provider_aliyun.h -- 阿里云ASR Provider常量定义和会话上下文结构体
 *
 * 本头文件定义了阿里云ASR服务所需的所有常量（API端点URL）和
 * 会话级私有数据结构(aliyun_asr_ctx_t)，供provider_aliyun.c使用。
 *
 * 在mod_asr的Provider架构中，每个provider需要一个私有上下文结构体
 * 来保存与服务端交互的状态，通过session->provider_private指针引用。
 */

#ifndef PROVIDER_ALIYUN_H
#define PROVIDER_ALIYUN_H

#include "mod_asr.h"

/* ---- 阿里云NLS服务端点 ---- */

/*
 * WebSocket实时流式识别URL
 * 协议流程：连接 → 发送StartTranscription → 收到TranscriptionStarted →
 *           发送音频(Binary帧) → 收到SentenceEnd结果 → 发送StopTranscription
 * URL中需附加token参数：wss://...?token=xxx
 */
#define ALIYUN_ASR_WS_URL     "wss://nls-gateway-cn-shanghai.aliyuncs.com/ws/v1"

/*
 * REST一句话识别URL
 * 适用场景：短语音（<60秒），一次性提交整段音频
 * 与WS模式对比：REST更简单但不支持中间结果，WS支持实时流式中间结果
 * 请求方式：HTTP POST，音频作为request body，参数通过query string传递
 */
#define ALIYUN_ASR_REST_URL   "https://nls-gateway-cn-shanghai.aliyuncs.com/stream/v1/asr"

/*
 * Token获取URL（阿里云POP API）
 * 鉴权机制：使用AccessKey+HMAC-SHA1签名方式调用CreateToken接口
 * Token有效期：通常24小时，每次会话打开时重新获取
 * 签名流程：构造规范化查询串 → HMAC-SHA1签名 → Base64编码 → URL编码附加到URL
 */
#define ALIYUN_TOKEN_URL      "https://nls-meta.cn-shanghai.aliyuncs.com/pop/2019-02-28/tokens"

/*
 * 阿里云ASR会话级私有上下文 - 每个asr_session对应一个实例
 * 通过session->provider_private指针引用，在aliyun_asr_open()中创建
 * 字段分为四组：认证凭据、识别参数、WS连接状态、REST音频缓冲
 */
typedef struct {
	/* ---- 认证凭据组 ---- 从全局配置复制到会话，因为会话可能覆盖默认值 */
	char *access_key_id;      /* 阿里云AccessKey ID，用于POP API签名（从全局配置复制） */
	char *access_key_secret;  /* 阿里云AccessKey Secret，仅用于签名计算，不在网络传输 */
	char *app_key;            /* NLS应用AppKey，阿里云ASR服务协议要求必填 */
	char *token;              /* 动态获取的NLS Token，双重用途：WS连接URL参数 + REST请求头 */

	/* ---- 识别参数组 ---- 控制ASR引擎的行为 */
	char *region;             /* 服务区域（如"cn-shanghai"），影响API端点域名 */
	char *format;             /* 音频格式（如"pcm"），必须与实际音频格式匹配否则识别失败 */
	int sample_rate;          /* 采样率(Hz)，优先使用FreeSWITCH native_rate(电话=8000,高清=16000) */
	switch_bool_t enable_intermediate_result;  /* 是否返回中间识别结果（SentenceBegin/SentenceChanging） */
	switch_bool_t enable_punctuation;          /* 是否在结果中添加标点符号 */
	int rest_timeout;         /* REST请求超时(毫秒)，默认10000ms */

	/* ---- WS连接状态组 ---- 跟踪WebSocket协议的状态机 */
	switch_bool_t ws_connected;       /* WS连接是否已建立，避免向断开的连接写数据 */
	switch_bool_t recognition_started;  /* 是否已发送StartTranscription命令 */
	switch_bool_t transcription_started; /* 是否已收到TranscriptionStarted确认（收到后才发音频） */
	char *task_id;                    /* 任务ID，阿里云NLS协议要求每条消息携带，用于关联请求和响应 */

	/* ---- REST音频缓冲组 ---- REST模式需要积攒音频后一次性提交 */
	switch_buffer_t *rest_audio_buffer;  /* 音频积攒缓冲区，feed()写入，recognize()读取 */
	switch_size_t rest_audio_len;        /* 当前缓冲区中的音频字节数 */
	switch_size_t rest_max_audio_len;    /* 最大音频长度(1MB≈32秒PCM@16kHz)，防内存无限增长 */
	switch_bool_t rest_submitted;        /* 是否已提交REST识别，防止重复提交 */

	/* ---- 调试统计组 ---- 会话级计数器，替代原来的static变量，避免多线程竞争
	 * 【线程安全修复】原实现使用 static uint32_t 全局变量，所有会话共享，
	 * 多个worker线程并发递增时无任何同步保护，属于未定义行为(UB)。
	 * 改为会话级变量后，每个ctx实例只被自己的worker线程访问（feed/poll_results
	 * 均在同一个worker线程中调用），天然线程安全，无需加锁。 */
	uint32_t ws_feed_count;              /* WS模式：本会话已发送帧计数（调试用），替代原static变量 */
	uint32_t ws_total_bytes;             /* WS模式：本会话已发送总字节数（调试用），替代原static变量 */
	uint32_t poll_count;                 /* WS/REST模式：本会话轮询计数（调试用），替代原static变量 */
} aliyun_asr_ctx_t;

/*
 * 加载阿里云ASR Provider - 读取asr.conf配置并注册provider接口
 * @param pool: 模块级内存池
 * @return: SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 配置缺失
 * 流程：1.读取XML配置 → 2.校验必填字段 → 3.注册aliyun_provider到全局哈希表
 */
switch_status_t asr_provider_aliyun_load(switch_memory_pool_t *pool);

#endif /* PROVIDER_ALIYUN_H */
