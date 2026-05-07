/*
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 * mod_asr.h -- 多云ASR模块头文件
 *
 * 本头文件定义了mod_asr模块的全部数据结构和接口声明，是整个ASR模块的核心抽象层。
 * 模块采用"Provider接口+Session管理"的分层架构：
 *   - Provider接口(asr_provider_interface_t)：每个ASR云服务(如阿里云)实现此接口
 *   - Session管理(asr_session_t)：管理一次ASR识别会话的完整生命周期
 *   - Worker线程：后台线程负责音频馈送和结果轮询，避免阻塞FreeSWITCH主线程
 *
 * 支持两种识别模式：
 *   - WebSocket模式：实时流式识别，音频持续推送，服务端异步返回结果
 *   - REST模式：一句话识别，积攒音频后一次性HTTP提交
 */

#ifndef MOD_ASR_H
#define MOD_ASR_H

#include <switch.h>

SWITCH_BEGIN_EXTERN_C

/*
 * ASR会话标志位 - 用位图表示会话的多种状态，支持同时设置多个标志
 * 这些标志用于FreeSWITCH ASR框架与mod_asr之间的状态同步
 */
typedef enum {
	ASR_SESSION_FLAG_HAS_TEXT = (1 << 0),        /* 已获得识别文本结果，可被get_results取出 */
	ASR_SESSION_FLAG_READY = (1 << 1),            /* 会话已就绪，worker线程已启动 */
	ASR_SESSION_FLAG_BARGE = (1 << 2),            /* 检测到打断(barge-in)，通知FreeSWITCH停止播放 */
	ASR_SESSION_FLAG_INPUT_TIMERS = (1 << 3),     /* 输入定时器已启动(用于no-input/speech超时检测) */
	ASR_SESSION_FLAG_START_OF_SPEECH = (1 << 4),  /* 检测到语音开始，触发"begin-speaking"事件 */
	ASR_SESSION_FLAG_NOINPUT_TIMEOUT = (1 << 5),  /* 未检测到语音输入超时(保留，当前未使用) */
	ASR_SESSION_FLAG_SPEECH_TIMEOUT = (1 << 6),   /* 语音识别超时(保留，当前未使用) */
	ASR_SESSION_FLAG_NOINPUT = (1 << 7),          /* 无输入，返回<noinput/>结果给FreeSWITCH */
	ASR_SESSION_FLAG_NOMATCH = (1 << 8),          /* 无匹配结果，返回<nomatch/>结果给FreeSWITCH */
	ASR_SESSION_FLAG_CLOSED = (1 << 9)            /* 会话已关闭，不再接受音频输入 */
} asr_session_flag_t;

/*
 * ASR会话状态机 - 描述一次识别会话从创建到结束的状态流转
 * 状态转换顺序：IDLE → LISTENING → (循环) RESULT_READY → LISTENING → CLOSED
 * LISTENING是稳态，RESULT_READY是瞬态（被get_results消费后立即回到LISTENING以支持连续识别）
 */
typedef enum {
	ASR_SESSION_STATE_IDLE = 0,         /* 初始态：会话已创建，worker线程尚未启动 */
	ASR_SESSION_STATE_LISTENING,        /* 监听态：worker线程运行中，等待/处理音频 */
	ASR_SESSION_STATE_PROCESSING,       /* 处理态：(保留，当前未使用) */
	ASR_SESSION_STATE_RESULT_READY,     /* 结果就绪态：有识别结果可被get_results取出 */
	ASR_SESSION_STATE_ERROR,            /* 错误态：(保留，当前未使用) */
	ASR_SESSION_STATE_CLOSED            /* 关闭态：worker线程已退出，会话销毁中 */
} asr_session_state_t;

/*
 * ASR识别模式 - 决定音频数据如何发送给ASR服务
 * WEBSOCKET：建立长连接，音频持续流式推送，服务端实时返回中间/最终结果，适合长时间通话
 * REST：积攒音频后一次性HTTP POST提交，适合短语音一句话识别
 */
typedef enum {
	ASR_MODE_WEBSOCKET = 0,  /* WebSocket实时流式识别（默认） */
	ASR_MODE_REST            /* REST一句话识别 */
} asr_mode_t;

/* 前向声明，避免循环依赖 */
typedef struct asr_session asr_session_t;
typedef struct asr_provider_interface asr_provider_interface_t;

/*
 * ASR服务提供者接口 - 每个ASR云服务必须实现此接口
 * 这是mod_asr的核心抽象：通过函数指针表实现"策略模式"，新增ASR服务只需
 * 实现此接口并注册即可，无需修改框架代码。
 *
 * 接口分为三组：
 *   生命周期：open/close - 建立和断开与服务端的连接
 *   数据流：feed/resume/pause - 送入音频、恢复/暂停识别
 *   结果获取：check_results/get_results/poll_results - 查询和获取识别结果
 */
struct asr_provider_interface {
	const char *name;  /* 提供者唯一标识名，用于dialplan指定：asr:<name> */
	/* --- 生命周期 --- */
	switch_status_t (*open)(asr_session_t *session, switch_asr_handle_t *ah);   /* 打开连接，初始化provider私有数据 */
	switch_status_t (*close)(asr_session_t *session);                            /* 关闭连接，释放provider私有数据 */
	/* --- 数据流 --- */
	switch_status_t (*feed)(asr_session_t *session, void *data, unsigned int len); /* 送入音频数据(PCM) */
	switch_status_t (*resume)(asr_session_t *session);                           /* 恢复识别(暂停后) */
	switch_status_t (*pause)(asr_session_t *session);                            /* 暂停识别 */
	/* --- 结果获取 --- */
	switch_status_t (*check_results)(asr_session_t *session);                    /* 非阻塞检查是否有结果 */
	switch_status_t (*get_results)(asr_session_t *session, char **result_xml);   /* 获取识别结果(XML格式) */
	switch_status_t (*start_input_timers)(asr_session_t *session);               /* 启动输入超时定时器 */
	/* --- 参数设置 --- */
	void (*text_param)(asr_session_t *session, const char *param, const char *val);   /* 设置文本参数(如app-key) */
	void (*numeric_param)(asr_session_t *session, const char *param, int val);        /* 设置数值参数(如sample-rate) */
	void (*float_param)(asr_session_t *session, const char *param, double val);       /* 设置浮点参数(当前未使用) */
	/* --- Worker线程回调 --- */
	switch_status_t (*poll_results)(asr_session_t *session);  /* 在worker线程中定期调用，接收WS消息或触发REST提交 */
	struct asr_provider_interface *next;  /* 链表指针，支持多provider注册(当前未使用链表遍历) */
};

/*
 * ASR会话结构体 - 代表一次ASR识别会话的完整状态
 * 每个FreeSWITCH ASR handle对应一个asr_session实例
 * 生命周期：asr_session_create() → worker运行 → asr_session_destroy()
 */
struct asr_session {
	char *id;                              /* 会话唯一ID(用指针地址生成)，作为全局sessions哈希表的key */
	asr_session_state_t state;             /* 会话当前状态(见asr_session_state_t) */
	asr_provider_interface_t *provider;    /* 绑定的ASR服务提供者(通过provider_name查找获得) */
	asr_mode_t mode;                       /* 识别模式：WEBSOCKET或REST */
	switch_mutex_t *mutex;                 /* 会话级互斥锁，保护state/flags/result等字段的并发访问 */
	switch_thread_cond_t *cond;            /* 条件变量，用于worker线程等待音频数据到达 */
	switch_memory_pool_t *pool;            /* FreeSWITCH内存池，所有pool分配的内存随会话销毁自动释放 */
	switch_thread_t *worker_thread;        /* worker线程句柄，用于join等待线程退出 */
	switch_bool_t running;                 /* worker线程运行标志，设为SWITCH_FALSE时线程退出 */
	switch_buffer_t *audio_buffer;         /* 音频缓冲区，主线程写入(worker feed)，worker线程读取(provider feed) */
	char *result_text;                     /* 最新识别结果文本(纯文本) */
	char *result_xml;                      /* 最新识别结果XML(MRCP格式，FreeSWITCH ASR框架要求的格式) */
	int result_confidence;                 /* 识别置信度(0-100) */
	uint32_t flags;                        /* 会话标志位(见asr_session_flag_t) */
	void *provider_private;                /* provider私有数据指针(如aliyun_asr_ctx_t)，各provider自行管理 */
	char *grammar;                         /* 语法名称(FreeSWITCH ASR框架要求，当前固定为"default") */
	int no_input_timeout;                  /* 无输入超时(毫秒，保留，当前未使用) */
	int speech_timeout;                    /* 语音超时(毫秒，保留，当前未使用) */
	switch_bool_t start_input_timers;      /* 是否启动输入定时器(默认SWITCH_TRUE) */
	char *language;                        /* 识别语言(如"zh-CN") */
	char *provider_name;                   /* 提供者名称(如"aliyun")，通过text_param设置 */
	int native_rate;                       /* FreeSWITCH原生采样率(如8000为电话，16000为高清) */
	void *ws_handle;                       /* WebSocket连接句柄(ws_conn_t*)，与provider_private分开存储 */
	int idle_count;                        /* 连续无音频的轮次计数，REST模式用此判断何时自动提交识别 */
};

/*
 * 模块全局配置 - 整个mod_asr模块共享的单例
 * 在mod_asr_load()中初始化，从asr.conf XML加载配置
 */
typedef struct {
	char *default_provider;     /* 默认提供者名称，dialplan未指定时使用(默认"aliyun") */
	asr_mode_t default_mode;    /* 默认识别模式(默认WEBSOCKET) */
	int max_sessions;           /* 最大并发会话数(默认100，当前未强制限制) */
	switch_memory_pool_t *pool; /* 模块级内存池，生命周期=模块加载期间 */
	switch_mutex_t *mutex;      /* 全局互斥锁，保护providers/sessions哈希表和active_sessions */
	switch_hash_t *providers;   /* 已注册的provider哈希表，key=provider name */
	switch_hash_t *sessions;    /* 活跃会话哈希表，key=session id */
	int active_sessions;        /* 当前活跃会话计数 */
	switch_event_node_t *reload_node; /* 事件订阅节点(保留，用于配置热重载) */
} asr_globals_t;

extern asr_globals_t asr_globals;  /* 全局单例，定义在mod_asr.c中 */

/* ---- Provider框架 ---- 注册、查找、列举ASR服务提供者 */
switch_status_t asr_provider_register(asr_provider_interface_t *provider);  /* 注册provider到全局哈希表 */
asr_provider_interface_t *asr_provider_find(const char *name);               /* 按名称查找provider */
void asr_provider_list(switch_stream_handle_t *stream);                     /* 列出所有已注册provider(用于CLI) */

/* ---- 会话管理 ---- ASR会话的创建、销毁、音频馈送、结果设置/获取 */
asr_session_t *asr_session_create(switch_memory_pool_t *pool, const char *provider_name, asr_mode_t mode);  /* 创建会话 */
switch_status_t asr_session_destroy(asr_session_t *session);                /* 销毁会话(停止worker+释放资源) */
switch_status_t asr_session_start_worker(asr_session_t *session);           /* 启动worker线程 */
void asr_session_stop_worker(asr_session_t *session);                       /* 停止worker线程(设running=false+join) */
switch_status_t asr_session_feed(asr_session_t *session, void *data, unsigned int len);  /* 写入音频到缓冲区 */
switch_status_t asr_session_set_result(asr_session_t *session, const char *text, int confidence);  /* provider调用：设置识别结果 */
switch_status_t asr_session_get_result(asr_session_t *session, char **xmlstr);  /* 获取并清除识别结果(支持连续识别) */

/* ---- WebSocket客户端 ---- 基于原始socket+OpenSSL的WS协议实现 */
switch_status_t asr_ws_connect(asr_session_t *session, const char *url, const char **headers, int header_count);  /* 建立WS连接 */
switch_status_t asr_ws_send_binary(asr_session_t *session, const void *data, size_t len);  /* 发送二进制WS帧(音频数据) */
switch_status_t asr_ws_send_text(asr_session_t *session, const char *text);  /* 发送文本WS帧(JSON命令) */
switch_status_t asr_ws_disconnect(asr_session_t *session);                   /* 断开WS连接(发送CLOSE帧+关闭SSL) */
switch_bool_t asr_ws_is_connected(asr_session_t *session);                   /* 检查WS是否已连接 */
switch_bool_t asr_ws_has_data(asr_session_t *session);                       /* 非阻塞检查是否有WS数据可读 */
char *asr_ws_recv_text(asr_session_t *session, switch_memory_pool_t *pool);  /* 接收一条WS文本帧(自动处理PING/CLOSE) */

/*
 * REST空闲阈值 - 连续多少个200ms空闲周期后自动提交REST识别请求
 * 值越小响应越快但可能截断语音，值越大等待越长但识别更完整
 * 3 × 200ms = 600ms 无新音频后触发提交
 */
#define ASR_REST_IDLE_THRESHOLD 3

/* REST client */
switch_status_t asr_rest_request(asr_session_t *session, const char *url, const char *method,
								 const char **headers, int header_count,
								 const void *body, size_t body_len,
								 char **response, switch_size_t *response_len);

/* ---- Provider加载函数 ---- 在mod_asr_load()中调用，加载各provider的配置并注册 */
switch_status_t asr_provider_aliyun_load(switch_memory_pool_t *pool);  /* 加载阿里云ASR provider */

SWITCH_END_EXTERN_C

#endif /* MOD_ASR_H */
