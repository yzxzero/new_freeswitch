/*
 * mod_asr.c -- 多云ASR模块主文件
 *
 * 本文件实现了FreeSWITCH ASR接口(switch_asr_interface_t)的完整回调集，
 * 是mod_asr模块与FreeSWITCH核心引擎之间的桥梁层。
 *
 * 【模块入口与FreeSWITCH ASR接口回调】
 *
 * FreeSWITCH的ASR框架通过switch_asr_interface_t结构体中的函数指针来驱动
 * 语音识别的完整生命周期。当拨号计划(dialplan)中使用play_and_detect_speech
 * 等应用时，FreeSWITCH核心会依次调用这些回调：
 *
 *   1. asr_open    - 创建识别会话，选择provider和模式
 *   2. asr_feed    - 持续送入PCM音频数据
 *   3. asr_check_results - 非阻塞查询是否有识别结果
 *   4. asr_get_results  - 获取识别结果(XML格式)
 *   5. asr_close   - 关闭会话，释放资源
 *
 * 模块架构：
 *   本模块采用"框架层 + Provider接口"的分层设计：
 *   - 框架层（本文件）：实现FreeSWITCH ASR接口，管理会话生命周期，路由参数
 *   - Provider层（如provider_aliyun.c）：实现具体的ASR云服务对接
 *   - Worker线程：后台线程负责音频转发和结果轮询，避免阻塞FreeSWITCH媒体线程
 *
 * 支持两种识别模式：
 *   - WebSocket模式：建立长连接，音频持续流式推送，服务端实时返回结果，适合连续识别
 *   - REST模式：积攒音频后一次性HTTP提交，适合短语音一句话识别
 *
 * 拨号计划使用示例：
 *   <action application="play_and_detect_speech" data="silence_stream://2000 asr:aliyun {mode=websocket}"/>
 *   <action application="play_and_detect_speech" data="silence_stream://2000 asr:aliyun {mode=rest}"/>
 *
 * fs_cli命令使用示例：
 *   asr status      - 查看模块状态(默认provider、模式、最大/活跃会话数)
 *   asr providers   - 列出所有已注册的ASR服务提供者
 *   asr list        - 列出当前所有活跃的识别会话
 */

#include "mod_asr.h"

/*
 * FreeSWITCH模块声明宏 - 这是FreeSWITCH动态模块加载机制的核心
 *
 * SWITCH_MODULE_LOAD_FUNCTION(mod_asr_load)：
 *   声明模块加载函数原型，FreeSWITCH在dlopen模块后调用此函数完成初始化。
 *   函数签名固定为：switch_status_t mod_asr_load(switch_loadable_module_interface_t **module_interface,
 *                                                   switch_memory_pool_t *pool)
 *
 * SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_asr_shutdown)：
 *   声明模块关闭函数原型，FreeSWITCH卸载模块时调用此函数完成资源释放。
 *
 * SWITCH_MODULE_DEFINITION(mod_asr, mod_asr_load, mod_asr_shutdown, NULL)：
 *   定义模块导出符号，FreeSWITCH通过dlsym找到此符号来识别合法模块。
 *   四个参数分别为：模块名、加载函数、关闭函数、运行时函数(NULL表示无后台运行时线程)
 *   该宏展开后生成一个名为"mod_asr_module_interface"的导出结构体，
 *   FreeSWITCH的模块加载器(switch_loadable_module.c)会查找这个符号。
 */
SWITCH_MODULE_LOAD_FUNCTION(mod_asr_load);
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_asr_shutdown);
SWITCH_MODULE_DEFINITION(mod_asr, mod_asr_load, mod_asr_shutdown, NULL);

asr_globals_t asr_globals = { 0 };

/* ---- ASR Interface Callbacks ---- */

/*
 * mod_asr_asr_open - 创建ASR识别会话
 *
 * FreeSWITCH ASR框架生命周期第一步：创建识别会话，选择provider和模式。
 *
 * dest参数解析格式："provider:mode"，例如"aliyun:websocket"、"aliyun:rest"
 * 如果未指定mode，默认使用asr.conf中配置的default-mode。
 * 如果未指定provider，回退到default-provider。
 *
 * 主要操作：
 *   1. 解析dest参数，提取provider名称和识别模式
 *   2. 创建asr_session实例（分配内存池、初始化锁、查找provider、创建音频缓冲区）
 *   3. 在主线程同步调用provider->open()建立网络连接
 *   4. 启动worker后台线程负责音频转发和结果轮询
 */
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

	/* 解析dest参数："provider:mode"格式，例如"aliyun:websocket"或"aliyun:rest"
	 * 冒号前为provider名，冒号后为模式（rest/websocket）
	 * 如果无冒号，整个dest作为provider名，模式使用默认值 */
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
			provider_name = dup_dest;  /* 无冒号，整个dest就是provider名 */
		}
	}

	/* 如果dest中未指定provider，回退到全局默认provider */
	if (zstr(provider_name)) {
		provider_name = asr_globals.default_provider;
	}

	/* 既没有从dest解析出provider，全局也没有默认provider，报错退出 */
	if (zstr(provider_name)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "No ASR provider specified\n");
		switch_safe_free(dup_dest);
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
		"mod_asr_asr_open: provider=%s, mode=%s, rate=%d\n",
		provider_name, mode == ASR_MODE_WEBSOCKET ? "websocket" : "rest", rate);

	/* 创建ASR会话实例：分配内存池、初始化互斥锁、查找provider、创建音频缓冲区
	 * asr_session_create()内部会根据provider_name在全局providers哈希表中查找
	 * 对应的asr_provider_interface_t，如果找不到则返回NULL */
	session = asr_session_create(ah->memory_pool, provider_name, mode);
	switch_safe_free(dup_dest);  /* dup_dest已完成使命（provider_name已提取），可以释放 */

	if (!session) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create ASR session\n");
		return SWITCH_STATUS_FALSE;
	}

	/* 将session绑定到FreeSWITCH ASR handle，后续所有回调通过ah->private_info获取session */
	ah->private_info = session;
	ah->codec = switch_core_strdup(ah->memory_pool, "L16");  /* L16 = 线性PCM，无压缩 */
	ah->rate = rate;            /* 采样率：8000(电话) 或 16000(高清) */
	session->native_rate = rate; /* 记录原始采样率，供provider使用 */

	/* 设置语法名称。FreeSWITCH ASR框架要求有grammar字段，当前固定为"default"，
	 * 因为本模块不做语法解析，所有识别结果由云端ASR引擎决定 */
	session->grammar = switch_core_strdup(ah->memory_pool, "default");

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
		"mod_asr_asr_open: session created, native_rate=%d, calling provider->open()\n", session->native_rate);

	/* 在主线程同步调用provider->open()建立网络连接
	 * 原因：错误即时反馈、避免竞态、连接通常很快 */
	if (session->provider && session->provider->open) {
		status = session->provider->open(session, ah);
		if (status != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Provider open failed for %s\n", provider_name);
			return SWITCH_STATUS_FALSE;
		}
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Provider open succeeded, starting worker\n");
	}

	/* 启动worker后台线程，负责：
	 *   - WebSocket模式：从音频缓冲区读取PCM数据通过WS发送给云端，同时接收云端返回的识别结果
	 *   - REST模式：监控音频缓冲区空闲状态，在语音结束后触发HTTP提交
	 * worker线程与主线程通过audio_buffer和mutex+cond同步 */
	status = asr_session_start_worker(session);
	if (status != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to start ASR session worker\n");
		return SWITCH_STATUS_FALSE;
	}

	return SWITCH_STATUS_SUCCESS;
}

/*
 * mod_asr_asr_load_grammar - 加载语法
 *
 * FreeSWITCH ASR框架要求实现此回调，但本模块不做语法解析——
 * 所有识别逻辑由云端ASR引擎决定。因此仅保存grammar名称，
 * 用于后续生成识别结果XML中的grammar属性。
 *
 * 注意：grammar字符串通过switch_core_strdup分配在session内存池上，
 * 随会话销毁自动释放，不能用switch_safe_free释放（pool分配的内存无此函数）。
 */
static switch_status_t mod_asr_asr_load_grammar(switch_asr_handle_t *ah, const char *grammar, const char *name)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session) return SWITCH_STATUS_FALSE;

	/* grammar is pool-allocated, do NOT free it with switch_safe_free */
	session->grammar = switch_core_strdup(ah->memory_pool, grammar ? grammar : "default");

	return SWITCH_STATUS_SUCCESS;
}

/*
 * mod_asr_asr_unload_grammar - 卸载语法
 *
 * 本模块不使用语法，因此直接返回成功，不做任何操作。
 */
static switch_status_t mod_asr_asr_unload_grammar(switch_asr_handle_t *ah, const char *name)
{
	return SWITCH_STATUS_SUCCESS;
}

/*
 * mod_asr_asr_close - 关闭ASR识别会话
 *
 * FreeSWITCH在识别结束或通道挂机时调用此回调。
 * 主要操作：
 *   1. 调用asr_session_destroy()停止worker线程、释放provider资源
 *   2. 设置SWITCH_ASR_FLAG_CLOSED标志，防止后续feed/check_results等回调继续操作
 */
static switch_status_t mod_asr_asr_close(switch_asr_handle_t *ah, switch_asr_flag_t *flags)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session) return SWITCH_STATUS_FALSE;

	/* asr_session_destroy内部会：
	 *   1. 设session->running = SWITCH_FALSE，通知worker线程退出
	 *   2. switch_thread_join()等待worker线程退出
	 *   3. 调用provider->close()关闭网络连接
	 *   4. 从全局sessions哈希表中移除此会话
	 *   5. 释放音频缓冲区等资源（内存池分配的无需手动释放） */
	asr_session_destroy(session);
	switch_set_flag(ah, SWITCH_ASR_FLAG_CLOSED);

	return SWITCH_STATUS_SUCCESS;
}

/*
 * mod_asr_asr_feed - 向ASR引擎送入音频数据
 *
 * FreeSWITCH在通话过程中持续调用此回调，将媒体流中的PCM音频数据
 * 转发给ASR引擎。这是音频流转发逻辑的核心：
 *
 * 数据流向：
 *   FreeSWITCH媒体线程 → mod_asr_asr_feed() → audio_buffer → worker线程 → provider->feed() → 云端ASR
 *
 * 本函数不直接调用provider->feed()，而是将音频数据写入session的
 * audio_buffer中。worker线程从buffer中读取数据后，通过provider->feed()
 * 发送给云端ASR服务。这种生产者-消费者模式的好处：
 *   1. 解耦：feed回调不会被网络I/O阻塞，写入buffer后立即返回
 *   2. 缓冲：网络波动时buffer可以吸收短暂的发送延迟
 *   3. 线程安全：buffer的读写由mutex保护
 *
 * 如果会话已关闭（SWITCH_ASR_FLAG_CLOSED），返回SWITCH_STATUS_BREAK
 * 通知FreeSWITCH停止送入音频。
 */
static switch_status_t mod_asr_asr_feed(switch_asr_handle_t *ah, void *data, unsigned int len, switch_asr_flag_t *flags)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session) return SWITCH_STATUS_FALSE;

	/* 会话已关闭，不再接受音频输入，返回BREAK让FreeSWITCH停止feed循环 */
	if (switch_test_flag(ah, SWITCH_ASR_FLAG_CLOSED)) {
		return SWITCH_STATUS_BREAK;
	}

	/* 将音频数据写入session的audio_buffer，worker线程会从中读取并发送给provider */
	return asr_session_feed(session, data, len);
}

/*
 * mod_asr_asr_resume - 恢复ASR识别
 *
 * 在暂停后恢复识别。直接调用provider的resume回调，
 * 对于WebSocket模式通常是恢复音频发送；REST模式无特殊操作。
 */
static switch_status_t mod_asr_asr_resume(switch_asr_handle_t *ah)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !session->provider || !session->provider->resume) return SWITCH_STATUS_FALSE;

	return session->provider->resume(session);
}

/*
 * mod_asr_asr_pause - 暂停ASR识别
 *
 * 暂停识别，音频仍会送入feed但不会被处理。
 * 对于WebSocket模式通常是暂停音频发送；REST模式无特殊操作。
 */
static switch_status_t mod_asr_asr_pause(switch_asr_handle_t *ah)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !session->provider || !session->provider->pause) return SWITCH_STATUS_FALSE;

	return session->provider->pause(session);
}

/*
 * mod_asr_asr_check_results - 非阻塞检查是否有识别结果
 *
 * FreeSWITCH ASR框架的"结果轮询协议"：
 *
 * FreeSWITCH在识别过程中会频繁调用check_results来查询是否有可用的识别结果。
 * 这是非阻塞的快速检查——FreeSWITCH不会在此等待，而是根据返回值决定下一步：
 *   - SWITCH_STATUS_SUCCESS：有结果可用，FreeSWITCH随后调用get_results获取
 *   - SWITCH_STATUS_FALSE：暂无结果，FreeSWITCH继续feed音频并再次轮询
 *
 * 检查逻辑分两层：
 *   1. 快速路径：先检查session标志位（HAS_TEXT/NOINPUT/NOMATCH/BARGE/START_OF_SPEECH），
 *      这些标志由worker线程或provider在收到云端结果时设置，检查是纯内存操作，极快
 *   2. 状态路径：如果标志位未命中，加锁检查session状态是否为RESULT_READY，
 *      RESULT_READY也是由worker线程在收到完整识别结果后设置的
 *
 * 注意：真正的结果获取（从云端接收）由worker线程通过provider->poll_results()
 * 完成，check_results只负责查询"结果是否已就绪"。
 */
static switch_status_t mod_asr_asr_check_results(switch_asr_handle_t *ah, switch_asr_flag_t *flags)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;
	switch_status_t status;

	if (!session) return SWITCH_STATUS_FALSE;

	/* 快速路径：检查标志位，这些标志由worker线程在收到云端结果时设置
	 * HAS_TEXT        - 收到识别文本
	 * NOINPUT         - 无输入超时
	 * NOMATCH         - 无匹配结果
	 * BARGE           - 检测到打断
	 * START_OF_SPEECH - 检测到语音开始
	 * 任何一个标志命中都表示"有事件需要处理"，返回SUCCESS
	 *
	 * 【线程安全说明】switch_test_flag是非原子的位测试操作，理论上存在
	 * 与worker线程set_flag/clear_flag的竞态。但此处刻意不加锁，原因：
	 *   1. check_results是极高频调用（每20ms一次），加锁会严重影响性能
	 *   2. 最坏情况是漏读一个刚设置的flag（false negative），下次轮询即可纠正
	 *   3. 不可能出现false positive（flag只被set，不会被并发清到已set的状态）
	 *   4. x86架构上对齐的int读操作天然原子，不会读到半写的值
	 * 这种"最终一致"的设计在FreeSWITCH核心代码中广泛使用。 */
	if (switch_test_flag(session, ASR_SESSION_FLAG_NOINPUT) ||
		switch_test_flag(session, ASR_SESSION_FLAG_NOMATCH) ||
		switch_test_flag(session, ASR_SESSION_FLAG_HAS_TEXT) ||
		switch_test_flag(session, ASR_SESSION_FLAG_BARGE) ||
		switch_test_flag(session, ASR_SESSION_FLAG_START_OF_SPEECH)) {
		return SWITCH_STATUS_SUCCESS;
	}

	/* 状态路径：加锁检查会话状态是否为RESULT_READY
	 * RESULT_READY由worker线程在收到完整识别结果后设置
	 * 加锁是因为state字段被worker线程和当前线程并发访问 */
	switch_mutex_lock(session->mutex);
	status = (session->state == ASR_SESSION_STATE_RESULT_READY) ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
	switch_mutex_unlock(session->mutex);

	return status;
}

/*
 * mod_asr_asr_get_results - 获取识别结果
 *
 * FreeSWITCH ASR框架的"结果获取协议"：
 *
 * 当check_results返回SUCCESS后，FreeSWITCH调用get_results获取识别结果。
 * 本函数按优先级依次检查不同类型的"结果事件"，返回对应的XML结果：
 *
 * 优先级顺序：
 *   1. BARGE（打断）：检测到用户说话，FreeSWITCH需停止播放提示音
 *   2. START_OF_SPEECH（语音开始）：触发"begin-speaking"事件，仅当没有文本结果时
 *   3. provider->get_results()：让provider自行处理结果获取
 *   4. session->result_xml：从session中取出缓存的识别结果XML
 *   5. NOINPUT：无输入超时，返回<noinput/>结果
 *   6. NOMATCH：无匹配，返回<nomatch/>结果
 *
 * 返回值含义：
 *   - SWITCH_STATUS_SUCCESS：成功获取结果，*xmlstr指向结果XML
 *   - SWITCH_STATUS_BREAK：特殊事件（barge-in或begin-speaking），FreeSWITCH据此
 *     执行特殊处理（如停止播放、触发事件）
 *   - SWITCH_STATUS_FALSE：无可用结果
 *
 * 【连续识别重置逻辑】（第4步，最核心的部分）
 *
 * 当从session->result_xml中取出识别结果后，必须执行以下重置操作：
 *
 *   a) 释放当前结果：switch_safe_free(result_text)和switch_safe_free(result_xml)
 *      这些是strdup/switch_mprintf分配的堆内存，必须手动释放，否则内存泄漏
 *
 *   b) 清除HAS_TEXT标志：switch_clear_flag(session, ASR_SESSION_FLAG_HAS_TEXT)
 *      这通知check_results"当前结果已被消费"，下次轮询时不再报告"有结果"
 *
 *   c) 重置状态为LISTENING：session->state = ASR_SESSION_STATE_LISTENING
 *      这是连续识别的关键！状态从RESULT_READY回到LISTENING，意味着：
 *      - worker线程可以继续接收新的识别结果（WebSocket模式下服务端会持续推送）
 *      - FreeSWITCH会继续feed音频并轮询check_results
 *      - 下次收到新结果时，worker会再次设置HAS_TEXT和RESULT_READY
 *      - 如此循环直到通话结束或用户主动停止
 *
 *   d) 清除START_OF_SPEECH标志：避免下次get_results误判为"begin-speaking"事件
 *
 *   e) 重置result_confidence为0：避免旧置信度干扰新结果
 *
 * 如果不重置，FreeSWITCH会在取出一次结果后认为识别完成，停止feed音频，
 * 导致后续的连续识别结果丢失。重置使得整个"feed→check→get"循环可以持续运行。
 */
static switch_status_t mod_asr_asr_get_results(switch_asr_handle_t *ah, char **xmlstr, switch_asr_flag_t *flags)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;
	switch_status_t pstatus;

	if (!session) return SWITCH_STATUS_FALSE;

	/* 优先级1：BARGE-IN打断事件
	 * 当用户在播放提示音期间开始说话，ASR引擎检测到语音活动后设置此标志。
	 * 返回SWITCH_STATUS_BREAK通知FreeSWITCH停止当前播放，进入识别阶段。
	 * 清除标志避免重复触发。 */
	if (switch_test_flag(session, ASR_SESSION_FLAG_BARGE)) {
		switch_clear_flag(session, ASR_SESSION_FLAG_BARGE);
		return SWITCH_STATUS_BREAK;
	}

	/* 优先级2：语音开始事件（begin-speaking）
	 * 当ASR引擎检测到语音开始时设置此标志。FreeSWITCH据此触发
	 * "DETECTED_SPEECH"事件中的begin-speaking子类型。
	 * 仅在HAS_TEXT未设置时返回BREAK（如果有文本结果，文本优先级更高），
	 * 避免语音开始事件抢先于实际识别结果。 */
	if (switch_test_flag(session, ASR_SESSION_FLAG_START_OF_SPEECH) &&
		!switch_test_flag(session, ASR_SESSION_FLAG_HAS_TEXT)) {
		switch_clear_flag(session, ASR_SESSION_FLAG_START_OF_SPEECH);
		return SWITCH_STATUS_BREAK;
	}

	/* 优先级3：让provider自行处理结果获取
	 * 某些provider可能有特殊的结果获取逻辑（如需要对结果做后处理），
	 * 如果provider->get_results()返回SUCCESS，直接使用其结果。 */
	if (session->provider && session->provider->get_results) {
		pstatus = session->provider->get_results(session, xmlstr);
		if (pstatus == SWITCH_STATUS_SUCCESS) {
			return SWITCH_STATUS_SUCCESS;
		}
	}

	/* 优先级4：从session缓存中获取识别结果（线程安全）
	 *
	 * 【线程安全修复】原实现直接访问session->result_xml并执行
	 * free+置NULL的重置操作，无任何锁保护。但worker线程可能同时在
	 * asr_session_set_result()（已加mutex）中修改这些字段，导致：
	 *   (1) 读取到半写入状态的指针 → segfault
	 *   (2) 双方同时free同一指针 → double-free
	 *   (3) get_results free后set_result又访问 → use-after-free
	 *
	 * 修复方式：委托给asr_session_get_result()，该函数在mutex保护下
	 * 完成相同的"检查-复制-释放-重置"序列，与set_result()互斥，
	 * 消除了双重释放和竞态访问的风险。同时也消除了代码重复。 */
	if (asr_session_get_result(session, xmlstr) == SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_SUCCESS;
	}

	/* 优先级5：NOINPUT - 无输入超时
	 * 在指定时间内未检测到任何语音输入，返回<noinput/>结果。
	 * FreeSWITCH据此触发no-input事件，拨号计划可以据此做超时处理。 */
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

	/* 优先级6：NOMATCH - 无匹配结果
	 * ASR引擎处理了音频但无法产生有效识别结果，返回<nomatch/>。
	 * FreeSWITCH据此触发no-match事件，拨号计划可以据此做重试等处理。 */
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

/*
 * mod_asr_asr_get_result_headers - 获取结果头信息
 *
 * 预留接口，当前未实现。某些ASR引擎可能在识别结果中
 * 附加额外的元数据头（如语音活动检测信息、说话人信息等），
 * 可通过此接口返回。当前直接返回SUCCESS，不设置任何头信息。
 */
static switch_status_t mod_asr_asr_get_result_headers(switch_asr_handle_t *ah, switch_event_t **headers, switch_asr_flag_t *flags)
{
	return SWITCH_STATUS_SUCCESS;
}

/*
 * mod_asr_asr_start_input_timers - 启动输入定时器
 *
 * FreeSWITCH在适当的时机调用此函数启动no-input/speech超时定时器。
 * 某些ASR场景需要先播放完提示音再开始计时，避免播放期间误判超时。
 *
 * 本函数做两件事：
 *   1. 设置ASR_SESSION_FLAG_INPUT_TIMERS标志，表示定时器已激活
 *   2. 调用provider->start_input_timers()让provider也启动其定时器
 */
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

/*
 * mod_asr_asr_text_param - 设置文本类型参数
 *
 * 参数路由逻辑：先匹配通用参数，未匹配的转发给provider处理
 *
 * 通用参数（本模块框架层处理）：
 *   - "provider"：切换ASR服务提供者（如从aliyun切换到其他provider）
 *   - "mode"：切换识别模式（"rest"或"websocket"）
 *   - "language"：设置识别语言（如"zh-CN"、"en-US"）
 *
 * Provider参数（转发给具体provider处理）：
 *   - 例如阿里云的"app-key"、"token"等特有参数
 *   - provider通过text_param回调自行解析和处理
 *
 * 这种"先框架后provider"的路由机制实现了关注点分离：
 * 框架层处理通用的会话配置，provider处理自身特有的参数。
 * 新增provider无需修改框架代码，只需在自己的text_param中处理新参数。
 */
static void mod_asr_asr_text_param(switch_asr_handle_t *ah, char *param, const char *val)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !param || zstr(val)) return;

	/* 框架层通用参数处理 */
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
		/* 未匹配通用参数，转发给provider处理其特有参数 */
		session->provider->text_param(session, param, val);
	}
}

/*
 * mod_asr_asr_numeric_param - 设置数值类型参数
 *
 * 参数路由逻辑：先匹配通用参数，未匹配的转发给provider处理
 *
 * 通用参数（本模块框架层处理）：
 *   - "no-input-timeout"：无输入超时时间（毫秒），超时后触发NOINPUT事件
 *   - "speech-timeout"：语音识别超时时间（毫秒），超时后触发NOMATCH事件
 *
 * Provider参数（转发给具体provider处理）：
 *   - 例如阿里云的"sample-rate"、"max-start-silence"等特有参数
 *
 * 注意：当前no_input_timeout和speech_timeout字段在框架层保存但未实际使用
 * 进行超时检测，超时逻辑由provider或FreeSWITCH核心处理。这两个参数
 * 主要用于传递给provider，由provider自行实现超时逻辑。
 */
static void mod_asr_asr_numeric_param(switch_asr_handle_t *ah, char *param, int val)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !param) return;

	/* 框架层通用参数处理 */
	if (!strcasecmp(param, "no-input-timeout")) {
		session->no_input_timeout = val;
	} else if (!strcasecmp(param, "speech-timeout")) {
		session->speech_timeout = val;
	} else if (session->provider && session->provider->numeric_param) {
		/* 未匹配通用参数，转发给provider处理其特有参数 */
		session->provider->numeric_param(session, param, val);
	}
}

/*
 * mod_asr_asr_float_param - 设置浮点类型参数
 *
 * 当前框架层没有通用浮点参数，所有浮点参数都直接转发给provider处理。
 * 这是预留接口，例如某些ASR引擎可能接受浮点类型的置信度阈值、
 * 语音活动检测灵敏度等参数。
 */
static void mod_asr_asr_float_param(switch_asr_handle_t *ah, char *param, double val)
{
	asr_session_t *session = (asr_session_t *) ah->private_info;

	if (!session || !param) return;

	/* 框架层无通用浮点参数，直接转发给provider */
	if (session->provider && session->provider->float_param) {
		session->provider->float_param(session, param, val);
	}
}

/* ====================================================================
 * CLI命令实现
 *
 * 通过SWITCH_STANDARD_API宏定义的FreeSWITCH CLI命令，
 * 可在fs_cli或ESL连接中通过"asr"命令调用。
 * ==================================================================== */

/*
 * mod_asr_api - "asr" CLI命令处理函数
 *
 * 支持三个子命令：
 *   asr status    - 显示模块运行状态信息
 *   asr providers - 列出所有已注册的ASR服务提供者
 *   asr list      - 列出当前所有活跃的识别会话
 *
 * 使用示例（fs_cli）：
 *   freeswitch@internal> asr status
 *   mod_asr Status:
 *     Default Provider: aliyun
 *     Default Mode: websocket
 *     Max Sessions: 100
 *     Active Sessions: 3
 *
 *   freeswitch@internal> asr providers
 *   ASR Providers:
 *     aliyun - Aliyun ASR Provider
 *
 *   freeswitch@internal> asr list
 *   Active ASR Sessions:
 *     ID: 0x7f3a2c  Provider: aliyun  Mode: ws  State: 2
 */
SWITCH_STANDARD_API(mod_asr_api)
{
	char *mycmd = NULL, *argv[5] = { 0 };

	/* 无参数时显示用法提示 */
	if (zstr(cmd)) {
		stream->write_function(stream, "Usage: asr <status|providers|list>\n");
		return SWITCH_STATUS_SUCCESS;
	}

	/* 复制命令字符串并按空格分割为参数数组
	 * switch_separate_string会修改mycmd内容并在argv中放置各部分的指针 */
	mycmd = strdup(cmd);
	switch_separate_string(mycmd, ' ', argv, (sizeof(argv) / sizeof(argv[0])));

	/* status子命令：显示模块全局状态 */
	if (!strcasecmp(argv[0], "status")) {
		stream->write_function(stream, "mod_asr Status:\n");
		stream->write_function(stream, "  Default Provider: %s\n", asr_globals.default_provider ? asr_globals.default_provider : "none");
		stream->write_function(stream, "  Default Mode: %s\n", asr_globals.default_mode == ASR_MODE_WEBSOCKET ? "websocket" : "rest");
		stream->write_function(stream, "  Max Sessions: %d\n", asr_globals.max_sessions);
		stream->write_function(stream, "  Active Sessions: %d\n", asr_globals.active_sessions);
	}
	/* providers子命令：列出所有已注册的provider */
	else if (!strcasecmp(argv[0], "providers")) {
		stream->write_function(stream, "ASR Providers:\n");
		asr_provider_list(stream);  /* 遍历全局providers哈希表，输出每个provider的名称和描述 */
	}
	/* list子命令：列出所有活跃的ASR会话 */
	else if (!strcasecmp(argv[0], "list")) {
		switch_hash_index_t *hi;
		asr_session_t *session;

		stream->write_function(stream, "Active ASR Sessions:\n");
		/* 遍历全局sessions哈希表，输出每个会话的关键信息
		 * 加锁保护，因为worker线程可能同时在修改哈希表 */
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
	}
	/* 未知子命令 */
	else {
		stream->write_function(stream, "Unknown command: %s\n", argv[0]);
		stream->write_function(stream, "Usage: asr <status|providers|list>\n");
	}

	switch_safe_free(mycmd);  /* 释放strdup分配的临时命令字符串 */
	return SWITCH_STATUS_SUCCESS;
}

/* ====================================================================
 * 配置加载
 * ==================================================================== */

/*
 * mod_asr_do_config - 从asr.conf XML配置文件加载模块配置
 *
 * 配置文件路径：conf/autoload_configs/asr.conf.xml
 * 配置文件格式示例：
 *   <configuration name="asr.conf" description="ASR Module">
 *     <settings>
 *       <param name="default-provider" value="aliyun"/>
 *       <param name="default-mode" value="websocket"/>
 *       <param name="max-sessions" value="100"/>
 *     </settings>
 *   </configuration>
 *
 * 加载流程：
 *   1. switch_xml_open_cfg()打开并解析XML配置文件
 *   2. 查找<settings>节点，遍历其下所有<param>子节点
 *   3. 读取每个param的name和value属性，匹配已知参数名并赋值
 *   4. 释放XML资源
 *   5. 对未配置的参数设置默认值：
 *      - default_provider默认为"aliyun"
 *      - max_sessions默认为100
 *
 * 注意：如果asr.conf文件不存在，函数返回SWITCH_STATUS_FALSE但不会阻止模块加载，
 * 所有参数将使用默认值。
 */
static switch_status_t mod_asr_do_config(switch_memory_pool_t *pool)
{
	switch_xml_t cfg, xml, settings, param;

	/* 打开asr.conf配置文件，cfg指向<configuration>根节点 */
	if (!(xml = switch_xml_open_cfg("asr.conf", &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Failed to open asr.conf, using defaults\n");
		return SWITCH_STATUS_FALSE;
	}

	/* 查找<settings>子节点，遍历其下所有<param>节点 */
	if ((settings = switch_xml_child(cfg, "settings"))) {
		for (param = switch_xml_child(settings, "param"); param; param = switch_xml_next(param)) {
			const char *pname = switch_xml_attr(param, "name");   /* 参数名 */
			const char *pval = switch_xml_attr(param, "value");   /* 参数值 */

			if (zstr(pname) || zstr(pval)) continue;  /* 跳过无效的param节点 */

			/* 匹配已知参数名并赋值到全局配置 */
			if (!strcasecmp(pname, "default-provider")) {
				/* 默认ASR服务提供者名称，使用pool分配保证生命周期与模块一致 */
				asr_globals.default_provider = switch_core_strdup(pool, pval);
			} else if (!strcasecmp(pname, "default-mode")) {
				/* 默认识别模式：websocket（实时流式）或rest（一句话） */
				if (!strcasecmp(pval, "rest")) {
					asr_globals.default_mode = ASR_MODE_REST;
				} else {
					asr_globals.default_mode = ASR_MODE_WEBSOCKET;
				}
			} else if (!strcasecmp(pname, "max-sessions")) {
				/* 最大并发识别会话数（当前未强制限制，仅记录） */
				asr_globals.max_sessions = atoi(pval);
			}
		}
	}

	/* 释放XML解析资源，避免内存泄漏 */
	switch_xml_free(xml);

	/* 设置默认值：配置文件未指定时使用 */
	if (zstr(asr_globals.default_provider)) {
		asr_globals.default_provider = "aliyun";  /* 默认使用阿里云ASR */
	}
	if (asr_globals.max_sessions == 0) {
		asr_globals.max_sessions = 100;  /* 默认最大100个并发会话 */
	}

	return SWITCH_STATUS_SUCCESS;
}

/* ====================================================================
 * 模块加载与关闭
 * ==================================================================== */

/*
 * mod_asr_load - 模块加载函数
 *
 * 这是FreeSWITCH在dlopen加载mod_asr.so后调用的入口函数。
 * 初始化顺序严格按以下步骤执行，每步依赖前一步的完成：
 *
 *   1. 初始化全局状态
 *      - 保存模块内存池引用
 *      - 创建嵌套互斥锁（保护全局哈希表）
 *      - 创建providers和sessions哈希表（用于provider注册和会话管理）
 *      - 初始化活跃会话计数为0
 *
 *   2. 加载XML配置
 *      - 从asr.conf读取default-provider、default-mode、max-sessions
 *      - 配置缺失时使用默认值（aliyun/websocket/100）
 *
 *   3. 加载所有Provider
 *      - 当前仅有阿里云provider（asr_provider_aliyun_load）
 *      - 该函数会创建provider接口实例、加载provider专属配置、
 *        并调用asr_provider_register()注册到全局providers哈希表
 *
 *   4. 创建模块接口
 *      - switch_loadable_module_create_module_interface()创建模块接口容器
 *
 *   5. 注册ASR接口
 *      - 创建switch_asr_interface_t并绑定所有回调函数
 *      - ASR接口名"mod_asr"，拨号计划中用"asr:provider"引用
 *
 *   6. 注册CLI命令
 *      - 创建switch_api_interface_t，命令名为"asr"
 *      - 注册mod_asr_api为处理函数
 *
 * 任何步骤失败都不影响后续步骤（除全局状态初始化失败会段错误外），
 * 这是FreeSWITCH模块的常见模式：尽可能注册所有接口，让模块可用。
 */
SWITCH_MODULE_LOAD_FUNCTION(mod_asr_load)
{
	switch_asr_interface_t *asr_interface;
	switch_api_interface_t *api_interface;

	/* ---- 步骤1：初始化全局状态 ---- */
	asr_globals.pool = pool;  /* 保存模块内存池，后续所有pool分配都使用此池 */
	switch_mutex_init(&asr_globals.mutex, SWITCH_MUTEX_NESTED, pool);  /* 嵌套锁：同一线程可多次加锁 */
	switch_core_hash_init(&asr_globals.providers);   /* provider注册表：key=provider name */
	switch_core_hash_init(&asr_globals.sessions);    /* 会话注册表：key=session id */
	asr_globals.active_sessions = 0;                 /* 活跃会话计数初始为0 */

	/* ---- 步骤2：加载XML配置 ---- */
	mod_asr_do_config(pool);

	/* ---- 步骤3：加载所有Provider ---- */
	asr_provider_aliyun_load(pool);  /* 加载阿里云ASR provider（注册到providers哈希表） */

	/* ---- 步骤4：创建模块接口容器 ---- */
	*module_interface = switch_loadable_module_create_module_interface(pool, modname);

	/* ---- 步骤5：注册ASR接口 ---- */
	/* 创建switch_asr_interface_t实例并绑定所有回调函数。
	 * FreeSWITCH核心在处理detect_speech/play_and_detect_speech应用时，
	 * 通过interface_name查找对应的ASR接口，然后调用这些回调。 */
	asr_interface = switch_loadable_module_create_interface(*module_interface, SWITCH_ASR_INTERFACE);
	asr_interface->interface_name = "mod_asr";              /* 接口名，拨号计划中用"asr:provider"引用 */
	asr_interface->asr_open = mod_asr_asr_open;             /* 打开识别会话 */
	asr_interface->asr_load_grammar = mod_asr_asr_load_grammar;  /* 加载语法（本模块不实际使用） */
	asr_interface->asr_unload_grammar = mod_asr_asr_unload_grammar; /* 卸载语法（空实现） */
	asr_interface->asr_close = mod_asr_asr_close;           /* 关闭识别会话 */
	asr_interface->asr_feed = mod_asr_asr_feed;             /* 送入音频数据 */
	asr_interface->asr_resume = mod_asr_asr_resume;         /* 恢复识别 */
	asr_interface->asr_pause = mod_asr_asr_pause;           /* 暂停识别 */
	asr_interface->asr_check_results = mod_asr_asr_check_results;  /* 检查是否有结果 */
	asr_interface->asr_get_results = mod_asr_asr_get_results;      /* 获取识别结果 */
	asr_interface->asr_get_result_headers = mod_asr_asr_get_result_headers; /* 获取结果头（空实现） */
	asr_interface->asr_start_input_timers = mod_asr_asr_start_input_timers; /* 启动输入定时器 */
	asr_interface->asr_text_param = mod_asr_asr_text_param;         /* 设置文本参数 */
	asr_interface->asr_numeric_param = mod_asr_asr_numeric_param;   /* 设置数值参数 */
	asr_interface->asr_float_param = mod_asr_asr_float_param;       /* 设置浮点参数 */

	/* ---- 步骤6：注册CLI命令 ---- */
	/* 注册"asr"命令，可在fs_cli中通过"asr status/providers/list"查看模块状态 */
	api_interface = switch_loadable_module_create_interface(*module_interface, SWITCH_API_INTERFACE);
	api_interface->interface_name = "asr";           /* 命令名 */
	api_interface->function = mod_asr_api;            /* 处理函数 */
	api_interface->syntax = "<status|providers|list>"; /* 语法提示 */

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "mod_asr loaded successfully\n");

	return SWITCH_STATUS_SUCCESS;
}

/*
 * mod_asr_shutdown - 模块关闭函数
 *
 * FreeSWITCH在卸载mod_asr模块时调用此函数，执行有序关闭。
 * 关闭顺序是加载顺序的逆序，确保依赖关系正确：
 *
 *   1. 停止所有worker线程
 *      - 遍历全局sessions哈希表中的所有活跃会话
 *      - 对每个会话调用asr_session_stop_worker()
 *      - stop_worker内部会设running=SWITCH_FALSE并join等待线程退出
 *      - 加锁保护遍历过程，因为worker线程可能同时在修改状态
 *
 *   2. 销毁全局哈希表
 *      - 销毁sessions哈希表（所有会话的worker已停止）
 *      - 销毁providers哈希表（释放provider接口实例）
 *
 * 注意：不显式销毁每个session，因为session内存来自pool分配，
 * pool在模块卸载时由FreeSWITCH核心统一释放。stop_worker只确保
 * 后台线程安全退出，避免线程访问已释放的内存。
 */
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_asr_shutdown)
{
	switch_hash_index_t *hi;
	asr_session_t *session;
	asr_session_t *sessions[1024];  /* 收集的会话指针数组 */
	int session_count = 0;
	int i;

	/* ---- 步骤1：收集所有活跃会话指针 ----
	 * 【死锁修复】原实现在持asr_globals.mutex期间调用asr_session_stop_worker()，
	 * 而stop_worker内部的switch_thread_join()会阻塞等待worker线程退出。
	 * 持全局锁期间阻塞会导致其他需要全局锁的操作（如新建/销毁session、
	 * provider查找等）死锁。例如：
	 *   shutdown线程: 持mutex → thread_join阻塞等待worker退出
	 *   worker线程:   需要mutex（如asr_session_destroy中的asr_globals.mutex获取会死锁）
	 *
	 * 修复：先在锁保护下快速收集所有session指针，然后释放锁，
	 * 再逐个停止worker线程。收集阶段是纯指针拷贝，O(N)极快，
	 * 不会长时间持锁。停止worker时不再持全局锁，其他线程可正常操作。
	 *
	 * 安全性：shutdown期间FreeSWITCH不会再新建session（模块正在卸载），
	 * 收集到的session指针在stop_worker完成前不会被释放（session
	 * 的生命周期由FreeSWITCH核心管理，卸载期间会等待所有引用释放）。 */
	switch_mutex_lock(asr_globals.mutex);
	for (hi = switch_core_hash_first(asr_globals.sessions); hi; hi = switch_core_hash_next(&hi)) {
		switch_core_hash_this(hi, NULL, NULL, (void **) &session);
		if (session && session_count < 1024) {
			sessions[session_count++] = session;
		}
	}
	switch_mutex_unlock(asr_globals.mutex);

	/* ---- 步骤2：停止所有worker线程（不持全局锁） ---- */
	for (i = 0; i < session_count; i++) {
		/* 通知worker线程退出并等待其结束
		 * stop_worker内部：设running=false + cond_signal + thread_join
		 * join可能阻塞数秒（等待worker处理完当前任务），但不影响其他线程 */
		asr_session_stop_worker(sessions[i]);
	}

	/* ---- 步骤3：销毁全局哈希表 ---- */
	switch_core_hash_destroy(&asr_globals.sessions);   /* 销毁会话哈希表 */
	switch_core_hash_destroy(&asr_globals.providers);   /* 销毁provider哈希表 */

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "mod_asr shutdown\n");

	return SWITCH_STATUS_SUCCESS;
}
