/*
 * asr_session.c -- ASR会话管理与Worker线程生命周期
 *
 * ============================================================================
 * Worker线程模型（核心架构）
 * ============================================================================
 *
 * 本文件实现了mod_asr模块的核心并发模型：主线程与Worker线程通过音频缓冲区
 * (audio_buffer)进行生产者-消费者协作。整个数据流如下：
 *
 *   FreeSWITCH主线程                 audio_buffer               Worker线程
 *   (媒体处理线程)                   (环形缓冲区)              (独立线程)
 *        |                              |                          |
 *        | -- asr_session_feed() -----> |                          |
 *        |    写入PCM音频数据            |                          |
 *        |    cond_signal唤醒           |                          |
 *        |                              | ---- 读取音频数据 -----> |
 *        |                              |      (cond_wait等待)     |
 *        |                              |                          |
 *        |                              |                   provider->feed()
 *        |                              |                   送入ASR引擎
 *        |                              |                          |
 *        |                              |                   provider->poll_results()
 *        |                              |                   轮询识别结果
 *        |                              |                          |
 *        | <--- asr_session_get_result() --- result_text/xml ----- |
 *        |      获取识别结果                                       |
 *
 * 关键设计要点：
 * 1. 线程安全：audio_buffer的读写、state/flags/result的访问均由session->mutex保护
 * 2. 唤醒机制：主线程feed()写入数据后cond_signal唤醒worker；worker在无数据时
 *    cond_timedwait(200ms)等待，既避免忙轮询，又能定期poll_results
 * 3. 超时轮询：200ms超时确保即使无新音频，WebSocket模式也能及时接收服务端
 *    异步返回的识别结果，REST模式也能检测空闲并自动提交
 * 4. 优雅退出：running标志+cond_signal确保worker线程能及时响应停止请求
 *
 * ============================================================================
 */

#include "mod_asr.h"

/*
 * asr_session_worker_thread -- ASR Worker线程主函数
 *
 * 这是整个ASR模块的核心执行线程。每个ASR会话对应一个worker线程，负责：
 *   1. 从audio_buffer中读取主线程写入的PCM音频数据
 *   2. 调用provider->feed()将音频送入ASR引擎
 *   3. 调用provider->poll_results()轮询识别结果（WebSocket接收/REST自动提交）
 *
 * 线程生命周期：
 *   启动 → 设置LISTENING状态 → 主循环 → 检测running=false → 设置CLOSED状态 → 退出
 *
 * 主循环流程：
 *   while(running) {
 *     1. 加锁检查audio_buffer是否有数据
 *     2. 无数据 → cond_timedwait(200ms)等待，超时后继续（确保定期poll_results）
 *     3. 有数据 → 从buffer读取 → provider->feed()送入引擎
 *     4. 无论是否有数据，都调用provider->poll_results()轮询结果
 *   }
 */
static void *SWITCH_THREAD_FUNC asr_session_worker_thread(switch_thread_t *thread, void *obj)
{
	asr_session_t *session = (asr_session_t *) obj;

	/* 循环内使用的局部变量，无需加锁保护（仅本线程访问） */
	switch_size_t avail;       /* 当前audio_buffer中的可用数据字节数 */
	uint8_t *data;             /* 从buffer读出的音频数据临时缓冲区 */
	switch_size_t read_len;    /* 实际从buffer读出的字节数 */

	/* 统计变量，用于日志输出和调试 */
	uint32_t feed_count = 0;       /* 累计feed调用次数 */
	uint32_t total_bytes_fed = 0;  /* 累计送入provider的总字节数 */
	uint32_t last_report_bytes = 0; /* 上次日志输出时的累计字节数（用于控制日志频率） */

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "ASR session worker started: %s (provider=%s, mode=%s, native_rate=%d)\n",
		session->id, session->provider ? session->provider->name : "none",
		session->mode == ASR_MODE_WEBSOCKET ? "ws" : "rest", session->native_rate);

	/*
	 * 注意：provider->open()现在在mod_asr_asr_open()中调用（在worker线程启动之前），
	 * 所以这里只需将session状态设为LISTENING，表示worker已就绪可接收音频。
	 * 这样设计是因为open()可能需要同步等待WebSocket握手完成，不宜放在worker中。
	 */
	switch_mutex_lock(session->mutex);
	session->state = ASR_SESSION_STATE_LISTENING;  /* 状态转换：IDLE → LISTENING */
	switch_set_flag(session, ASR_SESSION_FLAG_READY);  /* 标记会话就绪，主线程可开始feed */
	switch_mutex_unlock(session->mutex);

	/*
	 * ========== Worker主循环 ==========
	 *
	 * 循环条件：session->running为SWITCH_TRUE
	 * 退出方式：asr_session_stop_worker()将running设为SWITCH_FALSE并cond_signal唤醒
	 *
	 * 每轮循环执行两件事：
	 *   1. 音频搬运：audio_buffer → provider->feed()
	 *   2. 结果轮询：provider->poll_results()
	 */
	while (session->running) {
		switch_mutex_lock(session->mutex);

		/*
		 * 等待音频数据到达（带200ms超时）
		 *
		 * 为什么用timedwait而不是普通cond_wait？
		 * - 普通cond_wait会阻塞到有新音频才唤醒，但WebSocket模式下ASR服务端
		 *   会异步推送识别结果（如中间结果、SentenceEnd事件），这些结果需要
		 *   poll_results()来接收处理。如果一直等音频，结果就收不到了。
		 * - REST模式下，当用户说完一段话后会停顿，此时没有新音频但需要
		 *   检测空闲（idle_count累加）并自动提交识别请求。
		 * - 200ms是平衡值：太短浪费CPU，太长降低结果响应速度。
		 *
		 * 注意：cond_timedwait必须在持锁状态下调用，等待期间自动释放mutex，
		 * 被唤醒或超时后自动重新获取mutex。
		 */
		if (switch_buffer_inuse(session->audio_buffer) == 0 && session->running) {
			switch_thread_cond_timedwait(session->cond, session->mutex, 200000);  /* 200000微秒 = 200毫秒 */
		}

		/*
		 * 检查running标志（可能在cond_wait期间被stop_worker设为false）
		 * 必须在持锁状态下检查，确保与stop_worker的写入同步
		 */
		if (!session->running) {
			switch_mutex_unlock(session->mutex);
			break;  /* 跳出主循环，进入线程清理阶段 */
		}

		/*
		 * 读取audio_buffer中的音频数据
		 *
		 * 步骤：
		 * 1. 查询buffer中可用字节数(avail)
		 * 2. malloc分配临时缓冲区（注意：不能用pool分配，因为每轮循环都释放）
		 * 3. switch_buffer_read读取数据（读操作会自动移动buffer的读指针）
		 * 4. 重置idle_count（因为收到了音频数据，不再空闲）
		 * 5. 释放mutex后调用provider->feed()（feed可能耗时，不应长时间持锁）
		 */
		avail = switch_buffer_inuse(session->audio_buffer);
		if (avail > 0) {
			data = malloc(avail);
			if (!data) {
				switch_mutex_unlock(session->mutex);
				break;  /* 内存分配失败，退出循环 */
			}
			read_len = switch_buffer_read(session->audio_buffer, data, avail);

			/*
			 * 收到音频数据，重置空闲计数器
			 * idle_count用于REST模式判断是否应该自动提交识别请求：
			 *   当连续idle_count达到ASR_REST_IDLE_THRESHOLD(3)时，
			 *   表示用户已停顿约600ms(3×200ms)，可以提交了。
			 */
			session->idle_count = 0;
			switch_mutex_unlock(session->mutex);

			/*
			 * 调用provider的feed接口送入音频数据
			 * - WebSocket模式：feed将PCM数据通过WS连接发送给ASR服务端
			 * - REST模式：feed将PCM数据追加到内存缓冲区（等待后续一次性提交）
			 */
			if (read_len > 0 && session->provider && session->provider->feed) {
				session->provider->feed(session, data, (unsigned int) read_len);
				feed_count++;
				total_bytes_fed += (uint32_t) read_len;

				/*
				 * 控制日志频率：每累计约160KB音频数据输出一次统计日志
				 * 160KB ≈ 8000采样/秒 × 2字节 × 10秒（电话音质10秒的音频量）
				 * 避免高频feed导致的日志洪泛
				 */
				if (total_bytes_fed - last_report_bytes >= 160000) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
						"ASR worker: session %s, feed #%u, total_bytes=%u, last_chunk=%zu\n",
						session->id, feed_count, total_bytes_fed, read_len);
					last_report_bytes = total_bytes_fed;
				}
			}

			free(data);  /* 释放临时音频缓冲区 */
		} else {
			/*
			 * 无音频数据（200ms超时唤醒且buffer为空）
			 *
			 * idle_count递增的意义：
			 * - WebSocket模式：不影响，只是多了一次poll_results的机会
			 * - REST模式：当idle_count >= ASR_REST_IDLE_THRESHOLD(3)时，
			 *   provider->poll_results()会触发HTTP提交识别请求
			 *   3 × 200ms = 600ms 的停顿通常意味着用户一句话说完了
			 */
			session->idle_count++;
			switch_mutex_unlock(session->mutex);
		}

		/*
		 * 轮询ASR识别结果
		 *
		 * 这是worker线程除了音频搬运之外的第二个核心职责：
		 * - WebSocket模式：poll_results()接收WS消息帧，解析服务端推送的识别结果，
		 *   检测SentenceEnd事件并调用asr_session_set_result()设置结果
		 * - REST模式：当idle_count达到阈值时，poll_results()触发HTTP POST提交
		 *   积攒的音频并解析响应，调用asr_session_set_result()设置结果
		 *
		 * 关键：poll_results()是"非阻塞"的，不会长时间等待，确保循环继续执行
		 */
		if (session->provider && session->provider->poll_results) {
			session->provider->poll_results(session);
		}
	}

	/* ========== 线程退出清理 ========== */

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
		"ASR worker ending: session %s, feeds=%u, total_bytes=%u\n",
		session->id, feed_count, total_bytes_fed);

	/*
	 * 设置会话状态为CLOSED，标记线程已退出
	 * ASR_SESSION_FLAG_CLOSED标志用于：
	 * - 阻止asr_session_feed()继续写入（检查到CLOSED直接返回SWITCH_STATUS_BREAK）
	 * - 通知上层会话已不可用
	 */
	switch_mutex_lock(session->mutex);
	session->state = ASR_SESSION_STATE_CLOSED;
	switch_set_flag(session, ASR_SESSION_FLAG_CLOSED);
	switch_mutex_unlock(session->mutex);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "ASR session worker ended: %s\n", session->id);

	return NULL;
}

/*
 * asr_session_create -- 创建ASR会话
 *
 * 从FreeSWITCH内存池分配asr_session_t结构体并初始化所有字段，
 * 然后将其注册到全局会话哈希表中。
 *
 * 参数：
 *   pool          - FreeSWITCH内存池，由mod_asr_asr_open()传入，
 *                   与switch_asr_handle_t绑定，会话结束时随handle销毁
 *   provider_name - ASR提供者名称（如"aliyun"），用于从全局providers哈希表查找
 *   mode          - 识别模式：ASR_MODE_WEBSOCKET或ASR_MODE_REST
 *
 * 返回：
 *   成功 - 初始化完成的asr_session_t指针
 *   失败 - NULL（provider未找到或内存分配失败）
 *
 * 注意：此函数不启动worker线程，worker线程由asr_session_start_worker()单独启动
 */
asr_session_t *asr_session_create(switch_memory_pool_t *pool, const char *provider_name, asr_mode_t mode)
{
	asr_session_t *session;
	asr_provider_interface_t *provider;

	/* 根据名称查找ASR提供者，必须先通过asr_provider_register()注册 */
	provider = asr_provider_find(provider_name);
	if (!provider) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "ASR provider not found: %s\n", provider_name);
		return NULL;
	}

	/*
	 * 从FreeSWITCH内存池分配session结构体
	 * 使用switch_core_alloc而非malloc的原因：
	 * - pool分配的内存会在pool销毁时自动释放，无需手动free
	 * - pool分配效率高（预分配大块内存，小分配只需移动指针）
	 * - 与FreeSWITCH的内存管理哲学一致
	 */
	session = switch_core_alloc(pool, sizeof(*session));
	if (!session) {
		return NULL;
	}

	/* ========== 字段初始化 ========== */

	/* 基础属性 */
	session->pool = pool;                           /* 绑定内存池，后续pool分配都使用此pool */
	session->provider = provider;                   /* 绑定ASR提供者接口（函数指针表） */
	session->mode = mode;                           /* 识别模式，决定音频如何送入引擎 */
	session->state = ASR_SESSION_STATE_IDLE;        /* 初始状态为IDLE，worker启动后转为LISTENING */
	session->running = SWITCH_TRUE;                 /* worker线程运行标志，stop_worker时设为FALSE */

	/* 识别结果（初始为空） */
	session->result_text = NULL;                    /* 识别文本，由set_result分配，get_result释放 */
	session->result_xml = NULL;                     /* 识别结果XML，MRCP格式，FreeSWITCH框架要求 */
	session->result_confidence = 0;                 /* 置信度0-100，0表示未获得结果 */

	/* 标志位和Provider私有数据 */
	session->flags = 0;                             /* 初始无任何标志，worker启动后设READY */
	session->provider_private = NULL;               /* provider私有数据，在provider->open()中分配 */

	/* ASR参数（部分保留字段，当前未全部使用） */
	session->grammar = NULL;                        /* 语法名称，FreeSWITCH ASR框架要求 */
	session->no_input_timeout = 0;                  /* 无输入超时(ms)，保留未使用 */
	session->speech_timeout = 0;                    /* 语音超时(ms)，保留未使用 */
	session->start_input_timers = SWITCH_TRUE;      /* 默认启动输入定时器 */
	session->language = NULL;                       /* 识别语言（如"zh-CN"），通过text_param设置 */
	session->provider_name = switch_core_strdup(pool, provider_name);  /* 拷贝provider名称到pool内存 */

	/* WebSocket连接句柄（仅WebSocket模式使用） */
	session->ws_handle = NULL;                      /* ws_conn_t指针，在provider->open()中创建 */

	/* 空闲计数器（REST模式用于判断自动提交时机） */
	session->idle_count = 0;                        /* 连续无音频轮次，每200ms+1 */

	/*
	 * 初始化同步原语
	 * - mutex：保护session的state/flags/result/audio_buffer等共享状态
	 * - cond：用于worker线程等待音频数据到达
	 *
	 * SWITCH_MUTEX_NESTED：嵌套锁，允许同一线程多次加锁（避免死锁）
	 * 使用pool分配，无需手动销毁
	 */
	switch_mutex_init(&session->mutex, SWITCH_MUTEX_NESTED, pool);
	switch_thread_cond_create(&session->cond, pool);

	/*
	 * 创建动态音频缓冲区
	 * 参数：初始大小1024字节，最大块大小8192字节，上限0（无限制）
	 * 动态buffer会根据数据量自动扩容，0上限表示可无限增长
	 * 这是因为通话时长不确定，音频量可能很大
	 */
	switch_buffer_create_dynamic(&session->audio_buffer, 1024, 8192, 0);

	/*
	 * 生成会话唯一ID
	 * 使用session指针地址的十六进制字符串作为ID
	 * 优点：唯一性由内存分配保证，无需UUID生成
	 * 用途：作为全局sessions哈希表的key
	 */
	session->id = switch_core_sprintf(pool, "%p", (void *) session);

	/*
	 * 全局注册：将session加入全局会话哈希表
	 * - active_sessions计数器+1（用于模块状态监控）
	 * - 插入asr_globals.sessions哈希表（key=session->id, value=session指针）
	 * 这两步操作需要加全局锁，确保与destroy/delete操作互斥
	 */
	switch_mutex_lock(asr_globals.mutex);
	asr_globals.active_sessions++;
	switch_core_hash_insert(asr_globals.sessions, session->id, session);
	switch_mutex_unlock(asr_globals.mutex);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "ASR session created: %s (provider=%s, mode=%s)\n",
					  session->id, provider_name, mode == ASR_MODE_WEBSOCKET ? "websocket" : "rest");

	return session;
}

/*
 * asr_session_destroy -- 销毁ASR会话
 *
 * 销毁顺序至关重要，必须遵循"先停线程，后释放资源"的原则：
 *
 *   1. 停止worker线程（asr_session_stop_worker）
 *      - 设置running=false + cond_signal唤醒
 *      - switch_thread_join()等待线程真正退出
 *      必须先做这一步，否则worker线程可能访问正在被释放的资源
 *
 *   2. 从全局会话表注销
 *      - active_sessions计数器-1
 *      - 从sessions哈希表删除
 *      此时worker已退出，不再有并发访问
 *
 *   3. 销毁音频缓冲区
 *      - switch_buffer_destroy释放buffer内部数据
 *
 *   4. 释放识别结果
 *      - switch_safe_free释放result_text和result_xml
 *      （注意：session结构体本身由pool管理，不需要手动释放）
 *
 * 注意：session->pool分配的内存（mutex, cond, provider_name, id等）
 * 会随pool销毁自动释放，此处只需释放malloc/strdup分配的内存
 */
switch_status_t asr_session_destroy(asr_session_t *session)
{
	if (!session) {
		return SWITCH_STATUS_FALSE;
	}

	/* 第1步：停止worker线程（设running=false + join等待退出） */
	asr_session_stop_worker(session);

	/* 第1.5步：关闭provider连接（WS断开/REST缓冲销毁）
	 * 【资源泄漏修复】原实现未调用provider->close()，导致：
	 *   - WebSocket连接从未发送CLOSE帧，服务端不知道客户端已断开
	 *   - SSL/TCP资源在进程退出前不会被释放（连接泄漏）
	 *   - REST模式的音频缓冲区不会销毁（内存泄漏）
	 * 必须在stop_worker之后调用：worker线程已退出，不会有并发访问。
	 * 必须在destroy_buffer之前调用：close内部会销毁provider私有缓冲区。 */
	if (session->provider && session->provider->close) {
		session->provider->close(session);
	}

	/* 第2步：从全局会话哈希表注销 */
	switch_mutex_lock(asr_globals.mutex);
	if (asr_globals.active_sessions > 0) {
		asr_globals.active_sessions--;
	}
	switch_core_hash_delete(asr_globals.sessions, session->id);
	switch_mutex_unlock(asr_globals.mutex);

	/* 第3步：销毁音频缓冲区 */
	if (session->audio_buffer) {
		switch_buffer_destroy(&session->audio_buffer);
	}

	/* 第4步：释放识别结果（strdup分配的堆内存） */
	switch_safe_free(session->result_text);
	switch_safe_free(session->result_xml);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "ASR session destroyed: %s\n", session->id);

	return SWITCH_STATUS_SUCCESS;
}

/*
 * asr_session_start_worker -- 启动ASR Worker线程
 *
 * 创建并启动worker线程，线程函数为asr_session_worker_thread。
 * 使用FreeSWITCH的标准线程创建接口。
 *
 * 线程属性：
 * - 栈大小：SWITCH_THREAD_STACKSIZE（默认值，通常256KB或更大）
 * - detach：0（不分离），因为stop_worker需要join等待线程退出
 *
 * 返回：switch_thread_create的返回值
 *   SWITCH_STATUS_SUCCESS - 线程创建成功
 *   其他值 - 创建失败
 */
switch_status_t asr_session_start_worker(asr_session_t *session)
{
	switch_threadattr_t *thd_attr;

	if (!session) {
		return SWITCH_STATUS_FALSE;
	}

	switch_threadattr_create(&thd_attr, session->pool);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	switch_threadattr_detach_set(thd_attr, 0);  /* 不detach，需要join等待线程退出 */

	return switch_thread_create(&session->worker_thread, thd_attr, asr_session_worker_thread, session, session->pool);
}

/*
 * asr_session_stop_worker -- 停止ASR Worker线程
 *
 * 采用"标志+唤醒+等待"三步法优雅停止worker线程：
 *
 *   1. 加锁后设置running=SWITCH_FALSE，通知worker退出循环
 *   2. cond_signal唤醒可能在cond_wait中阻塞的worker线程
 *   3. switch_thread_join阻塞等待worker线程真正退出
 *
 * 为什么必须join？
 * - 确保worker线程不再访问session的任何资源后，destroy才能安全释放
 * - 如果不join，destroy释放buffer后worker可能还在读取，导致use-after-free
 */
void asr_session_stop_worker(asr_session_t *session)
{
	switch_status_t retval;

	if (!session) {
		return;
	}

	/*
	 * 原子地设置running=false并唤醒worker线程
	 * 必须在持锁状态下操作，确保worker线程能及时看到running的变化
	 */
	switch_mutex_lock(session->mutex);
	session->running = SWITCH_FALSE;          /* 通知worker退出主循环 */
	switch_thread_cond_signal(session->cond); /* 唤醒可能在cond_wait中阻塞的worker */
	switch_mutex_unlock(session->mutex);

	/*
	 * 阻塞等待worker线程退出
	 * switch_thread_join会阻塞直到线程函数返回
	 * retval接收线程的返回值（此处不使用，仅为接口要求）
	 */
	if (session->worker_thread) {
		switch_thread_join(&retval, session->worker_thread);
		session->worker_thread = NULL;  /* 清空线程句柄，防止重复join */
	}
}

/*
 * asr_session_feed -- 向ASR会话写入音频数据
 *
 * 由FreeSWITCH主线程（媒体处理线程）调用，将PCM音频数据写入session的
 * audio_buffer，并唤醒worker线程处理。
 *
 * 数据流：FreeSWITCH核心 → mod_asr_asr_feed() → asr_session_feed() → audio_buffer → worker线程
 *
 * 参数：
 *   session - 目标ASR会话
 *   data    - PCM音频数据指针（通常16位有符号整数，采样率由native_rate决定）
 *   len     - 音频数据长度（字节数）
 *
 * 返回：
 *   SWITCH_STATUS_SUCCESS - 写入成功
 *   SWITCH_STATUS_FALSE   - 参数无效
 *   SWITCH_STATUS_BREAK   - 会话已关闭，不再接受音频
 */
switch_status_t asr_session_feed(asr_session_t *session, void *data, unsigned int len)
{
	/* 【线程安全修复】原static uint32_t全局变量，所有会话共享，多线程并发递增无同步保护属于UB。
	 * 改为volatile + __sync_fetch_and_add原子递增，确保并发递增的正确性。
	 * volatile防止编译器优化掉对共享变量的读取，__sync_fetch_and_add保证
	 * 递增操作的原子性（返回递增前的值，用于日志条件判断）。 */
	static volatile uint32_t global_feed_count = 0;
	uint32_t my_feed_count;  /* 本线程递增后的快照，用于后续日志判断 */

	/* 参数校验 */
	if (!session || !data || len == 0) {
		return SWITCH_STATUS_FALSE;
	}

	/*
	 * 检查会话是否已关闭
	 * CLOSED标志由close函数设置，如果已关闭则不再接受音频
	 * 返回SWITCH_STATUS_BREAK通知调用方停止feed
	 *
	 * 【线程安全说明】switch_test_flag是非原子的，与close路径设置CLOSED
	 * 存在竞态。但此处不加锁，原因：
	 *   1. CLOSED标志只从0→1（单向），不会出现false positive
	 *   2. 最坏情况是漏读CLOSED，多写一帧到buffer，buffer随后被destroy释放
	 *   3. feed是高频调用，加锁代价不值得
	 * 真正防止close后feed的关键保障是SWITCH_ASR_FLAG_CLOSED（FreeSWITCH框架
	 * 级别），在asr_close返回后框架不再调用feed回调。 */
	if (switch_test_flag(session, ASR_SESSION_FLAG_CLOSED)) {
		return SWITCH_STATUS_BREAK;
	}

	/* 原子递增全局feed计数器，返回递增后的值用于日志判断 */
	my_feed_count = __sync_fetch_and_add(&global_feed_count, 1) + 1;

	/*
	 * 将音频数据写入audio_buffer并唤醒worker线程
	 *
	 * 操作顺序（在mutex保护下）：
	 * 1. switch_buffer_write：将PCM数据追加到audio_buffer尾部
	 * 2. switch_thread_cond_signal：唤醒在cond_wait中等待的worker线程
	 *
	 * cond_signal的作用：
	 * - 如果worker线程正在cond_timedwait中等待（buffer为空时），
	 *   signal会立即唤醒它，减少音频处理延迟
	 * - 如果worker线程正在处理上一批数据，signal会被"丢失"（无影响），
	 *   因为worker处理完会回到循环顶部再次检查buffer
	 */
	switch_mutex_lock(session->mutex);
	switch_buffer_write(session->audio_buffer, data, len);
	switch_thread_cond_signal(session->cond);  /* 唤醒worker线程处理新数据 */
	switch_mutex_unlock(session->mutex);

	/*
	 * 控制日志频率，避免高频feed（每20ms一次）导致日志洪泛
	 * - 前5次feed：每次都输出（会话刚建立的调试信息）
	 * - 之后每500次feed输出一次（约10秒一次，20ms×500=10s）
	 */
	if (my_feed_count <= 5 || my_feed_count % 500 == 0) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
			"asr_session_feed: session=%s, len=%u, feed_count=%u\n",
			session->id, len, my_feed_count);
	}

	return SWITCH_STATUS_SUCCESS;
}

/*
 * asr_session_set_result -- 设置ASR识别结果
 *
 * 由provider的poll_results()回调调用，当ASR引擎返回识别结果时设置。
 * 同时生成FreeSWITCH ASR框架要求的MRCP格式XML。
 *
 * 调用场景：
 * - WebSocket模式：收到SentenceEnd事件时调用
 * - REST模式：HTTP响应解析完成后调用
 *
 * 参数：
 *   session    - 目标ASR会话
 *   text       - 识别文本（如"你好世界"）
 *   confidence - 置信度（0-100，通常阿里云返回100）
 *
 * 线程安全：在mutex保护下操作result_text/result_xml/state/flags
 */
switch_status_t asr_session_set_result(asr_session_t *session, const char *text, int confidence)
{
	if (!session || zstr(text)) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(session->mutex);

	/*
	 * 释放之前的结果（如果有的话）
	 * 每次set_result都会覆盖之前的结果，只保留最新的
	 * switch_safe_free是NULL安全的free宏（先检查非NULL再free并置NULL）
	 */
	switch_safe_free(session->result_text);
	switch_safe_free(session->result_xml);

	/* 设置新的识别结果 */
	session->result_text = strdup(text);  /* strdup在堆上分配，需在get_result或destroy中释放 */
	session->result_confidence = confidence;

	/*
	 * 生成MRCP格式的识别结果XML
	 * FreeSWITCH ASR框架期望get_results返回此格式的XML
	 *
	 * 格式示例：
	 *   <?xml version="1.0"?>
	 *   <result grammar="default">
	 *     <interpretation grammar="default" confidence="100">
	 *       <input mode="speech">你好世界</input>
	 *     </interpretation>
	 *   </result>
	 */
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

	/* 更新会话状态和标志 */
	session->state = ASR_SESSION_STATE_RESULT_READY;     /* 状态转换：LISTENING → RESULT_READY */
	switch_set_flag(session, ASR_SESSION_FLAG_HAS_TEXT);  /* 设置HAS_TEXT标志，通知上层有结果可取 */

	switch_mutex_unlock(session->mutex);

	return SWITCH_STATUS_SUCCESS;
}

/*
 * asr_session_get_result -- 获取并清除ASR识别结果
 *
 * 由mod_asr_asr_check_results/mod_asr_asr_get_results调用，
 * 获取最新的识别结果XML，然后重置会话状态以支持连续识别。
 *
 * 连续识别的状态重置机制：
 *   当取走结果后，会话回到LISTENING状态，清除HAS_TEXT和START_OF_SPEECH标志，
 *   这样下一轮识别的SentenceEnd事件可以再次触发set_result，实现"一次WS连接，
 *   多轮识别结果"的连续识别模式。
 *
 *   状态流转：
 *     LISTENING → (set_result) → RESULT_READY → (get_result) → LISTENING → ...
 *
 * 参数：
 *   session - 目标ASR会话
 *   xmlstr  - 输出参数，接收strdup拷贝的结果XML字符串，调用方负责free
 *
 * 返回：
 *   SWITCH_STATUS_SUCCESS - 成功获取结果，*xmlstr指向结果XML
 *   SWITCH_STATUS_FALSE   - 无结果或参数无效
 */
switch_status_t asr_session_get_result(asr_session_t *session, char **xmlstr)
{
	if (!session || !xmlstr) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(session->mutex);

	if (session->result_xml) {
		/*
		 * 拷贝结果XML给调用方
		 * 使用strdup而非直接返回指针，因为接下来要释放session内部的结果
		 * 调用方（FreeSWITCH ASR框架）负责free此拷贝
		 */
		*xmlstr = strdup(session->result_xml);

		/*
		 * 清除HAS_TEXT标志
		 * 这告诉FreeSWITCH ASR框架"结果已被取走"，下次check_results会返回false
		 */
		switch_clear_flag(session, ASR_SESSION_FLAG_HAS_TEXT);

		/*
		 * ========== 连续识别状态重置 ==========
		 *
		 * 清空session内部的结果存储，将会话状态重置为LISTENING，
		 * 为下一轮识别做好准备。这是实现"连续识别"的关键：
		 *
		 * 典型的连续识别场景（WebSocket模式）：
		 *   用户说"你好" → SentenceEnd → set_result("你好")
		 *   → FreeSWITCH get_result取出"你好" → 状态回到LISTENING
		 *   → 用户继续说"请帮我查天气" → SentenceEnd → set_result("请帮我查天气")
		 *   → FreeSWITCH get_result取出"请帮我查天气" → 状态回到LISTENING
		 *   → ...
		 *
		 * 每一轮的结果是独立的，不会混淆。
		 * 单次WebSocket连接支持多轮结果，无需重新建连。
		 */
		switch_safe_free(session->result_text);
		switch_safe_free(session->result_xml);
		session->result_text = NULL;
		session->result_xml = NULL;
		session->result_confidence = 0;

		/* 状态回到LISTENING，等待下一轮识别 */
		session->state = ASR_SESSION_STATE_LISTENING;

		/*
		 * 清除START_OF_SPEECH标志
		 * 下一轮识别开始时，provider检测到语音会重新设置此标志
		 */
		switch_clear_flag(session, ASR_SESSION_FLAG_START_OF_SPEECH);

		switch_mutex_unlock(session->mutex);
		return SWITCH_STATUS_SUCCESS;
	}

	switch_mutex_unlock(session->mutex);

	return SWITCH_STATUS_FALSE;  /* 无结果可取 */
}
