/*
 * asr_provider.c -- Provider注册与查找框架
 *
 * 【文件概述】
 * 本文件实现了mod_asr模块的Provider（语音识别引擎提供者）注册、查找与列举功能，
 * 是mod_asr插件式架构的核心。
 *
 * 【设计思想】
 * mod_asr采用"Provider接口 + 具体实现"的插件架构：
 *   - 上层（asr_provider.c）提供统一的注册/查找框架，不关心具体引擎细节
 *   - 下层（如aliyun_asr.c等）实现asr_provider_interface_t接口，并通过本框架注册自身
 *   - 调用方通过名字查找Provider，实现解耦——上层代码无需硬编码依赖特定引擎
 *
 * 这种设计使得新增语音识别引擎时，只需实现接口并调用asr_provider_register()即可，
 * 无需修改任何已有代码，符合开闭原则（对扩展开放，对修改关闭）。
 *
 * 【线程安全】
 * 所有对全局哈希表(asr_globals.providers)的读写操作均在asr_globals.mutex互斥锁保护下进行，
 * 确保多线程环境下注册、查找、列举操作的原子性和一致性。
 *
 * 【相关文件】
 *   - mod_asr.h        : 定义asr_provider_interface_t接口结构体与asr_globals全局变量
 *   - mod_asr.c        : 模块入口，初始化asr_globals（包括mutex和providers哈希表）
 *   - aliyun_asr.c     : 阿里云ASR引擎的具体实现，在模块加载时调用asr_provider_register()
 */

#include "mod_asr.h"

/* asr_globals定义在mod_asr.c中，此处通过extern声明引用。
 * asr_globals包含：
 *   - mutex:     保护providers哈希表的互斥锁
 *   - providers: 以Provider名字为key的哈希表，value为asr_provider_interface_t指针
 */

/**
 * asr_provider_register - 注册一个ASR引擎提供者
 *
 * @param provider  指向待注册的Provider接口结构体的指针，不可为NULL，
 *                  且provider->name不可为空字符串
 *
 * @return SWITCH_STATUS_SUCCESS  注册成功
 *         SWITCH_STATUS_FALSE    参数无效（provider为NULL或name为空）
 *
 * 【设计理由】
 * 注册操作是Provider接入系统的唯一入口。每个具体引擎（如阿里云ASR）在模块加载时
 * 构造自己的asr_provider_interface_t实例，并调用此函数将其注册到全局哈希表中。
 * 后续调用方即可通过asr_provider_find()按名字找到该Provider。
 *
 * 【注意】
 * 如果同名Provider已存在，switch_core_hash_insert会直接覆盖旧值，
 * 这允许热更新Provider实现而不需要重启模块。
 */
switch_status_t asr_provider_register(asr_provider_interface_t *provider)
{
	/* 防御性检查：provider指针和name都不能为空，否则无法作为哈希表的key */
	if (!provider || zstr(provider->name)) {
		return SWITCH_STATUS_FALSE;
	}

	/* 加锁保护哈希表写入，防止并发注册导致数据竞争 */
	switch_mutex_lock(asr_globals.mutex);
	/* 以provider->name为key插入哈希表，同名会覆盖旧值（热更新语义） */
	switch_core_hash_insert(asr_globals.providers, provider->name, provider);
	switch_mutex_unlock(asr_globals.mutex);

	/* 记录注册日志，方便运维排查哪些引擎已加载 */
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "ASR provider registered: %s\n", provider->name);

	return SWITCH_STATUS_SUCCESS;
}

/**
 * asr_provider_find - 按名字查找已注册的ASR引擎提供者
 *
 * @param name  Provider的名字，如"aliyun"，不可为空字符串
 *
 * @return 找到的Provider接口指针；未找到或name无效时返回NULL
 *
 * 【设计理由】
 * 查找是解耦的关键：上层代码（如asr_session）只通过名字查找Provider，
 * 不直接依赖具体实现。这使得引擎可替换——换引擎只改配置，不改代码。
 *
 * 【调用场景】
 * 当ASR会话(asr_session_open)需要创建识别实例时，会根据用户指定的引擎名
 * 调用此函数查找对应的Provider，然后通过Provider的open回调创建具体实例。
 */
asr_provider_interface_t *asr_provider_find(const char *name)
{
	asr_provider_interface_t *provider = NULL;

	/* 名字为空则无法查找，直接返回NULL，避免无意义的哈希查找 */
	if (zstr(name)) {
		return NULL;
	}

	/* 加锁保护哈希表读取，防止与并发注册操作冲突 */
	switch_mutex_lock(asr_globals.mutex);
	/* 按名字在哈希表中查找，O(1)复杂度 */
	provider = switch_core_hash_find(asr_globals.providers, name);
	switch_mutex_unlock(asr_globals.mutex);

	return provider;
}

/**
 * asr_provider_list - 列举所有已注册的ASR引擎提供者
 *
 * @param stream  FreeSWITCH输出流句柄，用于向CLI等界面输出信息
 *
 * 【设计理由】
 * 提供运维可观测性：通过FreeSWITCH CLI命令（如"asr list_providers"）可查看
 * 当前系统加载了哪些ASR引擎，以及各引擎实现了哪些回调函数。
 * 输出中包含open/feed/get_results三个核心回调的地址，便于排查引擎是否
 * 正确实现了必要的接口——如果某个回调为NULL指针，说明该引擎缺少对应功能。
 *
 * 【注意】
 * 此函数仅在CLI调试时调用，不在热路径上，性能不是关注点。
 */
void asr_provider_list(switch_stream_handle_t *stream)
{
	switch_hash_index_t *hi;
	asr_provider_interface_t *provider;

	/* 加锁保护遍历过程，防止遍历期间哈希表被并发修改导致崩溃 */
	switch_mutex_lock(asr_globals.mutex);
	/* 遍历哈希表中所有条目，switch_core_hash_first/next是FreeSWITCH的标准迭代方式 */
	for (hi = switch_core_hash_first(asr_globals.providers); hi; hi = switch_core_hash_next(&hi)) {
		/* 从迭代器中提取key和value，此处只使用value（provider指针） */
		switch_core_hash_this(hi, NULL, NULL, (void **) &provider);
		/* 格式化输出：Provider名称 + 三个核心回调的函数地址
		 * %-20s 左对齐占20字符宽度，使多行输出对齐美观
		 * 将函数指针转为uintptr_t再转void*，避免严格别名问题和格式化警告 */
		stream->write_function(stream, "  %-20s  open=%p  feed=%p  get_results=%p\n",
							   provider->name,
							   (void *) (uintptr_t) provider->open,
							   (void *) (uintptr_t) provider->feed,
							   (void *) (uintptr_t) provider->get_results);
	}
	switch_mutex_unlock(asr_globals.mutex);
}
