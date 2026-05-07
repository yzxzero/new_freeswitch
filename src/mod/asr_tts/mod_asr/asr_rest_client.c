/*
 * asr_rest_client.c -- REST HTTP客户端，用于一次性（one-shot）语音识别
 *
 * 【模块用途】
 * 本文件实现了基于HTTP REST协议的一次性语音识别（ASR）客户端。
 * 所谓"一次性识别"，是指将完整的音频数据一次性发送给ASR服务端，
 * 服务端处理后返回最终的识别结果。与之相对的是WebSocket流式识别
 * （见 asr_ws_client.c），后者通过WebSocket长连接持续发送音频流，
 * 可以实时获得中间识别结果和最终结果。
 *
 * 【与WS流式识别的区别】
 * - REST一次性识别：适用于短音频、已有完整录音文件的场景，请求-响应模式，
 *   简单直接，无需维护长连接，但无法获取中间结果，延迟较高。
 * - WS流式识别：适用于实时语音流（如电话通话），可以边说边识别，
 *   延迟低，支持中间结果回调，但实现复杂度更高。
 *
 * 【技术依赖】
 * 使用FreeSWITCH封装的switch_curl接口（src/include/switch_curl.h），
 * 底层基于libcurl库实现HTTP通信。
 */

#include "mod_asr.h"
#include <switch_curl.h>

/*
 * rest_response_t -- REST响应数据的动态缓冲区结构体
 *
 * 【为何需要动态缓冲区】
 * libcurl在接收HTTP响应体时，采用分块回调（chunked callback）机制：
 * 不会一次性提供完整响应数据，而是根据网络传输情况，多次调用
 * CURLOPT_WRITEFUNCTION指定的回调函数，每次传入一部分数据。
 * 因此需要一个可以逐步扩展的动态缓冲区来拼接所有数据块。
 *
 * 【字段说明】
 * - body:    响应体内容，通过malloc/realloc动态分配，逐步拼接各次回调的数据
 * - size:    当前已接收的数据总字节数（不含末尾的'\0'）
 * - pool:    FreeSWITCH内存池引用（保留备用，当前未直接用于body分配，
 *            因为curl回调上下文中不适合使用内存池的push/pop语义）
 */
typedef struct {
	char *body;
	size_t size;
	switch_memory_pool_t *pool;
} rest_response_t;

/*
 * rest_write_callback -- curl写数据回调函数
 *
 * 【curl回调机制说明】
 * libcurl在接收到HTTP响应体数据时，会反复调用此回调函数。
 * 每次调用提供本次接收到的数据片段（ptr指向的数据，共realsize字节）。
 * 回调必须返回成功处理的字节数；若返回值与传入字节数不一致，curl会
 * 视为错误并中止传输。
 *
 * 【为何使用malloc/realloc而非FreeSWITCH内存池】
 * 1. curl回调在curl内部上下文中执行，而非FreeSWITCH的主线程上下文，
 *    使用内存池的switch_core_session_alloc等函数需要有效的session和pool，
 *    而回调中只能访问通过CURLOPT_WRITEDATA传入的有限数据。
 * 2. FreeSWITCH内存池（apr_pool）采用"一次性分配、统一释放"的模型，
 *    不支持 realloc 语义的内存重新分配。若要扩展缓冲区，只能新分配
 *    一块更大的内存并复制旧数据，效率不如 realloc。
 * 3. malloc/realloc是标准C库函数，在此场景下更直接、更可控，
 *    使用完毕后由调用方负责free释放（见asr_rest_request中的switch_safe_free）。
 *
 * 【内存管理策略】
 * - 首次调用时（resp->body为NULL），使用malloc分配初始缓冲区
 * - 后续调用时，使用realloc扩展缓冲区大小
 * - 每次扩展的大小 = 已有数据量 + 本次新数据量 + 1（末尾'\0'）
 * - 若realloc返回NULL（内存不足），返回0告知curl传输失败
 */
static size_t rest_write_callback(void *ptr, size_t size, size_t nmemb, void *data)
{
	rest_response_t *resp = (rest_response_t *) data;
	/* 计算本次回调实际接收的字节数 */
	size_t realsize = size * nmemb;

	if (!resp->body) {
		/* 首次回调，尚未分配缓冲区，使用malloc分配 */
		resp->body = malloc(resp->size + realsize + 1);
	} else {
		/* 非首次回调，使用realloc扩展已有缓冲区 */
		char *tmp = realloc(resp->body, resp->size + realsize + 1);
		if (!tmp) return 0;	/* realloc失败，返回0通知curl传输失败 */
		resp->body = tmp;
	}

	if (!resp->body) return 0;	/* malloc失败，返回0通知curl传输失败 */

	/* 将本次接收的数据追加到缓冲区末尾 */
	memcpy(&(resp->body[resp->size]), ptr, realsize);
	resp->size += realsize;
	/* 确保以'\0'结尾，方便后续作为字符串处理 */
	resp->body[resp->size] = '\0';

	/* 必须返回成功处理的字节数，否则curl会认为出错 */
	return realsize;
}

/*
 * asr_rest_request -- 发送REST HTTP请求，执行一次性语音识别
 *
 * 【功能概述】
 * 本函数是REST客户端的核心入口，负责：
 * 1. 初始化curl会话
 * 2. 配置HTTP请求参数（URL、方法、头部、超时、SSL等）
 * 3. 发送请求并接收响应
 * 4. 返回响应体内容供ASR模块解析识别结果
 *
 * 【参数说明】
 * - session:       ASR会话对象，包含FreeSWITCH内存池等上下文
 * - url:           请求的目标URL（ASR服务的HTTP接口地址）
 * - method:        HTTP方法（"GET"/"POST"/"PUT"/"DELETE"）
 * - headers:       自定义HTTP头部数组（如Content-Type、Authorization等）
 * - header_count:  headers数组中的头部数量
 * - body:          请求体数据（如音频二进制数据）
 * - body_len:      请求体长度
 * - response:      [输出] 响应体内容（调用方需负责free释放）
 * - response_len:  [输出] 响应体长度
 *
 * 【返回值】
 * SWITCH_STATUS_SUCCESS  -- 请求成功，response中包含响应数据
 * SWITCH_STATUS_FALSE    -- 请求失败（参数无效、curl错误、HTTP错误码等）
 */
switch_status_t asr_rest_request(asr_session_t *session, const char *url, const char *method,
									 const char **headers, int header_count,
									 const void *body, size_t body_len,
									 char **response, switch_size_t *response_len)
{
	switch_CURL *curl_handle = NULL;
	switch_curl_slist_t *curl_headers = NULL;
	rest_response_t resp = { 0 };	/* 零初始化：body=NULL, size=0, pool=NULL */
	long http_code = 0;
	CURLcode curl_res;
	int i;

	/* 参数有效性检查：session、url和response输出指针缺一不可 */
	if (!session || zstr(url) || !response) {
		return SWITCH_STATUS_FALSE;
	}

	/* 初始化curl会话句柄 */
	curl_handle = switch_curl_easy_init();
	if (!curl_handle) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to initialize curl\n");
		return SWITCH_STATUS_FALSE;
	}

	/* 保存session的内存池引用到响应结构体（供后续可能的扩展使用） */
	resp.pool = session->pool;

	/*
	 * 设置目标URL
	 * CURLOPT_URL: 指定请求的完整URL，包括协议、主机、端口、路径和查询参数
	 */
	switch_curl_easy_setopt(curl_handle, CURLOPT_URL, url);

	/*
	 * SSL/TLS验证选项
	 * CURLOPT_SSL_VERIFYPEER=0: 禁用对服务器证书的验证（不验证证书链的合法性）
	 * CURLOPT_SSL_VERIFYHOST=0: 禁用对服务器主机名的验证（不检查证书中的CN/SAN与主机名匹配）
	 *
	 * 【关闭验证的原因】
	 * 在内网部署或测试环境中，ASR服务可能使用自签名证书，开启严格验证会导致
	 * 连接失败。生产环境如需安全性，应改为启用验证并配置CA证书路径：
	 *   switch_curl_easy_setopt(curl_handle, CURLOPT_CAINFO, "/path/to/ca-bundle.crt");
	 *   switch_curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYPEER, 1);
	 *   switch_curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYHOST, 2);
	 */
	switch_curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYPEER, 0);
	switch_curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYHOST, 0);

	/*
	 * 超时设置
	 * CURLOPT_CONNECTTIMEOUT=10: TCP连接超时10秒。如果ASR服务不可达，
	 *   不会长时间阻塞在连接阶段，10秒后返回CURLE_OPERATION_TIMEDOUT。
	 * CURLOPT_TIMEOUT=30: 整个请求的最大耗时30秒（包括连接+传输+等待响应）。
	 *   对于一次性语音识别，30秒足以覆盖音频上传和服务端处理时间；
	 *   若音频较长或服务端处理较慢，可能需要适当增大此值。
	 */
	switch_curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, 10);
	switch_curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 30);

	/*
	 * HTTP重定向跟随
	 * CURLOPT_FOLLOWLOCATION=1: 启用自动跟随301/302等重定向响应。
	 *   某些ASR服务可能使用HTTP→HTTPS重定向或负载均衡重定向，开启此选项
	 *   可自动跳转到最终目标地址，无需调用方手动处理重定向逻辑。
	 * CURLOPT_MAXREDIRS=5: 最多跟随5次重定向，防止无限重定向循环。
	 */
	switch_curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);
	switch_curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 5);

	/*
	 * 注册响应体数据接收回调
	 * CURLOPT_WRITEFUNCTION: 指定接收响应数据的回调函数
	 * CURLOPT_WRITEDATA: 传递给回调函数的自定义数据指针（此处为响应缓冲区结构体）
	 * curl每接收到一块响应数据，就会调用rest_write_callback，将数据追加到resp中
	 */
	switch_curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, rest_write_callback);
	switch_curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *) &resp);

	/*
	 * HTTP方法和请求体设置
	 *
	 * 【各方法处理逻辑】
	 * - POST:   使用CURLOPT_POST=1设置，这是curl内建的POST方法，会自动添加
	 *           "Content-Type: application/x-www-form-urlencoded"头部。如需自定义
	 *           Content-Type（如audio/pcm），需通过自定义头部覆盖。
	 * - PUT:    使用CURLOPT_CUSTOMREQUEST="PUT"将方法改为PUT，curl没有专门的
	 *           PUT选项，需通过CUSTOMREQUEST覆盖默认的GET方法。
	 * - DELETE: 同PUT，使用CUSTOMREQUEST指定方法名，通常无请求体。
	 * - GET:    默认方法，使用CURLOPT_HTTPGET=1显式强制为GET（也会重置之前
	 *           可能设置过的POST/PUT状态），确保行为明确。
	 *
	 * POST和PUT均可携带请求体（音频数据），通过CURLOPT_POSTFIELDSIZE和
	 * CURLOPT_POSTFIELDS设置。注意CURLOPT_POSTFIELDS不复制数据，仅保存指针，
	 * 因此在curl_easy_perform执行完成前，body指向的内存必须保持有效。
	 */
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
		/* GET - 默认方法，显式强制确保curl行为明确 */
		switch_curl_easy_setopt(curl_handle, CURLOPT_HTTPGET, 1);
	}

	/*
	 * 自定义HTTP头部设置
	 *
	 * 常见的ASR服务需要的头部包括：
	 * - Content-Type: 指定音频格式（如"audio/pcm;rate=16000"）
	 * - Authorization: API访问令牌（如Bearer token）
	 * - Accept: 期望的响应格式（如"application/json"）
	 *
	 * 使用curl_slist链表管理自定义头部，每个头部格式为"Key: Value"。
	 * 这些头部会替换curl默认生成的同名头部（如自定义Content-Type会
	 * 覆盖curl默认的"application/x-www-form-urlencoded"）。
	 */
	for (i = 0; i < header_count && headers[i]; i++) {
		curl_headers = switch_curl_slist_append(curl_headers, headers[i]);
	}

	if (curl_headers) {
		switch_curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, curl_headers);
	}

	/*
	 * 执行HTTP请求
	 * switch_curl_easy_perform是同步阻塞调用，会一直等待直到：
	 * - 请求完成并接收到完整响应
	 * - 发生错误（网络故障、超时、SSL错误等）
	 * - 被中断
	 */
	curl_res = switch_curl_easy_perform(curl_handle);

	/* 请求执行失败处理 */
	if (curl_res != CURLE_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "REST request failed: %s\n", switch_curl_easy_strerror(curl_res));
		if (curl_headers) switch_curl_slist_free_all(curl_headers);
		switch_curl_easy_cleanup(curl_handle);
		switch_safe_free(resp.body);	/* 释放已分配的响应缓冲区，避免内存泄漏 */
		return SWITCH_STATUS_FALSE;
	}

	/* 获取HTTP响应状态码，用于判断请求是否成功 */
	switch_curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_code);

	/* 释放curl资源：头部链表和会话句柄 */
	if (curl_headers) switch_curl_slist_free_all(curl_headers);
	switch_curl_easy_cleanup(curl_handle);

	/*
	 * 检查HTTP状态码
	 * 仅2xx（200-299）视为成功。常见的非2xx状态码：
	 * - 401/403: 认证失败或权限不足（API密钥问题）
	 * - 400:     请求格式错误（音频格式不支持、参数缺失等）
	 * - 413:     请求体过大（音频数据超出服务端限制）
	 * - 500:     服务端内部错误
	 * 对于非2xx响应，释放响应缓冲区并返回失败。
	 */
	if (http_code < 200 || http_code >= 300) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "REST request returned HTTP %ld\n", http_code);
		switch_safe_free(resp.body);
		return SWITCH_STATUS_FALSE;
	}

	/*
	 * 成功响应处理
	 * 将响应体数据转移给调用方。注意：resp.body是通过malloc分配的，
	 * 调用方在使用完毕后必须调用free()释放，或者使用switch_safe_free宏释放。
	 * 这里不复制数据，而是直接转移指针，避免不必要的内存拷贝。
	 */
	if (resp.body && resp.size > 0) {
		*response = resp.body;
		if (response_len) *response_len = (switch_size_t) resp.size;
	} else {
		/* 响应体为空的情况（某些ASR服务可能返回204 No Content或空body） */
		*response = NULL;
		if (response_len) *response_len = 0;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "REST request completed: HTTP %ld, %zu bytes\n", http_code, resp.size);

	return SWITCH_STATUS_SUCCESS;
}
