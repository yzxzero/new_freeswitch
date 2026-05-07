/*
 * provider_aliyun.c -- 阿里云ASR Provider实现
 *
 * 本文件实现了mod_asr框架的阿里云NLS语音识别Provider，是整个ASR模块中
 * 与阿里云服务交互的核心代码。通过实现asr_provider_interface_t接口，
 * 将阿里云NLS服务适配到FreeSWITCH的ASR框架中。
 *
 * ========== 架构概览 ==========
 *
 * 模块分层（自上而下）：
 *   FreeSWITCH Core (switch_core_asr.c)
 *     → mod_asr框架 (mod_asr.c: asr_session管理 + worker线程)
 *       → Provider接口 (asr_provider_interface_t: 函数指针表)
 *         → 本文件 (provider_aliyun.c: 阿里云NLS协议实现)
 *           → 底层通信 (asr_ws / asr_rest: WebSocket/HTTP客户端)
 *
 * 支持两种识别模式：
 *   - WebSocket模式：实时流式识别（阿里云NLS SpeechTranscriber）
 *     适用场景：长时间通话、需要中间识别结果、连续语音识别
 *     特点：建立WS长连接，音频持续推送，服务端异步返回结果
 *   - REST模式：一句话识别（HTTP POST）
 *     适用场景：短语音（<60秒）、不需要中间结果
 *     特点：积攒音频后一次性HTTP提交，同步等待结果
 *
 * ========== WebSocket协议流程（匹配阿里云官方JS Demo） ==========
 *
 *   1. 获取Token（调用阿里云POP API CreateToken接口）
 *   2. 连接 wss://nls-gateway-cn-shanghai.aliyuncs.com/ws/v1?token=xxx
 *   3. 连接成功后立即发送 StartTranscription 命令（JSON文本帧）
 *   4. 等待服务端返回 TranscriptionStarted 事件（确认可以发音频）
 *   5. 持续发送音频数据（Binary帧）
 *   6. 接收事件：
 *      - SentenceBegin: 检测到语音开始
 *      - TranscriptionResultChanged: 中间识别结果（部分识别）
 *      - SentenceEnd: 句子结束，最终识别结果
 *      - TranscriptionCompleted: 识别完成
 *   7. 发送 StopTranscription 命令结束识别
 *   8. 接收 TranscriptionCompleted 事件
 *
 * ========== Token获取流程（阿里云POP API签名机制） ==========
 *
 *   1. 构造规范化查询字符串（参数按字母序排列，值做URL编码）
 *   2. 构造待签名字符串：GET&%2F&URL编码(规范化查询串)
 *   3. 使用AccessKeySecret+"&"作为HMAC-SHA1密钥，对待签名字符串签名
 *   4. 将签名Base64编码后再URL编码，附加到请求URL
 *   5. 发送HTTP GET请求，解析响应JSON获取Token
 *
 * ========== 关键数据流 ==========
 *
 *   音频流：FreeSWITCH → asr_session_feed() → audio_buffer → worker线程
 *           → provider feed() → WS Binary帧 / REST缓冲区
 *
 *   结果流：WS/REST响应 → provider poll_results() → asr_session_set_result()
 *           → session state=RESULT_READY → FreeSWITCH get_results()
 *
 * 参考：provider_aliyun.h 定义了常量和aliyun_asr_ctx_t结构体
 */

#include "mod_asr.h"
#include "provider_aliyun.h"
#include <switch_curl.h>
#include <switch_cJSON.h>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>

/*
 * aliyun_globals -- 阿里云全局配置结构体
 *
 * 在模块加载时（asr_provider_aliyun_load）从asr.conf XML配置文件读取并填充。
 * 所有会话（aliyun_asr_ctx_t）在open时从此全局配置复制一份到自己的上下文中，
 * 这样做的原因是：每个会话可能通过text_param/numeric_param覆盖默认配置值，
 * 而全局配置不应被个别会话的修改影响（会话间隔离）。
 *
 * 内存管理：所有字符串通过switch_core_strdup(pool, ...)分配到模块级内存池，
 * 随模块卸载自动释放，无需手动free。
 */
static struct {
	char *access_key_id;      /* 阿里云AccessKey ID，用于POP API签名标识身份 */
	char *access_key_secret;  /* 阿里云AccessKey Secret，仅用于HMAC-SHA1签名计算，不会在网络传输 */
	char *app_key;            /* NLS应用AppKey，每次WS/REST请求必须携带，标识具体的ASR应用 */
	char *region;             /* 服务区域（默认"cn-shanghai"），决定API端点的域名后缀 */
	char *format;             /* 音频格式（默认"pcm"），必须与实际音频编码匹配，否则识别失败 */
	int sample_rate;          /* 采样率Hz（默认16000），电话语音8000，高清语音16000 */
	switch_bool_t enable_intermediate_result;  /* 是否返回中间识别结果（默认true），WS模式下用于实时显示部分识别 */
	switch_bool_t enable_punctuation;          /* 是否在结果中添加标点符号（默认true），提升可读性 */
	int rest_timeout;         /* REST请求超时毫秒（默认10000），防止长时间无响应阻塞worker线程 */
	switch_memory_pool_t *pool; /* 模块级内存池，配置字符串的分配来源 */
} aliyun_globals;

/*
 * aliyun_curl_buf_t -- curl响应数据缓冲区
 *
 * curl的CURLOPT_WRITEFUNCTION回调需要一种机制来动态收集HTTP响应数据，
 * 因为curl不预先告知响应体大小，数据是分块到达的。
 * 此结构体配合aliyun_curl_write_cb使用，实现动态增长的缓冲区。
 *
 * 工作原理：
 *   1. 初始化时data=NULL, size=0
 *   2. 每次curl回调被调用时，realloc扩展data缓冲区
 *   3. 追加新数据到data末尾，更新size
 *   4. 始终在末尾添加'\0'保证字符串终止
 *   5. curl完成后，调用方负责free(buf.data)释放内存
 *
 * 注意：使用realloc而非switch_core_alloc，因为curl回调在FreeSWITCH
 * 内存池体系之外执行，且缓冲区大小不可预知需要动态扩展。
 */
typedef struct {
	char *data;    /* 动态分配的响应数据缓冲区，以'\0'结尾 */
	size_t size;   /* data中有效数据长度（不含终止符） */
} aliyun_curl_buf_t;

/*
 * aliyun_curl_write_cb -- curl响应数据写入回调
 *
 * 这是curl的CURLOPT_WRITEFUNCTION回调函数，curl每次接收到响应数据块时调用。
 * 采用经典的"realloc+memcpy"模式实现动态缓冲区增长。
 *
 * @param ptr      curl传入的数据块指针
 * @param size     每个元素的大小（curl文档中始终为1字节）
 * @param nmemb    数据块中的元素个数
 * @param data     用户数据指针，实际为aliyun_curl_buf_t*
 * @return         成功返回realsize，失败返回0（curl会中止传输）
 *
 * 返回0的时机：realloc失败（内存不足），这会导致curl中止当前传输，
 * curl_easy_perform()返回CURLE_WRITE_ERROR。
 */
static size_t aliyun_curl_write_cb(void *ptr, size_t size, size_t nmemb, void *data)
{
	aliyun_curl_buf_t *buf = (aliyun_curl_buf_t *) data;
	size_t realsize = size * nmemb;  /* 本次接收的实际字节数 */
	char *tmp = realloc(buf->data, buf->size + realsize + 1);  /* +1为终止符预留空间 */
	if (!tmp) return 0;  /* 内存分配失败，返回0通知curl中止传输 */
	buf->data = tmp;
	memcpy(buf->data + buf->size, ptr, realsize);  /* 追加新数据到缓冲区末尾 */
	buf->size += realsize;  /* 更新有效数据长度 */
	buf->data[buf->size] = '\0';  /* 确保字符串终止，方便后续直接作为C字符串使用 */
	return realsize;  /* 返回已处理字节数，curl期望此值等于realsize */
}

/*
 * aliyun_generate_id -- 生成32字符十六进制伪唯一ID
 *
 * 用于生成阿里云NLS协议要求的task_id和message_id字段。
 * 阿里云NLS协议要求每条WebSocket消息携带task_id（关联整个识别任务）
 * 和message_id（标识单条消息），这两个ID需要是唯一的但不需要加密安全。
 *
 * ID生成策略（非加密安全但足够唯一）：
 *   4个32位十六进制数拼接 = 32字符，组合来源：
 *   - 第1段：微秒时间戳低32位（提供时间唯一性）
 *   - 第2段：微秒时间戳右移16位后低32位（增加时间维度分散性）
 *   - 第3段：缓冲区指针地址低32位（提供空间唯一性，不同调用栈位置不同）
 *   - 第4段：线性同余伪随机数（LCG: x * 6364136223846793005 + 1442695040888963407）
 *             使用时间戳作为种子，提供额外随机性
 *
 * 最后遍历确保所有字符都是合法的十六进制数字，非法字符替换为'0'。
 * 这是一种防御性编码，防止snprintf格式化异常导致非法字符。
 *
 * @param buf    输出缓冲区，至少33字节（32字符+终止符）
 * @param buflen 缓冲区长度
 */
static void aliyun_generate_id(char *buf, size_t buflen)
{
	int i;
	/* 【并发唯一性修复】原子递增计数器，确保多线程并发调用时ID不重复。
	 * 原实现仅依赖时间戳+栈地址+LCG，并发时多个线程在同一微秒内调用
	 * 会生成相同的ID（时间戳相同、栈地址可能相近、LCG种子相同），
	 * 导致阿里云NLS协议中task_id/message_id冲突，可能引起请求关联错误。
	 * 使用__sync_fetch_and_add原子递增，保证每次调用得到不同的计数器值，
	 * 即使同一微秒内并发调用，第2段也会不同。 */
	static volatile uint32_t id_counter = 0;
	uint32_t my_counter = __sync_fetch_and_add(&id_counter, 1);

	/* 四段拼接为32字符十六进制串 */
	snprintf(buf, buflen, "%08x%08x%08x%08x",
		(unsigned int) (switch_micro_time_now() & 0xFFFFFFFF),                     /* 段1: 时间戳低32位 */
		(unsigned int) my_counter,                                                  /* 段2: 原子计数器（替代原来的时间戳移位，保证并发唯一） */
		(unsigned int) ((uintptr_t) buf & 0xFFFFFFFF),                              /* 段3: 栈地址低32位 */
		(unsigned int) ((switch_micro_time_now() * 6364136223846793005LL + 1442695040888963407LL + my_counter) & 0xFFFFFFFF));  /* 段4: LCG伪随机（加入counter增加分散性） */
	/* 防御性检查：确保所有字符都是合法的十六进制数字 */
	for (i = 0; i < 32 && i < (int)(buflen - 1); i++) {
		if (!isxdigit(buf[i])) buf[i] = '0';  /* 非法字符替换为'0' */
	}
	buf[32 < buflen - 1 ? 32 : buflen - 1] = '\0';  /* 确保终止符位置正确 */
}

/* ---- Token管理 ---- */

/*
 * aliyun_url_encode -- RFC 3986 URL编码
 *
 * 按照阿里云POP API规范要求，URL编码遵循RFC 3986标准：
 *   - 保留字符（unreserved）不编码：A-Z a-z 0-9 - _ . ~
 *   - 其他字符编码为%XX格式（大写十六进制）
 *
 * 这与标准URL编码（application/x-www-form-urlencoded）的区别：
 *   - RFC 3986不将空格编码为'+'，而是编码为'%20'
 *   - 阿里云POP API签名要求严格使用RFC 3986，否则签名校验会失败
 *
 * 编码后的字符串用于：
 *   1. 构造规范化查询字符串（canonical_query）中的参数值
 *   2. 构造待签名字符串时对规范化查询串整体编码
 *   3. 构造最终URL时对签名值编码
 *
 * @param src     待编码的原始字符串
 * @param dst     编码结果输出缓冲区
 * @param dst_len 输出缓冲区大小
 * @return        编码后字符串长度（不含终止符）
 */
static size_t aliyun_url_encode(const char *src, char *dst, size_t dst_len)
{
	static const char *unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";  /* RFC 3986 unreserved字符集 */
	size_t i, j = 0;
	for (i = 0; src[i] && j < dst_len - 4; i++) {  /* -4确保%XX编码有足够空间 */
		if (strchr(unreserved, src[i])) {
			dst[j++] = src[i];  /* unreserved字符直接复制 */
		} else {
			j += snprintf(dst + j, dst_len - j, "%%%02X", (unsigned char) src[i]);  /* 其他字符编码为%XX（大写十六进制） */
		}
	}
	dst[j] = '\0';
	return j;
}

/*
 * aliyun_hmac_sha1_base64 -- HMAC-SHA1签名 + Base64编码
 *
 * 这是阿里云POP API签名机制的核心函数。阿里云使用HMAC-SHA1作为签名算法，
 * 签名结果需要Base64编码后才能放入URL参数中。
 *
 * 签名流程：
 *   1. 构造签名密钥：AccessKeySecret + "&"（末尾的&是阿里云规范要求的固定后缀）
 *      原因：阿里云AccessKeySecret仅用于POP API签名，加上&后缀是为了与
 *            STS SecurityToken的签名密钥格式保持一致（STS密钥格式为"Secret&Token"）
 *   2. 使用HMAC-SHA1算法计算签名：HMAC-SHA1(密钥, 待签名字符串)
 *   3. 对签名结果进行Base64编码（无换行模式）
 *   4. Base64字符串即最终签名值，需要URL编码后附加到请求URL
 *
 * @param key     AccessKeySecret（不含&后缀，函数内部自动添加）
 * @param data    待签名字符串（格式为"GET&%2F&URL编码(规范化查询串)"）
 * @param out     Base64编码后的签名输出缓冲区
 * @param out_len 输出缓冲区大小
 */
static void aliyun_hmac_sha1_base64(const char *key, const char *data, char *out, size_t out_len)
{
	unsigned char hmac_result[EVP_MAX_MD_SIZE];  /* HMAC计算结果，最大可能长度 */
	unsigned int hmac_len = 0;                    /* HMAC结果实际长度 */
	char key_buf[512];                            /* 签名密钥缓冲区：AccessKeySecret + "&" */
	size_t key_len;
	BIO *bio, *b64;      /* OpenSSL BIO链：用于Base64编码 */
	BUF_MEM *bptr;        /* BIO内存缓冲区指针，用于获取Base64结果 */
	long b64_len;

	/* 构造签名密钥：AccessKeySecret + "&"
	 * 阿里云POP API规范要求密钥末尾附加"&" */
	key_len = snprintf(key_buf, sizeof(key_buf), "%s&", key);

	/* 计算HMAC-SHA1签名
	 * 参数：SHA1算法、密钥、密钥长度、待签名数据、数据长度、结果缓冲区、结果长度 */
	HMAC(EVP_sha1(), key_buf, (int) key_len,
			 (const unsigned char *) data, (int) strlen(data),
			 hmac_result, &hmac_len);

	/* 使用BIO链进行Base64编码（无换行模式）
	 * BIO链：b64(Base64过滤器) → bio(内存sink)
	 * BIO_FLAGS_BASE64_NO_NL：不插入换行符，输出连续Base64字符串 */
	b64 = BIO_new(BIO_f_base64());
	bio = BIO_new(BIO_s_mem());
	b64 = BIO_push(b64, bio);        /* 将b64过滤器推到bio之上，形成处理链 */
	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);  /* 禁止Base64输出中的换行符 */
	BIO_write(b64, hmac_result, (int) hmac_len);  /* 将HMAC结果写入BIO链 */
	BIO_flush(b64);                                  /* 刷新BIO缓冲区 */
	BIO_get_mem_ptr(b64, &bptr);                     /* 获取Base64编码后的内存指针 */

	/* 将Base64结果复制到输出缓冲区 */
	b64_len = bptr->length < (long)(out_len - 1) ? bptr->length : (long)(out_len - 1);
	memcpy(out, bptr->data, b64_len);
	out[b64_len] = '\0';

	/* 释放BIO链（BIO_free_all会释放整个链：b64 → bio） */
	BIO_free_all(b64);
}

/*
 * aliyun_get_token -- 通过阿里云POP API获取NLS Token
 *
 * 这是最复杂的函数，实现了完整的阿里云POP API签名认证流程。
 * Token是阿里云NLS服务的临时访问凭证，有效期通常24小时，
 * 每次ASR会话打开时都需要重新获取以确保有效性。
 *
 * ========== 完整流程 ==========
 *
 * 1. 生成时间戳（ISO 8601格式）和随机nonce（防重放攻击）
 *
 * 2. 构造规范化查询字符串（Canonical Query String）：
 *    - 参数按名称字母序排列（这是阿里云POP API签名规范的强制要求）
 *    - 每个参数值必须经过RFC 3986 URL编码
 *    - 参数之间用&连接
 *    包含的参数：
 *      AccessKeyId          - 标识哪个阿里云用户
 *      Action=CreateToken   - 要执行的API操作
 *      Format=JSON          - 响应格式
 *      RegionId             - 服务区域
 *      SignatureMethod=HMAC-SHA1  - 签名算法
 *      SignatureNonce       - 唯一随机数（防重放）
 *      SignatureVersion=1.0 - 签名协议版本
 *      Timestamp            - 请求时间戳
 *      Version=2019-02-28   - API版本号
 *
 * 3. 构造待签名字符串（String to Sign）：
 *    格式：HTTP方法 + "&" + URL编码("/") + "&" + URL编码(规范化查询串)
 *    例如："GET&%2F&AccessKeyId%3DLTAI...%26Action%3DCreateToken%26..."
 *    注意："/"编码为"%2F"是固定的，因为POP API路径始终是"/"
 *
 * 4. 计算签名：HMAC-SHA1(AccessKeySecret+"&", 待签名字符串) → Base64 → URL编码
 *
 * 5. 构造最终请求URL：基础URL + "?" + 规范化查询串 + "&Signature=" + URL编码(签名)
 *
 * 6. 发送HTTP GET请求，解析JSON响应
 *    成功响应格式：{"Token":{"Id":"xxx","ExpireTime":...}, "RequestId":"..."}
 *    失败响应格式：{"Code":"InvalidAccessKeyId.NotFound", "Message":"...", "RequestId":"..."}
 *
 * @param pool FreeSWITCH内存池，Token字符串将分配到此池中
 * @return     成功返回Token字符串指针（pool分配），失败返回NULL
 */
static char *aliyun_get_token(switch_memory_pool_t *pool)
{
	switch_CURL *curl;
	aliyun_curl_buf_t buf = { NULL, 0 };  /* 初始化curl响应缓冲区：空数据、零长度 */
	CURLcode res;
	char *response = NULL;
	size_t resp_len = 0;
	char *token = NULL;
	cJSON *json, *token_obj;

	/* ---- 第1步：准备签名所需的公共参数 ---- */
	char timestamp[64];        /* ISO 8601格式时间戳：2024-01-15T08:30:00Z */
	char nonce[37];            /* 唯一随机数（防重放攻击），32字符hex + 4分隔符 + 终止符 */
	switch_time_exp_t tm;      /* 时间分解结构体 */
	char enc_val[512];         /* URL编码结果临时缓冲区（复用于多个参数编码） */
	char canonical_query[4096]; /* 规范化查询字符串（所有参数按字母序排列拼接） */
	char string_to_sign[8192]; /* 待签名字符串：GET&%2F&URL编码(规范化查询串) */
	char signature[256];       /* HMAC-SHA1签名Base64结果 */
	char url[8192];            /* 最终请求URL */
	int offset = 0;            /* canonical_query写入偏移量 */

	/* 生成ISO 8601 UTC时间戳（阿里云POP API要求UTC时间） */
	switch_time_exp_gmt(&tm, switch_micro_time_now());
	snprintf(timestamp, sizeof(timestamp), "%04d-%02d-%02dT%02d:%02d:%02dZ",
			 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
			 tm.tm_hour, tm.tm_min, tm.tm_sec);

	/* 生成SignatureNonce（防重放随机数）
	 * 格式：8-8-8-8的hex串（类似UUID格式但不是真正的UUID v4）
	 * 组合了时间戳、栈地址和LCG伪随机数，保证唯一性 */
	snprintf(nonce, sizeof(nonce), "%08x%08x%08x%08x",
			 (unsigned int) (switch_micro_time_now() & 0xFFFFFFFF),
			 (unsigned int) ((switch_micro_time_now() >> 16) & 0xFFFFFFFF),
			 (unsigned int) ((uintptr_t) &offset & 0xFFFFFFFF),     /* 使用局部变量地址增加随机性 */
			 (unsigned int) ((switch_micro_time_now() * 6364136223846793005LL) & 0xFFFFFFFF));

	/* ---- 第2步：构造规范化查询字符串 ----
	 * 关键规则：
	 *   - 参数必须按名称的字典序排列（这是POP API签名校验的要求）
	 *   - 参数值必须经过RFC 3986 URL编码
	 *   - 参数名和分隔符（=、&）不做编码
	 *   - 当前参数已按字母序排列：AccessKeyId < Action < Format < RegionId < SignatureMethod < SignatureNonce < SignatureVersion < Timestamp < Version
	 */
	offset = 0;
	/* AccessKeyId：标识阿里云用户身份 */
	aliyun_url_encode(aliyun_globals.access_key_id, enc_val, sizeof(enc_val));
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"AccessKeyId=%s", enc_val);
	/* Action：API操作名，这里固定为CreateToken */
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&Action=CreateToken");
	/* Format：响应格式，固定为JSON */
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&Format=JSON");
	/* RegionId：服务区域 */
	aliyun_url_encode(aliyun_globals.region, enc_val, sizeof(enc_val));
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&RegionId=%s", enc_val);
	/* SignatureMethod：签名算法，固定HMAC-SHA1 */
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&SignatureMethod=HMAC-SHA1");
	/* SignatureNonce：防重放随机数 */
	aliyun_url_encode(nonce, enc_val, sizeof(enc_val));
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&SignatureNonce=%s", enc_val);
	/* SignatureVersion：签名协议版本 */
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&SignatureVersion=1.0");
	/* Timestamp：请求时间戳 */
	aliyun_url_encode(timestamp, enc_val, sizeof(enc_val));
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&Timestamp=%s", enc_val);
	/* Version：API版本号 */
	offset += snprintf(canonical_query + offset, sizeof(canonical_query) - offset,
						"&Version=2019-02-28");

	/* ---- 第3步：构造待签名字符串 ----
	 * 格式：HTTP方法 + "&" + URL编码("/") + "&" + URL编码(规范化查询串)
	 * "GET"是HTTP方法（POP API Token接口使用GET请求）
	 * "%2F"是"/"的URL编码（POP API路径始终为"/"）
	 * 规范化查询串需要整体再做一次URL编码（二次编码：参数值已编码一次，整体再编码一次）
	 * 这是阿里云POP API签名规范的"双层编码"要求，确保特殊字符不会干扰签名解析 */
	aliyun_url_encode(canonical_query, enc_val, sizeof(enc_val));
	snprintf(string_to_sign, sizeof(string_to_sign), "GET&%%2F&%s", enc_val);

	/* ---- 第4步：计算签名 ----
	 * HMAC-SHA1(AccessKeySecret + "&", 待签名字符串) → Base64 */
	aliyun_hmac_sha1_base64(aliyun_globals.access_key_secret, string_to_sign, signature, sizeof(signature));

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
		"Aliyun token string_to_sign: %s\n", string_to_sign);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
		"Aliyun token signature: %s\n", signature);

	/* ---- 第5步：构造最终请求URL ----
	 * 格式：ALIYUN_TOKEN_URL + "?" + canonical_query + "&Signature=" + URL编码(签名)
	 * 签名值需要URL编码是因为Base64结果中可能包含+、/、=等URL特殊字符 */
	aliyun_url_encode(signature, enc_val, sizeof(enc_val));
	snprintf(url, sizeof(url), "%s?%s&Signature=%s",
			 ALIYUN_TOKEN_URL, canonical_query, enc_val);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun token request URL: %s\n", url);

	/* ---- 第6步：发送HTTP GET请求 ---- */
	curl = switch_curl_easy_init();
	if (!curl) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "curl init failed\n");
		return NULL;
	}

	/* 配置curl选项 */
	switch_curl_easy_setopt(curl, CURLOPT_URL, url);
	switch_curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);   /* 跳过SSL证书验证（开发环境，生产应启用） */
	switch_curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);   /* 跳过主机名验证 */
	switch_curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10);          /* 总超时10秒 */
	switch_curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5);    /* 连接超时5秒 */
	switch_curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);          /* 线程安全：避免curl使用信号（多线程环境必须设置） */
	switch_curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);    /* 跟随HTTP重定向 */

	/* 设置响应数据写入回调和用户数据指针 */
	switch_curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, aliyun_curl_write_cb);
	switch_curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &buf);

	/* 执行HTTP请求 */
	res = switch_curl_easy_perform(curl);
	switch_curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Aliyun token request failed: %s\n",
						  switch_curl_easy_strerror(res));
		switch_safe_free(buf.data);  /* 释放curl缓冲区 */
		return NULL;
	}

	response = buf.data;
	resp_len = buf.size;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun token response: %s\n", response);

	if (!response || resp_len == 0) {
		return NULL;  /* 空响应，可能是网络问题 */
	}

	/* ---- 第7步：解析JSON响应 ----
	 * 成功响应示例：
	 *   {"Token":{"Id":"a1b2c3d4...","ExpireTime":1705305600},"RequestId":"xxx"}
	 * 失败响应示例：
	 *   {"Code":"InvalidAccessKeyId.NotFound","Message":"Specified access key is not found.","RequestId":"xxx"}
	 */
	json = cJSON_Parse(response);
	if (!json) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Aliyun token JSON parse failed\n");
		switch_safe_free(response);
		return NULL;
	}

	/* 从JSON中提取Token.Id */
	token_obj = cJSON_GetObjectItem(json, "Token");
	if (token_obj) {
		cJSON *id_obj = cJSON_GetObjectItem(token_obj, "Id");
		if (id_obj && cJSON_IsString(id_obj)) {
			/* 将Token字符串复制到FreeSWITCH内存池，随会话生命周期自动释放 */
			token = switch_core_strdup(pool, id_obj->valuestring);
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun token obtained OK\n");
		}
	} else {
		/* Token字段不存在，检查是否有错误信息 */
		cJSON *err_obj = cJSON_GetObjectItem(json, "Code");
		if (err_obj) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
				"Aliyun token error: %s - %s\n",
				err_obj->valuestring,
				cJSON_GetObjectItem(json, "Message") ? cJSON_GetObjectItem(json, "Message")->valuestring : "unknown");
		} else {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Aliyun token response missing Token field\n");
		}
	}

	cJSON_Delete(json);
	switch_safe_free(response);  /* 释放curl响应缓冲区 */

	return token;
}

/* 前向声明：REST识别函数（在aliyun_asr_poll_results中被调用，需要前向声明） */
static switch_status_t aliyun_rest_recognize(asr_session_t *session);

/* ---- WebSocket结果消息处理 ---- */

/*
 * aliyun_process_ws_result -- 处理阿里云NLS WebSocket返回的JSON消息
 *
 * 阿里云NLS WebSocket协议使用JSON格式的文本帧进行通信，每条消息包含
 * header和payload两部分。header中的name字段标识事件类型。
 *
 * ========== 事件类型及处理逻辑 ==========
 *
 * 1. TranscriptionStarted -- 识别任务已启动
 *    触发时机：发送StartTranscription后，服务端确认可以接收音频
 *    处理：设置ctx->transcription_started=SWITCH_TRUE，通知feed()可以开始发送音频
 *    关键：feed()在收到此事件前会丢弃音频数据（避免服务端返回错误）
 *
 * 2. SentenceBegin -- 检测到语音开始（VAD触发）
 *    触发时机：服务端VAD检测到用户开始说话
 *    处理：设置ASR_SESSION_FLAG_START_OF_SPEECH标志，通知FreeSWITCH框架
 *          触发"begin-speaking"事件（可用于barge-in打断播放）
 *
 * 3. TranscriptionResultChanged -- 中间识别结果（部分识别）
 *    触发时机：正在说话时，服务端持续返回当前最佳识别结果
 *    处理：仅记录日志，不设置会话结果（中间结果可能不完整）
 *    注意：仅在enable_intermediate_result=true时才会收到此事件
 *
 * 4. SentenceEnd -- 句子结束，最终识别结果
 *    触发时机：服务端VAD检测到用户停止说话（静音超过阈值）
 *    处理：提取result（识别文本）和confidence（置信度0-1），设置会话结果
 *    关键：这是最核心的事件，识别文本通过asr_session_set_result()传递给FreeSWITCH
 *          confidence从0-1浮点数转换为0-100整数
 *
 * 5. TranscriptionCompleted -- 识别任务完成
 *    触发时机：发送StopTranscription后，或服务端主动结束
 *    处理：仅记录日志（当前不触发任何状态变更）
 *
 * 6. TaskFailed -- 识别任务失败
 *    触发时机：服务端发生错误（如Token过期、参数错误等）
 *    处理：记录错误日志，包含status_text（错误描述）和完整消息
 *
 * 7. 其他未知事件 -- 记录警告日志
 *
 * @param session ASR会话指针
 * @param msg     WebSocket接收到的JSON文本消息
 */
static void aliyun_process_ws_result(asr_session_t *session, const char *msg)
{
	cJSON *json, *header, *payload, *name, *status_obj, *result, *conf_obj;
	int confidence;
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!msg) return;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun WS recv: %s\n", msg);

	/* 解析JSON消息 */
	json = cJSON_Parse(msg);
	if (!json) return;

	/* 提取header和payload节点 */
	header = cJSON_GetObjectItem(json, "header");
	payload = cJSON_GetObjectItem(json, "payload");

	if (header) {
		name = cJSON_GetObjectItem(header, "name");
		if (name && cJSON_IsString(name)) {
			if (!strcmp(name->valuestring, "TranscriptionStarted")) {
				/* 识别任务已启动：设置标志，允许feed()发送音频数据 */
				if (ctx) ctx->transcription_started = SWITCH_TRUE;
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun TranscriptionStarted: ready for audio\n");
			} else if (!strcmp(name->valuestring, "SentenceBegin")) {
				/* 检测到语音开始：通知FreeSWITCH框架（触发begin-speaking事件） */
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Aliyun SentenceBegin\n");
				switch_set_flag(session, ASR_SESSION_FLAG_START_OF_SPEECH);
			} else if (!strcmp(name->valuestring, "TranscriptionResultChanged")) {
				/* 中间识别结果：仅记录日志，不更新会话结果 */
				if (payload) {
					result = cJSON_GetObjectItem(payload, "result");
					if (result && cJSON_IsString(result)) {
						switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
										  "Aliyun intermediate: %s\n", result->valuestring);
					}
				}
			} else if (!strcmp(name->valuestring, "SentenceEnd")) {
				/* 句子结束：提取最终识别结果和置信度，设置到会话中
				 * 这是最核心的事件处理——识别文本通过asr_session_set_result()传递 */
				if (payload) {
					result = cJSON_GetObjectItem(payload, "result");
					if (result && cJSON_IsString(result)) {
						confidence = 100;  /* 默认置信度100% */
						conf_obj = cJSON_GetObjectItem(payload, "confidence");
						if (conf_obj && cJSON_IsNumber(conf_obj)) {
							/* 阿里云返回0-1浮点数，转换为0-100整数 */
							confidence = (int) (conf_obj->valuedouble * 100);
						}
						switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
										  "Aliyun SentenceEnd: %s (confidence=%d)\n",
										  result->valuestring, confidence);
						/* 将识别结果设置到会话中，触发状态变为RESULT_READY */
						asr_session_set_result(session, result->valuestring, confidence);
					}
				}
			} else if (!strcmp(name->valuestring, "TranscriptionCompleted")) {
				/* 识别任务完成：仅记录日志 */
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun TranscriptionCompleted\n");
			} else if (!strcmp(name->valuestring, "TaskFailed")) {
				/* 识别任务失败：记录错误信息 */
				status_obj = cJSON_GetObjectItem(header, "status_text");
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
								  "Aliyun ASR task failed: %s (msg=%s)\n",
								  status_obj ? status_obj->valuestring : "unknown", msg);
			} else {
				/* 未知事件类型：记录警告 */
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  "Aliyun unknown event: %s\n", name->valuestring);
			}
		}
	}

	cJSON_Delete(json);
}

/* ---- 阿里云Provider回调函数 ---- */

/*
 * aliyun_asr_open -- 打开阿里云ASR会话
 *
 * 这是Provider接口的open回调，负责初始化一次ASR识别会话的所有资源。
 * 根据会话模式（WebSocket/REST）执行不同的初始化流程。
 *
 * ========== WebSocket模式初始化流程 ==========
 *   1. 从全局配置复制参数到会话私有上下文（aliyun_asr_ctx_t）
 *   2. 调用aliyun_get_token()获取NLS Token
 *   3. 建立WebSocket连接（wss://...?token=xxx）
 *   4. 连接成功后立即发送StartTranscription命令（匹配阿里云官方JS Demo流程）
 *   5. 等待TranscriptionStarted事件（最多5秒超时）
 *   6. 收到确认后，会话进入就绪状态，feed()可以开始发送音频
 *
 * ========== REST模式初始化流程 ==========
 *   1. 从全局配置复制参数到会话私有上下文
 *   2. 调用aliyun_get_token()获取NLS Token
 *   3. 创建音频缓冲区（switch_buffer_t，动态增长，上限1MB）
 *   4. 不需要立即建立网络连接，REST请求在poll_results()中触发
 *
 * @param session ASR会话（框架已创建，需填充provider_private）
 * @param ah      FreeSWITCH ASR句柄（包含编解码信息等）
 * @return        SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 失败
 */
static switch_status_t aliyun_asr_open(asr_session_t *session, switch_asr_handle_t *ah)
{
	aliyun_asr_ctx_t *ctx;
	switch_status_t status;
	char ws_url[512];          /* WebSocket连接URL：基础URL + "?token=" + Token */
	const char *ws_headers[2]; /* WebSocket自定义请求头：X-NLS-Token */
	char task_id[33];          /* 识别任务ID（32字符hex + 终止符） */
	char message_id[33];       /* 消息ID（32字符hex + 终止符） */
	char *start_cmd;           /* StartTranscription命令JSON字符串 */

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: START\n");

	/* ---- 第1步：创建会话私有上下文并从全局配置复制参数 ---- */
	ctx = switch_core_alloc(session->pool, sizeof(*ctx));
	if (!ctx) return SWITCH_STATUS_MEMERR;

	memset(ctx, 0, sizeof(*ctx));  /* 清零所有字段，确保指针=NULL，布尔=SWITCH_FALSE */

	/* 从全局配置复制参数到会话上下文（会话级副本，可被text_param覆盖） */
	ctx->access_key_id = switch_core_strdup(session->pool, aliyun_globals.access_key_id);
	ctx->access_key_secret = switch_core_strdup(session->pool, aliyun_globals.access_key_secret);
	ctx->app_key = switch_core_strdup(session->pool, aliyun_globals.app_key);
	ctx->region = switch_core_strdup(session->pool, aliyun_globals.region);
	ctx->format = switch_core_strdup(session->pool, aliyun_globals.format);
	ctx->sample_rate = aliyun_globals.sample_rate;
	ctx->enable_intermediate_result = aliyun_globals.enable_intermediate_result;
	ctx->enable_punctuation = aliyun_globals.enable_punctuation;
	ctx->rest_timeout = aliyun_globals.rest_timeout;

	/* 初始化WS连接状态标志 */
	ctx->ws_connected = SWITCH_FALSE;       /* WS连接尚未建立 */
	ctx->recognition_started = SWITCH_FALSE; /* 尚未发送StartTranscription */
	ctx->transcription_started = SWITCH_FALSE; /* 尚未收到TranscriptionStarted确认 */
	ctx->task_id = NULL;                    /* 任务ID尚未生成 */
	ctx->rest_submitted = SWITCH_FALSE;     /* REST请求尚未提交 */

	/* 优先使用FreeSWITCH提供的原生采样率（电话8000Hz，高清16000Hz）
	 * 如果FreeSWITCH未指定（native_rate<=0），则使用配置中的默认值 */
	if (session->native_rate > 0) {
		ctx->sample_rate = session->native_rate;
	}

	/* 将上下文绑定到会话 */
	session->provider_private = ctx;

	/* ---- 第2步：获取NLS Token ---- */
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: getting token...\n");

	ctx->token = aliyun_get_token(session->pool);
	if (zstr(ctx->token)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to get Aliyun token\n");
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: token obtained (len=%zu)\n", strlen(ctx->token));

	/* ---- 根据模式执行不同的初始化 ---- */
	if (session->mode == ASR_MODE_WEBSOCKET) {
		/* ========== WebSocket模式初始化 ========== */

		/* 构造WebSocket URL：基础URL + token参数
		 * 阿里云NLS要求token同时出现在URL参数和HTTP头中 */
		snprintf(ws_url, sizeof(ws_url), "%s?token=%s", ALIYUN_ASR_WS_URL, ctx->token);
		ws_headers[0] = switch_core_sprintf(session->pool, "X-NLS-Token: %s", ctx->token);  /* HTTP头也携带token */
		ws_headers[1] = NULL;  /* 头数组终止标记 */

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: connecting WS to %s\n", ALIYUN_ASR_WS_URL);

		/* 建立WebSocket连接（包含SSL握手、HTTP升级请求） */
		status = asr_ws_connect(session, ws_url, ws_headers, 1);
		if (status != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to connect Aliyun WS: %s\n", ws_url);
			return SWITCH_STATUS_FALSE;
		}
		ctx->ws_connected = SWITCH_TRUE;

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR open: WS connected, sending StartTranscription\n");

		/* 发送StartTranscription命令（连接成功后必须立即发送，匹配阿里云官方JS Demo流程）
		 * 阿里云NLS协议要求：WS连接建立后，服务端不会主动发送任何消息，
		 * 客户端必须先发送StartTranscription命令来启动识别任务 */
		aliyun_generate_id(task_id, sizeof(task_id));      /* 生成任务ID */
		aliyun_generate_id(message_id, sizeof(message_id)); /* 生成消息ID */
		ctx->task_id = switch_core_strdup(session->pool, task_id);  /* 保存task_id，关闭时发送StopTranscription需要 */

		/* 构造StartTranscription命令JSON
		 * 字段说明：
		 *   header.appkey: 应用标识
		 *   header.namespace: 命名空间（SpeechTranscriber=实时语音转写）
		 *   header.name: 命令名称（StartTranscription=开始转写）
		 *   header.task_id: 任务ID，关联整个识别任务的所有消息
		 *   header.message_id: 消息ID，标识本条消息
		 *   payload.format: 音频格式（pcm=原始PCM）
		 *   payload.sample_rate: 采样率
		 *   payload.enable_intermediate_result: 是否返回中间结果
		 *   payload.enable_punctuation_prediction: 是否添加标点
		 *   payload.enable_inverse_text_normalization: 是否将数字转阿拉伯数字（"一百"→"100"） */
		start_cmd = switch_mprintf(
			"{"
			"\"header\":{"
			"\"appkey\":\"%s\","
			"\"namespace\":\"SpeechTranscriber\","
			"\"name\":\"StartTranscription\","
			"\"task_id\":\"%s\","
			"\"message_id\":\"%s\""
			"},"
			"\"payload\":{"
			"\"format\":\"%s\","
			"\"sample_rate\":%d,"
			"\"enable_intermediate_result\":%s,"
			"\"enable_punctuation_prediction\":%s,"
			"\"enable_inverse_text_normalization\":true"
			"}"
			"}",
			ctx->app_key, task_id, message_id,
			ctx->format, ctx->sample_rate,
			ctx->enable_intermediate_result ? "true" : "false",
			ctx->enable_punctuation ? "true" : "false");

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
			"Sending StartTranscription: appkey=%s, task_id=%s, format=%s, sample_rate=%d\n",
			ctx->app_key, task_id, ctx->format, ctx->sample_rate);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
			"StartTranscription JSON: %s\n", start_cmd);

		/* 通过WebSocket发送文本帧（JSON命令） */
		status = asr_ws_send_text(session, start_cmd);
		switch_safe_free(start_cmd);  /* switch_mprintf分配的内存需要手动释放 */

		if (status != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to send StartTranscription\n");
			return SWITCH_STATUS_FALSE;
		}
		ctx->recognition_started = SWITCH_TRUE;

		/* 等待TranscriptionStarted事件确认（最多5秒超时）
		 * 必须等待此确认才能开始发送音频，否则服务端会返回错误
		 * 轮询方式：检查WS是否有数据可读，有则接收并处理，无则yield 100ms后重试 */
		{
			int wait_ms;
			for (wait_ms = 0; wait_ms < 5000; wait_ms += 100) {
				if (ctx->transcription_started) break;  /* 收到确认，退出等待 */
				if (asr_ws_has_data(session)) {
					char *msg = asr_ws_recv_text(session, session->pool);
					if (msg) {
						aliyun_process_ws_result(session, msg);  /* 处理WS消息，可能设置transcription_started */
					}
				} else {
					switch_yield(100000);  /* 无数据可读，等待100ms（100000微秒） */
				}
			}

			if (!ctx->transcription_started) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
					"Timed out waiting for TranscriptionStarted after %dms\n", wait_ms);
				return SWITCH_STATUS_FALSE;
			}
		}

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
			"Aliyun ASR ready: mode=websocket, app_key=%s, sample_rate=%d\n",
			ctx->app_key, ctx->sample_rate);
	} else {
		/* ========== REST模式初始化 ========== */

		/* 创建音频缓冲区：动态增长，用于积攒音频数据
		 * 参数：初始大小4096字节，块大小65536字节，最大大小0（无上限由rest_max_audio_len控制） */
		switch_buffer_create_dynamic(&ctx->rest_audio_buffer, 4096, 65536, 0);
		ctx->rest_max_audio_len = 1024 * 1024;  /* 最大1MB（约32秒PCM@16kHz 16bit单声道） */

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
			"Aliyun ASR ready: mode=rest, app_key=%s, sample_rate=%d\n",
			ctx->app_key, ctx->sample_rate);
	}

	return SWITCH_STATUS_SUCCESS;
}

/*
 * aliyun_asr_close -- 关闭阿里云ASR会话
 *
 * 关闭流程：
 *   WebSocket模式：
 *     1. 如果task_id存在，发送StopTranscription命令通知服务端结束识别
 *     2. 断开WebSocket连接（发送CLOSE帧 + 关闭SSL）
 *   REST模式：
 *     1. 销毁音频缓冲区
 *
 * 注意：会话私有上下文（ctx）的内存由FreeSWITCH内存池管理，随会话销毁自动释放，
 * 无需手动free。但动态创建的缓冲区（switch_buffer_t）需要显式销毁。
 */
static switch_status_t aliyun_asr_close(asr_session_t *session)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!ctx) return SWITCH_STATUS_FALSE;
	/* 【线程安全修复】加锁保护WS断开序列，防止与aliyun_asr_feed()的
	 * "检查连接→发送音频"序列并发执行。不加锁时可能出现：
	 *   媒体线程feed: 检查connected=TRUE → 准备SSL_write
	 *   关闭线程close: disconnect → SSL_free → close(sockfd)   ← 先执行
	 *   媒体线程feed: SSL_write(已释放的SSL) → use-after-free崩溃
	 * 加锁后，feed和close互斥，不会出现"feed检查通过后SSL被释放"的窗口。 */
	switch_mutex_lock(session->mutex);

	if (session->mode == ASR_MODE_WEBSOCKET && asr_ws_is_connected(session)) {
		/* WebSocket模式：发送StopTranscription命令后断开连接 */
		if (ctx->task_id) {
			char stop_msg_id[33];
			char *stop_cmd;
			aliyun_generate_id(stop_msg_id, sizeof(stop_msg_id));
			/* 构造StopTranscription命令JSON */
			stop_cmd = switch_mprintf(
				"{\"header\":{\"appkey\":\"%s\",\"namespace\":\"SpeechTranscriber\",\"name\":\"StopTranscription\",\"task_id\":\"%s\",\"message_id\":\"%s\"},\"payload\":{}}",
				ctx->app_key, ctx->task_id, stop_msg_id);
			asr_ws_send_text(session, stop_cmd);
			switch_safe_free(stop_cmd);
		}
		asr_ws_disconnect(session);    /* 断开WebSocket连接 */
		ctx->ws_connected = SWITCH_FALSE;
	}

	switch_mutex_unlock(session->mutex);

	if (ctx->rest_audio_buffer) {
		/* REST模式：销毁音频缓冲区 */
		switch_buffer_destroy(&ctx->rest_audio_buffer);
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Aliyun ASR closed\n");

	return SWITCH_STATUS_SUCCESS;
}

/*
 * aliyun_asr_feed -- 向ASR引擎送入音频数据
 *
 * 根据会话模式的不同，音频数据处理方式完全不同：
 *
 * ========== WebSocket模式 ==========
 * 音频数据实时发送给阿里云NLS服务：
 *   - 前提条件：WS已连接 且 已收到TranscriptionStarted确认
 *   - 调用asr_ws_send_binary()将PCM音频作为WebSocket Binary帧发送
 *   - 发送失败时记录错误日志（可能是WS连接断开）
 *   - 调试日志：前5帧和每50帧记录一次，避免日志刷屏
 *   - 如果TranscriptionStarted尚未到达，音频被丢弃（避免服务端报错）
 *
 * ========== REST模式 ==========
 * 音频数据积攒到缓冲区中，等待poll_results()中自动触发提交：
 *   - 音频写入rest_audio_buffer，由switch_buffer动态管理内存
 *   - 超过rest_max_audio_len(1MB)的音频被丢弃（防止内存无限增长）
 *   - 实际提交在aliyun_asr_poll_results()中判断idle_count阈值后触发
 *
 * @param session ASR会话
 * @param data    PCM音频数据指针
 * @param len     音频数据长度（字节）
 * @return        SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 失败
 */
static switch_status_t aliyun_asr_feed(asr_session_t *session, void *data, unsigned int len)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;
	switch_size_t total;
	/* 【线程安全修复】原实现使用 static uint32_t 全局变量 ctx->ws_feed_count/ctx->ws_total_bytes，
	 * 所有ASR会话共享这两个计数器，多个worker线程并发递增时无同步保护，属于数据竞争(UB)。
	 * 现改为从ctx读取会话级计数器，每个ctx只被自己的worker线程访问，天然线程安全。
	 * 新字段在provider_aliyun.h的aliyun_asr_ctx_t中定义，由memset初始化为0。 */
	switch_status_t send_status;

	if (!ctx) return SWITCH_STATUS_FALSE;

	if (session->mode == ASR_MODE_WEBSOCKET) {
		/* ========== WebSocket模式：实时发送音频 ========== */

		/* 【线程安全修复】加锁保护"检查连接→发送音频"的原子性。
		 * 不加锁时，与aliyun_asr_close()存在竞态：
		 *   本线程: asr_ws_is_connected()=TRUE → 准备SSL_write
		 *   close线程: asr_ws_disconnect() → SSL_free()           ← 先执行
		 *   本线程: SSL_write(已释放的SSL) → use-after-free崩溃
		 * 加锁后，feed和close互斥：
		 *   - close持锁时，feed被阻塞，不会在disconnect期间发送
		 *   - feed持锁时，close被阻塞，不会在send期间释放SSL
		 * 持锁时间短（仅一次SSL_write，音频帧通常<640字节），不影响性能。 */
		switch_mutex_lock(session->mutex);

		/* 检查WS连接状态 */
		if (!asr_ws_is_connected(session)) {
			switch_mutex_unlock(session->mutex);
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
				"Aliyun feed: WS not connected, dropping %u bytes\n", len);
			return SWITCH_STATUS_FALSE;
		}

		/* 检查是否已收到TranscriptionStarted确认
		 * 在服务端确认前发送音频会导致协议错误 */
		if (!ctx->transcription_started) {
			switch_mutex_unlock(session->mutex);
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
				"Audio dropped: TranscriptionStarted not received yet (%u bytes)\n", len);
			return SWITCH_STATUS_SUCCESS;  /* 返回SUCCESS避免框架认为出错 */
		}

		ctx->ws_feed_count++;
		ctx->ws_total_bytes += len;

		/* 通过WebSocket Binary帧发送音频数据 */
		send_status = asr_ws_send_binary(session, data, len);

		/* 调试日志：前5帧详细记录，之后每50帧记录一次（避免日志刷屏） */
		if (ctx->ws_feed_count <= 5 || ctx->ws_feed_count % 50 == 0) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
				"Aliyun WS feed: #%u, bytes=%u, total=%u, send_status=%d, sample_rate=%d\n",
				ctx->ws_feed_count, len, ctx->ws_total_bytes, send_status, ctx->sample_rate);
		}

		if (send_status != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
				"Aliyun WS send_binary FAILED: feed #%u, bytes=%u, status=%d\n",
				ctx->ws_feed_count, len, send_status);
		}

		switch_mutex_unlock(session->mutex);
		return send_status;
	} else {
		/* ========== REST模式：积攒音频到缓冲区 ========== */
		if (ctx->rest_audio_buffer) {
			total = switch_buffer_inuse(ctx->rest_audio_buffer) + len;
			/* 检查是否超过最大音频长度限制（防止内存无限增长） */
			if (total <= ctx->rest_max_audio_len) {
				switch_buffer_write(ctx->rest_audio_buffer, data, len);
			}
			/* 超过限制的音频被静默丢弃（不记录日志避免刷屏） */
		}
		return SWITCH_STATUS_SUCCESS;
	}
}

/*
 * aliyun_asr_resume -- 恢复ASR识别（暂停后）
 * 阿里云NLS协议没有专门的暂停/恢复命令，此处为空实现
 */
static switch_status_t aliyun_asr_resume(asr_session_t *session)
{
	return SWITCH_STATUS_SUCCESS;
}

/*
 * aliyun_asr_pause -- 暂停ASR识别
 * 阿里云NLS协议没有专门的暂停/恢复命令，此处为空实现
 * 如果需要实现暂停功能，可以考虑关闭WS连接+重新连接的方式
 */
static switch_status_t aliyun_asr_pause(asr_session_t *session)
{
	return SWITCH_STATUS_SUCCESS;
}

/* ---- 结果轮询（在worker线程中定期调用） ---- */

/*
 * aliyun_asr_poll_results -- 轮询ASR结果
 *
 * 此函数由mod_asr框架的worker线程定期调用（约每200ms一次），
 * 负责接收WebSocket消息或触发REST识别提交。
 *
 * ========== WebSocket模式 ==========
 * 从WebSocket连接中读取所有待处理的文本帧，逐条交给
 * aliyun_process_ws_result()处理。处理流程：
 *   1. 检查asr_ws_has_data()是否有数据可读
 *   2. 有数据则asr_ws_recv_text()接收一条文本帧
 *   3. 调用aliyun_process_ws_result()解析JSON、分发事件处理
 *   4. 循环直到没有更多数据（一次性处理完所有待处理消息）
 *   5. 每100次轮询记录一次状态日志（避免日志刷屏）
 *
 * ========== REST模式 ==========
 * 检测音频积攒和静音条件，自动触发REST识别请求：
 *   1. 检查是否已提交（rest_submitted标志防止重复提交）
 *   2. 检查空闲计数是否达到阈值（idle_count >= ASR_REST_IDLE_THRESHOLD = 3）
 *      idle_count由worker线程在检测到无新音频时递增，3 × 200ms = 600ms无新音频
 *   3. 检查音频缓冲区是否有数据
 *   4. 条件满足时设置rest_submitted标志，调用aliyun_rest_recognize()提交识别
 *
 * @param session ASR会话
 * @return        SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 失败
 */
static switch_status_t aliyun_asr_poll_results(asr_session_t *session)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;
	char *msg;
	/* 【线程安全修复】原static变量改为ctx->poll_count会话级变量，避免多线程竞争 */
	uint32_t poll_count = ctx->poll_count;

	if (!ctx) return SWITCH_STATUS_FALSE;

	poll_count++;

	if (session->mode == ASR_MODE_WEBSOCKET) {
		/* ========== WebSocket模式：接收并处理所有待处理消息 ========== */
		while (asr_ws_has_data(session)) {
			msg = asr_ws_recv_text(session, session->pool);
			if (msg) {
				aliyun_process_ws_result(session, msg);  /* 处理WS消息（可能触发SentenceEnd→设置结果） */
			} else {
				break;  /* 接收失败，退出循环 */
			}
		}
		/* 每100次轮询记录一次状态（约20秒），用于排查连接状态问题 */
		if (poll_count % 100 == 0) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
				"Aliyun poll_results: #%u, ws_connected=%d, transcription_started=%d, has_data=%d\n",
				poll_count, ctx->ws_connected, ctx->transcription_started, asr_ws_has_data(session));
		}
	} else {
		/* ========== REST模式：检测静音并自动提交识别 ========== */
		/* 条件：未提交 + 空闲计数达到阈值 + 有音频数据 */
		if (!ctx->rest_submitted && session->idle_count >= ASR_REST_IDLE_THRESHOLD) {
			if (switch_buffer_inuse(ctx->rest_audio_buffer) > 0) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
								  "Aliyun REST auto-submit: idle_count=%d, audio=%zu\n",
								  session->idle_count, switch_buffer_inuse(ctx->rest_audio_buffer));
				ctx->rest_submitted = SWITCH_TRUE;  /* 设置已提交标志，防止重复提交 */
				aliyun_rest_recognize(session);       /* 提交REST识别请求 */
			}
		}
	}

	/* 【线程安全修复】将局部变量值回写到ctx，替代原static变量的持久化效果 */
	ctx->poll_count = poll_count;

	return SWITCH_STATUS_SUCCESS;
}

/*
 * aliyun_rest_recognize -- 执行REST一句话识别
 *
 * 构造HTTP POST请求，将缓冲区中的音频数据发送给阿里云NLS一句话识别API。
 *
 * ========== 请求构造 ==========
 *   URL：ALIYUN_ASR_REST_URL + 查询参数（appkey, format, sample_rate, enable_punctuation）
 *   Headers：
 *     - Content-Type: application/octet-stream（二进制音频数据）
 *     - X-NLS-Token: {token}（NLS服务认证令牌）
 *   Body：PCM音频原始数据（从rest_audio_buffer读取）
 *
 * ========== 响应处理 ==========
 *   成功响应示例：
 *     {"result":"你好世界","confidence":0.98,"task_id":"xxx"}
 *   从中提取result（识别文本）和confidence（置信度0-1，转换为0-100整数）
 *
 * ========== 与WebSocket模式的区别 ==========
 *   - REST是一次性提交整段音频，WS是流式推送
 *   - REST不支持中间结果，WS支持TranscriptionResultChanged
 *   - REST请求-响应模式简单可靠，WS需要维护长连接状态
 *   - REST适合<60秒短音频，WS适合长时间通话
 *
 * @param session ASR会话
 * @return        SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 失败
 */
static switch_status_t aliyun_rest_recognize(asr_session_t *session)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;
	char url[512];            /* REST请求URL（含查询参数） */
	char *response = NULL;    /* HTTP响应体（需要手动free） */
	switch_size_t resp_len = 0;
	switch_size_t audio_len;  /* 音频数据长度 */
	uint8_t *audio_data;      /* 音频数据临时缓冲区（malloc分配，需要手动free） */
	switch_status_t status;
	const char *headers[3];   /* HTTP请求头数组 */
	cJSON *json, *result_obj, *conf_obj;
	int confidence;

	if (!ctx || !ctx->rest_audio_buffer) return SWITCH_STATUS_FALSE;

	/* 从缓冲区读取音频数据 */
	audio_len = switch_buffer_inuse(ctx->rest_audio_buffer);
	if (audio_len == 0) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "No audio data for REST recognition\n");
		return SWITCH_STATUS_FALSE;
	}

	/* 分配临时缓冲区保存音频数据（switch_buffer_read会消耗缓冲区数据） */
	audio_data = malloc(audio_len);
	if (!audio_data) return SWITCH_STATUS_MEMERR;

	/* 从switch_buffer中读取音频数据（读取后缓冲区数据被消耗） */
	switch_buffer_read(ctx->rest_audio_buffer, audio_data, audio_len);

	/* 构造REST请求URL（查询参数格式）
	 * 参数说明：
	 *   appkey: 应用标识
	 *   format: 音频格式（如pcm）
	 *   sample_rate: 采样率
	 *   enable_punctuation_prediction: 是否添加标点 */
	snprintf(url, sizeof(url),
			 "%s?appkey=%s&format=%s&sample_rate=%d&enable_punctuation_prediction=%s",
			 ALIYUN_ASR_REST_URL,
			 ctx->app_key,
			 ctx->format,
			 ctx->sample_rate,
			 ctx->enable_punctuation ? "true" : "false");

	/* 构造HTTP请求头 */
	headers[0] = "Content-Type: application/octet-stream";  /* 音频数据为二进制格式 */
	headers[1] = switch_core_sprintf(session->pool, "X-NLS-Token: %s", ctx->token);  /* NLS认证令牌 */
	headers[2] = NULL;  /* 头数组终止标记 */

	/* 发送HTTP POST请求 */
	status = asr_rest_request(session, url, "POST", headers, 2,
							  audio_data, audio_len, &response, &resp_len);
	free(audio_data);  /* 释放音频临时缓冲区（请求已完成） */

	if (status != SWITCH_STATUS_SUCCESS || !response) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Aliyun REST recognition failed\n");
		return SWITCH_STATUS_FALSE;
	}

	/* 解析JSON响应 */
	json = cJSON_Parse(response);
	if (json) {
		/* 提取识别结果文本 */
		result_obj = cJSON_GetObjectItem(json, "result");
		if (result_obj && cJSON_IsString(result_obj)) {
			confidence = 100;  /* 默认置信度100% */
			conf_obj = cJSON_GetObjectItem(json, "confidence");
			if (conf_obj && cJSON_IsNumber(conf_obj)) {
				/* REST模式confidence直接是0-100整数（与WS模式的0-1浮点数不同） */
				confidence = (int) (conf_obj->valuedouble);
			}
			/* 将识别结果设置到会话中 */
			asr_session_set_result(session, result_obj->valuestring, confidence);
		}
		cJSON_Delete(json);
	}

	switch_safe_free(response);  /* 释放HTTP响应缓冲区 */

	return SWITCH_STATUS_SUCCESS;
}

/*
 * aliyun_asr_check_results -- 非阻塞检查是否有识别结果
 *
 * 由FreeSWITCH ASR框架调用（通常在主线程），检查会话状态是否为RESULT_READY。
 * 使用互斥锁保护状态读取，因为worker线程可能同时更新状态。
 *
 * @return SWITCH_STATUS_SUCCESS 有结果，SWITCH_STATUS_FALSE 无结果
 */
static switch_status_t aliyun_asr_check_results(asr_session_t *session)
{
	switch_status_t status;

	if (!session) return SWITCH_STATUS_FALSE;

	/* 加锁读取会话状态（worker线程可能在同时写入） */
	switch_mutex_lock(session->mutex);
	status = (session->state == ASR_SESSION_STATE_RESULT_READY) ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
	switch_mutex_unlock(session->mutex);

	if (status == SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
			"Aliyun check_results: RESULT READY for session %s\n", session->id);
	}

	return status;
}

/*
 * aliyun_asr_get_results -- 获取识别结果XML
 *
 * 委托给asr_session_get_result()，该函数会：
 *   1. 读取result_text和result_confidence
 *   2. 构造MRCP格式的XML结果
 *   3. 清除结果，将状态从RESULT_READY变回LISTENING（支持连续识别）
 */
static switch_status_t aliyun_asr_get_results(asr_session_t *session, char **result_xml)
{
	return asr_session_get_result(session, result_xml);
}

/*
 * aliyun_asr_start_input_timers -- 启动输入超时定时器
 * 阿里云NLS协议本身不提供输入超时功能，此功能由mod_asr框架层面实现，
 * 因此此处返回SUCCESS但不执行任何操作。
 */
static switch_status_t aliyun_asr_start_input_timers(asr_session_t *session)
{
	return SWITCH_STATUS_SUCCESS;
}

/*
 * aliyun_asr_text_param -- 设置文本类型的ASR参数
 *
 * 由FreeSWITCH ASR框架调用，允许在会话运行时动态覆盖默认配置。
 * 支持的参数：
 *   - app-key: 覆盖默认AppKey（用于多应用场景）
 *   - format: 覆盖音频格式（如从pcm改为wav）
 *   - sample-rate: 覆盖采样率
 *   - enable-intermediate-result: 覆盖是否返回中间结果
 *   - enable-punctuation: 覆盖是否添加标点
 *   - language: 设置识别语言（如zh-CN、en-US）
 *
 * 注意：参数值通过switch_core_strdup分配到会话内存池，覆盖原有指针，
 * 不会释放旧值（由内存池统一管理）。
 */
static void aliyun_asr_text_param(asr_session_t *session, const char *param, const char *val)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!param || zstr(val)) return;

	if (!strcasecmp(param, "app-key") && ctx) {
		ctx->app_key = switch_core_strdup(session->pool, val);
	} else if (!strcasecmp(param, "format") && ctx) {
		ctx->format = switch_core_strdup(session->pool, val);
	} else if (!strcasecmp(param, "sample-rate") && ctx) {
		ctx->sample_rate = atoi(val);
	} else if (!strcasecmp(param, "enable-intermediate-result") && ctx) {
		ctx->enable_intermediate_result = switch_true(val);
	} else if (!strcasecmp(param, "enable-punctuation") && ctx) {
		ctx->enable_punctuation = switch_true(val);
	} else if (!strcasecmp(param, "language") && session) {
		session->language = switch_core_strdup(session->pool, val);
	}
}

/*
 * aliyun_asr_numeric_param -- 设置数值类型的ASR参数
 *
 * 支持的参数：
 *   - sample-rate: 覆盖采样率（如从16000改为8000）
 *   - rest-timeout: 覆盖REST请求超时（毫秒）
 */
static void aliyun_asr_numeric_param(asr_session_t *session, const char *param, int val)
{
	aliyun_asr_ctx_t *ctx = (aliyun_asr_ctx_t *) session->provider_private;

	if (!param) return;

	if (!strcasecmp(param, "sample-rate") && ctx) {
		ctx->sample_rate = val;
	} else if (!strcasecmp(param, "rest-timeout") && ctx) {
		ctx->rest_timeout = val;
	}
}

/*
 * aliyun_asr_float_param -- 设置浮点类型的ASR参数
 * 当前阿里云Provider不需要浮点参数，空实现
 */
static void aliyun_asr_float_param(asr_session_t *session, const char *param, double val)
{
}

/*
 * aliyun_provider -- 阿里云ASR Provider接口定义
 *
 * 这是asr_provider_interface_t接口的阿里云实现，通过函数指针表实现"策略模式"。
 * 在asr_provider_aliyun_load()中注册到全局providers哈希表后，
 * 框架通过provider名称"aliyun"查找此结构体，调用对应的函数指针。
 *
 * 接口函数分为四组：
 *
 * 1. 生命周期管理：
 *    open  - 打开ASR会话（获取Token+建立连接+启动识别）
 *    close - 关闭ASR会话（发送停止命令+断开连接+释放资源）
 *
 * 2. 数据流控制：
 *    feed   - 送入PCM音频数据（WS模式实时发送/REST模式缓冲积攒）
 *    resume - 恢复识别（空实现，NLS协议无此功能）
 *    pause  - 暂停识别（空实现，NLS协议无此功能）
 *
 * 3. 结果获取：
 *    check_results      - 非阻塞检查是否有结果（检查session state）
 *    get_results        - 获取识别结果XML（委托给asr_session_get_result）
 *    start_input_timers - 启动输入超时定时器（空实现，框架层面实现）
 *    poll_results       - worker线程轮询（WS接收消息/REST触发提交）
 *
 * 4. 参数设置：
 *    text_param    - 设置文本参数（app-key, format等）
 *    numeric_param - 设置数值参数（sample-rate, rest-timeout）
 *    float_param   - 设置浮点参数（空实现）
 *
 * next指针：用于链表结构，当前框架使用哈希表注册，此字段保留未使用。
 */
static asr_provider_interface_t aliyun_provider = {
	.name = "aliyun",                  /* Provider唯一标识名，dialplan中通过asr:aliyun引用 */
	.open = aliyun_asr_open,           /* 打开ASR会话 */
	.close = aliyun_asr_close,         /* 关闭ASR会话 */
	.feed = aliyun_asr_feed,           /* 送入音频数据 */
	.resume = aliyun_asr_resume,       /* 恢复识别（空实现） */
	.pause = aliyun_asr_pause,         /* 暂停识别（空实现） */
	.check_results = aliyun_asr_check_results,   /* 检查是否有结果 */
	.get_results = aliyun_asr_get_results,       /* 获取识别结果XML */
	.start_input_timers = aliyun_asr_start_input_timers,  /* 启动输入定时器（空实现） */
	.text_param = aliyun_asr_text_param,         /* 设置文本参数 */
	.numeric_param = aliyun_asr_numeric_param,   /* 设置数值参数 */
	.float_param = aliyun_asr_float_param,       /* 设置浮点参数（空实现） */
	.poll_results = aliyun_asr_poll_results,     /* worker线程轮询回调 */
	.next = NULL                       /* 链表指针（保留，当前未使用） */
};

/*
 * aliyun_load_config -- 从XML配置文件加载阿里云Provider配置
 *
 * 从FreeSWITCH的asr.conf配置文件中读取aliyun provider的参数。
 * 配置文件格式示例：
 *   <configuration name="asr.conf">
 *     <providers>
 *       <provider name="aliyun">
 *         <param name="access-key-id" value="LTAI5t..."/>
 *         <param name="access-key-secret" value="abc123..."/>
 *         <param name="app-key" value="NLSxxx..."/>
 *         <param name="region" value="cn-shanghai"/>
 *         <param name="format" value="pcm"/>
 *         <param name="sample-rate" value="16000"/>
 *         <param name="enable-intermediate-result" value="true"/>
 *         <param name="enable-punctuation" value="true"/>
 *         <param name="rest-timeout" value="10000"/>
 *       </provider>
 *     </providers>
 *   </configuration>
 *
 * 解析流程：
 *   1. 打开asr.conf XML配置文件
 *   2. 查找<providers>节点
 *   3. 遍历<provider>子节点，找到name="aliyun"的provider
 *   4. 遍历该provider的<param>子节点，按name匹配填充aliyun_globals
 *   5. 释放XML资源
 *   6. 为未配置的参数设置默认值
 *
 * 注意：所有字符串值通过switch_core_strdup(pool, ...)分配到模块级内存池，
 * 随模块卸载自动释放，无需手动free。
 *
 * @param pool 模块级内存池
 * @return     SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 配置文件打开失败
 */
static switch_status_t aliyun_load_config(switch_memory_pool_t *pool)
{
	switch_xml_t cfg, xml, settings, xprovider, param;

	/* 打开asr.conf配置文件 */
	if (!(xml = switch_xml_open_cfg("asr.conf", &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Failed to open asr.conf\n");
		return SWITCH_STATUS_FALSE;
	}

	/* 查找<providers>节点 */
	if ((settings = switch_xml_child(cfg, "providers"))) {
		/* 遍历所有<provider>子节点 */
		for (xprovider = switch_xml_child(settings, "provider"); xprovider; xprovider = switch_xml_next(xprovider)) {
			const char *name = switch_xml_attr(xprovider, "name");
			if (!name || strcasecmp(name, "aliyun")) continue;  /* 跳过非aliyun的provider */

			/* 遍历aliyun provider的<param>子节点 */
			for (param = switch_xml_child(xprovider, "param"); param; param = switch_xml_next(param)) {
				const char *pname = switch_xml_attr(param, "name");   /* 参数名 */
				const char *pval = switch_xml_attr(param, "value");   /* 参数值 */

				if (zstr(pname) || zstr(pval)) continue;  /* 跳过空参数 */

				/* 按参数名匹配，填充aliyun_globals */
				if (!strcasecmp(pname, "access-key-id")) {
					aliyun_globals.access_key_id = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "access-key-secret")) {
					aliyun_globals.access_key_secret = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "app-key")) {
					aliyun_globals.app_key = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "region")) {
					aliyun_globals.region = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "format")) {
					aliyun_globals.format = switch_core_strdup(pool, pval);
				} else if (!strcasecmp(pname, "sample-rate")) {
					aliyun_globals.sample_rate = atoi(pval);
				} else if (!strcasecmp(pname, "enable-intermediate-result")) {
					aliyun_globals.enable_intermediate_result = switch_true(pval);
				} else if (!strcasecmp(pname, "enable-punctuation")) {
					aliyun_globals.enable_punctuation = switch_true(pval);
				} else if (!strcasecmp(pname, "rest-timeout")) {
					aliyun_globals.rest_timeout = atoi(pval);
				}
			}
			break;  /* 找到aliyun provider后停止遍历 */
		}
	}

	switch_xml_free(xml);  /* 释放XML解析资源 */

	/* ---- 设置默认值（未配置的参数使用合理默认值） ---- */
	if (zstr(aliyun_globals.region)) aliyun_globals.region = "cn-shanghai";  /* 默认上海区域 */
	if (zstr(aliyun_globals.format)) aliyun_globals.format = "pcm";          /* 默认PCM格式 */
	if (aliyun_globals.sample_rate == 0) aliyun_globals.sample_rate = 16000;  /* 默认16kHz */
	if (aliyun_globals.rest_timeout == 0) aliyun_globals.rest_timeout = 10000; /* 默认10秒超时 */

	return SWITCH_STATUS_SUCCESS;
}

/*
 * asr_provider_aliyun_load -- 加载阿里云ASR Provider
 *
 * 这是阿里云Provider的入口函数，在mod_asr模块加载时调用。
 * 执行流程：
 *   1. 保存模块级内存池到aliyun_globals（供后续配置字符串分配使用）
 *   2. 调用aliyun_load_config()从XML配置文件读取参数
 *   3. 校验必填字段（access-key-id、access-key-secret、app-key）
 *   4. 调用asr_provider_register()将aliyun_provider注册到全局哈希表
 *
 * 注册完成后，dialplan中可以通过"asr:aliyun"引用此Provider。
 * 如果必填字段缺失，返回SWITCH_STATUS_FALSE，模块加载不会失败（仅该Provider不可用）。
 *
 * @param pool 模块级内存池
 * @return     SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 必填配置缺失
 */
switch_status_t asr_provider_aliyun_load(switch_memory_pool_t *pool)
{
	aliyun_globals.pool = pool;  /* 保存模块级内存池 */

	/* 从XML配置文件加载参数 */
	if (aliyun_load_config(pool) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Aliyun provider config not found, using defaults\n");
	}

	/* 校验必填字段：AccessKey ID、AccessKey Secret、AppKey
	 * 这三个字段缺失时Provider无法工作（无法获取Token、无法标识应用） */
	if (zstr(aliyun_globals.access_key_id) || zstr(aliyun_globals.access_key_secret) || zstr(aliyun_globals.app_key)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
						  "Aliyun ASR provider requires access-key-id, access-key-secret, and app-key in config\n");
		return SWITCH_STATUS_FALSE;
	}

	/* 注册Provider到全局哈希表
	 * 注册后框架可通过asr_provider_find("aliyun")找到此Provider */
	return asr_provider_register(&aliyun_provider);
}
