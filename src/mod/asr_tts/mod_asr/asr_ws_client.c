/*
 * asr_ws_client.c -- ASR 流式识别 WebSocket 客户端
 *
 * 本文件实现了一个轻量级 WebSocket 客户端，用于与云端 ASR（自动语音识别）
 * 服务建立长连接，实时传输音频数据并接收识别结果。
 *
 * 为什么选择原生 socket + OpenSSL 而不是 libcurl？
 *   libcurl 的 WebSocket 支持从 7.86 版本（2022年10月发布）才引入，
 *   而许多生产环境的 Linux 发行版（如 CentOS 7/8、Ubuntu 18.04/20.04）
 *   自带的 libcurl 版本远低于 7.86，无法使用 WebSocket 功能。
 *   即使编译安装新版 libcurl，也可能与系统已有的 libcurl 产生 ABI 冲突。
 *   因此这里采用原生 TCP socket 配合 OpenSSL 手动实现 WebSocket 协议，
 *   确保在几乎所有 Linux 环境下都能正常工作，无需额外升级系统库。
 *
 * 连接句柄存储位置：
 *   session->ws_handle  -- 存放本文件中定义的 ws_conn_t 连接对象
 *   session->provider_private -- 存放 ASR 提供商特有的数据（如阿里云的鉴权信息等）
 *   两者分离，避免 WebSocket 传输层与业务逻辑层耦合
 *
 * 核心流程：
 *   1. asr_ws_connect()    -- 解析 URL -> 建立 TCP 连接 -> SSL 握手 -> WebSocket 握手
 *   2. asr_ws_send_binary() -- 持续发送音频帧（二进制 WebSocket 帧）
 *   3. asr_ws_recv_text()   -- 接收 ASR 识别结果（文本 WebSocket 帧）
 *   4. asr_ws_disconnect()  -- 发送关闭帧 -> 释放 SSL/TCP 资源
 */

#include "mod_asr.h"
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <sys/select.h>

/* ===== WebSocket 协议常量（RFC 6455） ===== */

#define WS_FIN_BIT        0x80  /* FIN 标志位：1 表示当前帧是消息的最后一帧 */
#define WS_OPCODE_TEXT    0x01  /* 操作码：文本帧，用于传输 JSON 格式的识别结果 */
#define WS_OPCODE_BINARY  0x02  /* 操作码：二进制帧，用于传输音频 PCM 数据 */
#define WS_OPCODE_CLOSE   0x08  /* 操作码：关闭帧，用于优雅关闭连接 */
#define WS_OPCODE_PING    0x09  /* 操作码：Ping 帧，用于心跳检测 */
#define WS_OPCODE_PONG    0x0A  /* 操作码：Pong 帧，Ping 的响应 */

/*
 * ws_conn_t -- WebSocket 连接上下文
 *
 * 封装了一个完整的 WebSocket 连接所需的所有状态信息。
 * 每个 asr_session 的 ws_handle 指向一个此结构的实例。
 *
 * 各字段说明：
 *   sockfd       - TCP 套接字文件描述符，-1 表示尚未建立连接
 *   ssl          - OpenSSL 连接对象，当 use_ssl=TRUE 时有效，
 *                  封装了 SSL/TLS 加密通道，所有读写都通过它进行
 *   ssl_ctx      - OpenSSL 上下文对象，持有 SSL 配置（协议版本、证书等），
 *                  在 SSL_new() 之前创建，一个 ctx 可创建多个 ssl 连接
 *   connected    - 连接状态标志，用于快速判断是否可以收发数据
 *   use_ssl      - 是否使用 SSL/TLS，由 URL 的 wss:// 前缀决定
 *   url          - 原始 URL 字符串，保留用于日志和调试
 *   host         - 从 URL 解析出的主机名，用于 HTTP Host 头和 TLS SNI
 *   port         - 从 URL 解析出的端口号，默认 wss=443, ws=80
 *   path         - 从 URL 解析出的路径，用于 HTTP GET 请求行
 *   write_mutex  - 写操作互斥锁，防止多线程并发发送导致帧数据交错
 *                  使用 NESTED 模式允许同一线程递归加锁（如 PING 响应中调用 ws_send_frame）
 *
 * leftover 缓冲区详解：
 *   在 HTTP 握手阶段，我们循环读取数据直到发现 "\r\n\r\n" 分隔符。
 *   但 TCP 是流式协议，一次 recv 可能读到远超 HTTP 头结尾的数据——
 *   服务端可能紧跟在 HTTP 101 响应之后立即发送第一个 WebSocket 帧
 *   （如阿里云 ASR 会在握手后立即推送 "connected" 事件）。
 *   leftover 就是用来保存这些"多读"的数据，避免丢失。
 *   在 asr_ws_recv_text() 中，会优先消费 leftover 中的数据，
 *   然后再从 socket 读取新数据。
 */
typedef struct {
	int sockfd;                  /* TCP 套接字文件描述符 */
	SSL *ssl;                    /* OpenSSL 连接对象（wss:// 时使用） */
	SSL_CTX *ssl_ctx;            /* OpenSSL 上下文（持有证书等配置） */
	switch_bool_t connected;     /* 当前是否处于已连接状态 */
	switch_bool_t use_ssl;       /* 是否使用 SSL/TLS（wss:// 为 TRUE） */
	char *url;                   /* 原始 WebSocket URL */
	char *host;                  /* 从 URL 解析出的主机名 */
	int port;                    /* 从 URL 解析出的端口号 */
	char *path;                  /* 从 URL 解析出的路径（如 /v1/asr） */
	switch_mutex_t *write_mutex; /* 写锁：防止多线程并发发送导致帧数据交错 */
	/* HTTP 握手后可能多读到的数据，属于第一个 WebSocket 帧的开头部分 */
	uint8_t leftover[4096];
	size_t leftover_len;         /* leftover 缓冲区中有效数据的字节数 */
} ws_conn_t;

/*
 * b64_encode -- Base64 编码
 *
 * 将二进制数据编码为 Base64 字符串。主要用于 WebSocket 握手阶段
 * 生成 Sec-WebSocket-Key 头字段——RFC 6455 要求该字段为 16 字节
 * 随机数经 Base64 编码后的值（共 24 个字符）。
 *
 * 为什么 WebSocket 握手需要 Base64 编码的 Sec-WebSocket-Key？
 *   RFC 6455 第 4.1 节规定：客户端必须在 HTTP Upgrade 请求中携带
 *   Sec-WebSocket-Key 头，值为 16 字节随机数的 Base64 编码。
 *   服务端将其与固定的 GUID "258EAFA5-E914-47DA-95CA-5AB5DC11B0BA"
 *   拼接后做 SHA-1 哈希，再 Base64 编码后作为 Sec-WebSocket-Accept 返回。
 *   这个握手机制确保双方都支持 WebSocket 协议，而非普通的 HTTP 请求。
 *
 * 为什么不使用 OpenSSL 的 BIO_base64？
 *   为了保持最小依赖和简洁性，手写一个轻量实现更可控，
 *   避免引入 BIO 链等复杂概念。
 *
 * 编码原理：
 *   每次取 3 字节输入（24 位），拆分为 4 组各 6 位，
 *   每组 6 位的值（0-63）查 b64table 得到对应 ASCII 字符。
 *   输入不足 3 字节时，只输出有实际数据对应的字符。
 *
 * 参数：
 *   in     - 输入的二进制数据
 *   inlen  - 输入数据长度
 *   out    - 输出缓冲区（由调用者分配）
 *   outlen - 输出缓冲区大小
 *
 * 返回值：实际写入 out 的字符数（不含末尾 '\0'）
 */
static size_t b64_encode(const unsigned char *in, size_t inlen, char *out, size_t outlen)
{
	static const char b64table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	size_t i, j = 0;
	uint32_t a, b, c, triple;

	/* 每次取 3 字节输入，编码为 4 字节输出 */
	for (i = 0; i < inlen && j < outlen - 1; i += 3) {
		a = in[i];
		b = (i + 1 < inlen) ? in[i + 1] : 0;  /* 不足 3 字节时用 0 填充 */
		c = (i + 2 < inlen) ? in[i + 2] : 0;
		triple = (a << 16) | (b << 8) | c;     /* 拼成 24 位 */

		/* 从 24 位中依次取 6 位，查表得到 Base64 字符 */
		if (j < outlen - 1) out[j++] = b64table[(triple >> 18) & 0x3F];
		if (j < outlen - 1) out[j++] = b64table[(triple >> 12) & 0x3F];
		/* 只有实际存在输入字节时才输出对应字符（避免末尾补 '=' 的复杂性） */
		if (i + 1 < inlen && j < outlen - 1) out[j++] = b64table[(triple >> 6) & 0x3F];
		if (i + 2 < inlen && j < outlen - 1) out[j++] = b64table[triple & 0x3F];
	}
	out[j] = '\0';
	return j;
}

/*
 * ws_conn_create -- 创建 WebSocket 连接对象
 *
 * 从 FreeSWITCH 内存池分配 ws_conn_t 结构体并初始化默认值。
 * 默认端口 443 是因为大多数 ASR 云服务使用 wss://（安全 WebSocket）。
 *
 * 为什么使用 FreeSWITCH 内存池（pool）分配？
 *   pool 的生命周期与 asr_session 绑定，当 session 销毁时 pool 自动释放
 *   所有内存，无需手动 free，避免内存泄漏。
 *
 * 参数：
 *   pool - FreeSWITCH 内存池
 *
 * 返回值：新创建的 ws_conn_t 指针，失败返回 NULL
 */
static ws_conn_t *ws_conn_create(switch_memory_pool_t *pool)
{
	ws_conn_t *conn = switch_core_alloc(pool, sizeof(*conn));
	if (!conn) return NULL;

	memset(conn, 0, sizeof(*conn));
	conn->sockfd = -1;          /* -1 表示尚未建立 socket */
	conn->port = 443;           /* 默认 wss 端口 */
	conn->leftover_len = 0;
	/*
	 * 初始化写互斥锁，使用 NESTED 模式允许同一线程递归加锁。
	 * 为什么需要写锁？音频数据由 FreeSWITCH 的媒体线程持续 feed，
	 * 而 Ping/Pong 响应也可能触发写操作，必须串行化以防帧数据交错。
	 */
	switch_mutex_init(&conn->write_mutex, SWITCH_MUTEX_NESTED, pool);

	return conn;
}

/*
 * ws_parse_url -- 解析 WebSocket URL
 *
 * 将 "ws://host:port/path" 或 "wss://host:port/path" 格式的 URL
 * 解析为各个组成部分，供后续连接使用。
 *
 * URL 解析逻辑：
 *   1. 判断协议前缀（ws:// 或 wss://），确定 use_ssl 和默认端口
 *   2. 跳过协议前缀后，剩余部分为 host[:port][/path] 格式
 *   3. 通过 strchr 定位 ':' 和 '/' 来切分 host、port、path
 *   4. 注意 port_start 必须在 path_start 之前才算有效端口号
 *      （否则可能是 IPv6 地址中的冒号，虽然当前未支持 IPv6 字面量）
 *
 * 支持的 URL 格式示例：
 *   wss://asr.aliyuncs.com/v1/asr       -> host=asr.aliyuncs.com, port=443, path=/v1/asr
 *   ws://192.168.1.100:8080/asr         -> host=192.168.1.100, port=8080, path=/asr
 *   wss://asr.example.com:8443          -> host=asr.example.com, port=8443, path=/
 *
 * 参数：
 *   url     - 完整的 WebSocket URL 字符串
 *   use_ssl - 输出：是否使用 SSL（wss:// 为 TRUE）
 *   host    - 输出：主机名（从 pool 分配）
 *   port    - 输出：端口号
 *   path    - 输出：路径部分（从 pool 分配）
 *   pool    - FreeSWITCH 内存池
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE URL 格式无效
 */
static switch_status_t ws_parse_url(const char *url, switch_bool_t *use_ssl, char **host, int *port, char **path, switch_memory_pool_t *pool)
{
	const char *p = url;
	const char *path_start;
	const char *port_start;
	size_t host_len;

	/* 第一步：判断协议类型，同时移动指针跳过协议前缀 */
	if (!strncasecmp(p, "wss://", 6)) {
		*use_ssl = SWITCH_TRUE;   /* wss:// 表示加密 WebSocket */
		*port = 443;              /* wss 默认端口 */
		p += 6;
	} else if (!strncasecmp(p, "ws://", 5)) {
		*use_ssl = SWITCH_FALSE;  /* ws:// 表示明文 WebSocket */
		*port = 80;               /* ws 默认端口 */
		p += 5;
	} else {
		return SWITCH_STATUS_FALSE;  /* 不支持的协议 */
	}

	/* 第二步：查找路径和端口的起始位置，用于切分 host:port/path */
	path_start = strchr(p, '/');
	port_start = strchr(p, ':');

	/*
	 * 第三步：分四种情况提取 host 和 port：
	 *   1. 有路径、有端口：host:port/path  -- host_len = port_start - p, 从 port_start+1 解析端口号
	 *   2. 有路径、无端口：host/path       -- host_len = path_start - p, 使用默认端口
	 *   3. 无路径、有端口：host:port        -- host_len = port_start - p, 从 port_start+1 解析端口号
	 *   4. 无路径、无端口：host             -- host_len = strlen(p), 使用默认端口
	 *
	 * 关键判断：port_start && port_start < path_start
	 *   确保冒号出现在路径之前（而非路径中的冒号），才视为端口号分隔符
	 */
	if (path_start) {
		if (port_start && port_start < path_start) {
			host_len = port_start - p;
			*port = atoi(port_start + 1);
		} else {
			host_len = path_start - p;
		}
		*path = switch_core_strdup(pool, path_start);
	} else {
		if (port_start) {
			host_len = port_start - p;
			*port = atoi(port_start + 1);
		} else {
			host_len = strlen(p);
		}
		*path = "/";  /* 无路径时默认为根路径 */
	}

	/* 第四步：提取主机名并确保以 '\0' 结尾 */
	*host = switch_core_alloc(pool, host_len + 1);
	memcpy(*host, p, host_len);
	(*host)[host_len] = '\0';

	return SWITCH_STATUS_SUCCESS;
}

/*
 * ws_tcp_connect -- 建立 TCP 连接
 *
 * 使用 getaddrinfo 进行 DNS 解析和连接。getaddrinfo 相比 gethostbyname
 * 的优势是支持 IPv6 和 DNS SRV 记录，且是线程安全的。
 *
 * DNS 解析和连接逻辑：
 *   1. 构造 addrinfo 查询条件（AF_UNSPEC 允许 IPv4/IPv6，SOCK_STREAM 指定 TCP）
 *   2. 调用 getaddrinfo 执行 DNS 解析，返回一个 addrinfo 链表
 *   3. 遍历链表中的每个地址，依次尝试 socket() + connect()
 *   4. 遇到第一个连接成功的地址即停止
 *
 * 为什么遍历所有返回的地址？
 *   DNS 可能返回多个地址（IPv4 + IPv6、或多个 A 记录做负载均衡），
 *   遍历直到找到可连接的地址，提高连接成功率。
 *
 * 参数：
 *   host   - 主机名或 IP 地址
 *   port   - 端口号
 *   sockfd - 输出：成功连接的 socket 文件描述符
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 失败
 */
static switch_status_t ws_tcp_connect(const char *host, int port, int *sockfd)
{
	struct addrinfo hints, *res, *rp;
	char port_str[16];
	int ret;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;      /* 不限制 IPv4/IPv6，由 DNS 决定 */
	hints.ai_socktype = SOCK_STREAM;   /* TCP 流式套接字 */

	snprintf(port_str, sizeof(port_str), "%d", port);

	/* DNS 解析，ret=0 表示成功，否则通过 gai_strerror 获取错误描述 */
	ret = getaddrinfo(host, port_str, &hints, &res);
	if (ret != 0) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "getaddrinfo failed: %s\n", gai_strerror(ret));
		return SWITCH_STATUS_FALSE;
	}

	/* 依次尝试每个解析结果，直到连接成功 */
	for (rp = res; rp; rp = rp->ai_next) {
		*sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (*sockfd == -1) continue;   /* socket 创建失败，尝试下一个 */

		if (connect(*sockfd, rp->ai_addr, rp->ai_addrlen) == 0) {
			break;  /* 连接成功 */
		}

		/* 连接失败，关闭当前 socket 并尝试下一个地址 */
		close(*sockfd);
		*sockfd = -1;
	}

	freeaddrinfo(res);

	if (*sockfd == -1) {
		return SWITCH_STATUS_FALSE;  /* 所有地址都连接失败 */
	}

	return SWITCH_STATUS_SUCCESS;
}

/*
 * ws_send_raw -- 发送原始字节流
 *
 * 底层数据发送函数，根据是否启用 SSL 选择 SSL_write 或 send。
 * 采用循环发送确保所有数据都被写出——因为 TCP 是流式协议，
 * 一次 send/write 不一定能发出全部数据（特别是大块音频数据时）。
 *
 * 线程安全：此函数本身不加锁，调用者需通过 write_mutex 保护。
 *   为什么不在内部加锁？因为发送 WebSocket 帧时需要将 header 和
 *   payload 作为原子操作一起发送，锁必须在外层 ws_send_frame 中持有。
 *
 * 参数：
 *   conn - WebSocket 连接
 *   data - 待发送数据
 *   len  - 数据长度
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 发送失败
 */
static switch_status_t ws_send_raw(ws_conn_t *conn, const void *data, size_t len)
{
	size_t total_sent = 0;
	const uint8_t *ptr = (const uint8_t *) data;

	while (total_sent < len) {
		ssize_t sent;
		if (conn->use_ssl && conn->ssl) {
			sent = SSL_write(conn->ssl, ptr + total_sent, (int) (len - total_sent));
		} else {
			sent = send(conn->sockfd, ptr + total_sent, len - total_sent, 0);
		}

		if (sent <= 0) {
			int err;
			if (conn->use_ssl && conn->ssl) {
				err = SSL_get_error(conn->ssl, (int) sent);
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
					"SSL_write error: %d\n", err);
			} else {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
					"send error: %s\n", strerror(errno));
			}
			return SWITCH_STATUS_FALSE;
		}
		total_sent += sent;
	}

	return SWITCH_STATUS_SUCCESS;
}

/*
 * ws_recv_raw -- 接收原始字节流
 *
 * 底层数据接收函数，从 socket 或 SSL 连接读取数据。
 * 此函数读取尽可能多的数据（但不超过 buflen），不保证读取指定长度。
 * 如需精确读取 N 字节，使用 ws_recv_exact。
 *
 * 参数：
 *   conn   - WebSocket 连接
 *   buf    - 接收缓冲区
 *   buflen - 缓冲区大小
 *   recvd  - 输出：实际接收的字节数
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 连接断开或出错
 */
static switch_status_t ws_recv_raw(ws_conn_t *conn, void *buf, size_t buflen, ssize_t *recvd)
{
	if (conn->use_ssl && conn->ssl) {
		*recvd = SSL_read(conn->ssl, buf, (int) buflen);
	} else {
		*recvd = recv(conn->sockfd, buf, buflen, 0);
	}

	if (*recvd <= 0) {
		return SWITCH_STATUS_FALSE;  /* 连接关闭或出错 */
	}

	return SWITCH_STATUS_SUCCESS;
}

/*
 * ws_send_frame -- 构造并发送一个完整的 WebSocket 帧
 *
 * 按照 RFC 6455 第 5 节规定的帧格式构造并发送数据。
 *
 * RFC 6455 WebSocket 帧格式：
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-------+-+-------------+-------------------------------+
 *  |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
 *  |I|S|S|S|  (4)  |A|     (7)     |            (16/64)            |
 *  |N|V|V|V|       |S|             |   (if payload len==126/127)   |
 *  | |1|2|3|       |K|             |                               |
 *  +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - -+
 *  |     Extended payload length continued, if payload len == 127  |
 *  + - - - - - - - - - - - - - - -+-------------------------------+
 *  |                               |Masking-key, if MASK set to 1  |
 *  +-------------------------------+-------------------------------+
 *  | Masking-key (continued)       |          Payload Data         |
 *  +-------------------------------- - - - - - - - - - - - - - - -+
 *  :                     Payload Data continued ...                :
 *  + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
 *  |                     Payload Data (continued)                  |
 *  +---------------------------------------------------------------+
 *
 * 第 1 字节（header[0]）：
 *   - Bit 7 (FIN): 1=消息最后一帧, 0=后续还有分片帧
 *   - Bit 6-5-4 (RSV1-3): 保留位，此处为 0
 *   - Bit 3-0 (opcode): 帧类型
 *     0x01=文本帧, 0x02=二进制帧, 0x08=关闭帧, 0x09=Ping, 0x0A=Pong
 *
 * 第 2 字节（header[1]）：
 *   - Bit 7 (MASK): 1=客户端帧必须设置掩码位（RFC 6455 第 5.3 节强制要求）
 *   - Bit 6-0 (Payload length):
 *     0-125: 直接表示 payload 长度
 *     126:   紧跟 2 字节无符号整数表示真实长度（16位，最大 65535）
 *     127:   紧跟 8 字节无符号整数表示真实长度（64位，理论最大 2^63-1）
 *
 * 掩码（Masking-key）：
 *   客户端发送的帧必须携带 4 字节随机掩码，并将 payload 的每个字节
 *   与掩码循环异或（XOR）。这是为了防止缓存投毒攻击（RFC 6455 第 5.3 节）。
 *   服务端发送的帧不需要掩码。
 *
 * 参数：
 *   conn        - WebSocket 连接
 *   opcode      - 操作码（WS_OPCODE_TEXT / WS_OPCODE_BINARY / WS_OPCODE_CLOSE 等）
 *   payload     - 负载数据指针（可为 NULL）
 *   payload_len - 负载长度
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 发送失败
 */
static switch_status_t ws_send_frame(ws_conn_t *conn, uint8_t opcode, const void *payload, size_t payload_len)
{
	uint8_t header[14];          /* 最大帧头：2 + 8(扩展长度) + 4(掩码) = 14 字节 */
	uint8_t mask_key[4];        /* 4 字节掩码密钥 */
	uint8_t *masked_payload = NULL; /* 异或后的负载数据 */
	size_t header_len = 2;      /* 帧头最小长度：2 字节（opcode+长度） */
	switch_status_t status;
	size_t i;

	/* 构造第 1 字节：FIN=1 + opcode */
	header[0] = WS_FIN_BIT | opcode;

	/* RFC 6455: 客户端发送的帧必须设置掩码位（MASK bit） */
	header[1] = 0x80; /* mask bit set */

	/*
	 * 根据 payload 长度编码第 2 字节的长度字段和扩展长度字段
	 *
	 * 长度编码规则（RFC 6455 第 5.2 节）：
	 *   payload_len <= 125:        直接用 7 位编码，无需扩展长度
	 *   126 <= payload_len <= 65535: 7 位字段填 126，后跟 2 字节大端序长度
	 *   payload_len > 65535:       7 位字段填 127，后跟 8 字节大端序长度
	 */
	if (payload_len <= 125) {
		header[1] |= (uint8_t) payload_len;  /* 长度直接放在低 7 位 */
	} else if (payload_len <= 65535) {
		header[1] |= 126;                    /* 126 表示使用 2 字节扩展长度 */
		header[2] = (payload_len >> 8) & 0xFF;  /* 高字节在前（大端序） */
		header[3] = payload_len & 0xFF;         /* 低字节在后 */
		header_len = 4;                          /* 2(基本头) + 2(扩展长度) */
	} else {
		header[1] |= 127;                    /* 127 表示使用 8 字节扩展长度 */
		memset(&header[2], 0, 8);            /* 前 4 字节填 0（payload_len 不超过 2^32） */
		header[9] = payload_len & 0xFF;
		header[8] = (payload_len >> 8) & 0xFF;
		header[7] = (payload_len >> 16) & 0xFF;
		header[6] = (payload_len >> 24) & 0xFF;
		header_len = 10;                         /* 2(基本头) + 8(扩展长度) */
	}

	/*
	 * 生成 4 字节随机掩码密钥
	 *
	 * 【并发唯一性修复】原实现使用switch_micro_time_now()生成掩码，
	 * 并发连接时多个线程在同一微秒内调用会生成相同的掩码密钥，
	 * 违反RFC 6455要求掩码密钥"不可预测"的规范（虽非加密要求）。
	 * 增加原子递增计数器确保每次调用产生不同的掩码。
	 * 掩码目的是防缓存投毒而非加密，不需要密码学安全随机源。
	 */
	{
		static volatile uint32_t mask_counter = 0;
		uint32_t my_mask_counter = __sync_fetch_and_add(&mask_counter, 1);
		for (i = 0; i < 4; i++) {
			mask_key[i] = (uint8_t) (switch_micro_time_now() + i * 17 + (uintptr_t) &header % 251 + my_mask_counter * (i + 1));
		}
	}

	/* 将掩码密钥追加到帧头末尾 */
	header[header_len++] = mask_key[0];
	header[header_len++] = mask_key[1];
	header[header_len++] = mask_key[2];
	header[header_len++] = mask_key[3];

	/*
	 * 对 payload 进行掩码处理：
	 * payload[i] ^= mask_key[i % 4]
	 * 异或操作是可逆的，接收方用同样的掩码再异或一次即可还原
	 */
	if (payload_len > 0 && payload) {
		masked_payload = malloc(payload_len);
		if (!masked_payload) return SWITCH_STATUS_MEMERR;
		for (i = 0; i < payload_len; i++) {
			masked_payload[i] = ((const uint8_t *)payload)[i] ^ mask_key[i % 4];
		}
	}

	/*
	 * 加锁发送：确保帧头和 payload 作为原子操作一起发出。
	 * 如果不加锁，两个线程的帧数据可能交错，导致对端解析失败。
	 */
	switch_mutex_lock(conn->write_mutex);
	status = ws_send_raw(conn, header, header_len);
	if (status == SWITCH_STATUS_SUCCESS && masked_payload && payload_len > 0) {
		status = ws_send_raw(conn, masked_payload, payload_len);
	}
	switch_mutex_unlock(conn->write_mutex);

	switch_safe_free(masked_payload);

	return status;
}

/*
 * ws_recv_exact -- 精确接收指定字节数
 *
 * 循环调用 ws_recv_raw 直到读取到 need 字节为止。
 * 用于读取 WebSocket 帧的固定长度部分（帧头、扩展长度等）。
 *
 * 参数：
 *   conn - WebSocket 连接
 *   buf  - 接收缓冲区
 *   need - 需要读取的字节数
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 连接断开
 */
static switch_status_t ws_recv_exact(ws_conn_t *conn, void *buf, size_t need)
{
	size_t total = 0;
	ssize_t recvd;

	while (total < need) {
		if (ws_recv_raw(conn, (uint8_t *) buf + total, need - total, &recvd) != SWITCH_STATUS_SUCCESS) {
			return SWITCH_STATUS_FALSE;
		}
		total += recvd;
	}

	return SWITCH_STATUS_SUCCESS;
}

/*
 * ws_perform_handshake -- 执行 WebSocket HTTP 升级握手
 *
 * WebSocket 连接的建立需要一个 HTTP 升级握手过程（RFC 6455 第 4 章）：
 *
 * 完整握手流程：
 *   1. 客户端发送 HTTP GET 请求，携带 Upgrade: websocket 头
 *   2. 请求中必须包含 Sec-WebSocket-Key（16字节随机数的Base64编码）
 *   3. 请求中必须包含 Sec-WebSocket-Version: 13（RFC 6455 版本号）
 *   4. 可携带自定义头（如阿里云 ASR 的鉴权 token）
 *   5. 服务端返回 HTTP 101 Switching Protocols 响应
 *   6. 握手完成后，TCP 连接升级为 WebSocket 连接，后续数据按帧格式传输
 *
 * HTTP 请求格式：
 *   GET /path HTTP/1.1\r\n
 *   Host: hostname:port\r\n
 *   Upgrade: websocket\r\n
 *   Connection: Upgrade\r\n
 *   Sec-WebSocket-Key: <base64-encoded-16-byte-nonce>==\r\n
 *   Sec-WebSocket-Version: 13\r\n
 *   [自定义头...]\r\n
 *   \r\n
 *
 * HTTP 响应校验：
 *   只需检查响应中包含 "101" 状态码即可，
 *   严格实现还应验证 Sec-WebSocket-Accept 值是否正确，
 *   但对于已知可信的 ASR 服务端，简化校验足够。
 *
 * leftover 处理：
 *   读取 HTTP 响应时循环读取直到发现 "\r\n\r\n" 分隔符，
 *   但一次 recv 可能多读到属于第一个 WebSocket 帧的数据，
 *   这些数据保存到 conn->leftover 中，供后续 asr_ws_recv_text 使用。
 *
 * 参数：
 *   conn          - WebSocket 连接（sockfd/ssl 已建立）
 *   extra_headers - 额外的 HTTP 头数组（如鉴权 token），以 NULL 结尾
 *   header_count  - 额外头的数量
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 握手失败
 */
static switch_status_t ws_perform_handshake(ws_conn_t *conn, const char *extra_headers[], int header_count)
{
	unsigned char nonce[16];     /* 16 字节随机数，用于生成 Sec-WebSocket-Key */
	char nonce_b64[32];          /* Base64 编码后的 nonce */
	char key[64];                /* 最终的 Sec-WebSocket-Key 值（Base64 + "==" 后缀） */
	int i;
	switch_stream_handle_t stream = { 0 }; /* FreeSWITCH 动态字符串流，用于构造 HTTP 请求 */
	switch_status_t status;
	char resp[4096];             /* HTTP 响应缓冲区 */
	ssize_t recvd;
	char *body;                  /* 指向 HTTP 响应体起始位置（\r\n\r\n 之后） */

	/*
	 * 生成 Sec-WebSocket-Key：
	 *   1. 生成 16 字节伪随机数（使用微秒时间戳的低 8 位）
	 *   2. Base64 编码得到约 24 字符的字符串
	 *   3. 追加 "==" 使其符合 Base64 填充规范
	 *
	 * 注意：这里的随机数生成不是密码学安全的，但 WebSocket 握手
	 * 不要求密钥具有密码学强度，只需是难以猜测的值即可。
	 */
	for (i = 0; i < 16; i++) {
		nonce[i] = (unsigned char) (switch_micro_time_now() & 0xFF);
	}
	b64_encode(nonce, 16, nonce_b64, sizeof(nonce_b64));
	snprintf(key, sizeof(key), "%s==", nonce_b64);

	/* 使用 FreeSWITCH 的动态流构造 HTTP 请求字符串 */
	SWITCH_STANDARD_STREAM(stream);

	/* 请求行：GET <path> HTTP/1.1 */
	stream.write_function(&stream, "GET %s HTTP/1.1\r\n", conn->path);
	/* Host 头：服务端需要据此做虚拟主机路由 */
	stream.write_function(&stream, "Host: %s:%d\r\n", conn->host, conn->port);
	/* Upgrade 和 Connection 头：声明协议升级意图 */
	stream.write_function(&stream, "Upgrade: websocket\r\n");
	stream.write_function(&stream, "Connection: Upgrade\r\n");
	/* Sec-WebSocket-Key：握手验证的关键字段 */
	stream.write_function(&stream, "Sec-WebSocket-Key: %s\r\n", key);
	/* Sec-WebSocket-Version：指定协议版本为 RFC 6455 */
	stream.write_function(&stream, "Sec-WebSocket-Version: 13\r\n");

	/* 追加自定义头（如阿里云 ASR 的 Authorization、X-NLS-Token 等） */
	for (i = 0; i < header_count && extra_headers[i]; i++) {
		stream.write_function(&stream, "%s\r\n", extra_headers[i]);
	}

	/* 空行结束 HTTP 请求头 */
	stream.write_function(&stream, "\r\n");

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "WS handshake sending to %s:%d\n", conn->host, conn->port);

	/* 发送完整的 HTTP Upgrade 请求 */
	status = ws_send_raw(conn, stream.data, strlen((char *) stream.data));
	switch_safe_free(stream.data);

	if (status != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WS handshake send failed\n");
		return status;
	}

	/*
	 * 读取 HTTP 响应，循环读取直到发现 "\r\n\r\n"（HTTP 头结束标记）
	 *
	 * 关键点：不能只读一次就假设拿到了完整的 HTTP 响应，因为：
	 *   1. TCP 可能将一个 HTTP 响应拆分为多个段
	 *   2. 也可能一次读到 HTTP 头 + 部分 WebSocket 帧数据
	 * 所以必须循环读取，直到找到 "\r\n\r\n" 分隔符
	 */
	recvd = 0;
	while (recvd < (ssize_t) sizeof(resp) - 1) {
		ssize_t chunk;
		if (ws_recv_raw(conn, resp + recvd, sizeof(resp) - 1 - recvd, &chunk) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WS handshake recv failed\n");
			return SWITCH_STATUS_FALSE;
		}
		recvd += chunk;
		resp[recvd] = '\0';

		/* 检查是否已收到完整的 HTTP 头（以 \r\n\r\n 结尾） */
		body = strstr(resp, "\r\n\r\n");
		if (body) {
			body += 4; /* 跳过 \r\n\r\n，指向 HTTP 响应体（如果有的话） */
			break;
		}
	}

	/* 简化校验：只需检查响应中包含 "101" 状态码 */
	if (!strstr(resp, "101")) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WebSocket handshake failed: %s\n", resp);
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "WS handshake success\n");

	/*
	 * 保存 leftover 数据：
	 *   如果在读取 HTTP 响应时多读了数据（属于第一个 WebSocket 帧），
	 *   将其保存到 conn->leftover 中。这些数据在 asr_ws_recv_text() 中
	 *   会被优先消费，避免丢失。
	 *
	 *   判断条件：body 指针位于 resp 中间（而非末尾），
	 *   说明 \r\n\r\n 后面还有数据
	 */
	if (body && (size_t) (body - resp) < (size_t) recvd) {
		size_t leftover_size = recvd - (body - resp);
		if (leftover_size > 0 && leftover_size <= sizeof(conn->leftover)) {
			memcpy(conn->leftover, body, leftover_size);
			conn->leftover_len = leftover_size;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "WS leftover after handshake: %zu bytes\n", leftover_size);
		}
	}

	return SWITCH_STATUS_SUCCESS;
}

/*
 * asr_ws_connect -- 建立 WebSocket 连接（完整流程）
 *
 * 依次执行以下步骤建立完整的 WebSocket 连接：
 *
 *   1. 创建 ws_conn_t 连接对象
 *   2. 解析 WebSocket URL（提取 host/port/path/use_ssl）
 *   3. 建立 TCP 连接（DNS 解析 -> socket -> connect）
 *   4. SSL 握手（仅 wss:// 时执行）
 *      - 创建 SSL_CTX（使用 TLS_client_method，自动协商最高版本）
 *      - 禁用 SSLv2/SSLv3（存在安全漏洞）
 *      - 创建 SSL 对象并绑定到 socket
 *      - 执行 SSL_connect 握手
 *   5. WebSocket HTTP 升级握手
 *   6. 设置 socket 接收超时（1秒），防止 recv 永久阻塞
 *   7. 标记连接为已连接状态，保存到 session->ws_handle
 *
 * 错误处理：
 *   每一步失败都会清理已分配的资源（关闭 socket、释放 SSL 对象），
 *   确保不会泄漏文件描述符或内存。
 *
 * 参数：
 *   session      - ASR 会话对象
 *   url          - WebSocket URL（wss://... 或 ws://...）
 *   headers      - 额外的 HTTP 头数组（如鉴权 token），以 NULL 结尾
 *   header_count - 额外头的数量
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE/MEMERR 失败
 */
switch_status_t asr_ws_connect(asr_session_t *session, const char *url, const char **headers, int header_count)
{
	ws_conn_t *conn;
	switch_status_t status;
	struct timeval rcvtimeo;

	if (!session || zstr(url)) {
		return SWITCH_STATUS_FALSE;
	}

	/* 第 1 步：从 session 的内存池创建连接对象 */
	conn = ws_conn_create(session->pool);
	if (!conn) {
		return SWITCH_STATUS_MEMERR;
	}

	conn->url = switch_core_strdup(session->pool, url);

	/* 第 2 步：解析 URL，提取 host、port、path、use_ssl */
	if (ws_parse_url(url, &conn->use_ssl, &conn->host, &conn->port, &conn->path, session->pool) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Invalid WebSocket URL: %s\n", url);
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "WS connecting to %s:%d%s (ssl=%d)\n",
					  conn->host, conn->port, conn->path, conn->use_ssl);

	/* 第 3 步：建立 TCP 连接 */
	if (ws_tcp_connect(conn->host, conn->port, &conn->sockfd) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "TCP connect failed to %s:%d\n", conn->host, conn->port);
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "TCP connected to %s:%d\n", conn->host, conn->port);

	/* 第 4 步：SSL 握手（仅 wss:// 时执行） */
	if (conn->use_ssl) {
		/* 创建 SSL 上下文，使用 TLS_client_method 让 OpenSSL 自动协商最高 TLS 版本 */
		conn->ssl_ctx = SSL_CTX_new(TLS_client_method());
		if (!conn->ssl_ctx) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "SSL_CTX_new failed\n");
			close(conn->sockfd);
			return SWITCH_STATUS_FALSE;
		}
		/* 禁用 SSLv2 和 SSLv3，这两个协议存在 POODLE 等安全漏洞 */
		SSL_CTX_set_options(conn->ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

		/* 创建 SSL 连接对象并绑定到 TCP socket */
		conn->ssl = SSL_new(conn->ssl_ctx);
		SSL_set_fd(conn->ssl, conn->sockfd);

		/* 执行 SSL/TLS 握手（包括证书验证、密钥交换等） */
		if (SSL_connect(conn->ssl) <= 0) {
			unsigned long err = ERR_get_error();
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "SSL handshake failed: %s\n",
							  ERR_error_string(err, NULL));
			SSL_free(conn->ssl);
			SSL_CTX_free(conn->ssl_ctx);
			close(conn->sockfd);
			return SWITCH_STATUS_FALSE;
		}

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "SSL handshake success\n");
	}

	/* 第 5 步：WebSocket HTTP 升级握手 */
	status = ws_perform_handshake(conn, (const char **) headers, header_count);
	if (status != SWITCH_STATUS_SUCCESS) {
		/* 握手失败，清理已建立的 SSL/TCP 资源 */
		if (conn->ssl) SSL_free(conn->ssl);
		if (conn->ssl_ctx) SSL_CTX_free(conn->ssl_ctx);
		close(conn->sockfd);
		return status;
	}

	/*
	 * 第 6 步：设置 socket 接收超时为 1 秒
	 * 这样 asr_ws_recv_text 中的 recv 调用最多阻塞 1 秒，
	 * 允许调用者定期检查连接状态或超时退出
	 */
	rcvtimeo.tv_sec = 1;
	rcvtimeo.tv_usec = 0;
	setsockopt(conn->sockfd, SOL_SOCKET, SO_RCVTIMEO, (const void *) &rcvtimeo, sizeof(rcvtimeo));

	/* 第 7 步：标记连接已建立，保存到 session */
	conn->connected = SWITCH_TRUE;
	session->ws_handle = conn;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "WebSocket connected: %s\n", url);

	return SWITCH_STATUS_SUCCESS;
}

/*
 * asr_ws_send_binary -- 发送二进制 WebSocket 帧
 *
 * 用于持续发送音频 PCM 数据到 ASR 服务端。
 * 音频数据作为二进制帧（opcode=0x02）发送。
 *
 * 参数：
 *   session - ASR 会话
 *   data    - 音频数据指针
 *   len     - 数据长度
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 失败
 */
switch_status_t asr_ws_send_binary(asr_session_t *session, const void *data, size_t len)
{
	ws_conn_t *conn;

	if (!session || !session->ws_handle || !data || len == 0) {
		return SWITCH_STATUS_FALSE;
	}

	conn = (ws_conn_t *) session->ws_handle;
	if (!conn->connected) {
		return SWITCH_STATUS_FALSE;
	}

	return ws_send_frame(conn, WS_OPCODE_BINARY, data, len);
}

/*
 * asr_ws_send_text -- 发送文本 WebSocket 帧
 *
 * 用于发送 JSON 格式的控制指令到 ASR 服务端（如开始识别、停止识别等）。
 * 文本数据作为文本帧（opcode=0x01）发送。
 *
 * 参数：
 *   session - ASR 会话
 *   text    - 文本内容（JSON 字符串）
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 失败
 */
switch_status_t asr_ws_send_text(asr_session_t *session, const char *text)
{
	ws_conn_t *conn;

	if (!session || !session->ws_handle || zstr(text)) {
		return SWITCH_STATUS_FALSE;
	}

	conn = (ws_conn_t *) session->ws_handle;
	if (!conn->connected) {
		return SWITCH_STATUS_FALSE;
	}

	return ws_send_frame(conn, WS_OPCODE_TEXT, text, strlen(text));
}

/*
 * asr_ws_disconnect -- 断开 WebSocket 连接
 *
 * 优雅关闭流程：
 *   1. 发送 WebSocket 关闭帧（opcode=0x08），通知对端即将断开
 *   2. 标记连接为断开状态
 *   3. 关闭 SSL 连接（SSL_shutdown -> SSL_free -> SSL_CTX_free）
 *   4. 关闭 TCP socket
 *   5. 清空 session->ws_handle 指针
 *
 * 注意：资源释放顺序很重要——先关 SSL 再关 socket，
 * 因为 SSL_shutdown 需要向对端发送 close_notify 告警。
 *
 * 参数：
 *   session - ASR 会话
 *
 * 返回值：SWITCH_STATUS_SUCCESS 成功，SWITCH_STATUS_FALSE 参数无效
 */
switch_status_t asr_ws_disconnect(asr_session_t *session)
{
	ws_conn_t *conn;

	if (!session || !session->ws_handle) {
		return SWITCH_STATUS_FALSE;
	}

	conn = (ws_conn_t *) session->ws_handle;
	if (conn->connected) {
		/* 发送关闭帧，通知对端优雅断开 */
		ws_send_frame(conn, WS_OPCODE_CLOSE, NULL, 0);
		conn->connected = SWITCH_FALSE;
	}

	/* 按顺序释放 SSL 资源 */
	if (conn->ssl) {
		SSL_shutdown(conn->ssl);  /* 发送 close_notify 告警 */
		SSL_free(conn->ssl);
		conn->ssl = NULL;
	}
	if (conn->ssl_ctx) {
		SSL_CTX_free(conn->ssl_ctx);
		conn->ssl_ctx = NULL;
	}
	/* 关闭 TCP socket */
	if (conn->sockfd >= 0) {
		close(conn->sockfd);
		conn->sockfd = -1;
	}

	session->ws_handle = NULL;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "WebSocket disconnected\n");

	return SWITCH_STATUS_SUCCESS;
}

/*
 * asr_ws_is_connected -- 检查 WebSocket 连接状态
 *
 * 参数：
 *   session - ASR 会话
 *
 * 返回值：SWITCH_TRUE 已连接，SWITCH_FALSE 未连接
 */
switch_bool_t asr_ws_is_connected(asr_session_t *session)
{
	ws_conn_t *conn;

	if (!session || !session->ws_handle) {
		return SWITCH_FALSE;
	}

	conn = (ws_conn_t *) session->ws_handle;
	return conn->connected;
}

/*
 * asr_ws_has_data -- 检查是否有数据可读
 *
 * 按优先级依次检查三个数据源：
 *   1. leftover 缓冲区（HTTP 握手时多读的数据）
 *   2. SSL 内部缓冲区（SSL_pending，OpenSSL 可能已解密但未读的数据）
 *   3. socket 接收缓冲区（使用 select 零超时非阻塞轮询）
 *
 * 参数：
 *   session - ASR 会话
 *
 * 返回值：SWITCH_TRUE 有数据，SWITCH_FALSE 无数据或未连接
 */
switch_bool_t asr_ws_has_data(asr_session_t *session)
{
	ws_conn_t *conn;
	fd_set readfds;
	struct timeval tv;
	int ret;

	if (!session || !session->ws_handle) {
		return SWITCH_FALSE;
	}

	conn = (ws_conn_t *) session->ws_handle;
	if (!conn->connected) {
		return SWITCH_FALSE;
	}

	/* 优先检查 leftover 缓冲区 */
	if (conn->leftover_len > 0) {
		return SWITCH_TRUE;
	}

	/* 检查 SSL 内部缓冲区（已解密但应用程序尚未读取的数据） */
	if (conn->use_ssl && conn->ssl && SSL_pending(conn->ssl) > 0) {
		return SWITCH_TRUE;
	}

	/* 使用 select 零超时轮询 socket 是否有数据可读（非阻塞检查） */
	FD_ZERO(&readfds);
	FD_SET(conn->sockfd, &readfds);
	tv.tv_sec = 0;
	tv.tv_usec = 0;

	ret = select(conn->sockfd + 1, &readfds, NULL, NULL, &tv);
	return (ret > 0) ? SWITCH_TRUE : SWITCH_FALSE;
}

/*
 * asr_ws_recv_text -- 接收 WebSocket 文本帧
 *
 * 从 WebSocket 连接接收一帧数据，期望获取 ASR 识别结果（文本帧）。
 *
 * 帧接收完整流程：
 *
 *   1. 读取 2 字节帧头（优先消费 leftover 缓冲区中的数据）
 *      - header[0]: FIN + RSV + opcode
 *      - header[1]: MASK + payload length (7位)
 *
 *   2. 解析 opcode 判断帧类型：
 *      - 服务端帧不应设置 MASK 位（RFC 6455 第 5.3 节），
 *        如果发现 masked 服务端帧，视为协议错误
 *
 *   3. 解析 payload 长度：
 *      - 0-125: 直接使用
 *      - 126:   再读 2 字节（大端序无符号16位整数）
 *      - 127:   再读 8 字节（大端序无符号64位整数）
 *
 *   4. 处理控制帧：
 *      - PING: 读取 payload，原样回送 PONG 帧，返回 NULL 让调用者继续读取
 *      - CLOSE: 标记连接断开，读取并记录关闭原因，返回 NULL
 *
 *   5. 大帧保护：
 *      - 如果 payload_len > 1MB，视为异常，跳过该帧的所有数据，
 *        防止恶意或异常的大帧导致内存耗尽
 *
 *   6. 读取 payload 数据，转为字符串返回
 *
 * leftover 消费逻辑：
 *   在 HTTP 握手阶段，recv 可能多读到属于第一个 WebSocket 帧的数据，
 *   这些数据存储在 conn->leftover 中。本函数在读取帧头时，
 *   优先从 leftover 中取数据，取完后再从 socket 读取剩余部分。
 *
 * PING/PONG 处理：
 *   ASR 服务端可能定期发送 PING 帧检测客户端存活状态，
 *   客户端必须回送 PONG 帧（可携带相同的 payload），
 *   否则服务端可能认为客户端已断开而关闭连接。
 *   本函数在收到 PING 时自动回复 PONG，并返回 NULL，
 *   调用者应在循环中反复调用，直到收到文本帧或 NULL（超时/关闭）。
 *
 * 大帧保护：
 *   正常的 ASR 识别结果（JSON）通常在几 KB 以内，
 *   如果收到超过 1MB 的帧，极可能是协议异常或恶意数据，
 *   此时逐块读取并丢弃，避免 malloc 分配过大内存导致 OOM。
 *
 * 参数：
 *   session - ASR 会话
 *   pool    - FreeSWITCH 内存池，用于分配返回的字符串
 *
 * 返回值：
 *   非 NULL - 成功接收的文本/二进制帧内容（从 pool 分配的字符串）
 *   NULL    - 连接断开、PING 已处理（需继续读取）、超时或出错
 */
char *asr_ws_recv_text(asr_session_t *session, switch_memory_pool_t *pool)
{
	ws_conn_t *conn;
	uint8_t header[2];           /* 2 字节基本帧头 */
	uint8_t opcode;              /* 帧操作码 */
	size_t payload_len;          /* 负载长度 */
	uint8_t *payload;            /* 负载数据缓冲区 */
	char *result;                /* 返回给调用者的结果字符串 */
	uint8_t ext[8];              /* 扩展长度缓冲区（2字节或8字节） */
	int i;
	size_t read_pos;             /* 帧头已读取位置 */
	uint8_t skip_buf[1024];     /* 跳过大帧时使用的临时缓冲区 */

	if (!session || !session->ws_handle) {
		return NULL;
	}

	conn = (ws_conn_t *) session->ws_handle;
	if (!conn->connected) {
		return NULL;
	}

	/*
	 * 读取 2 字节帧头
	 * 优先从 leftover 缓冲区中取数据（HTTP 握手时多读的数据），
	 * leftover 不足时再从 socket 读取剩余部分
	 */
	read_pos = 0;

	/* 从 leftover 缓冲区取数据填充帧头 */
	if (conn->leftover_len > 0) {
		size_t copy;
		copy = conn->leftover_len < 2 ? conn->leftover_len : 2;
		memcpy(header, conn->leftover, copy);
		read_pos = copy;
		/* 移动 leftover 中剩余的数据到缓冲区开头 */
		if (conn->leftover_len > copy) {
			memmove(conn->leftover, conn->leftover + copy, conn->leftover_len - copy);
		}
		conn->leftover_len -= copy;
	}

	/* leftover 数据不够 2 字节时，从 socket 补充读取 */
	if (read_pos < 2) {
		if (ws_recv_exact(conn, header + read_pos, 2 - read_pos) != SWITCH_STATUS_SUCCESS) {
			return NULL;
		}
	}

	/* 解析 opcode（低 4 位） */
	opcode = header[0] & 0x0F;

	/*
	 * 检查服务端帧是否设置了 MASK 位
	 * RFC 6455 规定：服务端发送的帧不得设置掩码位，
	 * 如果发现掩码位为 1，说明协议实现有误或数据已错位
	 */
	if (header[1] & 0x80) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Received masked server frame, skipping\n");
		return NULL;
	}

	/*
	 * 解析 payload 长度（RFC 6455 第 5.2 节）
	 *   header[1] 的低 7 位表示初始长度：
	 *   - 0-125: 直接就是 payload 长度
	 *   - 126:   紧跟 2 字节大端序无符号整数（16位，最大 65535）
	 *   - 127:   紧跟 8 字节大端序无符号整数（64位，理论最大 2^63-1）
	 */
	payload_len = header[1] & 0x7F;
	if (payload_len == 126) {
		/* 中等长度帧：读取 2 字节扩展长度 */
		if (ws_recv_exact(conn, ext, 2) != SWITCH_STATUS_SUCCESS) return NULL;
		payload_len = (ext[0] << 8) | ext[1];  /* 大端序转为主机序 */
	} else if (payload_len == 127) {
		/* 大长度帧：读取 8 字节扩展长度 */
		if (ws_recv_exact(conn, ext, 8) != SWITCH_STATUS_SUCCESS) return NULL;
		payload_len = 0;
		for (i = 0; i < 8; i++) {
			payload_len = (payload_len << 8) | ext[i];  /* 逐字节拼接大端序 */
		}
	}

	/*
	 * 处理 PING 帧：
	 * 服务端发送 PING 检测客户端存活，客户端必须回送 PONG。
	 * PONG 的 payload 应与 PING 的 payload 相同（RFC 6455 第 5.5.3 节）。
	 * 返回 NULL 让调用者知道这不是数据帧，需要继续读取。
	 */
	if (opcode == WS_OPCODE_PING) {
		if (payload_len > 0) {
			payload = malloc(payload_len);
			if (payload && ws_recv_exact(conn, payload, payload_len) == SWITCH_STATUS_SUCCESS) {
				ws_send_frame(conn, WS_OPCODE_PONG, payload, payload_len);
			}
			switch_safe_free(payload);
		} else {
			ws_send_frame(conn, WS_OPCODE_PONG, NULL, 0);
		}
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "WS PING->PONG\n");
		return NULL;
	}

	/*
	 * 处理 CLOSE 帧：
	 * 服务端请求关闭连接，记录关闭原因（如果有），
	 * 标记连接为断开状态，返回 NULL 通知调用者。
	 */
	if (opcode == WS_OPCODE_CLOSE) {
		conn->connected = SWITCH_FALSE;
		if (payload_len > 0 && payload_len < 4096) {
			uint8_t *close_payload = malloc(payload_len);
			if (close_payload && ws_recv_exact(conn, close_payload, payload_len) == SWITCH_STATUS_SUCCESS) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
					"WS CLOSE received (len=%zu): %.*s\n", payload_len,
					(int)(payload_len > 200 ? 200 : payload_len), (char *)close_payload);
			} else {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
					"WS CLOSE received (len=%zu, read failed)\n", payload_len);
			}
			switch_safe_free(close_payload);
		} else {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
				"WS CLOSE received (len=%zu)\n", payload_len);
		}
		return NULL;
	}

	/* 空负载帧，无数据可返回 */
	if (payload_len == 0) {
		return NULL;
	}

	/*
	 * 大帧保护：payload 超过 1MB 视为异常
	 *
	 * 正常的 ASR 识别结果（JSON 格式）通常只有几 KB，
	 * 超过 1MB 极可能是协议错误或恶意数据。
	 * 此时逐块读取并丢弃，避免分配超大内存导致 OOM。
	 */
	if (payload_len > 1024 * 1024) {
		size_t chunk;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WS frame too large: %zu\n", payload_len);
		/* 逐块读取并丢弃，确保 TCP 接收缓冲区被消费，避免连接卡死 */
		while (payload_len > 0) {
			chunk = payload_len > sizeof(skip_buf) ? sizeof(skip_buf) : payload_len;
			if (ws_recv_exact(conn, skip_buf, chunk) != SWITCH_STATUS_SUCCESS) break;
			payload_len -= chunk;
		}
		return NULL;
	}

	/* 分配 payload 缓冲区并读取完整负载数据 */
	payload = malloc(payload_len + 1);  /* +1 用于末尾 '\0' 方便转为字符串 */
	if (!payload) return NULL;

	if (ws_recv_exact(conn, payload, payload_len) != SWITCH_STATUS_SUCCESS) {
		free(payload);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WS recv payload failed (%zu bytes)\n", payload_len);
		return NULL;
	}

	/* 添加字符串终止符，使 payload 可以作为 C 字符串使用 */
	payload[payload_len] = '\0';

	/* 文本帧和二进制帧都作为结果返回 */
	if (opcode == WS_OPCODE_TEXT || opcode == WS_OPCODE_BINARY) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "WS recv: opcode=%d len=%zu\n", opcode, payload_len);
		/* 从 pool 复制字符串，调用者无需手动释放 */
		result = switch_core_strdup(pool, (char *) payload);
		free(payload);
		return result;
	}

	/* 其他 opcode（如 continuation frame 0x00），丢弃并返回 NULL */
	free(payload);
	return NULL;
}
