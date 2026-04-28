/*
 * asr_ws_client.c -- WebSocket client for streaming ASR
 *
 * Uses a raw socket with OpenSSL for WebSocket connections.
 * This avoids dependency on libcurl WebSocket support (requires curl 7.86+).
 *
 * The WS connection handle is stored in session->ws_handle,
 * separate from session->provider_private which holds provider-specific data.
 */

#include "mod_asr.h"
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <sys/select.h>

#define WS_FIN_BIT        0x80
#define WS_OPCODE_TEXT    0x01
#define WS_OPCODE_BINARY  0x02
#define WS_OPCODE_CLOSE   0x08
#define WS_OPCODE_PING    0x09
#define WS_OPCODE_PONG    0x0A

typedef struct {
	int sockfd;
	SSL *ssl;
	SSL_CTX *ssl_ctx;
	switch_bool_t connected;
	switch_bool_t use_ssl;
	char *url;
	char *host;
	int port;
	char *path;
	switch_mutex_t *write_mutex;
} ws_conn_t;

static size_t b64_encode(const unsigned char *in, size_t inlen, char *out, size_t outlen)
{
	static const char b64table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	size_t i, j = 0;
	uint32_t a, b, c, triple;

	for (i = 0; i < inlen && j < outlen - 1; i += 3) {
		a = in[i];
		b = (i + 1 < inlen) ? in[i + 1] : 0;
		c = (i + 2 < inlen) ? in[i + 2] : 0;
		triple = (a << 16) | (b << 8) | c;

		if (j < outlen - 1) out[j++] = b64table[(triple >> 18) & 0x3F];
		if (j < outlen - 1) out[j++] = b64table[(triple >> 12) & 0x3F];
		if (i + 1 < inlen && j < outlen - 1) out[j++] = b64table[(triple >> 6) & 0x3F];
		if (i + 2 < inlen && j < outlen - 1) out[j++] = b64table[triple & 0x3F];
	}
	out[j] = '\0';
	return j;
}

static ws_conn_t *ws_conn_create(switch_memory_pool_t *pool)
{
	ws_conn_t *conn = switch_core_alloc(pool, sizeof(*conn));
	if (!conn) return NULL;

	memset(conn, 0, sizeof(*conn));
	conn->sockfd = -1;
	conn->port = 443;
	switch_mutex_init(&conn->write_mutex, SWITCH_MUTEX_NESTED, pool);

	return conn;
}

static switch_status_t ws_parse_url(const char *url, switch_bool_t *use_ssl, char **host, int *port, char **path, switch_memory_pool_t *pool)
{
	const char *p = url;
	const char *path_start;
	const char *port_start;
	size_t host_len;

	if (!strncasecmp(p, "wss://", 6)) {
		*use_ssl = SWITCH_TRUE;
		*port = 443;
		p += 6;
	} else if (!strncasecmp(p, "ws://", 5)) {
		*use_ssl = SWITCH_FALSE;
		*port = 80;
		p += 5;
	} else {
		return SWITCH_STATUS_FALSE;
	}

	path_start = strchr(p, '/');
	port_start = strchr(p, ':');

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
		*path = "/";
	}

	*host = switch_core_alloc(pool, host_len + 1);
	memcpy(*host, p, host_len);
	(*host)[host_len] = '\0';

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t ws_tcp_connect(const char *host, int port, int *sockfd)
{
	struct addrinfo hints, *res, *rp;
	char port_str[16];
	int ret;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	snprintf(port_str, sizeof(port_str), "%d", port);

	ret = getaddrinfo(host, port_str, &hints, &res);
	if (ret != 0) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "getaddrinfo failed: %s\n", gai_strerror(ret));
		return SWITCH_STATUS_FALSE;
	}

	for (rp = res; rp; rp = rp->ai_next) {
		*sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (*sockfd == -1) continue;

		if (connect(*sockfd, rp->ai_addr, rp->ai_addrlen) == 0) {
			break;
		}

		close(*sockfd);
		*sockfd = -1;
	}

	freeaddrinfo(res);

	if (*sockfd == -1) {
		return SWITCH_STATUS_FALSE;
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t ws_send_raw(ws_conn_t *conn, const void *data, size_t len)
{
	ssize_t sent;

	if (conn->use_ssl && conn->ssl) {
		sent = SSL_write(conn->ssl, data, (int) len);
	} else {
		sent = send(conn->sockfd, data, len, 0);
	}

	if (sent <= 0) {
		return SWITCH_STATUS_FALSE;
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t ws_recv_raw(ws_conn_t *conn, void *buf, size_t buflen, ssize_t *recvd)
{
	if (conn->use_ssl && conn->ssl) {
		*recvd = SSL_read(conn->ssl, buf, (int) buflen);
	} else {
		*recvd = recv(conn->sockfd, buf, buflen, 0);
	}

	if (*recvd <= 0) {
		return SWITCH_STATUS_FALSE;
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t ws_send_frame(ws_conn_t *conn, uint8_t opcode, const void *payload, size_t payload_len)
{
	uint8_t header[14];
	size_t header_len = 2;
	switch_status_t status;

	header[0] = WS_FIN_BIT | opcode;

	if (payload_len <= 125) {
		header[1] = (uint8_t) payload_len;
	} else if (payload_len <= 65535) {
		header[1] = 126;
		header[2] = (payload_len >> 8) & 0xFF;
		header[3] = payload_len & 0xFF;
		header_len = 4;
	} else {
		header[1] = 127;
		memset(&header[2], 0, 8);
		header[9] = payload_len & 0xFF;
		header[8] = (payload_len >> 8) & 0xFF;
		header[7] = (payload_len >> 16) & 0xFF;
		header[6] = (payload_len >> 24) & 0xFF;
		header_len = 10;
	}

	switch_mutex_lock(conn->write_mutex);
	status = ws_send_raw(conn, header, header_len);
	if (status == SWITCH_STATUS_SUCCESS && payload_len > 0) {
		status = ws_send_raw(conn, payload, payload_len);
	}
	switch_mutex_unlock(conn->write_mutex);

	return status;
}

static switch_status_t ws_perform_handshake(ws_conn_t *conn, const char *extra_headers[], int header_count)
{
	unsigned char nonce[16];
	char nonce_b64[32];
	char key[64];
	int i;
	switch_stream_handle_t stream = { 0 };
	switch_status_t status;
	char resp[1024];
	ssize_t recvd;

	for (i = 0; i < 16; i++) {
		nonce[i] = (unsigned char) (switch_micro_time_now() & 0xFF);
	}
	b64_encode(nonce, 16, nonce_b64, sizeof(nonce_b64));
	snprintf(key, sizeof(key), "%s==", nonce_b64);

	SWITCH_STANDARD_STREAM(stream);

	stream.write_function(&stream, "GET %s HTTP/1.1\r\n", conn->path);
	stream.write_function(&stream, "Host: %s:%d\r\n", conn->host, conn->port);
	stream.write_function(&stream, "Upgrade: websocket\r\n");
	stream.write_function(&stream, "Connection: Upgrade\r\n");
	stream.write_function(&stream, "Sec-WebSocket-Key: %s\r\n", key);
	stream.write_function(&stream, "Sec-WebSocket-Version: 13\r\n");

	for (i = 0; i < header_count && extra_headers[i]; i++) {
		stream.write_function(&stream, "%s\r\n", extra_headers[i]);
	}

	stream.write_function(&stream, "\r\n");

	status = ws_send_raw(conn, stream.data, strlen((char *) stream.data));
	switch_safe_free(stream.data);

	if (status != SWITCH_STATUS_SUCCESS) {
		return status;
	}

	status = ws_recv_raw(conn, resp, sizeof(resp) - 1, &recvd);
	if (status != SWITCH_STATUS_SUCCESS || recvd <= 0) {
		return SWITCH_STATUS_FALSE;
	}
	resp[recvd] = '\0';

	if (!strstr(resp, "101")) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WebSocket handshake failed: %s\n", resp);
		return SWITCH_STATUS_FALSE;
	}

	return SWITCH_STATUS_SUCCESS;
}

switch_status_t asr_ws_connect(asr_session_t *session, const char *url, const char **headers, int header_count)
{
	ws_conn_t *conn;
	switch_status_t status;
	struct timeval rcvtimeo;

	if (!session || zstr(url)) {
		return SWITCH_STATUS_FALSE;
	}

	conn = ws_conn_create(session->pool);
	if (!conn) {
		return SWITCH_STATUS_MEMERR;
	}

	conn->url = switch_core_strdup(session->pool, url);

	if (ws_parse_url(url, &conn->use_ssl, &conn->host, &conn->port, &conn->path, session->pool) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Invalid WebSocket URL: %s\n", url);
		return SWITCH_STATUS_FALSE;
	}

	if (ws_tcp_connect(conn->host, conn->port, &conn->sockfd) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "TCP connect failed to %s:%d\n", conn->host, conn->port);
		return SWITCH_STATUS_FALSE;
	}

	if (conn->use_ssl) {
		conn->ssl_ctx = SSL_CTX_new(SSLv23_client_method());
		if (!conn->ssl_ctx) {
			close(conn->sockfd);
			return SWITCH_STATUS_FALSE;
		}
		SSL_CTX_set_options(conn->ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

		conn->ssl = SSL_new(conn->ssl_ctx);
		SSL_set_fd(conn->ssl, conn->sockfd);

		if (SSL_connect(conn->ssl) <= 0) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "SSL handshake failed\n");
			SSL_free(conn->ssl);
			SSL_CTX_free(conn->ssl_ctx);
			close(conn->sockfd);
			return SWITCH_STATUS_FALSE;
		}
	}

	status = ws_perform_handshake(conn, (const char **) headers, header_count);
	if (status != SWITCH_STATUS_SUCCESS) {
		if (conn->ssl) SSL_free(conn->ssl);
		if (conn->ssl_ctx) SSL_CTX_free(conn->ssl_ctx);
		close(conn->sockfd);
		return status;
	}

	/* Set socket receive timeout to 1 second so recv doesn't block indefinitely */
	rcvtimeo.tv_sec = 1;
	rcvtimeo.tv_usec = 0;
	setsockopt(conn->sockfd, SOL_SOCKET, SO_RCVTIMEO, (const void *) &rcvtimeo, sizeof(rcvtimeo));

	conn->connected = SWITCH_TRUE;
	session->ws_handle = conn;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "WebSocket connected: %s\n", url);

	return SWITCH_STATUS_SUCCESS;
}

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

switch_status_t asr_ws_disconnect(asr_session_t *session)
{
	ws_conn_t *conn;

	if (!session || !session->ws_handle) {
		return SWITCH_STATUS_FALSE;
	}

	conn = (ws_conn_t *) session->ws_handle;
	if (conn->connected) {
		ws_send_frame(conn, WS_OPCODE_CLOSE, NULL, 0);
		conn->connected = SWITCH_FALSE;
	}

	if (conn->ssl) {
		SSL_shutdown(conn->ssl);
		SSL_free(conn->ssl);
		conn->ssl = NULL;
	}
	if (conn->ssl_ctx) {
		SSL_CTX_free(conn->ssl_ctx);
		conn->ssl_ctx = NULL;
	}
	if (conn->sockfd >= 0) {
		close(conn->sockfd);
		conn->sockfd = -1;
	}

	session->ws_handle = NULL;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "WebSocket disconnected\n");

	return SWITCH_STATUS_SUCCESS;
}

switch_bool_t asr_ws_is_connected(asr_session_t *session)
{
	ws_conn_t *conn;

	if (!session || !session->ws_handle) {
		return SWITCH_FALSE;
	}

	conn = (ws_conn_t *) session->ws_handle;
	return conn->connected;
}

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

	/* Check SSL buffer first */
	if (conn->use_ssl && conn->ssl && SSL_pending(conn->ssl) > 0) {
		return SWITCH_TRUE;
	}

	/* Check socket with zero timeout (non-blocking poll) */
	FD_ZERO(&readfds);
	FD_SET(conn->sockfd, &readfds);
	tv.tv_sec = 0;
	tv.tv_usec = 0;

	ret = select(conn->sockfd + 1, &readfds, NULL, NULL, &tv);
	return (ret > 0) ? SWITCH_TRUE : SWITCH_FALSE;
}

char *asr_ws_recv_text(asr_session_t *session, switch_memory_pool_t *pool)
{
	ws_conn_t *conn;
	uint8_t header[2];
	ssize_t recvd;
	uint8_t opcode;
	size_t payload_len;
	uint8_t *payload;
	char *result;
	uint8_t ext[8];
	int i;

	if (!session || !session->ws_handle) {
		return NULL;
	}

	conn = (ws_conn_t *) session->ws_handle;
	if (!conn->connected) {
		return NULL;
	}

	if (ws_recv_raw(conn, header, 2, &recvd) != SWITCH_STATUS_SUCCESS || recvd < 2) {
		return NULL;
	}

	opcode = header[0] & 0x0F;

	if (header[1] & 0x80) {
		return NULL;
	}

	payload_len = header[1] & 0x7F;
	if (payload_len == 126) {
		if (ws_recv_raw(conn, ext, 2, &recvd) != SWITCH_STATUS_SUCCESS || recvd < 2) return NULL;
		payload_len = (ext[0] << 8) | ext[1];
	} else if (payload_len == 127) {
		if (ws_recv_raw(conn, ext, 8, &recvd) != SWITCH_STATUS_SUCCESS || recvd < 8) return NULL;
		payload_len = 0;
		for (i = 0; i < 8; i++) {
			payload_len = (payload_len << 8) | ext[i];
		}
	}

	if (opcode == WS_OPCODE_CLOSE) {
		conn->connected = SWITCH_FALSE;
		return NULL;
	}

	if (payload_len == 0) {
		return NULL;
	}

	payload = malloc(payload_len + 1);
	if (!payload) return NULL;

	if (ws_recv_raw(conn, payload, payload_len, &recvd) != SWITCH_STATUS_SUCCESS || (size_t) recvd < payload_len) {
		free(payload);
		return NULL;
	}

	payload[payload_len] = '\0';

	if (opcode == WS_OPCODE_TEXT || opcode == WS_OPCODE_BINARY) {
		result = switch_core_strdup(pool, (char *) payload);
		free(payload);
		return result;
	}

	free(payload);
	return NULL;
}
