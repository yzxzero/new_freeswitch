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
	/* Buffer for leftover data after HTTP handshake */
	uint8_t leftover[4096];
	size_t leftover_len;
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
	conn->leftover_len = 0;
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
	uint8_t mask_key[4];
	uint8_t *masked_payload = NULL;
	size_t header_len = 2;
	switch_status_t status;
	size_t i;

	header[0] = WS_FIN_BIT | opcode;

	/* RFC 6455: client frames MUST be masked */
	header[1] = 0x80; /* mask bit set */

	if (payload_len <= 125) {
		header[1] |= (uint8_t) payload_len;
	} else if (payload_len <= 65535) {
		header[1] |= 126;
		header[2] = (payload_len >> 8) & 0xFF;
		header[3] = payload_len & 0xFF;
		header_len = 4;
	} else {
		header[1] |= 127;
		memset(&header[2], 0, 8);
		header[9] = payload_len & 0xFF;
		header[8] = (payload_len >> 8) & 0xFF;
		header[7] = (payload_len >> 16) & 0xFF;
		header[6] = (payload_len >> 24) & 0xFF;
		header_len = 10;
	}

	/* Generate random mask key */
	for (i = 0; i < 4; i++) {
		mask_key[i] = (uint8_t) (switch_micro_time_now() + i * 17 + (uintptr_t) &header % 251);
	}

	/* Append mask key to header */
	header[header_len++] = mask_key[0];
	header[header_len++] = mask_key[1];
	header[header_len++] = mask_key[2];
	header[header_len++] = mask_key[3];

	/* Mask the payload */
	if (payload_len > 0 && payload) {
		masked_payload = malloc(payload_len);
		if (!masked_payload) return SWITCH_STATUS_MEMERR;
		for (i = 0; i < payload_len; i++) {
			masked_payload[i] = ((const uint8_t *)payload)[i] ^ mask_key[i % 4];
		}
	}

	switch_mutex_lock(conn->write_mutex);
	status = ws_send_raw(conn, header, header_len);
	if (status == SWITCH_STATUS_SUCCESS && masked_payload && payload_len > 0) {
		status = ws_send_raw(conn, masked_payload, payload_len);
	}
	switch_mutex_unlock(conn->write_mutex);

	switch_safe_free(masked_payload);

	return status;
}

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

static switch_status_t ws_perform_handshake(ws_conn_t *conn, const char *extra_headers[], int header_count)
{
	unsigned char nonce[16];
	char nonce_b64[32];
	char key[64];
	int i;
	switch_stream_handle_t stream = { 0 };
	switch_status_t status;
	char resp[4096];
	ssize_t recvd;
	char *body;

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

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "WS handshake sending to %s:%d\n", conn->host, conn->port);

	status = ws_send_raw(conn, stream.data, strlen((char *) stream.data));
	switch_safe_free(stream.data);

	if (status != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WS handshake send failed\n");
		return status;
	}

	/* Read HTTP response - keep reading until we find \r\n\r\n */
	recvd = 0;
	while (recvd < (ssize_t) sizeof(resp) - 1) {
		ssize_t chunk;
		if (ws_recv_raw(conn, resp + recvd, sizeof(resp) - 1 - recvd, &chunk) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WS handshake recv failed\n");
			return SWITCH_STATUS_FALSE;
		}
		recvd += chunk;
		resp[recvd] = '\0';

		/* Check if we have the complete HTTP headers */
		body = strstr(resp, "\r\n\r\n");
		if (body) {
			body += 4; /* skip \r\n\r\n */
			break;
		}
	}

	if (!strstr(resp, "101")) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WebSocket handshake failed: %s\n", resp);
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "WS handshake success\n");

	/* Save any leftover data (part of first WS frame after HTTP response) */
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

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "WS connecting to %s:%d%s (ssl=%d)\n",
					  conn->host, conn->port, conn->path, conn->use_ssl);

	if (ws_tcp_connect(conn->host, conn->port, &conn->sockfd) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "TCP connect failed to %s:%d\n", conn->host, conn->port);
		return SWITCH_STATUS_FALSE;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "TCP connected to %s:%d\n", conn->host, conn->port);

	if (conn->use_ssl) {
		conn->ssl_ctx = SSL_CTX_new(TLS_client_method());
		if (!conn->ssl_ctx) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "SSL_CTX_new failed\n");
			close(conn->sockfd);
			return SWITCH_STATUS_FALSE;
		}
		SSL_CTX_set_options(conn->ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

		conn->ssl = SSL_new(conn->ssl_ctx);
		SSL_set_fd(conn->ssl, conn->sockfd);

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

	/* Check leftover buffer first */
	if (conn->leftover_len > 0) {
		return SWITCH_TRUE;
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
	uint8_t opcode;
	size_t payload_len;
	uint8_t *payload;
	char *result;
	uint8_t ext[8];
	int i;
	size_t read_pos;
	uint8_t skip_buf[1024];

	if (!session || !session->ws_handle) {
		return NULL;
	}

	conn = (ws_conn_t *) session->ws_handle;
	if (!conn->connected) {
		return NULL;
	}

	/* Read 2-byte frame header, consuming leftover data first */
	read_pos = 0;

	/* Use leftover data from handshake first */
	if (conn->leftover_len > 0) {
		size_t copy;
		copy = conn->leftover_len < 2 ? conn->leftover_len : 2;
		memcpy(header, conn->leftover, copy);
		read_pos = copy;
		if (conn->leftover_len > copy) {
			memmove(conn->leftover, conn->leftover + copy, conn->leftover_len - copy);
		}
		conn->leftover_len -= copy;
	}

	/* Read remaining header bytes */
	if (read_pos < 2) {
		if (ws_recv_exact(conn, header + read_pos, 2 - read_pos) != SWITCH_STATUS_SUCCESS) {
			return NULL;
		}
	}

	opcode = header[0] & 0x0F;

	/* Server frames should not be masked */
	if (header[1] & 0x80) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Received masked server frame, skipping\n");
		return NULL;
	}

	payload_len = header[1] & 0x7F;
	if (payload_len == 126) {
		if (ws_recv_exact(conn, ext, 2) != SWITCH_STATUS_SUCCESS) return NULL;
		payload_len = (ext[0] << 8) | ext[1];
	} else if (payload_len == 127) {
		if (ws_recv_exact(conn, ext, 8) != SWITCH_STATUS_SUCCESS) return NULL;
		payload_len = 0;
		for (i = 0; i < 8; i++) {
			payload_len = (payload_len << 8) | ext[i];
		}
	}

	/* Handle PING: respond with PONG and return NULL to continue reading */
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

	if (payload_len == 0) {
		return NULL;
	}

	/* Sanity check: don't allocate absurd amounts */
	if (payload_len > 1024 * 1024) {
		size_t chunk;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WS frame too large: %zu\n", payload_len);
		while (payload_len > 0) {
			chunk = payload_len > sizeof(skip_buf) ? sizeof(skip_buf) : payload_len;
			if (ws_recv_exact(conn, skip_buf, chunk) != SWITCH_STATUS_SUCCESS) break;
			payload_len -= chunk;
		}
		return NULL;
	}

	payload = malloc(payload_len + 1);
	if (!payload) return NULL;

	if (ws_recv_exact(conn, payload, payload_len) != SWITCH_STATUS_SUCCESS) {
		free(payload);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "WS recv payload failed (%zu bytes)\n", payload_len);
		return NULL;
	}

	payload[payload_len] = '\0';

	if (opcode == WS_OPCODE_TEXT || opcode == WS_OPCODE_BINARY) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "WS recv: opcode=%d len=%zu\n", opcode, payload_len);
		result = switch_core_strdup(pool, (char *) payload);
		free(payload);
		return result;
	}

	free(payload);
	return NULL;
}
