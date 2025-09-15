/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation. All rights reserved.
 *   Copyright (c) 2020 Mellanox Technologies LTD. All rights reserved.
 *   Copyright (c) 2023-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

/** \file
 * TCP socket abstraction layer
 */

#ifndef SPDK_SOCK_H
#define SPDK_SOCK_H

#include "spdk/stdinc.h"

#include "spdk/queue.h"
#include "spdk/json.h"
#include "spdk/assert.h"
#include "spdk/fd_group.h"

#ifdef __cplusplus
extern "C" {
#endif

struct spdk_sock;
struct spdk_sock_group;

struct spdk_sock_buf {
	struct iovec iov;
	struct spdk_sock_buf *next;
};

/**
 * Anywhere this struct is used, an iovec array is assumed to
 * immediately follow the last member in memory, without any
 * padding.
 *
 * A simpler implementation would be to place a 0-length array
 * of struct iovec at the end of this request. However, embedding
 * a structure that ends with a variable length array inside of
 * another structure is a GNU C extension and not standard.
 */
struct spdk_sock_request {
	/* When the request is completed, this callback will be called.
	 * On success, err will be:
	 *   - for writes: 0,
	 *   - for reads: number of bytes read.
	 * On failure: negative errno value.
	 */
	void	(*cb_fn)(void *cb_arg, int err);
	void				*cb_arg;

	/**
	 * These fields are used by the socket layer and should not be modified
	 */
	struct __sock_request_internal {
		TAILQ_ENTRY(spdk_sock_request)	link;

		/**
		 * curr_list is only used in DEBUG mode, but we include it in
		 * release builds too to ensure ABI compatibility between debug
		 * and release builds.
		 */
		void				*curr_list;

		uint32_t			offset;

		/* Indicate if the whole req or part of it is sent with zerocopy */
		bool				is_zcopy;
	} internal;

	uint32_t			*mkeys;
	int				iovcnt;
	/* struct iovec			iov[]; */
};

#define SPDK_SOCK_REQUEST_IOV(req, i) ((struct iovec *)(((uint8_t *)req + sizeof(struct spdk_sock_request)) + (sizeof(struct iovec) * i)))

enum spdk_placement_mode {
	PLACEMENT_NONE,
	PLACEMENT_NAPI,
	PLACEMENT_CPU,
	PLACEMENT_MARK,
};

#define SPDK_TLS_VERSION_1_1 11
#define SPDK_TLS_VERSION_1_2 12
#define SPDK_TLS_VERSION_1_3 13

/**
 * SPDK socket implementation options.
 *
 * A pointer to this structure is used by spdk_sock_impl_get_opts() and spdk_sock_impl_set_opts()
 * to allow the user to request options for the socket module implementation.
 * Each socket module defines which options from this structure are applicable to the module.
 */
struct spdk_sock_impl_opts {
	/**
	 * Minimum size of sock receive buffer. Used by posix and uring socket modules.
	 */
	uint32_t recv_buf_size;

	/**
	 * Minimum size of sock send buffer. Used by posix and uring socket modules.
	 */
	uint32_t send_buf_size;

	/**
	 * Enable or disable receive pipe. Used by posix and uring socket modules.
	 */
	bool enable_recv_pipe;

	/**
	 * **Deprecated, please use enable_zerocopy_send_server or enable_zerocopy_send_client instead**
	 * Enable or disable use of zero copy flow on send. Used by posix socket module.
	 */
	bool enable_zerocopy_send;

	/**
	 * Enable or disable quick ACK. Used by posix and uring socket modules.
	 */
	bool enable_quickack;

	/**
	 * Enable or disable placement_id. Used by posix and uring socket modules.
	 * Valid values in the enum spdk_placement_mode.
	 */
	uint32_t enable_placement_id;

	/**
	 * Enable or disable use of zero copy flow on send for server sockets. Used by posix and uring socket modules.
	 */
	bool enable_zerocopy_send_server;

	/**
	 * Enable or disable use of zero copy flow on send for client sockets. Used by posix and uring socket modules.
	 */
	bool enable_zerocopy_send_client;

	/**
	 * Set zerocopy threshold in bytes. A consecutive sequence of requests' iovecs that fall below this
	 * threshold may be sent without zerocopy flag set.
	 */
	uint32_t zerocopy_threshold;

	/**
	 * Wait for microseconds to skip flushing requests.
	 */
	uint32_t flush_batch_timeout;

	/**
	 * Skip flushing requests if queued iovcnt is smaller than this threshold.
	 */
	int flush_batch_iovcnt_threshold;

	/**
	 * Skip flushing requests if queued total Length is smaller than this threshold.
	 */
	uint32_t flush_batch_bytes_threshold;

	/**
	 * TLS protocol version. Used by ssl socket module.
	 */
	uint32_t tls_version;

	/**
	 * Enable or disable kernel TLS. Used by ssl socket modules.
	 */
	bool enable_ktls;

	/**
	 * Set default PSK key. Used by ssl socket module.
	 */
	uint8_t *psk_key;

	/**
	 * Size of psk_key.
	 */
	uint32_t psk_key_size;

	/**
	 * Set default PSK identity. Used by ssl socket module.
	 */
	char *psk_identity;

	/**
	 * Optional callback to retrieve PSK based on client's identity.
	 *
	 * \param out Buffer for PSK in binary format to be filled with found key.
	 * \param out_len Length of "out" buffer.
	 * \param cipher Cipher suite to be set by this callback.
	 * \param psk_identity PSK identity for which the key needs to be found.
	 * \param get_key_ctx Context for this callback.
	 *
	 * \return key length on success, -1 on failure.
	 */
	int (*get_key)(uint8_t *out, int out_len, const char **cipher, const char *psk_identity,
		       void *get_key_ctx);

	/**
	 * Context to be passed to get_key() callback.
	 */
	void *get_key_ctx;

	/**
	 * Cipher suite. Used by ssl socket module.
	 * For connecting side, it must contain a single cipher:
	 * example: "TLS_AES_256_GCM_SHA384"
	 *
	 * For listening side, it may be a colon separated list of ciphers:
	 * example: "TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256"
	 */
	const char *tls_cipher_suites;

	/**
	 * Enable or disable use of zero copy flow on receive. Used by xlio socket module.
	 */
	bool enable_zerocopy_recv;

	/**
	 * Enable or disable use of TCP_NODELAY socket option. Used by xlio socket module.
	 */
	bool enable_tcp_nodelay;

	/**
	 * Socket buffers per poll group pool size. Used by xlio socket module.
	 */
	uint32_t buffers_pool_size;

	/**
	 * Packets per poll group pool size. Used by xlio socket module.
	 */
	uint32_t packets_pool_size;

	/**
	 * Enable or disable early initialization. Used by xlio socket module.
	 */
	bool enable_early_init;
};

/**
 * SPDK socket capabilities. The structure is used to get capabilities specific for a socket instance
 */
struct spdk_sock_caps {
	bool zcopy_send;
	void *ibv_pd;
	bool zcopy_recv;
};

/**
 * Spdk socket initialization options.
 *
 * A pointer to this structure will be used by spdk_sock_listen_ext() or spdk_sock_connect_ext() to
 * allow the user to request non-default options on the socket.
 */
struct spdk_sock_opts {
	/**
	 * The size of spdk_sock_opts according to the caller of this library is used for ABI
	 * compatibility.  The library uses this field to know how many fields in this
	 * structure are valid. And the library will populate any remaining fields with default values.
	 */
	size_t opts_size;

	/**
	 * The priority on the socket and default value is zero.
	 */
	int priority;

	/**
	 * Used to enable or disable zero copy on socket layer.
	 */
	bool zcopy;

	/* Hole at bytes 13-15. */
	uint8_t reserved13[3];

	/**
	 * Time in msec to wait ack until connection is closed forcefully.
	 */
	uint32_t ack_timeout;

	/* Hole at bytes 20-23. */
	uint8_t reserved[4];

	/**
	 * Socket implementation options.  If non-NULL, these will override those set by
	 * spdk_sock_impl_set_opts().  The library copies this structure internally, so the user can
	 * free it immediately after a spdk_sock_connect()/spdk_sock_listen() call.
	 */
	struct spdk_sock_impl_opts *impl_opts;

	/**
	 * Size of the impl_opts structure.
	 */
	size_t impl_opts_size;

	/**
	 * Source address.  If NULL, any available address will be used.  Only valid for connect().
	 */
	const char *src_addr;

	/**
	 * Source port.  If zero, a random ephemeral port will be used.  Only valid for connect().
	 */
	uint16_t src_port;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_sock_opts) == 56, "Incorrect size");

/**
 * Initialize the default value of opts.
 *
 * \param opts Data structure where SPDK will initialize the default sock options.
 * Users must set opts_size to sizeof(struct spdk_sock_opts).  This will ensure that the
 * libraryonly tries to fill as many fields as allocated by the caller. This allows ABI
 * compatibility with future versions of this library that may extend the spdk_sock_opts
 * structure.
 */
void spdk_sock_get_default_opts(struct spdk_sock_opts *opts);

/**
 * Get client and server addresses of the given socket.
 *
 * \param sock Socket to get address.
 * \param saddr A pointer to the buffer to hold the address of server.
 * \param slen Length of the buffer 'saddr'.
 * \param sport A pointer(May be NULL) to the buffer to hold the port info of server.
 * \param caddr A pointer to the buffer to hold the address of client.
 * \param clen Length of the buffer 'caddr'.
 * \param cport A pointer(May be NULL) to the buffer to hold the port info of server.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_sock_getaddr(struct spdk_sock *sock, char *saddr, int slen, uint16_t *sport,
		      char *caddr, int clen, uint16_t *cport);

/**
 * Get socket implementation name.
 *
 * \param sock Pointer to SPDK socket.
 *
 * \return Implementation name of given socket.
 */
const char *spdk_sock_get_impl_name(struct spdk_sock *sock);

/**
 * Create a socket using the specific sock implementation, connect the socket
 * to the specified address and port (of the server), and then return the socket.
 * This function is used by client.
 *
 * \param ip IP address of the server.
 * \param port Port number of the server.
 * \param impl_name The sock_implementation to use, such as "posix". If impl_name is
 * specified, it will *only* try to connect on that impl. If it is NULL, it will try
 * all the sock implementations in order and uses the first sock implementation which
 * can connect.
 *
 * \return a pointer to the connected socket on success, or NULL on failure.
 */
struct spdk_sock *spdk_sock_connect(const char *ip, int port, const char *impl_name);

/**
 * Create a socket using the specific sock implementation, connect the socket
 * to the specified address and port (of the server), and then return the socket.
 * This function is used by client.
 *
 * \param ip IP address of the server.
 * \param port Port number of the server.
 * \param impl_name The sock_implementation to use, such as "posix". If impl_name is
 * specified, it will *only* try to connect on that impl. If it is NULL, it will try
 * all the sock implementations in order and uses the first sock implementation which
 * can connect.
 * \param opts The sock option pointer provided by the user which should not be NULL pointer.
 *
 * \return a pointer to the connected socket on success, or NULL on failure.
 */
struct spdk_sock *spdk_sock_connect_ext(const char *ip, int port, const char *impl_name,
					struct spdk_sock_opts *opts);

/**
 * Create a socket using the specific sock implementation, bind the socket to
 * the specified address and port and listen on the socket, and then return the socket.
 * This function is used by server.
 *
 * \param ip IP address to listen on.
 * \param port Port number.
 * \param impl_name The sock_implementation to use, such as "posix". If impl_name is
 * specified, it will *only* try to listen on that impl. If it is NULL, it will try
 * all the sock implementations in order and uses the first sock implementation which
 * can listen.
 *
 * \return a pointer to the listened socket on success, or NULL on failure.
 */
struct spdk_sock *spdk_sock_listen(const char *ip, int port, const char *impl_name);

/**
 * Create a socket using the specific sock implementation, bind the socket to
 * the specified address and port and listen on the socket, and then return the socket.
 * This function is used by server.
 *
 * \param ip IP address to listen on.
 * \param port Port number.
 * \param impl_name The sock_implementation to use, such as "posix". If impl_name is
 * specified, it will *only* try to listen on that impl. If it is NULL, it will try
 * all the sock implementations in order and uses the first sock implementation which
 * can listen.
 * \param opts The sock option pointer provided by the user, which should not be NULL pointer.
 *
 * \return a pointer to the listened socket on success, or NULL on failure.
 */
struct spdk_sock *spdk_sock_listen_ext(const char *ip, int port, const char *impl_name,
				       struct spdk_sock_opts *opts);

/**
 * Accept a new connection from a client on the specified socket and return a
 * socket structure which holds the connection.
 *
 * \param sock Listening socket.
 *
 * \return a pointer to the accepted socket on success, or NULL on failure.
 */
struct spdk_sock *spdk_sock_accept(struct spdk_sock *sock);

/**
 * Gets the name of the network interface of the local port for the socket.
 *
 * \param sock socket to find the interface name for
 *
 * \return null-terminated string containing interface name if found, NULL
 *	   interface name could not be found
 */
const char *spdk_sock_get_interface_name(struct spdk_sock *sock);


/**
 * Gets the NUMA node ID for the network interface of the local port for the TCP socket.
 *
 * \param sock TCP socket to find the NUMA socket ID for
 *
 * \return NUMA ID, or SPDK_ENV_NUMA_ID_ANY if the NUMA ID is unknown
 */
int32_t spdk_sock_get_numa_id(struct spdk_sock *sock);

/**
 * Close a socket.
 *
 * \param sock Socket to close.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_sock_close(struct spdk_sock **sock);

/**
 * Flush a socket from data gathered in previous writev_async calls.
 *
 * \param sock Socket to flush.
 *
 * \return number of bytes sent on success, -1 (with errno set) on failure
 */
int spdk_sock_flush(struct spdk_sock *sock);

/**
 * Receive a message from the given socket.
 *
 * \param sock Socket to receive message.
 * \param buf Pointer to a buffer to hold the data.
 * \param len Length of the buffer.
 *
 * \return the length of the received message on success, -1 on failure.
 */
ssize_t spdk_sock_recv(struct spdk_sock *sock, void *buf, size_t len);

/**
 * Write message to the given socket from the I/O vector array.
 *
 * \param sock Socket to write to.
 * \param iov I/O vector.
 * \param iovcnt Number of I/O vectors in the array.
 *
 * \return the length of written message on success, -1 on failure.
 */
ssize_t spdk_sock_writev(struct spdk_sock *sock, struct iovec *iov, int iovcnt);

/**
 * Write data to the given socket asynchronously, calling
 * the provided callback when the data has been written.
 *
 * \param sock Socket to write to.
 * \param req The write request to submit.
 */
void spdk_sock_writev_async(struct spdk_sock *sock, struct spdk_sock_request *req);

/**
 * Read message from the given socket to the I/O vector array.
 *
 * \param sock Socket to receive message.
 * \param iov I/O vector.
 * \param iovcnt Number of I/O vectors in the array.
 *
 * \return the length of the received message on success, -1 on failure.
 */
ssize_t spdk_sock_readv(struct spdk_sock *sock, struct iovec *iov, int iovcnt);

/**
 * Receive the next portion of the stream from the socket.
 *
 * A buffer provided to this socket's group's pool using
 * spdk_sock_group_provide_buf() will contain the data and be
 * returned in *buf.
 *
 * Note that the amount of data in buf is determined entirely by
 * the sock layer. You cannot request to receive only a limited
 * amount here. You simply get whatever the next portion of the stream
 * is, as determined by the sock module. You can place an upper limit
 * on the size of the buffer since these buffers are originally
 * provided to the group through spdk_sock_group_provide_buf().
 *
 * This code path will only work if the recvbuf is disabled. To disable
 * the recvbuf, call spdk_sock_set_recvbuf with a size of 0.
 *
 * \param sock Socket to receive from.
 * \param buf Populated with the next portion of the stream
 * \param ctx Returned context pointer from when the buffer was provided.
 *
 * \return On success, the length of the buffer placed into buf, On failure, -1 with errno set.
 */
int spdk_sock_recv_next(struct spdk_sock *sock, void **buf, void **ctx);

/**
 * Set the value used to specify the low water mark (in bytes) for this socket.
 *
 * \param sock Socket to set for.
 * \param nbytes Value for recvlowat.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_sock_set_recvlowat(struct spdk_sock *sock, int nbytes);

/**
 * Set receive buffer size for the given socket.
 *
 * \param sock Socket to set buffer size for.
 * \param sz Buffer size in bytes.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_sock_set_recvbuf(struct spdk_sock *sock, int sz);

/**
 * Set send buffer size for the given socket.
 *
 * \param sock Socket to set buffer size for.
 * \param sz Buffer size in bytes.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_sock_set_sendbuf(struct spdk_sock *sock, int sz);

/**
 * Check whether the address of socket is ipv6.
 *
 * \param sock Socket to check.
 *
 * \return true if the address of socket is ipv6, or false otherwise.
 */
bool spdk_sock_is_ipv6(struct spdk_sock *sock);

/**
 * Check whether the address of socket is ipv4.
 *
 * \param sock Socket to check.
 *
 * \return true if the address of socket is ipv4, or false otherwise.
 */
bool spdk_sock_is_ipv4(struct spdk_sock *sock);

/**
 * Check whether the socket is currently connected.
 *
 * \param sock Socket to check
 *
 * \return true if the socket is connected or false otherwise.
 */
bool spdk_sock_is_connected(struct spdk_sock *sock);

/**
 * Callback function for spdk_sock_group_add_sock().
 *
 * \param arg Argument for the callback function.
 * \param group Socket group.
 * \param sock Socket.
 */
typedef void (*spdk_sock_cb)(void *arg, struct spdk_sock_group *group, struct spdk_sock *sock);

struct spdk_sock_group_opts {
	/** Size of this structure */
	size_t		size;
	/** User context */
	void		*ctx;
	/** Enable interrupt support */
	bool		interrupt;
};

/**
 * Create a new socket group.
 *
 * \param opts Socket group options.
 *
 * \return a pointer to the created group on success, or NULL on failure.
 */
struct spdk_sock_group *spdk_sock_group_create_ext(struct spdk_sock_group_opts *opts);

/**
 * Create a new socket group with user provided pointer
 *
 * \param ctx the context provided by user.
 * \return a pointer to the created group on success, or NULL on failure.
 */
struct spdk_sock_group *spdk_sock_group_create(void *ctx);

/**
 * Get the ctx of the sock group
 *
 * \param sock_group Socket group.
 * \return a pointer which is ctx of the sock_group.
 */
void *spdk_sock_group_get_ctx(struct spdk_sock_group *sock_group);


/**
 * Add a socket to the group.
 *
 * \param group Socket group.
 * \param sock Socket to add.
 * \param cb_fn Called when the operation completes.
 * \param cb_arg Argument passed to the callback function.
 *
 * \return 0 on success, -1 on failure with errno set.
 */
int spdk_sock_group_add_sock(struct spdk_sock_group *group, struct spdk_sock *sock,
			     spdk_sock_cb cb_fn, void *cb_arg);

/**
 * Remove a socket from the group.
 *
 * \param group Socket group.
 * \param sock Socket to remove.
 *
 * \return 0 on success, -1 on failure with errno set.
 */
int spdk_sock_group_remove_sock(struct spdk_sock_group *group, struct spdk_sock *sock);

/**
 * Provides a buffer to the group to be used in its receive pool.
 * See spdk_sock_recv_next() for more details.
 *
 * \param group Socket group.
 * \param buf Pointer the buffer provided.
 * \param len Length of the buffer.
 * \param ctx Pointer that will be returned in spdk_sock_recv_next()
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_sock_group_provide_buf(struct spdk_sock_group *group, void *buf, size_t len, void *ctx);

/**
 * Poll incoming events for each registered socket.
 *
 * \param group Group to poll.
 *
 * \return the number of events on success, -1 on failure.
 */
int spdk_sock_group_poll(struct spdk_sock_group *group);

/**
 * Poll incoming events up to max_events for each registered socket.
 *
 * \param group Group to poll.
 * \param max_events Number of maximum events to poll for each socket.
 *
 * \return the number of events on success, -1 on failure.
 */
int spdk_sock_group_poll_count(struct spdk_sock_group *group, int max_events);

/**
 * Close all registered sockets of the group and then remove the group.
 *
 * \param group Group to close.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_sock_group_close(struct spdk_sock_group **group);

/**
 * Get the optimal sock group for this sock.
 *
 * \param sock The socket
 * \param group Returns the optimal sock group. If there is no optimal sock group, returns NULL.
 * \param hint When return is 0 and group is set to NULL, hint is used to set optimal sock group for the socket.
 *
 * \return 0 on success. Negated errno on failure.
 */
int spdk_sock_get_optimal_sock_group(struct spdk_sock *sock, struct spdk_sock_group **group,
				     struct spdk_sock_group *hint);

/**
 * Get current socket implementation options.
 *
 * \param impl_name The socket implementation to use, such as "posix".
 * \param opts Pointer to allocated spdk_sock_impl_opts structure that will be filled with actual values.
 * \param len On input specifies size of passed opts structure. On return it is set to actual size that was filled with values.
 *
 * \return 0 on success, -1 on failure. errno is set to indicate the reason of failure.
 */
int spdk_sock_impl_get_opts(const char *impl_name, struct spdk_sock_impl_opts *opts, size_t *len);

/**
 * Set socket implementation options.
 *
 * \param impl_name The socket implementation to use, such as "posix".
 * \param opts Pointer to allocated spdk_sock_impl_opts structure with new options values.
 * \param len Size of passed opts structure.
 *
 * \return 0 on success, -1 on failure. errno is set to indicate the reason of failure.
 */
int spdk_sock_impl_set_opts(const char *impl_name, const struct spdk_sock_impl_opts *opts,
			    size_t len);

/**
 * Set the given sock implementation to be used as the default one.
 *
 * Note: passing a specific sock implementation name in some sock API functions
 * (such as @ref spdk_sock_connect, @ref spdk_sock_listen and etc) ignores the default value set by this function.
 *
 * \param impl_name The socket implementation to use, such as "posix".
 * \return 0 on success, -1 on failure. errno is set to indicate the reason of failure.
 */
int spdk_sock_set_default_impl(const char *impl_name);

/**
 * Get the name of the current default implementation
 *
 * \return The name of the default implementation
 */
const char *spdk_sock_get_default_impl(void);;

/**
 * Write socket subsystem configuration into provided JSON context.
 *
 * \param w JSON write context
 */
void spdk_sock_write_config_json(struct spdk_json_write_ctx *w);

/**
 * Obtain an fd that becomes ready when one of the sockets in this group is ready. This fd
 * is suitable for use in APIs such as spdk_interrupt_register_for_events().  This fd is only
 * available if the group was created with interrupt support enabled.
 *
 * \param group Socket group.
 *
 * \return fd (>0) on success or negative errno on failure.
 */
int spdk_sock_group_get_interruptfd(struct spdk_sock_group *group);

/**
 * Obtain an fd_group that becomes ready when one of the sockets in this group is ready. This
 * fd_group is suitable for use in APIs such as spdk_interrupt_register_fd_group().  fd_group is
 * available only if the group was created with interrupt support enabled.
 *
 * \param group Socket group.
 *
 * \return fd_group on success or NULL if it's not available.
 */
struct spdk_fd_group *spdk_sock_group_get_fd_group(struct spdk_sock_group *group);

/**
 * Get socket capabilities
 *
 * \param sock Socket to be quered
 * \param caps Capabilities structure to be filled
 * \return 0 on success, negated errno on failure
 */
int spdk_sock_get_caps(struct spdk_sock *sock, struct spdk_sock_caps *caps);

/**
 * Receive a message from the given socket in zero copy mode.
 *
 * Messages are delivered in chunks as a list of @ref spdk_sock_buf
 * structures. These structures point to buffers which belong to
 * socket layer. When data in the buffers is consumed buffers shall be
 * released with @ref spdk_sock_free_bufs function.
 *
 * \param sock Socket to receive message.
 * \param len Length of the data to be read.
 * \param sock_buf Placeholder for pointer to socket buffers list.
 *
 * \return the length of the received message on success, -1 on failure.
 */
ssize_t spdk_sock_recv_zcopy(struct spdk_sock *sock, size_t len, struct spdk_sock_buf **sock_buf);

/**
 * Release socket buffers.
 *
 * This function can be used to release multiple buffers at once if
 * they are chained together. Buffers do not have to be released in
 * the same order and same batches as they were received.
 *
 * \param sock Socket on which buffers were received.
 * \param sock_buf Pointer to buffers list to release.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_sock_free_bufs(struct spdk_sock *sock, struct spdk_sock_buf *sock_buf);

/**
 * Initialize socket layer.
 *
 * This function will initialize generic socket layer and all
 * registered socket implementations.
 */
void spdk_sock_initialize(void);

/**
 * Deinitialize socket layer.
 *
 * This function will deinitialize generic socket layer and all
 * registered socket implementations.
 */
void spdk_sock_deinitialize(void);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_SOCK_H */
