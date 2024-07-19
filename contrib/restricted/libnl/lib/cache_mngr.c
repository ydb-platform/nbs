/* SPDX-License-Identifier: LGPL-2.1-only */
/*
 * Copyright (c) 2003-2012 Thomas Graf <tgraf@suug.ch>
 */

/**
 * @ingroup cache_mngt
 * @defgroup cache_mngr Manager
 * @brief Manager keeping caches up to date automatically.
 *
 * The cache manager keeps caches up to date automatically by listening to
 * netlink notifications and integrating the received information into the
 * existing cache.
 *
 * @note This functionality is still considered experimental.
 *
 * Related sections in the development guide:
 * - @core_doc{_cache_manager,Cache Manager}
 *
 * @{
 *
 * Header
 * ------
 * ~~~~{.c}
 * #include <netlink/cache.h>
 * ~~~~
 */

#include "nl-default.h"

#include <netlink/netlink.h>
#include <netlink/cache.h>
#include <netlink/utils.h>

#include "nl-core.h"
#include "nl-priv-dynamic-core/nl-core.h"
#include "nl-priv-dynamic-core/cache-api.h"
#include "nl-aux-core/nl-core.h"

/** @cond SKIP */
struct nl_cache_mngr
{
	int			cm_protocol;
	int			cm_flags;
	int			cm_nassocs;
	struct nl_sock *	cm_sock;
	struct nl_sock *	cm_sync_sock;
	struct nl_cache_assoc *	cm_assocs;
};

#define NASSOC_INIT		16
#define NASSOC_EXPAND		8
/** @endcond */

static int include_cb(struct nl_object *obj, struct nl_parser_param *p)
{
	struct nl_cache_assoc *ca = p->pp_arg;
	struct nl_cache_ops *ops = ca->ca_cache->c_ops;

	NL_DBG(2, "Including object %p into cache %p\n", obj, ca->ca_cache);
#ifdef NL_DEBUG
	if (nl_debug >= 4)
		nl_object_dump(obj, &nl_debug_dp);
#endif

	if (ops->co_event_filter)
		if (ops->co_event_filter(ca->ca_cache, obj) != NL_OK)
			return 0;

	if (ops->co_include_event)
		return ops->co_include_event(ca->ca_cache, obj, ca->ca_change,
					     ca->ca_change_v2,
					     ca->ca_change_data);
	else {
		if (ca->ca_change_v2)
			return nl_cache_include_v2(ca->ca_cache, obj, ca->ca_change_v2, ca->ca_change_data);
		else
			return nl_cache_include(ca->ca_cache, obj, ca->ca_change, ca->ca_change_data);
	}

}

static int event_input(struct nl_msg *msg, void *arg)
{
	struct nl_cache_mngr *mngr = arg;
	int protocol = nlmsg_get_proto(msg);
	int type = nlmsg_hdr(msg)->nlmsg_type;
	struct nl_cache_ops *ops;
	int i, n;
	struct nl_parser_param p = {
		.pp_cb = include_cb,
	};

	NL_DBG(2, "Cache manager %p, handling new message %p as event\n",
	       mngr, msg);
#ifdef NL_DEBUG
	if (nl_debug >= 4)
		nl_msg_dump(msg, stderr);
#endif

	if (mngr->cm_protocol != protocol)
		BUG();

	for (i = 0; i < mngr->cm_nassocs; i++) {
		if (mngr->cm_assocs[i].ca_cache) {
			ops = mngr->cm_assocs[i].ca_cache->c_ops;
			for (n = 0; ops->co_msgtypes[n].mt_id >= 0; n++)
				if (ops->co_msgtypes[n].mt_id == type)
					goto found;
		}
	}

	return NL_SKIP;

found:
	NL_DBG(2, "Associated message %p to cache %p\n",
	       msg, mngr->cm_assocs[i].ca_cache);
	p.pp_arg = &mngr->cm_assocs[i];

	return nl_cache_parse(ops, NULL, nlmsg_hdr(msg), &p);
}

/**
 * Allocate new cache manager
 * @arg sk		Netlink socket or NULL to auto allocate
 * @arg protocol	Netlink protocol this manager is used for
 * @arg flags		Flags (\c NL_AUTO_PROVIDE)
 * @arg result		Result pointer
 *
 * Allocates a new cache manager for the specified netlink protocol.
 *
 * 1. If sk is not specified (\c NULL) a netlink socket matching the
 *    specified protocol will be automatically allocated.
 *
 * 2. The socket will be put in non-blocking mode and sequence checking
 *    will be disabled regardless of whether the socket was provided by
 *    the caller or automatically allocated.
 *
 * 3. The socket will be connected.
 *
 * If the flag \c NL_AUTO_PROVIDE is specified, any cache added to the
 * manager will automatically be made available to other users using
 * nl_cache_mngt_provide().
 *
 * @note If the socket is provided by the caller, it is NOT recommended
 *       to use the socket for anything else besides receiving netlink
 *       notifications.
 *
 * @return 0 on success or a negative error code.
 */
int nl_cache_mngr_alloc(struct nl_sock *sk, int protocol, int flags,
			struct nl_cache_mngr **result)
{
	struct nl_cache_mngr *mngr;
	int err = -NLE_NOMEM;

	/* Catch abuse of flags */
	if (flags & NL_ALLOCATED_SOCK)
		BUG();

	mngr = calloc(1, sizeof(*mngr));
	if (!mngr)
		return -NLE_NOMEM;

	if (!sk) {
		if (!(sk = nl_socket_alloc()))
			goto errout;

		flags |= NL_ALLOCATED_SOCK;
	}

	mngr->cm_sock = sk;
	mngr->cm_nassocs = NASSOC_INIT;
	mngr->cm_protocol = protocol;
	mngr->cm_flags = flags;
	mngr->cm_assocs = calloc(mngr->cm_nassocs,
				 sizeof(struct nl_cache_assoc));
	if (!mngr->cm_assocs)
		goto errout;

	/* Required to receive async event notifications */
	nl_socket_disable_seq_check(mngr->cm_sock);

	if ((err = nl_connect(mngr->cm_sock, protocol)) < 0)
		goto errout;

	if ((err = nl_socket_set_nonblocking(mngr->cm_sock)) < 0)
		goto errout;

	/* Create and allocate socket for sync cache fills */
	mngr->cm_sync_sock = nl_socket_alloc();
	if (!mngr->cm_sync_sock) {
		err = -NLE_NOMEM;
		goto errout;
	}
	if ((err = nl_connect(mngr->cm_sync_sock, protocol)) < 0)
		goto errout_free_sync_sock;

	NL_DBG(1, "Allocated cache manager %p, protocol %d, %d caches\n",
	       mngr, protocol, mngr->cm_nassocs);

	*result = mngr;
	return 0;

errout_free_sync_sock:
	nl_socket_free(mngr->cm_sync_sock);
errout:
	nl_cache_mngr_free(mngr);
	return err;
}

/**
 * Set change_func_v2 for cache manager
 * @arg mngr		Cache manager.
 * @arg cache		Cache associated with the callback
 * @arg cb		Function to be called upon changes.
 * @arg data		Argument passed on to change callback
 *
 * Adds callback change_func_v2 to a registered cache. This callback provides
 * in like the standard change_func the added or remove netlink object. In case
 * of a change the old and the new object is provided as well as the according
 * diff. If this callback is registered this has a higher priority then the
 * change_func registered during cache registration. Hence only one callback is
 * executed.
 *
 * The first netlink object in the callback is refering to the old object and
 * the second to the new. This means on NL_ACT_CHANGE the first is the previous
 * object in the cache and the second the updated version. On NL_ACT_DEL the
 * first is the deleted object the second is NULL. On NL_ACT_NEW the first is
 * NULL and the second the new netlink object.
 *
 * The user is responsible for calling nl_cache_mngr_poll() or monitor
 * the socket and call nl_cache_mngr_data_ready() to allow the library
 * to process netlink notification events.
 *
 * @see nl_cache_mngr_poll()
 * @see nl_cache_mngr_data_ready()
 *
 * @return 0 on success or a negative error code.
 * @return -NLE_PROTO_MISMATCH Protocol mismatch between cache manager and
 * 			       cache type
 * @return -NLE_OPNOTSUPP Cache type does not support updates
 * @return -NLE_RANGE Cache of this type is not registered
 */
static int nl_cache_mngr_set_change_func_v2(struct nl_cache_mngr *mngr,
					    struct nl_cache *cache,
					    change_func_v2_t cb, void *data)
{
	struct nl_cache_ops *ops;
	int i;

	ops = cache->c_ops;
	if (!ops)
		return -NLE_INVAL;

	if (ops->co_protocol != mngr->cm_protocol)
		return -NLE_PROTO_MISMATCH;

	if (ops->co_groups == NULL)
		return -NLE_OPNOTSUPP;

	for (i = 0; i < mngr->cm_nassocs; i++)
		if (mngr->cm_assocs[i].ca_cache == cache)
			break;

	if (i >= mngr->cm_nassocs) {
		return -NLE_RANGE;
	}

	mngr->cm_assocs[i].ca_change_v2 = cb;
	mngr->cm_assocs[i].ca_change_data = data;

	return 0;
}

/**
 * Add cache to cache manager
 * @arg mngr		Cache manager.
 * @arg cache		Cache to be added to cache manager
 * @arg cb		Function to be called upon changes.
 * @arg data		Argument passed on to change callback
 *
 * Adds cache to the manager. The operation will trigger a full
 * dump request from the kernel to initially fill the contents
 * of the cache. The manager will subscribe to the notification group
 * of the cache and keep track of any further changes.
 *
 * The user is responsible for calling nl_cache_mngr_poll() or monitor
 * the socket and call nl_cache_mngr_data_ready() to allow the library
 * to process netlink notification events.
 *
 * @see nl_cache_mngr_poll()
 * @see nl_cache_mngr_data_ready()
 *
 * @return 0 on success or a negative error code.
 * @return -NLE_PROTO_MISMATCH Protocol mismatch between cache manager and
 * 			       cache type
 * @return -NLE_OPNOTSUPP Cache type does not support updates
 * @return -NLE_EXIST Cache of this type already being managed
 */
int nl_cache_mngr_add_cache(struct nl_cache_mngr *mngr, struct nl_cache *cache,
		      change_func_t cb, void *data)
{
	struct nl_cache_ops *ops;
	struct nl_af_group *grp;
	int err, i;

	ops = cache->c_ops;
	if (!ops)
		return -NLE_INVAL;

	if (ops->co_protocol != mngr->cm_protocol)
		return -NLE_PROTO_MISMATCH;

	if (ops->co_groups == NULL)
		return -NLE_OPNOTSUPP;

	for (i = 0; i < mngr->cm_nassocs; i++)
		if (mngr->cm_assocs[i].ca_cache &&
		    mngr->cm_assocs[i].ca_cache->c_ops == ops)
			return -NLE_EXIST;

	for (i = 0; i < mngr->cm_nassocs; i++)
		if (!mngr->cm_assocs[i].ca_cache)
			break;

	if (i >= mngr->cm_nassocs) {
		struct nl_cache_assoc *cm_assocs;
		int cm_nassocs = mngr->cm_nassocs + NASSOC_EXPAND;

		cm_assocs = realloc(mngr->cm_assocs,
				    cm_nassocs * sizeof(struct nl_cache_assoc));
		if (cm_assocs == NULL)
			return -NLE_NOMEM;

		memset(cm_assocs + mngr->cm_nassocs, 0,
		       NASSOC_EXPAND * sizeof(struct nl_cache_assoc));
		mngr->cm_assocs = cm_assocs;
		mngr->cm_nassocs = cm_nassocs;

		NL_DBG(1, "Increased capacity of cache manager %p " \
			  "to %d\n", mngr, mngr->cm_nassocs);
	}

	for (grp = ops->co_groups; grp->ag_group; grp++) {
		err = nl_socket_add_membership(mngr->cm_sock, grp->ag_group);
		if (err < 0)
			return err;
	}

	err = nl_cache_refill(mngr->cm_sync_sock, cache);
	if (err < 0)
		goto errout_drop_membership;

	mngr->cm_assocs[i].ca_cache = cache;
	mngr->cm_assocs[i].ca_change = cb;
	mngr->cm_assocs[i].ca_change_data = data;

	if (mngr->cm_flags & NL_AUTO_PROVIDE)
		nl_cache_mngt_provide(cache);

	NL_DBG(1, "Added cache %p <%s> to cache manager %p\n",
	       cache, nl_cache_name(cache), mngr);

	return 0;

errout_drop_membership:
	for (grp = ops->co_groups; grp->ag_group; grp++)
		nl_socket_drop_membership(mngr->cm_sock, grp->ag_group);

	return err;
}

/**
 * Add cache to cache manager
 * @arg mngr		Cache manager.
 * @arg cache		Cache to be added to cache manager
 * @arg cb		V2 function to be called upon changes.
 * @arg data		Argument passed on to change callback
 *
 * Adds cache to the manager. The operation will trigger a full
 * dump request from the kernel to initially fill the contents
 * of the cache. The manager will subscribe to the notification group
 * of the cache and keep track of any further changes.
 *
 * The user is responsible for calling nl_cache_mngr_poll() or monitor
 * the socket and call nl_cache_mngr_data_ready() to allow the library
 * to process netlink notification events.
 *
 * @see nl_cache_mngr_poll()
 * @see nl_cache_mngr_data_ready()
 *
 * @return 0 on success or a negative error code.
 * @return -NLE_PROTO_MISMATCH Protocol mismatch between cache manager and
 * 			       cache type
 * @return -NLE_OPNOTSUPP Cache type does not support updates
 * @return -NLE_EXIST Cache of this type already being managed
 */
int nl_cache_mngr_add_cache_v2(struct nl_cache_mngr *mngr, struct nl_cache *cache,
		      change_func_v2_t cb, void *data) {
	int err;
	err = nl_cache_mngr_add_cache(mngr, cache, NULL, NULL);
	if (err < 0)
		return err;

	return nl_cache_mngr_set_change_func_v2(mngr, cache, cb, data);
}

/**
 * Add cache to cache manager
 * @arg mngr		Cache manager.
 * @arg name		Name of cache to keep track of
 * @arg cb		Function to be called upon changes.
 * @arg data		Argument passed on to change callback
 * @arg result		Pointer to store added cache (optional)
 *
 * Allocates a new cache of the specified type and adds it to the manager.
 * The operation will trigger a full dump request from the kernel to
 * initially fill the contents of the cache. The manager will subscribe
 * to the notification group of the cache and keep track of any further
 * changes.
 *
 * The user is responsible for calling nl_cache_mngr_poll() or monitor
 * the socket and call nl_cache_mngr_data_ready() to allow the library
 * to process netlink notification events.
 *
 * @note Versions up to 3.4.0 actually required the result argument, preventing
 * 	 NULL to be passed.
 *
 * @see nl_cache_mngr_poll()
 * @see nl_cache_mngr_data_ready()
 *
 * @return 0 on success or a negative error code.
 * @return -NLE_NOCACHE Unknown cache type
 * @return -NLE_PROTO_MISMATCH Protocol mismatch between cache manager and
 * 			       cache type
 * @return -NLE_OPNOTSUPP Cache type does not support updates
 * @return -NLE_EXIST Cache of this type already being managed
 */
int nl_cache_mngr_add(struct nl_cache_mngr *mngr, const char *name,
		      change_func_t cb, void *data, struct nl_cache **result)
{
	struct nl_cache_ops *ops;
	struct nl_cache *cache;
	int err;

	ops = nl_cache_ops_lookup_safe(name);
	if (!ops)
		return -NLE_NOCACHE;

	cache = nl_cache_alloc(ops);
	nl_cache_ops_put(ops);
	if (!cache)
		return -NLE_NOMEM;

	err = nl_cache_mngr_add_cache(mngr, cache, cb, data);
	if (err < 0)
		goto errout_free_cache;

	if (result)
		*result = cache;
	return 0;

errout_free_cache:
	nl_cache_free(cache);

	return err;
}

/**
 * Get socket file descriptor
 * @arg mngr		Cache Manager
 *
 * Get the file descriptor of the socket associated with the manager.
 *
 * @note Do not use the socket for anything besides receiving
 *       notifications.
 */
int nl_cache_mngr_get_fd(struct nl_cache_mngr *mngr)
{
	return nl_socket_get_fd(mngr->cm_sock);
}

/**
 * Check for event notifications
 * @arg mngr		Cache Manager
 * @arg timeout		Upper limit poll() will block, in milliseconds.
 *
 * Causes poll() to be called to check for new event notifications
 * being available. Calls nl_cache_mngr_data_ready() to process
 * available data.
 *
 * This functionally is ideally called regularly during an idle
 * period.
 *
 * A timeout can be specified in milliseconds to limit the time the
 * function will wait for updates.
 *
 * @see nl_cache_mngr_data_ready()
 *
 * @return The number of messages processed or a negative error code.
 */
int nl_cache_mngr_poll(struct nl_cache_mngr *mngr, int timeout)
{
	int ret;
	struct pollfd fds = {
		.fd = nl_socket_get_fd(mngr->cm_sock),
		.events = POLLIN,
	};

	NL_DBG(3, "Cache manager %p, poll() fd %d\n", mngr, fds.fd);
	ret = poll(&fds, 1, timeout);
	NL_DBG(3, "Cache manager %p, poll() returned %d\n", mngr, ret);
	if (ret < 0) {
		NL_DBG(4, "nl_cache_mngr_poll(%p): poll() failed with %d (%s)\n",
			mngr, errno, nl_strerror_l(errno));
		return -nl_syserr2nlerr(errno);
	}

	/* No events, return */
	if (ret == 0)
		return 0;

	return nl_cache_mngr_data_ready(mngr);
}

/**
 * Receive available event notifications
 * @arg mngr		Cache manager
 *
 * This function can be called if the socket associated to the manager
 * contains updates to be received. This function should only be used
 * if nl_cache_mngr_poll() is not used.
 *
 * The function will process messages until there is no more data to
 * be read from the socket.
 *
 * @see nl_cache_mngr_poll()
 *
 * @return The number of messages processed or a negative error code.
 */
int nl_cache_mngr_data_ready(struct nl_cache_mngr *mngr)
{
	int err, nread = 0;
	struct nl_cb *cb;

	NL_DBG(2, "Cache manager %p, reading new data from fd %d\n",
	       mngr, nl_socket_get_fd(mngr->cm_sock));

	cb = nl_cb_clone(mngr->cm_sock->s_cb);
	if (cb == NULL)
		return -NLE_NOMEM;

	nl_cb_set(cb, NL_CB_VALID, NL_CB_CUSTOM, event_input, mngr);

	while ((err = nl_recvmsgs_report(mngr->cm_sock, cb)) > 0) {
		NL_DBG(2, "Cache manager %p, recvmsgs read %d messages\n",
		       mngr, err);
		nread += err;
	}

	nl_cb_put(cb);
	if (err < 0 && err != -NLE_AGAIN)
		return err;

	return nread;
}

/**
 * Print information about cache manager
 * @arg mngr		Cache manager
 * @arg p		Dumping parameters
 *
 * Prints information about the cache manager including all managed caches.
 *
 * @note This is a debugging function.
 */
void nl_cache_mngr_info(struct nl_cache_mngr *mngr, struct nl_dump_params *p)
{
	char buf[128];
	int i;

	nl_dump_line(p, "cache-manager <%p>\n", mngr);
	nl_dump_line(p, "  .protocol = %s\n",
		     nl_nlfamily2str(mngr->cm_protocol, buf, sizeof(buf)));
	nl_dump_line(p, "  .flags    = %#x\n", mngr->cm_flags);
	nl_dump_line(p, "  .nassocs  = %u\n", mngr->cm_nassocs);
	nl_dump_line(p, "  .sock     = <%p>\n", mngr->cm_sock);

	for (i = 0; i < mngr->cm_nassocs; i++) {
		struct nl_cache_assoc *assoc = &mngr->cm_assocs[i];

		if (assoc->ca_cache) {
			nl_dump_line(p, "  .cache[%d] = <%p> {\n", i, assoc->ca_cache);
			nl_dump_line(p, "    .name = %s\n", assoc->ca_cache->c_ops->co_name);
			nl_dump_line(p, "    .change_func = <%p>\n", assoc->ca_change);
			nl_dump_line(p, "    .change_data = <%p>\n", assoc->ca_change_data);
			nl_dump_line(p, "    .nitems = %u\n", nl_cache_nitems(assoc->ca_cache));
			nl_dump_line(p, "    .objects = {\n");

			p->dp_prefix += 6;
			nl_cache_dump(assoc->ca_cache, p);
			p->dp_prefix -= 6;

			nl_dump_line(p, "    }\n");
			nl_dump_line(p, "  }\n");
		}
	}
}

/**
 * Free cache manager and all caches.
 * @arg mngr		Cache manager.
 *
 * Release all resources held by a cache manager.
 */
void nl_cache_mngr_free(struct nl_cache_mngr *mngr)
{
	int i;

	if (!mngr)
		return;

	if (mngr->cm_sock)
		nl_close(mngr->cm_sock);

	if (mngr->cm_sync_sock) {
		nl_close(mngr->cm_sync_sock);
		nl_socket_free(mngr->cm_sync_sock);
	}

	if (mngr->cm_flags & NL_ALLOCATED_SOCK)
		nl_socket_free(mngr->cm_sock);

	for (i = 0; i < mngr->cm_nassocs; i++) {
		if (mngr->cm_assocs[i].ca_cache) {
			nl_cache_mngt_unprovide(mngr->cm_assocs[i].ca_cache);
			nl_cache_free(mngr->cm_assocs[i].ca_cache);
		}
	}

	free(mngr->cm_assocs);

	NL_DBG(1, "Cache manager %p freed\n", mngr);

	free(mngr);
}

/** @} */
