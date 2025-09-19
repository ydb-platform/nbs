/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

/** \file
 * Operations on a FUSE fsdev dispatcher
 */

#ifndef SPDK_FUSE_DISPATCHER_H
#define SPDK_FUSE_DISPATCHER_H

#include "spdk/stdinc.h"
#include "spdk/fsdev.h"

#ifdef __cplusplus
extern "C" {
#endif

struct spdk_fuse_dispatcher;

/**
 * FUSE fsdev dispatcher submit completion callback.
 *
 * \param cb_arg Callback argument specified upon submit operation.
 * \param error 0 if the operation succeeded, a negative error code otherwise.
 */
typedef void (*spdk_fuse_dispatcher_submit_cpl_cb)(void *cb_arg, int error);

/**
 * FUSE fsdev dispatcher notify reply callback.
 *
 * \param cb_arg Callback argument specified upon submit operation.
 * \param notify_reply_data Decoded notification data for fsdev.
 * \param unique_id Unique ID value.
 */
typedef void (*spdk_fuse_dispatcher_notify_reply_cb)(void *cb_arg,
		const struct spdk_fsdev_notify_reply_data *notify_reply_data,
		uint64_t unique_id);

/**
 * Create a FUSE fsdev dispatcher
 *
 * \param desc fsdev descriptor to work with
 * \param recovery_mode true if the dispatcher's state should be recovered, false otherwise.
 * \param notify_reply_cb Callback to invoke on FUSE_NOTIFY_REPLY requests
 * \param notify_reply_cb_arg Argument that will be passed to notify_reply_cb
 *
 * NOTE: \p recovery_mode is ignored if rmem pool functionality is disabled
 *
 * \return FUSE fsdev dispatcher object on success, NULL otherwise.
 */
struct spdk_fuse_dispatcher *spdk_fuse_dispatcher_create(struct spdk_fsdev_desc *desc,
		bool recovery_mode,
		spdk_fuse_dispatcher_notify_reply_cb notify_reply_cb,
		void *notify_reply_cb_arg);

/**
 * Get the size of the io_ctx buffer.
 *
 * \return The size of struct fuse_io
 */
size_t spdk_fuse_dispatcher_get_io_ctx_size(void);

/**
 * Submit FUSE request
 *
 * \param disp FUSE fsdev dispatcher object.
 * \param ch I/O channel obtained from the \p spdk_fuse_dispatcher_get_io_channel.
 * \param in_iov Input IO vectors array.
 * \param in_iovcnt Size of the input IO vectors array.
 * \param out_iov Output IO vectors array.
 * \param out_iovcnt Size of the output IO vectors array.
 * \param source_id Source ID
 * \param source_unique per Source ID unique value
 * \param cb Completion callback.
 * \param cb_arg Context to be passed to the completion callback.
 *
 * \return 0 on success. On success, the callback will always
 * be called (even if the request ultimately failed). Return
 * negated errno on failure, in which case the callback will not be called.
 *  -ENOBUFS - the request cannot be submitted due to a lack of the internal IO objects
 *  -EINVAL - the request cannot be submitted as some FUSE request data is incorrect
 *
 * NOTE: each source_id is pinned to a thread. Which means that requests with a specific source_id
 * can only be submitted on one thread. Multiple source_ids per thread are allowed.
 */
int spdk_fuse_dispatcher_submit_request(struct spdk_fuse_dispatcher *disp,
					struct spdk_io_channel *ch,
					struct iovec *in_iov, int in_iovcnt,
					struct iovec *out_iov, int out_iovcnt, void *io_ctx,
					uint16_t source_id, uint64_t source_unique,
					spdk_fuse_dispatcher_submit_cpl_cb cb, void *cb_arg);

/**
 * Delete a FUSE fsdev dispatcher
 *
 * \param disp FUSE fsdev dispatcher object.
 */
void spdk_fuse_dispatcher_delete(struct spdk_fuse_dispatcher *disp);

/**
 * Encode FUSE notification
 *
 * \param disp FUSE fsdev dispatcher object.
 * \param iov Output IO vectors array.
 * \param iovcnt Size of the output IO vectors array.
 * \param notify_data Notification data received from fsdev.
 * Pass NULL to encode "empty" notification that is used to indicate device reset.
 * \param unique_id Unique ID of the notification.
 *
 * \return 0 on success, negated errno on failure.
 */
int spdk_fuse_dispatcher_encode_notify(struct spdk_fuse_dispatcher *disp,
				       struct iovec *iov, int iovcnt,
				       const struct spdk_fsdev_notify_data *notify_data,
				       uint64_t unique_id,
				       bool *has_reply);

/**
 * Get minimum buffer size required to fit FUSE notification.
 *
 * \param disp FUSE fsdev dispatcher object.
 *
 * \return notify buffer size in bytes. Zero means that notifications are not supported.
 */
uint32_t spdk_fuse_dispatcher_get_notify_buf_size(struct spdk_fuse_dispatcher *disp);

/**
 * Return the name of a FUSE operation.
 *
 * \param opcode opcode of the operation.
 *
 * \return name of the operation or "UNKNOWN" in case the opcode is not known.
 */
const char *spdk_fuse_dispatcher_get_operation_name(uint32_t opcode);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_FUSE_DISPATCHER_H */
