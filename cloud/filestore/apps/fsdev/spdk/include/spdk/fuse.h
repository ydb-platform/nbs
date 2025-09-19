/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#ifndef SPDK_FUSE_H
#define SPDK_FUSE_H

#include "spdk/fsdev.h"
#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct spdk_fuse_mount_opts {
	/** Size of this structure */
	size_t		size;
	/** Maximum IO depth on each thread */
	size_t		max_io_depth;
	/** Maximum transfer size */
	size_t		max_xfer_size;
	/** Clone FUSE device fd */
	bool		clone_fd;
	/** Overrides filesystem type passed to mount(2) */
	const char	*fstype;
	/** Flags passed to mount(2) */
	uint64_t	flags;
};

/**
 * Get default mount options.
 *
 * \param opts mount options.
 * \param size size of `opts`.
 */
void spdk_fuse_get_default_mount_opts(struct spdk_fuse_mount_opts *opts, size_t size);

struct spdk_fuse_mount;

typedef void (*spdk_fuse_mount_cb)(void *ctx, struct spdk_fuse_mount *mount, int status);

/**
 * Mount an fsdev via FUSE.
 *
 * \param fsdev Name of the fsdev to mount.
 * \param mountpoint Location on the filesystem to mount at.
 * \param opts Extra mount options.
 * \param cb_fn Callback to be executed once the fsdev is mounted.
 * \param cb_ctx Argument passed to `cb_fn`.
 *
 * \return 0 on success, negated errno otherwise.
 */
int spdk_fuse_mount(const char *fsdev, const char *mountpoint, struct spdk_fuse_mount_opts *opts,
		    spdk_fuse_mount_cb cb_fn, void *cb_ctx);

typedef void (*spdk_fuse_umount_cb)(void *ctx);

/**
 * Unmount an fsdev mounted via FUSE.  After a successful unmount, the `mount` pointer is no longer
 * valid.
 *
 * \param mount FUSE mount to unmount.
 * \param cb_fn Callback to be executed once the filesystem is unmounted.
 * \param cb_ctx Argument passed to `cb_fn`.
 *
 * \return 0 on success, negated errno otherwise.
 */
int spdk_fuse_umount(struct spdk_fuse_mount *mount, spdk_fuse_umount_cb cb_fn, void *cb_ctx);

typedef int (*spdk_fuse_for_each_mount_cb)(struct spdk_fuse_mount *mount, void *ctx);

/**
 * Call a function on each fsdev mount.
 *
 * \param cb_fn Function to call.
 * \param cb_ctx Argument passed to `cb_fn`.
 *
 * \return 0 on success, negated errno otherwise.
 */
int spdk_fuse_for_each_mount(spdk_fuse_for_each_mount_cb cb_fn, void *ctx);

/**
 * Return fsdev associated with a mount.
 *
 * \param mount FUSE mount.
 *
 * \return fsdev associated with the mount.
 */
struct spdk_fsdev *spdk_fuse_mount_get_fsdev(struct spdk_fuse_mount *mount);

/**
 * Return the path where the fsdev is mounted.
 *
 * \param mount FUSE mount.
 *
 * \return path where the fsdev is mounted.
 */
const char *spdk_fuse_mount_get_mountpoint(struct spdk_fuse_mount *mount);

struct spdk_fuse_poll_group;

/**
 * Create a FUSE poll group.  Groups must be created prior to any mount calls.
 *
 * \return FUSE poll group or NULL in case of an error.
 */
struct spdk_fuse_poll_group *spdk_fuse_poll_group_create(void);

/**
 * Destroy a FUSE poll group.
 *
 * \param group FUSE poll group to destroy.
 */
void spdk_fuse_poll_group_destroy(struct spdk_fuse_poll_group *group);

typedef void (*spdk_fuse_mount_error_cb)(void *ctx, struct spdk_fuse_mount *mount, int error);

/**
 * Poll I/O channels added to this poll group and service any new FUSE requests.
 *
 * \param group FUSE poll group to poll.
 * \param cb_fn Callback executed when polling error occurs.
 * \param cb_ctx Argument passed to `cb_fn`.
 *
 * \return number of requests processed or negated errno otherwise.
 */
int spdk_fuse_poll_group_poll(struct spdk_fuse_poll_group *group,
			      spdk_fuse_mount_error_cb cb_fn, void *cb_ctx);

struct spdk_fuse_opts {
	/** Size of this structure */
	size_t		size;
	/** Maximum IO depth on each thread */
	size_t		max_io_depth;
	/** Maximum transfer size */
	size_t		max_xfer_size;
	/** Clone FUSE device fd */
	bool		clone_fd;
	/** Overrides filesystem type passed to mount(2) */
	const char	*fstype;
};

/**
 * Get FUSE options global to all mounts.
 *
 * \param opts options.
 * \param size size of `opts`.
 */
void spdk_fuse_get_opts(struct spdk_fuse_opts *opts, size_t size);

/**
 * Set FUSE options global to all mounts.
 *
 * \param opts options.
 */
int spdk_fuse_set_opts(struct spdk_fuse_opts *opts);

/**
 * Initialize the FUSE library.
 *
 * \return 0 on success, negative errno otherwise.
 */
int spdk_fuse_init(struct spdk_fuse_opts *opts);

typedef void (*spdk_fuse_cleanup_cb)(void *ctx);

/**
 * Release any resources allocated by the FUSE library.
 *
 * \param cb_fn Callback to be executed once clean up is completed.
 * \param cb_ctx Argument passed to `cb_fn`.
 */
void spdk_fuse_cleanup(spdk_fuse_cleanup_cb cb_fn, void *cb_ctx);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_FUSE_H */
