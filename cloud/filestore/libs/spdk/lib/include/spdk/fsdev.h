/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

/** \file
 * Filesystem device abstraction layer
 */

#ifndef SPDK_FSDEV_H
#define SPDK_FSDEV_H

#include "spdk/stdinc.h"
#include "spdk/json.h"
#include "spdk/assert.h"
#include "spdk/dma.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * \brief SPDK filesystem device.
 *
 * This is a virtual representation of a filesystem device that is exported by the backend.
 */
struct spdk_fsdev;

/** Asynchronous event type */
enum spdk_fsdev_event_type {
	SPDK_FSDEV_EVENT_REMOVE,
};

/**
 * Filesystem device event callback.
 *
 * \param type Event type.
 * \param fsdev Filesystem device that triggered event.
 * \param event_ctx Context for the filesystem device event.
 */
typedef void (*spdk_fsdev_event_cb_t)(enum spdk_fsdev_event_type type,
				      struct spdk_fsdev *fsdev,
				      void *event_ctx);

struct spdk_fsdev_fn_table;
struct spdk_io_channel;

/** fsdev status */
enum spdk_fsdev_status {
	SPDK_FSDEV_STATUS_INVALID,
	SPDK_FSDEV_STATUS_READY,
	SPDK_FSDEV_STATUS_UNREGISTERING,
	SPDK_FSDEV_STATUS_REMOVING,
};

/** fsdev library options */
struct spdk_fsdev_opts {
	/**
	 * The size of spdk_fsdev_opts according to the caller of this library is used for ABI
	 * compatibility.  The library uses this field to know how many fields in this
	 * structure are valid. And the library will populate any remaining fields with default values.
	 * New added fields should be put at the end of the struct.
	 */
	uint32_t opts_size;
	uint32_t reserved1;
	uint32_t reserved2;
	/**
	 * Max number of sources.
	 */
	uint16_t max_num_sources;
} __attribute__((packed));
SPDK_STATIC_ASSERT(sizeof(struct spdk_fsdev_opts) == 14, "Incorrect size");

/** fsdev mount options */
struct spdk_fsdev_mount_opts {
	/**
	 * The size of spdk_fsdev_mount_opts according to the caller of this library is used for ABI
	 * compatibility.  The library uses this field to know how many fields in this
	 * structure are valid. And the library will populate any remaining fields with default values.
	 * New added fields should be put at the end of the struct.
	 */
	uint32_t opts_size;

	/**
	 * OUT The maximum size allowed for data transfers, in bytes. 0 value means unlimited.
	 */
	uint32_t max_xfer_size;

	/**
	 * OUT Max readahead size.
	 */
	uint32_t max_readahead;

	/**
	 * IN/OUT Contains requested and negotiated fsdev mount flags.
	 */
	uint32_t flags;
} __attribute__((packed));
SPDK_STATIC_ASSERT(sizeof(struct spdk_fsdev_mount_opts) == 16, "Incorrect size");

/**
 * IN/OUT Mount flags. These control user behavior with regard to the fsdev API.
 * The user provides the set of flags they'd like set and the fsdev can modify them.
 *
 * SPDK_FSDEV_MOUNT_DOT_PATH_LOOKUP: "." and ".." are valid paths for a lookup operation.
 *
 * SPDK_FSDEV_MOUNT_AUTO_INVAL_DATA: The user will invalidate any cached data pages for
 * objects if fsdev reports a modified 'mtime'. Additionally, the user will check
 * for attribute changes (e.g. size) prior to issuing a read, rather than assuming
 * their latest cached attributes are valid.
 *
 * SPDK_FSDEV_MOUNT_EXPLICIT_INVAL_DATA: The user will receive cache invalidation requests
 * when necessary. This ensures that data cached by user is correctly invalidated
 * and updated.
 *
 * SPDK_FSDEV_MOUNT_WRITEBACK_CACHE: The user will maintain their own cache of write data,
 * without immediately forwarding writes to the fsdev. The user will assume their
 * cached versions of the file attributes are newer than the ones reported by fsdev.
 *
 * SPDK_FSDEV_MOUNT_POSIX_ACL: The user will assume that the fsdev is performing ACL checks
 * on setxattr_flags.
 *
 * SPDK_FSDEV_MOUNT_POSIX_LOCKS: remote locking for POSIX file locks supported.
 *
 * SPDK_FSDEV_MOUNT_FLOCK_LOCKS: remote locking for BSD style file locks supported.
 *
 * SPDK_FSDEV_MOUNT_O_TRUNC: O_TRUNC open flag supported.
 */
#define SPDK_FSDEV_MOUNT_DOT_PATH_LOOKUP      (1 << 0)
#define SPDK_FSDEV_MOUNT_AUTO_INVAL_DATA      (1 << 1)
#define SPDK_FSDEV_MOUNT_EXPLICIT_INVAL_DATA  (1 << 2)
#define SPDK_FSDEV_MOUNT_WRITEBACK_CACHE      (1 << 3)
#define SPDK_FSDEV_MOUNT_POSIX_ACL            (1 << 4)
#define SPDK_FSDEV_MOUNT_POSIX_LOCKS          (1 << 5)
#define SPDK_FSDEV_MOUNT_FLOCK_LOCKS          (1 << 6)
#define SPDK_FSDEV_MOUNT_O_TRUNC              (1 << 7)

/**
 * Structure with optional fsdev IO parameters
 * The content of this structure must be valid until the IO is completed
 */
struct spdk_fsdev_io_opts {
	/** Size of this structure in bytes */
	size_t size;
	/** Memory domain which describes payload in this IO. fsdev must support DMA device type that
	 * can access this memory domain, refer to \ref spdk_fsdev_get_memory_domains and
	 * \ref spdk_memory_domain_get_dma_device_type
	 * If set, that means that data buffers can't be accessed directly and the memory domain must
	 * be used to fetch data to local buffers or to translate data to another memory domain */
	struct spdk_memory_domain *memory_domain;
	/** Context to be passed to memory domain operations */
	void *memory_domain_ctx;
} __attribute__((packed));
SPDK_STATIC_ASSERT(sizeof(struct spdk_fsdev_io_opts) == 24, "Incorrect size");

/**
 * fsdev IO type
 */
enum spdk_fsdev_io_type {
	SPDK_FSDEV_IO_MOUNT,
	SPDK_FSDEV_IO_UMOUNT,
	SPDK_FSDEV_IO_LOOKUP,
	SPDK_FSDEV_IO_FORGET,
	SPDK_FSDEV_IO_GETATTR,
	SPDK_FSDEV_IO_SETATTR,
	SPDK_FSDEV_IO_READLINK,
	SPDK_FSDEV_IO_SYMLINK,
	SPDK_FSDEV_IO_MKNOD,
	SPDK_FSDEV_IO_MKDIR,
	SPDK_FSDEV_IO_UNLINK,
	SPDK_FSDEV_IO_RMDIR,
	SPDK_FSDEV_IO_RENAME,
	SPDK_FSDEV_IO_LINK,
	SPDK_FSDEV_IO_OPEN,
	SPDK_FSDEV_IO_READ,
	SPDK_FSDEV_IO_WRITE,
	SPDK_FSDEV_IO_STATFS,
	SPDK_FSDEV_IO_RELEASE,
	SPDK_FSDEV_IO_FSYNC,
	SPDK_FSDEV_IO_SETXATTR,
	SPDK_FSDEV_IO_GETXATTR,
	SPDK_FSDEV_IO_LISTXATTR,
	SPDK_FSDEV_IO_REMOVEXATTR,
	SPDK_FSDEV_IO_FLUSH,
	SPDK_FSDEV_IO_OPENDIR,
	SPDK_FSDEV_IO_READDIR,
	SPDK_FSDEV_IO_RELEASEDIR,
	SPDK_FSDEV_IO_FSYNCDIR,
	SPDK_FSDEV_IO_FLOCK,
	SPDK_FSDEV_IO_CREATE,
	SPDK_FSDEV_IO_ABORT,
	SPDK_FSDEV_IO_FALLOCATE,
	SPDK_FSDEV_IO_COPY_FILE_RANGE,
	SPDK_FSDEV_IO_SYNCFS,
	SPDK_FSDEV_IO_ACCESS,
	SPDK_FSDEV_IO_LSEEK,
	SPDK_FSDEV_IO_POLL,
	SPDK_FSDEV_IO_IOCTL,
	SPDK_FSDEV_IO_GETLK,
	SPDK_FSDEV_IO_SETLK,
	__SPDK_FSDEV_IO_LAST
};

/** Notification type */
enum spdk_fsdev_notify_type {
	SPDK_FSDEV_NOTIFY_INVAL_DATA,
	SPDK_FSDEV_NOTIFY_INVAL_ENTRY,
	SPDK_FSDEV_NOTIFY_NUM_TYPES
};

/**
 * fsdev IO statistics
 */
struct spdk_fsdev_io_stat {
	/** Stats by IO type */
	struct {
		/* Number of handled IOs */
		uint64_t count;
		/* Max latency */
		uint64_t max_ticks;
		/* Min latency */
		uint64_t min_ticks;
		/* Total latency */
		uint64_t total_ticks;
		/* Number of IOs outstanding */
		uint64_t io_outstanding;
	} io[__SPDK_FSDEV_IO_LAST];
	/** Number of bytes read */
	uint64_t bytes_read;
	/** Number of bytes written */
	uint64_t bytes_written;
	/** Number of IOs which couldn't be handled due to lack of the IO objects */
	uint64_t num_out_of_io;
	/** Number of IOs completed with an error */
	uint64_t num_io_errors;
	/** Notifications statistics by type */
	struct {
		/** Number of emitted notifications */
		uint64_t count;
		/** Number of received notification replies */
		uint64_t replies;
	} notify[SPDK_FSDEV_NOTIFY_NUM_TYPES];
};

/**
 * \brief Handle to an opened SPDK filesystem device.
 */
struct spdk_fsdev_desc;

/**
 * Filesystem device initialization callback.
 *
 * \param cb_arg Callback argument.
 * \param rc 0 if filesystem device initialized successfully or negative errno if it failed.
 */
typedef void (*spdk_fsdev_init_cb)(void *cb_arg, int rc);

/**
 * Filesystem device finish callback.
 *
 * \param cb_arg Callback argument.
 */
typedef void (*spdk_fsdev_fini_cb)(void *cb_arg);

/**
 * Initialize filesystem device modules.
 *
 * \param cb_fn Called when the initialization is complete.
 * \param cb_arg Argument passed to function cb_fn.
 */
void spdk_fsdev_initialize(spdk_fsdev_init_cb cb_fn, void *cb_arg);

/**
 * Perform cleanup work to remove the registered filesystem device modules.
 *
 * \param cb_fn Called when the removal is complete.
 * \param cb_arg Argument passed to function cb_fn.
 */
void spdk_fsdev_finish(spdk_fsdev_fini_cb cb_fn, void *cb_arg);

/**
 * Get struct spdk_fsdev_io ctx size
 *
 * \return size of buffer needed to store struct spdk_fsdev_io
 */
int spdk_fsdev_get_io_ctx_size(void);

/**
 * Get the full configuration options for the registered filesystem device modules and created fsdevs.
 *
 * \param w pointer to a JSON write context where the configuration will be written.
 */
void spdk_fsdev_subsystem_config_json(struct spdk_json_write_ctx *w);

/**
 * Get filesystem device module name.
 *
 * \param fsdev Filesystem device to query.
 * \return Name of fsdev module as a null-terminated string.
 */
const char *spdk_fsdev_get_module_name(const struct spdk_fsdev *fsdev);

/**
 * Open a filesystem device for I/O operations.
 *
 * \param fsdev_name Filesystem device name to open.
 * \param event_cb notification callback to be called when the fsdev triggers
 * asynchronous event such as fsdev removal. This will always be called on the
 * same thread that spdk_fsdev_open() was called on. In case of removal event
 * the descriptor will have to be manually closed to make the fsdev unregister
 * proceed.
 * \param event_ctx param for event_cb.
 * \param desc output parameter for the descriptor when operation is successful
 * \return 0 if operation is successful, suitable errno value otherwise
 */
int spdk_fsdev_open(const char *fsdev_name, spdk_fsdev_event_cb_t event_cb,
		    void *event_ctx, struct spdk_fsdev_desc **desc);

/**
 * Close a previously opened filesystem device.
 *
 * Must be called on the same thread that the spdk_fsdev_open()
 * was performed on.
 *
 * \param desc Filesystem device descriptor to close.
 */
void spdk_fsdev_close(struct spdk_fsdev_desc *desc);

/**
 * Callback function for spdk_for_each_fsdev().
 *
 * \param ctx Context passed to the callback.
 * \param fsdev filesystem device the callback handles.
 */
typedef int (*spdk_for_each_fsdev_fn)(void *ctx, struct spdk_fsdev *fsdev);

/**
 * Call the provided callback function for every registered filesystem device.
 * If fn returns negated errno, spdk_for_each_fsdev() terminates iteration.
 *
 * spdk_for_each_fsdev() opens before and closes after executing the provided
 * callback function for each fsdev internally.
 *
 * \param ctx Context passed to the callback function.
 * \param fn Callback function for each filesystem device.
 *
 * \return 0 if operation is successful, or suitable errno value one of the
 * callback returned otherwise.
 */
int spdk_for_each_fsdev(void *ctx, spdk_for_each_fsdev_fn fn);

/**
 * Get spdk_fsdev_io_type name
 *
 * \param type IO type
 *
 * \return non-NULL IO type name if operation is successful, or NULL otherwise.
 */
const char *spdk_fsdev_io_type_get_name(enum spdk_fsdev_io_type type);

/**
 * Return spdk_fsdev_io_type from its name.
 *
 * \param name name of the IO type.
 *
 * \return Corresponding spdk_fsdev_io_type or -EINVAL on failure.
 */
int spdk_fsdev_io_type_from_name(const char *name);

/**
 * Get filesystem device name.
 *
 * \param fsdev filesystem device to query.
 * \return Name of fsdev as a null-terminated string.
 */
const char *spdk_fsdev_get_name(const struct spdk_fsdev *fsdev);

/**
 * Get the fsdev associated with a fsdev descriptor.
 *
 * \param desc Open filesystem device descriptor
 * \return fsdev associated with the descriptor
 */
struct spdk_fsdev *spdk_fsdev_desc_get_fsdev(struct spdk_fsdev_desc *desc);

/**
 * Obtain an I/O channel for the filesystem device opened by the specified
 * descriptor. I/O channels are bound to threads, so the resulting I/O
 * channel may only be used from the thread it was originally obtained
 * from.
 *
 * \param desc Filesystem device descriptor.
 *
 * \return A handle to the I/O channel or NULL on failure.
 */
struct spdk_io_channel *spdk_fsdev_get_io_channel(struct spdk_fsdev_desc *desc);

/**
 * Set the options for the fsdev library.
 *
 * \param opts options to set
 * \return 0 on success.
 * \return -EINVAL if the options are invalid.
 */
int spdk_fsdev_set_opts(const struct spdk_fsdev_opts *opts);

/**
 * Get the options for the fsdev library.
 *
 * \param opts Output parameter for options.
 * \param opts_size sizeof(*opts)
 */
int spdk_fsdev_get_opts(struct spdk_fsdev_opts *opts, size_t opts_size);

/**
 * Get SPDK memory domains used by the given fsdev. If fsdev reports that it uses memory domains
 * that means that it can work with data buffers located in those memory domains.
 *
 * The user can call this function with \b domains set to NULL and \b array_size set to 0 to get the
 * number of memory domains used by fsdev
 *
 * \param fsdev filesystem device
 * \param domains pointer to an array of memory domains to be filled by this function. The user should allocate big enough
 * array to keep all memory domains used by fsdev and all underlying fsdevs
 * \param array_size size of \b domains array
 * \return the number of entries in \b domains array or negated errno. If returned value is bigger than \b array_size passed by the user
 * then the user should increase the size of \b domains array and call this function again. There is no guarantees that
 * the content of \b domains array is valid in that case.
 *         -EINVAL if input parameters were invalid
 */
int spdk_fsdev_get_memory_domains(struct spdk_fsdev *fsdev, struct spdk_memory_domain **domains,
				  int array_size);

/**
 * Output driver-specific information to a JSON stream.
 *
 * The JSON write context will be initialized with an open object, so the fsdev
 * driver should write a name (based on the driver name) followed by a JSON value
 * (most likely another nested object).
 *
 * \param fsdev Filesystem to query.
 * \param w JSON write context. It will store the driver-specific configuration context.
 * \return 0 on success, negated errno on failure.
 */
int spdk_fsdev_dump_info_json(struct spdk_fsdev *fsdev, struct spdk_json_write_ctx *w);

/**
 * \brief SPDK fsdev channel iterator.
 *
 * This is a virtual representation of a fsdev channel iterator.
 */
struct spdk_fsdev_channel_iter;

/**
 * Called on the appropriate thread for each channel associated with the given fsdev.
 *
 * \param i fsdev channel iterator.
 * \param fsdev filesystem device.
 * \param ch I/O channel.
 * \param ctx context of the fsdev channel iterator.
 */
typedef void (*spdk_fsdev_for_each_channel_msg)(struct spdk_fsdev_channel_iter *i,
		struct spdk_fsdev *fsdev, struct spdk_io_channel *ch, void *ctx);

/**
 * spdk_fsdev_for_each_channel() function's final callback with the given fsdev.
 *
 * \param fsdev filesystem device.
 * \param ctx context of the fsdev channel iterator.
 * \param status 0 if it completed successfully, or negative errno if it failed.
 */
typedef void (*spdk_fsdev_for_each_channel_done)(struct spdk_fsdev *fsdev, void *ctx, int status);

/**
 * Helper function to iterate the next channel for spdk_fsdev_for_each_channel().
 *
 * \param i fsdev channel iterator.
 * \param status Status for the fsdev channel iterator;
 * for non 0 status remaining iterations are terminated.
 */
void spdk_fsdev_for_each_channel_continue(struct spdk_fsdev_channel_iter *i, int status);

/**
 * Call 'fn' on each channel associated with the given fsdev.
 *
 * This happens asynchronously, so fn may be called after spdk_fsdev_for_each_channel
 * returns. 'fn' will be called for each channel serially, such that two calls
 * to 'fn' will not overlap in time. After 'fn' has been called, call
 * spdk_fsdev_for_each_channel_continue() to continue iterating. Note that the
 * spdk_fsdev_for_each_channel_continue() function can be called asynchronously.
 *
 * \param fsdev 'fn' will be called on each channel associated with this given fsdev.
 * \param fn Called on the appropriate thread for each channel associated with the given fsdev.
 * \param ctx Context for the caller.
 * \param cpl Called on the thread that spdk_fsdev_for_each_channel was initially called
 * from when 'fn' has been called on each channel.
 */
void spdk_fsdev_for_each_channel(struct spdk_fsdev *fsdev, spdk_fsdev_for_each_channel_msg fn,
				 void *ctx, spdk_fsdev_for_each_channel_done cpl);

/**
 * Filesystem device reset completion callback.
 *
 * \param desc Filesystem device descriptor.
 * \param success True if reset completed successfully or false if it failed.
 * \param cb_arg Callback argument specified upon reset operation.
 */
typedef void (*spdk_fsdev_reset_completion_cb)(struct spdk_fsdev_desc *desc, bool success,
		void *cb_arg);

/**
 * Issue reset operation to the fsdev.
 *
 * \param desc Filesystem device descriptor.
 * \param cb Called when the reset is complete.
 * \param cb_arg Argument passed to cb.
 *
 * \return 0 on success. On success, the callback will always
 * be called (even if the request ultimately failed). Return
 * negated errno on failure, in which case the callback will not be called.
 */
int spdk_fsdev_reset(struct spdk_fsdev_desc *desc, spdk_fsdev_reset_completion_cb cb, void *cb_arg);

struct spdk_fsdev_notify_data {
	/** Notification type */
	enum spdk_fsdev_notify_type type;
	union {
		/** Data for SPDK_FSDEV_NOTIFY_INVAL_DATA notification type */
		struct {
			struct spdk_fsdev_file_object *fobject;
			uint64_t offset;
			size_t size;
		} inval_data;

		/** Data for SPDK_FSDEV_NOTIFY_INVAL_ENTRY notification type */
		struct {
			struct spdk_fsdev_file_object *parent_fobject;
			const char *name;
		} inval_entry;
	};
};

struct spdk_fsdev_notify_reply_data {
	/** Notification handling status */
	int status;
};

/**
 * Filesystem device notification reply callback.
 *
 * \param notify_reply_data Reply data for the filesystem device notification.
 * Data is only valid in the context of this callback.
 * \param reply_ctx Context for the filesystem device notification.
 */
typedef void (*spdk_fsdev_notify_reply_cb_t)(
	const struct spdk_fsdev_notify_reply_data *notify_reply_data,
	void *reply_ctx);

/**
 * Filesystem device notification callback.
 *
 * \param fsdev Filesystem device that triggered event.
 * \param ctx Context that was passed in spdk_fsdev_enable_notifications().
 * \param notify_data Data for the filesystem device notification.
 * Data is only valid in the context of this callback.
 * \param reply_cb Optional notification reply callback. If NULL, fsdev doesn't need a reply for this notification.
 * Fsdev should be ready to get the reply callback in the context of notify callback.
 * \param reply_ctx Context for the filesystem device notification. Should be passed in reply_cb.
 */
typedef void (*spdk_fsdev_notify_cb_t)(struct spdk_fsdev *fsdev,
				       void *ctx,
				       const struct spdk_fsdev_notify_data *notify_data,
				       spdk_fsdev_notify_reply_cb_t reply_cb,
				       void *reply_ctx);

/**
 * Enable notifications for fsdev.
 * Notifications can be enabled only once for filesystem device.
 * Notifications can be delivered on any thread.
 * It must be called before spdk_fsdev_mount().
 *
 * \param desc Filesystem device descriptor.
 * \param notify_cb Callback to be invoked on notification.
 * \param ctx Context that will be passed to notify_cb.
 *
 * \return 0 on success.
 * \return -EALREADY if notifications were already enabled on this filesystem device.
 * \return negated errno on other errors.
 */
int spdk_fsdev_enable_notifications(struct spdk_fsdev_desc *desc, spdk_fsdev_notify_cb_t notify_cb,
				    void *ctx);

/**
 * Disable notifications for fsdev.
 * It must be called after spdk_fsdev_umount().
 *
 * \param desc Filesystem device descriptor.
 *
 * \return 0 on success.
 * \return -EALREADY if notifications were already disabled on this filesystem device.
 * \return negated errno on other errors.
 */
int spdk_fsdev_disable_notifications(struct spdk_fsdev_desc *desc);

/**
 * Get filesystem device maximum notification data size.
 * It indicates the maximum size of varibale sized data in the notification
 * and does not include fixed size fields in spdk_fsdev_notify_data structure.
 * Example of variable sized data is 'name' in SPDK_FSDEV_NOTIFY_INVAL_ENTRY notification.
 *
 * \param fsdev Filesystem device to query.
 *
 * \return Maximum size of variable sized notification data for this fsdev in bytes.
 * Zero means that fsdev does not support notifications.
 */
uint32_t spdk_fsdev_get_notify_max_data_size(const struct spdk_fsdev *fsdev);

/**
 * Check whether the Filesystem device supports reset.
 *
 * \param fsdev Filesystem device to check.
 * \return true if support, false otherwise.
 */
bool spdk_fsdev_reset_supported(struct spdk_fsdev *fsdev);

/**
 * Check whether the Filesystem device is recovered.
 *
 * \param fsdev Filesystem device to check.
 * \return true if support, false otherwise.
 */
bool spdk_fsdev_is_recovered(struct spdk_fsdev *fsdev);

/**
 * Return I/O statistics for this channel.
 *
 * \param fsdev Filesystem device.
 * \param ch I/O channel. Obtained by calling spdk_fsdev_get_io_channel().
 * \param stat The per-channel statistics.
 *
 */
void spdk_fsdev_get_io_stat(struct spdk_fsdev *fsdev, struct spdk_io_channel *ch,
			    struct spdk_fsdev_io_stat *stat);

/**
 * Get fsdev statistics completion callback.
 *
 * \param fsdev Filesystem device.
 * \param stat Pointer received in the spdk_fsdev_get_device_stat call
 * \param cb_arg Callback argument specified upon get stat.
 * \param rc Statistics collection operation result. 0 if succeeded, a negative error code otherwise.
 */
typedef void (*spdk_fsdev_get_device_stat_cb)(struct spdk_fsdev *fsdev,
		struct spdk_fsdev_io_stat *stat, void *cb_arg, int rc);

/**
 * Get fsdev statistics.
 *
 * \param fsdev Filesystem device.
 * \param stat Pointer to the structure where the stats should be stored.
 * \param cb Called when stats are ready to be consumed.
 * \param cb_arg Argument passed to cb.
 */
void spdk_fsdev_get_device_stat(struct spdk_fsdev *fsdev, struct spdk_fsdev_io_stat *stat,
				spdk_fsdev_get_device_stat_cb cb, void *cb_arg);

/**
 * Reset fsdev statistics completion callback.
 *
 * \param fsdev Filesystem device.
 * \param cb_arg Callback argument specified upon get stat.
 * \param rc Statistics collection operation result. 0 if succeeded, a negative error code otherwise.
 */
typedef void (*spdk_fsdev_reset_device_stat_cb)(struct spdk_fsdev *fsdev,
		void *cb_arg, int rc);

/**
 * Reset fsdev statistics.
 *
 * \param fsdev Filesystem device.
 * \param cb Called when reset is done.
 * \param cb_arg Argument passed to cb.
 */
void spdk_fsdev_reset_device_stat(struct spdk_fsdev *fsdev,  spdk_fsdev_reset_device_stat_cb cb,
				  void *cb_arg);

/* Valid flags to set in spdk_fsdev_setattr */
#define SPDK_FSDEV_ATTR_MODE		(1 << 0)
#define SPDK_FSDEV_ATTR_UID		(1 << 1)
#define SPDK_FSDEV_ATTR_GID		(1 << 2)
#define SPDK_FSDEV_ATTR_SIZE		(1 << 3)
#define SPDK_FSDEV_ATTR_ATIME		(1 << 4)
#define SPDK_FSDEV_ATTR_MTIME		(1 << 5)
#define SPDK_FSDEV_ATTR_ATIME_NOW	(1 << 6)
#define SPDK_FSDEV_ATTR_MTIME_NOW	(1 << 7)
#define SPDK_FSDEV_ATTR_CTIME		(1 << 8)

struct spdk_fsdev_file_object;
struct spdk_fsdev_file_handle;

struct spdk_fsdev_file_attr {
	uint64_t ino;
	uint64_t size;
	uint64_t blocks;
	uint64_t atime;
	uint64_t mtime;
	uint64_t ctime;
	uint32_t atimensec;
	uint32_t mtimensec;
	uint32_t ctimensec;
	uint32_t mode;
	uint32_t nlink;
	uint32_t uid;
	uint32_t gid;
	uint32_t rdev;
	uint32_t blksize;
	uint32_t valid_ms;
};

struct spdk_fsdev_file_statfs {
	uint64_t blocks;
	uint64_t bfree;
	uint64_t bavail;
	uint64_t files;
	uint64_t ffree;
	uint32_t bsize;
	uint32_t namelen;
	uint32_t frsize;
};

/* Resembling file lock types. */
enum spdk_fsdev_file_lock_type {
	SPDK_FSDEV_RDLCK = 0,
	SPDK_FSDEV_WRLCK = 1,
	SPDK_FSDEV_UNLCK = 2
};

/* Resembling flock operation type */
enum spdk_fsdev_file_lock_op {
	/*
	 * Place a shared lock. More than one process may hold
	 * a shared lock for a given file at a given time.
	 */
	SPDK_FSDEV_LOCK_SH = 0,

	/*
	 * Place an exclusive lock.  Only one process may hold
	 * an exclusive lock for a given file at a given time.
	 */
	SPDK_FSDEV_LOCK_EX = 1,

	/* Remove an existing lock held by this process. */
	SPDK_FSDEV_LOCK_UN = 2
};

/*
 * This structure provides the info/description on/of a specific file lock
 * and is used for delivering the lock params to and from the fsdev API and
 * the lower layers.
 */
struct spdk_fsdev_file_lock {
	/* SPDK variant of F_RDLCK. F_WRLCK, F_UNLCK */
	enum spdk_fsdev_file_lock_type type;

	/* Starting offset for lock */
	uint64_t start;

	/* End of the lock region in bytes */
	uint64_t end;

	/*
	 * Originally PID of process blocking our lock.
	 * In context of virtiofs this can be used for
	 * similar task but this needs to be taken care
	 * of specially. For now we have it here.
	 */
	uint32_t pid;
};

/*
 * Used for denoting the end of the file when specifying the
 * file lock params.
 */
#define SPDK_FSDEV_FILE_LOCK_END_OF_FILE LONG_MAX

enum spdk_fsdev_seek_whence {
	SPDK_FSDEV_SEEK_SET = (1 << 0),
	SPDK_FSDEV_SEEK_CUR = (1 << 1),
	SPDK_FSDEV_SEEK_END = (1 << 2),
	SPDK_FSDEV_SEEK_HOLE = (1 << 3),
	SPDK_FSDEV_SEEK_DATA = (1 << 4)
};

struct spdk_fsdev_io;

typedef void (spdk_fsdev_cpl_cb)(void *cb_arg, int status, struct spdk_fsdev_io *fsdev_io);

/**
 * Read directory per-entry callback
 *
 * \param cb_arg Context passed to the corresponding spdk_fsdev_io_submit API
 * \param fsdev_io Filesystem device I/O.
 * \param name Name of the entry
 * \param fobject File object. NULL for "." and "..".
 * \param attr File attributes.
 * \param offset Offset of the next entry
 * \param forget Whether to forget the \p fobject. Default: false
 *
 * \return 0 to continue the enumeration, an error code otherwise.
 *
 * NOTE: the \p spdk_fsdev_readdir effectively executes lookup and the \p fobject remains
 *       referenced unless this callback sets the \p forget to true. Otherwise, it's up to
 *       the user to call \p spdk_fsdev_forget when the \p fobject is no longer needed.
 */
typedef int (spdk_fsdev_readdir_entry_cb)(void *cb_arg, struct spdk_fsdev_io *fsdev_io,
		const char *name, struct spdk_fsdev_file_object *fobject, const struct spdk_fsdev_file_attr *attr,
		off_t offset, bool *forget);

/**
 * Filesystem device IO completion callback.
 *
 * \param fsdev_io Filesystem device I/O that has completed.
 * \param cb_arg Callback argument specified when fsdev_io was submitted.
 */
typedef void (*spdk_fsdev_io_completion_cb)(struct spdk_fsdev_io *fsdev_io, void *cb_arg);

struct spdk_fsdev_io {
	/** The filesystem device that this I/O belongs to. */
	struct spdk_fsdev *fsdev;

	/** A single iovec element for use by this fsdev_io. */
	struct iovec iov;

	union {
		struct {
			struct spdk_fsdev_mount_opts opts;
		} mount;
		struct {
			struct spdk_fsdev_file_object *parent_fobject;
			const char *name;
		} lookup;
		struct {
			struct spdk_fsdev_file_object *fobject;
			uint64_t nlookup;
		} forget;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
		} getattr;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			struct spdk_fsdev_file_attr attr;
			uint32_t to_set;
		} setattr;
		struct {
			struct spdk_fsdev_file_object *fobject;
		} readlink;
		struct {
			struct spdk_fsdev_file_object *parent_fobject;
			const char *target;
			const char *linkpath;
			uid_t euid;
			gid_t egid;
		} symlink;
		struct {
			struct spdk_fsdev_file_object *parent_fobject;
			const char *name;
			mode_t mode;
			uint32_t umask;
			dev_t rdev;
			uid_t euid;
			gid_t egid;
		} mknod;
		struct {
			struct spdk_fsdev_file_object *parent_fobject;
			const char *name;
			mode_t mode;
			uint32_t umask;
			uid_t euid;
			gid_t egid;
		} mkdir;
		struct {
			struct spdk_fsdev_file_object *parent_fobject;
			const char *name;
		} unlink;
		struct {
			struct spdk_fsdev_file_object *parent_fobject;
			const char *name;
		} rmdir;
		struct {
			struct spdk_fsdev_file_object *parent_fobject;
			const char *name;
			struct spdk_fsdev_file_object *new_parent_fobject;
			const char *new_name;
			uint32_t flags;
		} rename;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_object *new_parent_fobject;
			const char *name;
		} link;
		struct {
			struct spdk_fsdev_file_object *fobject;
			uint32_t flags;
		} open;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			size_t size;
			uint64_t offs;
			uint32_t flags;
			struct iovec *iov;
			uint32_t iovcnt;
			struct spdk_fsdev_io_opts *opts;
		} read;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			size_t size;
			uint64_t offs;
			uint64_t flags;
			const struct iovec *iov;
			uint32_t iovcnt;
			struct spdk_fsdev_io_opts *opts;
		} write;
		struct {
			struct spdk_fsdev_file_object *fobject;
		} statfs;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
		} release;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			bool datasync;
		} fsync;
		struct {
			struct spdk_fsdev_file_object *fobject;
			const char *name;
			const char *value;
			size_t size;
			uint64_t flags;
		} setxattr;
		struct {
			struct spdk_fsdev_file_object *fobject;
			const char *name;
			void *buffer;
			size_t size;
		} getxattr;
		struct {
			struct spdk_fsdev_file_object *fobject;
			char *buffer;
			size_t size;
		} listxattr;
		struct {
			struct spdk_fsdev_file_object *fobject;
			const char *name;
		} removexattr;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
		} flush;
		struct {
			struct spdk_fsdev_file_object *fobject;
			uint32_t flags;
		} opendir;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			uint64_t offset;
			spdk_fsdev_readdir_entry_cb *entry_cb_fn;
		} readdir;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
		} releasedir;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			bool datasync;
		} fsyncdir;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			enum spdk_fsdev_file_lock_op operation;
		} flock;
		struct {
			struct spdk_fsdev_file_object *parent_fobject;
			const char *name;
			mode_t mode;
			uint32_t flags;
			mode_t umask;
			uid_t euid;
			gid_t egid;
		} create;
		struct {
			uint64_t unique_to_abort;
		} abort;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			int mode;
			off_t offset;
			off_t length;
		} fallocate;
		struct {
			struct spdk_fsdev_file_object *fobject_in;
			struct spdk_fsdev_file_handle *fhandle_in;
			off_t off_in;
			struct spdk_fsdev_file_object *fobject_out;
			struct spdk_fsdev_file_handle *fhandle_out;
			off_t off_out;
			size_t len;
			uint32_t flags;
		} copy_file_range;
		struct {
			struct spdk_fsdev_file_object *fobject;
		} syncfs;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			uint32_t mask;
			uid_t uid;
			uid_t gid;
		} access;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			off_t offset;
			enum spdk_fsdev_seek_whence whence;
		} lseek;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			uint32_t events;
			bool wait;
		} poll;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			uint32_t request;
			uint64_t arg;
			struct iovec *in_iov;
			uint32_t in_iovcnt;
			struct iovec *out_iov;
			uint32_t out_iovcnt;
		} ioctl;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			struct spdk_fsdev_file_lock lock;
			uint64_t owner;
		} getlk;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			struct spdk_fsdev_file_lock lock;
			uint64_t owner;
			bool wait;
		} setlk;
	} u_in;

	union {
		struct {
			struct spdk_fsdev_mount_opts opts;
			struct spdk_fsdev_file_object *root_fobject;
		} mount;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_attr attr;
		} lookup;
		struct {
			struct spdk_fsdev_file_attr attr;
		} getattr;
		struct {
			struct spdk_fsdev_file_attr attr;
		} setattr;
		struct {
			char *linkname; /* will be freed by the fsdev layer */
		} readlink;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_attr attr;
		} symlink;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_attr attr;
		} mknod;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_attr attr;
		} mkdir;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_attr attr;
		} link;
		struct {
			struct spdk_fsdev_file_handle *fhandle;
		} open;
		struct {
			uint32_t data_size;
		} read;
		struct {
			uint32_t data_size;
		} write;
		struct {
			struct spdk_fsdev_file_statfs statfs;
		} statfs;
		struct {
			size_t value_size;
		} getxattr;
		struct {
			size_t data_size;
			bool size_only;
		} listxattr;
		struct {
			struct spdk_fsdev_file_handle *fhandle;
		} opendir;
		struct {
			struct spdk_fsdev_file_object *fobject;
			struct spdk_fsdev_file_handle *fhandle;
			struct spdk_fsdev_file_attr attr;
		} create;
		struct {
			size_t data_size;
		} copy_file_range;
		struct {
			struct spdk_fsdev_file_attr attr;
			uint32_t mask;
			uid_t uid;
			uid_t gid;
		} access;
		struct {
			off_t offset;
			enum spdk_fsdev_seek_whence whence;
		} lseek;
		struct {
			uint32_t revents;
		} poll;
		struct {
			int32_t result;
			struct iovec *in_iov;
			uint32_t in_iovcnt;
			struct iovec *out_iov;
			uint32_t out_iovcnt;
		} ioctl;
		struct {
			struct spdk_fsdev_file_lock lock;
		} getlk;
	} u_out;

	/**
	 *  Fields that are used internally by the fsdev subsystem. Fsdev modules
	 *  must not read or write to these fields.
	 */
	struct __fsdev_io_internal_fields {
		/** The fsdev I/O channel that this was handled on. */
		struct spdk_fsdev_channel *ch;

		/** The fsdev descriptor that was used when submitting this I/O. */
		struct spdk_fsdev_desc *desc;

		/**
		 * Set to true while the fsdev module submit_request function is in progress.
		 *
		 * This is used to decide whether spdk_fsdev_io_complete() can complete the I/O directly
		 * or if completion must be deferred via an event.
		 */
		bool in_submit_request;

		/** IO operation */
		enum spdk_fsdev_io_type type;

		/** IO unique ID */
		uint64_t unique;

		/** User callback */
		spdk_fsdev_cpl_cb *usr_cb_fn;

		/** The context for the user callback */
		void *usr_cb_arg;

		/** Status for the IO */
		int status;

		/** Member used for linking child I/Os together. */
		TAILQ_ENTRY(spdk_fsdev_io) link;

		/** Entry to the list per_thread_cache of struct spdk_fsdev_mgmt_channel. */
		STAILQ_ENTRY(spdk_fsdev_io) buf_link;

		/** Entry to the list io_submitted of struct spdk_fsdev_channel */
		TAILQ_ENTRY(spdk_fsdev_io) ch_link;

		/** Cleanup callback - an optional callback which can be set by module */
		void *cleanup_cb_fn;

		/** Cleanup callback's param */
		void *cleanup_cb_arg;

		/* Timestamp */
		uint64_t submit_tsc;
	} internal;

	/**
	 * Per I/O context for use by the fsdev module.
	 */
	uint8_t driver_ctx[0];

	/* No members may be added after driver_ctx! */
};

/**
 * Get I/O type
 *
 * \param fsdev_io I/O to complete.
 *
 * \return operation code associated with the I/O
 */
static inline enum spdk_fsdev_io_type
spdk_fsdev_io_get_type(struct spdk_fsdev_io *fsdev_io) {
	return fsdev_io->internal.type;
}

/**
 * Init I/O request structure.
 *
 * \param fsdev_io I/O request.
 * \param desc Filesystem device descriptor.
 * \param ch I/O channel.
 * \param unique Unique I/O id.
 * \param type I/O type.
 * \param cb_fn Completion callback.
 * \param cb_arg Context to be passed to the completion callback.
 */
void spdk_fsdev_io_init(struct spdk_fsdev_io *fsdev_io, struct spdk_fsdev_desc *desc,
			struct spdk_io_channel *ch,
			uint64_t unique, enum spdk_fsdev_io_type type,
			spdk_fsdev_cpl_cb *cb_fn, void *cb_arg);

/**
 * Submit I/O request.
 *
 * \param fsdev_io I/O request.
 */
void spdk_fsdev_io_submit(struct spdk_fsdev_io *fsdev_io);

/* Poll operation type. */
enum spdk_fsdev_poll_event_type {
	/* Indicates that there is data to read (regular data). */
	SPDK_FSDEV_POLLIN     = 0x0001,

	/* Indicates that normal data (not out-of-band) can be read. */
	SPDK_FSDEV_POLLRDNORM = 0x0040,

	/* Indicates that priority data (out-of-band) can be read. */
	SPDK_FSDEV_POLLRDBAND = 0x0080,

	/* Indicates that high-priority data (such as out-of-band data) is available
	 * to read. */
	SPDK_FSDEV_POLLPRI    = 0x0002,

	/* Indicates that writing is possible without blocking. */
	SPDK_FSDEV_POLLOUT    = 0x0004,

	/* Equivalent to SPDK_FSDEV_POLLOUT; indicates that normal data can be written. */
	SPDK_FSDEV_POLLWRNORM = 0x0100,

	/* Indicates that priority data can be written. */
	SPDK_FSDEV_POLLWRBAND = 0x0200,

	/* Indicates that an error has occurred on the file descriptor (only
	 * returned in revents). */
	SPDK_FSDEV_POLLERR    = 0x0008,

	/* Indicates a hang-up on the file descriptor, such as a disconnected
	 * device (only returned in revents). */
	SPDK_FSDEV_POLLHUP    = 0x0010,

	/* Indicates that the file descriptor is invalid (only returned in revents). */
	SPDK_FSDEV_POLLNVAL   = 0x0020
};

/**
 * Rename2 API flags.
 *
 * SPDK_FSDEV_RENAME_EXCHANGE - Atomically exchange oldpath and newpath. Both pathnames must
 * exist but may be of different types (e.g., one could be a non-empty directory and the other
 * a symbolic link).
 * SPDK_FSDEV_RENAME_NOREPLACE - Don't overwrite newpath of the rename. Return an error if
 * newpath already exists.
 * SPDK_FSDEV_RENAME_WHITEOUT - Specifying RENAME_WHITEOUT creates a "whiteout" object at the
 * source of the rename at the same time as performing the rename. The whole operation is
 * atomic, so that if the rename succeeds then the whiteout will also have been created.
 */
#define SPDK_FSDEV_RENAME_EXCHANGE  (1 << 0)
#define SPDK_FSDEV_RENAME_NOREPLACE (1 << 1)
#define SPDK_FSDEV_RENAME_WHITEOUT  (1 << 2)

/*
 * Flags used in setxattr operation.
 *
 * SPDK_FSDEV_XATTR_CREATE - Perform a pure create, which fails if the named attribute exists already.
 * SPDK_FSDEV_XATTR_REPLACE - Perform a pure replace operation, which fails if the named attribute
 * does not already exist.
 * SPDK_FSDEV_SETXATTR_ACL_KILL_SGID - Clear SGID when system.posix_acl_access is set.
 */
#define SPDK_FSDEV_XATTR_CREATE (1 << 0)
#define SPDK_FSDEV_XATTR_REPLACE (1 << 1)
#define SPDK_FSDEV_SETXATTR_ACL_KILL_SGID (1 << 2)

/**
 * WRITE flags
 *
 * SPDK_FSDEV_WRITE_CACHE: delayed write from page cache, file handle is guessed
 * SPDK_FSDEV_WRITE_LOCKOWNER: lock_owner field is valid
 * SPDK_FSDEV_WRITE_KILL_SUIDGID: kill suid and sgid bits
 */
#define SPDK_FSDEV_WRITE_CACHE		(1ULL << 32)
#define SPDK_FSDEV_WRITE_LOCKOWNER	(1ULL << 33)
#define SPDK_FSDEV_WRITE_KILL_SUIDGID	(1ULL << 34)

#define SPDK_FSDEV_FALLOC_FL_KEEP_SIZE     0x01 /* default is extend size */
#define SPDK_FSDEV_FALLOC_FL_PUNCH_HOLE    0x02 /* de-allocates range */
#define SPDK_FSDEV_FALLOC_FL_NO_HIDE_STALE 0x04 /* reserved codepoint */

/*
 * SPDK_FSDEV_FALLOC_FL_COLLAPSE_RANGE is used to remove a range of a file
 * without leaving a hole in the file. The contents of the file beyond
 * the range being removed is appended to the start offset of the range
 * being removed (i.e. the hole that was punched is "collapsed"),
 * resulting in a file layout that looks like the range that was
 * removed never existed. As such collapsing a range of a file changes
 * the size of the file, reducing it by the same length of the range
 * that has been removed by the operation.
 *
 * Different filesystems may implement different limitations on the
 * granularity of the operation. Most will limit operations to
 * filesystem block size boundaries, but this boundary may be larger or
 * smaller depending on the filesystem and/or the configuration of the
 * filesystem or file.
 *
 * Attempting to collapse a range that crosses the end of the file is
 * considered an illegal operation - just use ftruncate(2) if you need
 * to collapse a range that crosses EOF.
 */
#define SPDK_FSDEV_FALLOC_FL_COLLAPSE_RANGE        0x08

/*
 * SPDK_FSDEV_FALLOC_FL_ZERO_RANGE is used to convert a range of file to zeros
 * preferably without issuing data IO. Blocks should be preallocated for the
 * regions that span holes in the file, and the entire range is preferable
 * converted to unwritten extents - even though file system may choose to zero
 * out the extent or do whatever which will result in reading zeros from the range
 * while the range remains allocated for the file.
 *
 * This can be also used to preallocate blocks past EOF in the same way as
 * with fallocate. Flag FALLOC_FL_KEEP_SIZE should cause the inode
 * size to remain the same.
 */
#define SPDK_FSDEV_FALLOC_FL_ZERO_RANGE            0x10

/*
 * SPDK_FSDEV_FALLOC_FL_INSERT_RANGE is use to insert space within the file size
 * without overwriting any existing data. The contents of the file beyond offset
 * are shifted towards right by len bytes to create a hole. As such, this
 * operation will increase the size of the file by len bytes.
 *
 * Different filesystems may implement different limitations on the granularity
 * of the operation. Most will limit operations to filesystem block size
 * boundaries, but this boundary may be larger or smaller depending on
 * the filesystem and/or the configuration of the filesystem or file.
 *
 * Attempting to insert space using this flag at OR beyond the end of
 * the file is considered an illegal operation - just use ftruncate(2) or
 * fallocate(2) with mode 0 for such type of operations.
 */
#define SPDK_FSDEV_FALLOC_FL_INSERT_RANGE          0x20

/*
 * SPDK_FSDEV_FALLOC_FL_UNSHARE_RANGE is used to unshare shared blocks within the
 * file size without overwriting any existing data. The purpose of this
 * call is to preemptively reallocate any blocks that are subject to
 * copy-on-write.
 *
 * Different filesystems may implement different limitations on the
 * granularity of the operation. Most will limit operations to filesystem
 * block size boundaries, but this boundary may be larger or smaller
 * depending on the filesystem and/or the configuration of the filesystem
 * or file.
 *
 * This flag can only be used with allocate-mode fallocate, which is
 * to say that it cannot be used with the punch, zero, collapse, or
 * insert range modes.
 */
#define SPDK_FSDEV_FALLOC_FL_UNSHARE_RANGE         0x40

#ifdef __cplusplus
}
#endif

#endif /* SPDK_FSDEV_H */
