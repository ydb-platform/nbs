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

/* FUSE stuff may have already been included through a different header file. */
#ifndef FUSE_KERNEL_VERSION
#include "spdk/linux/fuse.h"
#elif FUSE_KERNEL_VERSION < 7 || (FUSE_KERNEL_VERSION == 7 && FUSE_KERNEL_MINOR_VERSION < 31)
#error FUSE kernel header conflict - fsdev requires at least version 7.31
#endif

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
	 * The size of spdk_fsdev_opts (obtained by SPDK_SIZEOF(opts, last_member)) according to the
	 * caller of this library is used for ABI compatibility.  The library uses this field to know
	 * how many fields in this structure are valid. And the library will populate any remaining
	 * fields with default values. New added fields should be put at the end of the struct.
	 */
	uint32_t opts_size;
	uint32_t reserved1;
	uint32_t reserved2;
	/**
	 * Max source ID.
	 */
	uint16_t max_source_id;
	/**
	 * Recovery enabled.
	 */
	uint8_t recovery_enabled;
	/** Verify source_unique values */
	bool verify_source_unique;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_fsdev_opts) == 16, "Incorrect size");

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

	/** Hole at bytes 12-15. */
	uint8_t reserved[4];

	/**
	 * IN/OUT Contains requested and negotiated fsdev mount flags.
	 */
	uint64_t flags;
} __attribute__((packed));
SPDK_STATIC_ASSERT(sizeof(struct spdk_fsdev_mount_opts) == 24, "Incorrect size");

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

/* There are fewer than 64 FUSE opcodes (52 as of writing this comment in
 * September 2025), but leave some space to account for more being added
 * in the future. Only 3 opcodes were added between 2019 and 2025.
 */
#define SPDK_FSDEV_MAX_FUSE_OPC	64

/* We define it ourselves a bit higher than what's in fuse.h to avoid
 * changing ABI as new notification types are added. Currently fuse.h has
 * 9 (as of v6.18).
 */
#define SPDK_FSDEV_MAX_NOTIFY_OPC 16

/**
 * fsdev IO statistics
 */
struct spdk_fsdev_io_stat {
	/** Stats by opcode */
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
	} io[SPDK_FSDEV_MAX_FUSE_OPC];
	/** Number of bytes read */
	uint64_t bytes_read;
	/** Number of bytes written */
	uint64_t bytes_written;
	/** Notifications statistics by type */
	struct {
		/** Number of emitted notifications */
		uint64_t count;
		/** Number of received notification replies */
		uint64_t replies;
	} notify[SPDK_FSDEV_MAX_NOTIFY_OPC];
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

struct spdk_fsdev_open_opts {
	/** Size of this structure */
	size_t			size;
	/** Event notification callback */
	spdk_fsdev_event_cb_t	event_cb_fn;
	/** Event notification context */
	void			*event_cb_ctx;
	/** Maximum number of segments per I/O operation */
	uint32_t		max_segments;
	/** Maximum size of payload in a single I/O operation */
	uint32_t		max_xfer_size;
};

/**
 * Open a filesystem device for I/O operations.
 *
 * \param name Filesystem device name to open.
 * \param opts Open options.
 * \param desc Open descriptor.
 *
 * \return 0 on success, negative errno otherwise.
 */
int spdk_fsdev_open_ext(const char *name, struct spdk_fsdev_open_opts *opts,
			struct spdk_fsdev_desc **desc);

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
 * Get fuse opc name
 *
 * \param opc FUSE opcode
 *
 * \return non-NULL name if operation is successful, or NULL otherwise.
 */
const char *spdk_fsdev_get_opcode_name(uint32_t opc);

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
 * Get the name of the filesystem associated with an fsdev descriptor.
 *
 * \param desc Open filesystem device descriptor
 *
 * \return Name of the filesystem.
 */
static inline const char *
spdk_fsdev_desc_get_name(struct spdk_fsdev_desc *desc)
{
	return spdk_fsdev_get_name(spdk_fsdev_desc_get_fsdev(desc));
}

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
 * Get SPDK memory domain types used by the given fsdev. If fsdev reports that it uses memory domains
 * that means that it can work with data buffers located in those memory domains.
 *
 * The user can call this function with \b types set to NULL and \b array_size set to 0 to get the
 * number of memory domains types used by fsdev
 *
 * \param fsdev filesystem device
 * \param types pointer to an array of memory domain types to be filled by this function. The user should allocate big enough
 * array to keep all memory domain types used by fsdev and all underlying fsdevs
 * \param array_size size of \b types array
 * \return the number of entries in \b types array or negated errno. If returned value is bigger than \b array_size passed by the user
 * then the user should increase the size of the array and call this function again. There is no guarantee that
 * the content of the array is valid in that case.
 *         -EINVAL if input parameters were invalid
 */
int spdk_fsdev_get_memory_domain_types(struct spdk_fsdev *fsdev,
				       enum spdk_dma_device_type *types,
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

struct spdk_fuse_notify_request;

typedef void (*spdk_fuse_notify_request_cb)(struct spdk_fuse_notify_request *req, int status);

struct spdk_fuse_notify_request {
	struct spdk_fsdev			*fsdev;
	struct iovec				*iovs;
	uint32_t				iovcnt;
	struct {
		union {
			struct {
				uint8_t		in_submit_notify : 1;
				uint8_t		reserved : 7;
			};
			uint8_t			raw;
		} flags;
		uint8_t				reserved[3];
		int				status;
	} internal;
	spdk_fuse_notify_request_cb		cb_fn;
	STAILQ_ENTRY(spdk_fuse_notify_request)	stailq;
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
 * \param request Data for the filesystem device notification.
 * Data is only valid in the context of this callback.
 * \param reply_cb Optional notification reply callback. If NULL, fsdev doesn't need a reply for this notification.
 * Fsdev should be ready to get the reply callback in the context of notify callback.
 * \param reply_ctx Context for the filesystem device notification. Should be passed in reply_cb.
 */
typedef void (*spdk_fsdev_notify_cb_t)(struct spdk_fsdev *fsdev,
				       void *ctx,
				       const struct spdk_fuse_notify_request *request,
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
 * and does not include fixed size fields in spdk_fuse_notify_request structure.
 * Example of variable sized data is 'name' in FUSE_NOTIFY_INVAL_ENTRY notification.
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
 * Filesystem device IO completion callback.
 *
 * \param fsdev_io Filesystem device I/O that has completed.
 * \param cb_arg Callback argument specified when fsdev_io was submitted.
 */
typedef void (*spdk_fsdev_io_completion_cb)(struct spdk_fsdev_io *fsdev_io, void *cb_arg);

struct spdk_fuse_in {
	struct fuse_in_header *hdr;
	union {
		void *raw;
		struct fuse_init_in *init;
		struct fuse_forget_in *forget;
		struct fuse_batch_forget_in *batch_forget;
		struct fuse_getattr_in *getattr;
		struct fuse_statx_in *statx;
		struct fuse_mknod_in *mknod;
		struct fuse_mkdir_in *mkdir;
		struct fuse_rename_in *rename;
		struct fuse_rename2_in *rename2;
		struct fuse_link_in *link;
		struct fuse_setattr_in *setattr;
		struct fuse_open_in *open;
		struct fuse_create_in *create;
		struct fuse_release_in *release;
		struct fuse_flush_in *flush;
		struct fuse_read_in *read;
		struct fuse_write_in *write;
		struct fuse_fsync_in *fsync;
		struct fuse_setxattr_in *setxattr;
		struct fuse_getxattr_in *getxattr;
		struct fuse_lk_in *lk;
		struct fuse_access_in *access;
		struct fuse_interrupt_in *interrupt;
		struct fuse_ioctl_in *ioctl;
		struct fuse_poll_in *poll;
		struct fuse_fallocate_in *fallocate;
		struct fuse_lseek_in *lseek;
		struct fuse_copy_file_range_in *copy_file_range;
		struct fuse_syncfs_in *syncfs;
	} op;
	struct iovec *iov;
	uint32_t iovcnt;
	struct spdk_memory_domain *memory_domain;
	void *memory_domain_ctx;
};

struct spdk_fuse_create_out {
	struct fuse_entry_out	entry;
	struct fuse_open_out	open;
};

struct spdk_fuse_out {
	struct fuse_out_header *hdr;
	union {
		void *raw;
		struct fuse_init_out *init;
		struct fuse_entry_out *entry;
		struct fuse_open_out *open;
		struct fuse_attr_out *attr;
		struct fuse_write_out *write;
		struct fuse_getxattr_out *getxattr;
		struct fuse_statfs_out *statfs;
		struct spdk_fuse_create_out *create;
		struct fuse_lseek_out *lseek;
		struct fuse_lk_out *lk;
		struct fuse_poll_out *poll;
		struct fuse_ioctl_out *ioctl;
	} op;
	struct iovec *iov;
	uint32_t iovcnt;
	struct spdk_memory_domain *memory_domain;
	void *memory_domain_ctx;
};

struct spdk_fsdev_io {
	/** The filesystem device that this I/O belongs to. */
	struct spdk_fsdev *fsdev;

	/** A single iovec element for use by this fsdev_io. */
	struct iovec iov;

	union {
		struct spdk_fuse_in fuse;
	} u_in;

	union {
		struct spdk_fuse_out fuse;
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

		/** iovecs from caller containing fuse_in_header, command header
		 *  and payload, for fsdev to build u_in.fuse. These are not
		 *  used if the caller sets up u_in.fuse directly.
		 */
		struct iovec *in_iov;

		/** Number of iovecs in in_iov. */
		uint32_t in_iovcnt;

		/** iovecs from caller containing fuse_out_header, command header
		 *  and payload, for fsdev to build u_out.fuse. These are not
		 *  used if the caller sets up u_out.fuse directly.
		 */
		struct iovec *out_iov;

		/** Number of iovecs in out_iov. */
		uint32_t out_iovcnt;

		/** Copy of up to first 2 iovecs in in_iov. These are needed
		 *  if fsdev needs to modify the original iovs when combinations
		 *  of headers and payload appear in the same iov.
		 */
		struct iovec orig_in_iov[2];

		/** Copy of up to first 2 iovecs in out_iov (if one exists). This is
		 *  needed if fsdev needs to modify the original iovs when
		 *  a combination of headers and payload appear in the same iov.
		 */
		struct iovec orig_out_iov[2];

		/**
		 * Set to true while the fsdev module submit_request function is in progress.
		 *
		 * This is used to decide whether spdk_fsdev_io_complete() can complete the I/O directly
		 * or if completion must be deferred via an event.
		 */
		bool in_submit_request;

		/** User callback */
		spdk_fsdev_cpl_cb *usr_cb_fn;

		/** The context for the user callback */
		void *usr_cb_arg;

		/** Status for the IO */
		int status;

		/* Source ID */
		uint16_t source_id;

		/* Source Unique */
		uint64_t source_unique;

		/** Member used for linking child I/Os together. */
		TAILQ_ENTRY(spdk_fsdev_io) link;

		/** Entry to the list per_thread_cache of struct spdk_fsdev_mgmt_channel. */
		STAILQ_ENTRY(spdk_fsdev_io) buf_link;

		/** Entry to the list io_submitted of struct spdk_fsdev_channel */
		TAILQ_ENTRY(spdk_fsdev_io) ch_link;

		/* Timestamp */
		uint64_t submit_tsc;

		/* TSC when delayed IO will be sent to fsdev module */
		uint64_t delayed_submit_tsc;

		/* TSC when delayed IO will be completed to upper layer */
		uint64_t delayed_complete_tsc;

		/** Entry to the list io_submitted of struct spdk_fsdev_channel */
		TAILQ_ENTRY(spdk_fsdev_io) delay_link;
	} internal;

	/**
	 * Per I/O context for use by the fsdev module.
	 */
	uint8_t driver_ctx[0];

	/* No members may be added after driver_ctx! */
};

/**
 * Init I/O request structure.
 *
 * \param fsdev_io I/O request.
 * \param desc Filesystem device descriptor.
 * \param ch I/O channel.
 * \param source_id Source id.
 * \param source_unique Source unique.
 * \param cb_fn Completion callback.
 * \param cb_arg Context to be passed to the completion callback.
 */
void spdk_fsdev_io_init(struct spdk_fsdev_io *fsdev_io, struct spdk_fsdev_desc *desc,
			struct spdk_io_channel *ch,
			uint16_t source_id, uint64_t source_unique,
			spdk_fsdev_cpl_cb *cb_fn, void *cb_arg);

/**
 * Submit I/O request.
 *
 * \param fsdev_io I/O request.
 */
void spdk_fsdev_io_submit(struct spdk_fsdev_io *fsdev_io);

/**
 * Build and submit I/O request from FUSE iovecs
 *
 * \param fsdev_io fsdev_io data buffer to be built and submitted by this function
 * \param desc fsdev descriptor
 * \param ch I/O channel
 * \param in_iov Input IO vectors array containing FUSE headers (and payload if domain is NULL).
 * \param in_iovcnt Size of the input IO vectors array.
 * \param out_iov Output IO vectors array containing FUSE headers (and payload if domain is NULL).
 * \param out_iovcnt Size of the output IO vectors array.
 * \param source_id Source ID
 * \param source_unique per Source ID unique value
 * \param domain Memory domain describing the data buffers. If non-NULL, must be FUSE_READ or
 *               FUSE_WRITE opcode and domain_iov/domain_iovcnt describe the payload buffers.
 * \param domain_ctx Memory domain context.
 * \param domain_iov IO vectors array for payload when domain is non-NULL.
 * \param domain_iovcnt Size of the domain IO vectors array.
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
int spdk_fsdev_io_submit_from_fuse_iovs(struct spdk_fsdev_io *fsdev_io,
					struct spdk_fsdev_desc *desc,
					struct spdk_io_channel *ch,
					struct iovec *in_iov, int in_iovcnt,
					struct iovec *out_iov, int out_iovcnt,
					uint16_t source_id, uint64_t source_unique,
					struct spdk_memory_domain *domain, void *domain_ctx,
					struct iovec *domain_iov, int domain_iovcnt,
					spdk_fsdev_cpl_cb cb, void *cb_arg);

/**
 * Specify submission and/or completions delays for I/O for the specified fsdev.
 *
 * 99% completion delay means that 1-out-of-100 I/O on each channel will incur
 * the specified amount of delay before completion to the upper layer. The other
 * 99 I/O will incur the regular completion delay. If 99% completion delay is 0
 * (disabled), then all I/O inclur the regular completion delay.
 *
 * Note that all I/O must always be submitted to the fsdev module in source
 * unique order, so 99% submission delay is not an option.
 *
 * \param fsdev The fsdev to set delays for
 * \param submit_us Submission delay in microseconds
 * \param complete_us Completion delay in microseconds
 * \param complete_99_us 99% completion delay in microseconds
 * \return 0 for success, negative errno for failure
 */
int spdk_fsdev_set_delays(struct spdk_fsdev *fsdev, uint64_t submit_us,
			  uint64_t complete_us, uint64_t complete_99_us);

/**
 * Encode FUSE notification
 *
 * \param iov Output IO vectors array.
 * \param iovcnt Size of the output IO vectors array.
 * \param req Notification request received from fsdev.
 * \param unique_id Unique ID of the notification.
 *
 * \return 0 on success, negated errno on failure.
 */
int spdk_fsdev_encode_notify(struct iovec *iov, int iovcnt,
			     const struct spdk_fuse_notify_request *req,
			     uint64_t unique_id);


#ifdef __cplusplus
}
#endif

#endif /* SPDK_FSDEV_H */
