/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#ifndef SPDK_FSDEV_MODULE_H
#define SPDK_FSDEV_MODULE_H

#include "spdk/stdinc.h"
#include "spdk/fsdev.h"
#include "spdk/queue.h"
#include "spdk/tree.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Filesystem device I/O
 *
 * This is an I/O that is passed to an spdk_fsdev.
 */
struct spdk_fsdev_io;

/** Filesystem device module */
struct spdk_fsdev_module {
	/**
	 * Initialization function for the module. Called by the fsdev library
	 * during startup.
	 *
	 * Modules are required to define this function.
	 */
	int (*module_init)(void);

	/**
	 * Finish function for the module. Called by the fsdev library
	 * after all fsdevs for all modules have been unregistered.  This allows
	 * the module to do any final cleanup before the fsdev library finishes operation.
	 *
	 * Modules are not required to define this function.
	 */
	void (*module_fini)(void);

	/**
	 * Function called to return a text string representing the module-level
	 * JSON RPCs required to regenerate the current configuration.  This will
	 * include module-level configuration options, or methods to construct
	 * fsdevs when one RPC may generate multiple fsdevs.
	 *
	 * Per-fsdev JSON RPCs (where one "construct" RPC always creates one fsdev)
	 * may be implemented here, or by the fsdev's write_config_json function -
	 * but not both.  Fsdev module implementers may choose which mechanism to
	 * use based on the module's design.
	 *
	 * \return 0 on success or Fsdev specific negative error code.
	 */
	int (*config_json)(struct spdk_json_write_ctx *w);

	/** Name for the module being defined. */
	const char *name;

	/**
	 * Returns the allocation size required for the backend for uses such as local
	 * command structs, local SGL, iovecs, or other user context.
	 */
	int (*get_ctx_size)(void);

	/**
	 * Fields that are used by the internal fsdev subsystem. Fsdev modules
	 *  must not read or write to these fields.
	 */
	struct __fsdev_module_internal_fields {
		TAILQ_ENTRY(spdk_fsdev_module) tailq;
	} internal;
};

typedef void (*spdk_fsdev_unregister_cb)(void *cb_arg, int rc);

typedef void (*spdk_fsdev_reset_done_cb)(void *cb_arg, int rc);

/**
 * Function table for a filesystem device backend.
 *
 * The backend filesystem device function table provides a set of APIs to allow
 * communication with a backend.
 */
struct spdk_fsdev_fn_table {
	/** Destroy the backend filesystem device object */
	int (*destruct)(void *ctx);

	/** Process the I/O request.
	 *
	 * Note regarding the fobject reference counting.
	 *
	 * The following IO types increase the fobject reference counter for the result fobject in case of success:
	 * - SPDK_FSDEV_IO_LOOKUP
	 * - SPDK_FSDEV_IO_CREATE
	 * - SPDK_FSDEV_IO_LINK
	 * - SPDK_FSDEV_IO_MKDIR
	 * - SPDK_FSDEV_IO_MKNOD
	 * - SPDK_FSDEV_IO_SYMLINK
	 * - SPDK_FSDEV_IO_READDIR (depends on the value of \p forget set by the \p spdk_fsdev_readdir_entry_cb)
	 *
	 * The SPDK_FSDEV_IO_FORGET decreases the fobject reference counter by \p nlookup.
	 *
	 * The SPDK_FSDEV_IO_UMOUNT causes all the fobject reference counters to be implicitly dropped to zero.
	 * It is not guaranteed that the fsdev will receive corresponding SPDK_FSDEV_IO_FORGET I/Os for the affected
	 * fobjects.
	 */
	void (*submit_request)(struct spdk_io_channel *ch, struct spdk_fsdev_io *);

	/** Get an I/O channel for the specific fsdev for the calling thread. */
	struct spdk_io_channel *(*get_io_channel)(void *ctx);

	/**
	 * Output fsdev-specific RPC configuration to a JSON stream. Optional - may be NULL.
	 *
	 * The JSON write context will be initialized with an open object, so the fsdev
	 * driver should write all data necessary to recreate this fsdev by invoking
	 * constructor method. No other data should be written.
	 */
	void (*write_config_json)(struct spdk_fsdev *fsdev, struct spdk_json_write_ctx *w);

	/** Get memory domains used by fsdev. Optional - may be NULL.
	 * Vfsdev module implementation should call \ref spdk_fsdev_get_memory_domains for underlying fsdev.
	 * Vfsdev module must inspect types of memory domains returned by base fsdev and report only those
	 * memory domains that it can work with. */
	int (*get_memory_domains)(void *ctx, struct spdk_memory_domain **domains, int array_size);

	/**
	 * Perform an asynchronous device reset. Optional - may be NULL. NULL means that the device cannot be reset.
	 *
	 * Resets all the IOs associated with the fsdev.
	 * This will pass a message to every other thread associated with the fsdev for which an I/O channel exists for the fsdev.
	 * Regardless of device type, all outstanding I/Os to the filesystem device will be completed prior to the reset completing.
	 * Moreover, the module must guarantee to complete all pending IOs, and not depend on existence of remote services which may be down.
	 */
	int (*reset)(void *ctx, spdk_fsdev_reset_done_cb cb, void *cb_arg);

	/**
	 * Check whether the device state was recovered upon creation. Optional - may be NULL.
	 * NULL means that the device does not support recovery.
	 */
	bool (*is_recovered)(void *ctx);

	/**
	 * Output driver-specific information to a JSON stream. Optional - may be NULL.
	 *
	 * The JSON write context will be initialized with an open object, so the fsdev
	 * driver should write a name (based on the driver name) followed by a JSON value
	 * (most likely another nested object).
	 */
	int (*dump_info_json)(void *ctx, struct spdk_json_write_ctx *w);

	/**
	 * Enable or disable notifications.
	 */
	int (*set_notifications)(void *ctx, bool enabled);
};

struct spdk_fsdev_name {
	char *name;
	struct spdk_fsdev *fsdev;
	RB_ENTRY(spdk_fsdev_name) node;
};

struct spdk_fsdev_file_handle;
struct spdk_fsdev_file_object;


/** The node ID of the root inode */
#define SPDK_FUSE_ROOT_ID 1 /* Must be the same as FUSE_ROOT_ID in the fuse_kernel.h to avoid translation */

struct spdk_fsdev {
	/** User context passed in by the backend */
	void *ctxt;

	/** Unique name for this filesystem device. */
	char *name;

	/**
	 * Pointer to the fsdev module that registered this fsdev.
	 */
	struct spdk_fsdev_module *module;

	/** function table for all ops */
	const struct spdk_fsdev_fn_table *fn_table;

	/** Maximum size of variable sized notification data in bytes. */
	uint32_t notify_max_data_size;

	/** Fields that are used internally by the fsdev subsystem. Fsdev modules
	 *  must not read or write to these fields.
	 */
	struct __fsdev_internal_fields {
		/** Lock protecting fsdev */
		struct spdk_spinlock spinlock;

		/** The fsdev status */
		enum spdk_fsdev_status status;

		/** unique ID for trace */
		uint16_t trace_id;

		/** Callback function that will be called after fsdev destruct is completed. */
		spdk_fsdev_unregister_cb unregister_cb;

		/** Unregister call context */
		void *unregister_ctx;

		/** List of open descriptors for this filesystem device. */
		TAILQ_HEAD(, spdk_fsdev_desc) open_descs;

		/** Notifications callback. */
		spdk_fsdev_notify_cb_t notify_cb;

		/** Notifications callback context. */
		void *notify_ctx;

		TAILQ_ENTRY(spdk_fsdev) link;

		/** Fsdev name used for quick lookup */
		struct spdk_fsdev_name fsdev_name;

		/** true if fsdev reset is in progress */
		bool reset_in_progress;

		/** accumulated I/O statistics for previously deleted channels of this fsdev */
		struct spdk_fsdev_io_stat *hist_stat;
	} internal;
};

/**
 * Register a new fsdev.
 *
 * \param fsdev Filesystem device to register.
 *
 * \return 0 on success.
 * \return -EINVAL if the fsdev name is NULL.
 * \return -EEXIST if a fsdev with the same name already exists.
 */
int spdk_fsdev_register(struct spdk_fsdev *fsdev);

/**
 * Start unregistering a fsdev. This will notify each currently open descriptor
 * on this fsdev of the hotremoval to request the upper layers to stop using this fsdev
 * and manually close all the descriptors with spdk_fsdev_close().
 * The actual fsdev unregistration may be deferred until all descriptors are closed.
 *
 * Note: spdk_fsdev_unregister() can be unsafe unless the fsdev is not opened before and
 * closed after unregistration. It is recommended to use spdk_fsdev_unregister_by_name().
 *
 * \param fsdev Filesystem device to unregister.
 * \param cb_fn Callback function to be called when the unregister is complete.
 * \param cb_arg Argument to be supplied to cb_fn
 */
void spdk_fsdev_unregister(struct spdk_fsdev *fsdev, spdk_fsdev_unregister_cb cb_fn, void *cb_arg);

/**
 * Start unregistering a fsdev. This will notify each currently open descriptor
 * on this fsdev of the hotremoval to request the upper layer to stop using this fsdev
 * and manually close all the descriptors with spdk_fsdev_close().
 * The actual fsdev unregistration may be deferred until all descriptors are closed.
 *
 * \param fsdev_name Filesystem device name to unregister.
 * \param module Module by which the filesystem device was registered.
 * \param cb_fn Callback function to be called when the unregister is complete.
 * \param cb_arg Argument to be supplied to cb_fn
 *
 * \return 0 on success, or suitable errno value otherwise
 */
int spdk_fsdev_unregister_by_name(const char *fsdev_name, struct spdk_fsdev_module *module,
				  spdk_fsdev_unregister_cb cb_fn, void *cb_arg);

/**
 * Invokes the unregister callback of a fsdev backing a virtual fsdev.
 *
 * A Fsdev with an asynchronous destruct path should return 1 from its
 * destruct function and call this function at the conclusion of that path.
 * Fsdevs with synchronous destruct paths should return 0 from their destruct
 * path.
 *
 * \param fsdev Filesystem device that was destroyed.
 * \param fsdeverrno Error code returned from fsdev's destruct callback.
 */
void spdk_fsdev_destruct_done(struct spdk_fsdev *fsdev, int fsdeverrno);

/**
 * Indicate to the fsdev layer that the module is done initializing.
 *
 * To be called once during module_init or asynchronously after
 * an asynchronous operation required for module initialization is completed.
 *
 * \param module Pointer to the module completing the initialization.
 */
void spdk_fsdev_module_init_done(struct spdk_fsdev_module *module);

/**
 * Complete a fsdev_io
 *
 * \param fsdev_io I/O to complete.
 * \param status The I/O completion status.
 */
void spdk_fsdev_io_complete(struct spdk_fsdev_io *fsdev_io, int status);

/**
 * Get I/O unique id
 *
 * \param fsdev_io I/O to complete.
 *
 * \return I/O unique id
 */
static inline uint64_t
spdk_fsdev_io_get_unique(struct spdk_fsdev_io *fsdev_io)
{
	return fsdev_io->internal.unique;
}

/**
 * Get a thread that given fsdev_io was submitted on.
 *
 * \param fsdev_io I/O
 * \return thread that submitted the I/O
 */
struct spdk_thread *spdk_fsdev_io_get_thread(struct spdk_fsdev_io *fsdev_io);

/**
 * Get the fsdev module's I/O channel that the given fsdev_io was submitted on.
 *
 * \param fsdev_io I/O
 * \return the fsdev module's I/O channel that the given fsdev_io was submitted on.
 */
struct spdk_io_channel *spdk_fsdev_io_get_io_channel(struct spdk_fsdev_io *fsdev_io);

typedef void (*spdk_fsdev_io_cleanup_cb)(void *cb_arg);

/**
 * Set optional I/O cleanup callback.
 *
 * \param fsdev_io I/O
 * \param cb_fn Called when the I/O object is no longer in use, so a module can clean up any associated resources.
 * \param cb_arg Argument passed to function cb_fn.
 */
void spdk_fsdev_io_set_cleanup_callback(struct spdk_fsdev_io *fsdev_io,
					spdk_fsdev_io_cleanup_cb cb_fn,
					void *cb_arg);

/**
 * Add the given module to the list of registered modules.
 * This function should be invoked by referencing the macro
 * SPDK_FSDEV_MODULE_REGISTER in the module c file.
 *
 * \param fsdev_module Module to be added.
 */
void spdk_fsdev_module_list_add(struct spdk_fsdev_module *fsdev_module);

/**
 * Find registered module with name pointed by \c name.
 *
 * \param name name of module to be searched for.
 * \return pointer to module or NULL if no module with \c name exist
 */
struct spdk_fsdev_module *spdk_fsdev_module_list_find(const char *name);

static inline struct spdk_fsdev_io *
spdk_fsdev_io_from_ctx(void *ctx)
{
	return SPDK_CONTAINEROF(ctx, struct spdk_fsdev_io, driver_ctx);
}

/**
 * Send a SPDK_FSDEV_NOTIFY_INVAL_DATA notification to the user.
 *
 * \param fsdev Filesystem device.
 * \param fobject File object to invalidate.
 * \param offset Offset of data region to invalidate.
 * \param size Size of data region to invalidate.
 * \param reply_cb Callback to deliver notification handling status
 * Fsdev should be ready to get the reply callback in the context of this call.
 * \param reply_ctx Reply context
 *
 * \return 0 on success.
 * \return -ENODEV if notifications are not enabled.
 */
int spdk_fsdev_notify_inval_data(struct spdk_fsdev *fsdev,
				 struct spdk_fsdev_file_object *fobject,
				 uint64_t offset, size_t size,
				 spdk_fsdev_notify_reply_cb_t reply_cb,
				 void *reply_ctx);

/**
 * Send a SPDK_FSDEV_NOTIFY_INVAL_ENTRY notification to the user.
 *
 * \param fsdev Filesystem device.
 * \param parent_fobject Parent file object to invalidate.
 * \param name Name of entry in the parent_fobject to invalidate.
 * \param reply_cb Callback to deliver notification handling status
 * Fsdev should be ready to get the reply callback in the context of this call.
 * \param reply_ctx Reply context
 *
 * \return 0 on success.
 * \return -ENODEV if notifications are not enabled.
 */
int spdk_fsdev_notify_inval_entry(struct spdk_fsdev *fsdev,
				  struct spdk_fsdev_file_object *parent_fobject,
				  const char *name,
				  spdk_fsdev_notify_reply_cb_t reply_cb,
				  void *reply_ctx);

/**
 * Increment filesystem device notification reply statistics.
 *
 * \param fsdev Filesystem device.
 * \param type Notification type.
 */
void spdk_fsdev_notify_reply_add_stat(struct spdk_fsdev *fsdev, enum spdk_fsdev_notify_type type);

/*
 *  Macro used to register module for later initialization.
 */
#define SPDK_FSDEV_MODULE_REGISTER(name, module) \
static void __attribute__((constructor)) _spdk_fsdev_module_register_##name(void) \
{ \
	spdk_fsdev_module_list_add(module); \
}

#ifdef __cplusplus
}
#endif

#endif /* SPDK_FSDEV_MODULE_H */
