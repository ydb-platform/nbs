/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#ifndef SPDK_INTERNAL_BDEV_QOS_MODULE_H
#define SPDK_INTERNAL_BDEV_QOS_MODULE_H

#include "spdk/stdinc.h"
#include "spdk/bdev.h"

struct spdk_bdev_qos_channel;
struct spdk_bdev_qos_impl;

/** Abstract base class for module-dependent object per QoS channel */
struct spdk_bdev_qos_channel_impl {
	struct spdk_bdev_qos_channel *qos_ch;

	struct spdk_bdev_qos_impl *qos_impl;

	TAILQ_ENTRY(spdk_bdev_qos_channel_impl) link;
};

/** Abstract base class for module-dependent object per QoS device */
struct spdk_bdev_qos_impl {
	struct spdk_bdev_qos *qos;

	struct spdk_bdev_qos_module *module;

	TAILQ_ENTRY(spdk_bdev_qos_impl) link;
};

/** QoS module */
struct spdk_bdev_qos_module {
	const char *name;

	/**
	 * Initialization function for the module. Called by the bdev library during startup.
	 */
	int (*module_init)(void);

	/**
	 * Finish function for the module. Called by the bdev library after all QoS devices
	 * have been destroyed. This allows the module to do any final cleanup before the bdev
	 * library finishes operation.
	 */
	void (*module_fini)(void);

	/**
	 * Output RPC configuration to a JSON stream for the QoS module.
	 */
	void (*module_config_json)(struct spdk_json_write_ctx *w);

	/**
	 * Allocate a module-dependent object per QoS device.
	 */
	struct spdk_bdev_qos_impl *(*get_impl)(void);

	/**
	 * Free the module-dependent object per QoS device.
	 */
	void (*put_impl)(struct spdk_bdev_qos_impl *qos_impl);

	/**
	 * Output RPC configuration to a JSON stream for the module-dependent object of a QoS device.
	 */
	void (*impl_config_json)(struct spdk_bdev_qos_impl *qos_impl, struct spdk_json_write_ctx *w);

	/**
	 * Output information to a JSON stream for the module-dependent object of a QoS device.
	 */
	void (*impl_info_json)(struct spdk_bdev_qos_impl *qos_impl, struct spdk_json_write_ctx *w);

	/**
	 * Allocate a module-dependent object per QoS channel.
	 */
	struct spdk_bdev_qos_channel_impl *(*get_channel_impl)(struct spdk_bdev_qos_impl *qos_impl);

	/**
	 * Free the module-dependent object per QoS channel.
	 */
	void (*put_channel_impl)(struct spdk_bdev_qos_channel_impl *qos_ch_impl);

	/**
	 * Process the module's QoS rate limit check for the I/O. If the check passed, the module
	 * must call spdk_bdev_qos_module_allow_io() for the I/O.
	 */
	void (*queue_io)(struct spdk_bdev_qos_channel_impl *qos_ch_impl,
			 struct spdk_bdev_io *bdev_io);

	/**
	 * Check whether the module-dependent object per QoS channel is throttled.
	 */
	bool (*is_throttled)(struct spdk_bdev_qos_channel_impl *qos_ch_impl);

	/**
	 * Try aborting the I/O if it is queued in the module-dependent object per QoS channel.
	 * The bdev layer will use this function in spdk_bdev_abort().
	 * The module must use spdk_bdev_abort_queued_io() to actually abort the I/O.
	 */
	bool (*abort_queued_io)(struct spdk_bdev_qos_channel_impl *qos_ch_impl,
				struct spdk_bdev_io *bdev_io);

	/**
	 * Unblock all I/Os queued in the module-dependent object per QoS channel.
	 * The bdev layer will call this function just before freeing the module-dependent object
	 * per QoS channel via put_channel_impl.
	 * The module must use spdk_bdev_unblock_queued_io() to actually unblock I/Os.
	 */
	void (*unblock_all_queued_io)(struct spdk_bdev_qos_channel_impl *qos_ch_impl,
				      struct spdk_bdev_channel *bdev_ch);

	/*
	 * Abort all I/Os queued in the module-dependent object per QoS channel.
	 * The bdev layer will use this function during spdk_bdev_reset().
	 * The module must use spdk_bdev_abort_all_queued_io() to actually abort I/Os.
	 */
	void (*abort_all_queued_io)(struct spdk_bdev_qos_channel_impl *qos_ch_impl,
				    struct spdk_bdev_channel *bdev_ch);

	/**
	 * Denote if the module_fini function may complete asynchronously. If set to true
	 * finishing has to be explicitly completed by calling spdk_bdev_qos_module_fini_done().
	 */
	bool async_fini;

	TAILQ_ENTRY(spdk_bdev_qos_module) link;
};

/**
 * Add the given module to the list of the registered modules.
 * This function should be invoked by referencing the macro
 * SPDK_BDEV_QOS_MODULE_REGISTER in the module c file.
 *
 * \param qos_module Module to be added.
 */
void spdk_bdev_qos_module_list_add(struct spdk_bdev_qos_module *qos_module);

/**
 * Find a registered module with name pointed by \c name.
 *
 * \param name name of module to be searched for.
 * \return pointer to module.
 */
struct spdk_bdev_qos_module *spdk_bdev_qos_module_list_find(const char *name);

/*
 *  Macro used to register module for later initialization.
 */
#define SPDK_BDEV_QOS_MODULE_REGISTER(name, module) \
static void __attribute__((constructor)) _spdk_bdev_qos_module_register_##name(void) \
{ \
        spdk_bdev_qos_module_list_add(module); \
}

/**
 * Inidicate that the module finish has completed.
 *
 * To be called in response to the module_fini, only if async_fini is set.
 */
void spdk_bdev_qos_module_fini_done(void);

/**
 * Find a module-dependent object from a QoS device.
 *
 * \param qos QoS object.
 * \param module QoS module.
 * \return pointer to a module-dependent object.
 */
struct spdk_bdev_qos_impl *spdk_bdev_qos_find_impl(struct spdk_bdev_qos *qos,
		const struct spdk_bdev_qos_module *module);

/**
 * Find a module-dependent object from a QoS channel.
 *
 * \param qos_ch QoS channel.
 * \param module QoS module.
 * \return pointer to a module-dependent object.
 */
struct spdk_bdev_qos_channel_impl *spdk_bdev_qos_channel_find_impl(
	struct spdk_bdev_qos_channel *qos_ch,
	const struct spdk_bdev_qos_module *module);

struct spdk_bdev_qos_desc;

struct spdk_bdev_qos {
	/** Name of the QoS object. */
	char *name;

	bool pending_unregister;

	/** QoS implementations provided by QoS modules. */
	TAILQ_HEAD(, spdk_bdev_qos_impl) impl_list;

	/** List of open descriptors for this QoS object. */
	TAILQ_HEAD(, spdk_bdev_qos_desc) open_descs;

	/** List of user bdevs of this QoS object. */
	TAILQ_HEAD(, spdk_bdev) bdevs;

	/** Parent QoS object. */
	struct spdk_bdev_qos *parent;

	/** Child QoS list. */
	TAILQ_HEAD(, spdk_bdev_qos) children;

	TAILQ_ENTRY(spdk_bdev_qos) tailq;

	/** Link pointer to the sibling QoS list. */
	TAILQ_ENTRY(spdk_bdev_qos) sibling_link;
};

struct spdk_bdev_qos_channel {
	/** QoS channel implementations provided by QoS modules. */
	TAILQ_HEAD(, spdk_bdev_qos_channel_impl) impl_list;

	/** Global QoS rate limits pool */
	struct spdk_bdev_qos *qos;

	/** Poll group to which this cache belongs. */
	struct spdk_bdev_qos_poll_group *group;

	/** Pointer to parent QoS channel. */
	struct spdk_bdev_qos_channel *parent_ch;
};

/**
 * Notify that the bdev I/O passed the current QoS limit check
 * to move to the next QoS limit check.
 *
 * \param bdev_io The bdev_io
 */
void spdk_bdev_qos_channel_impl_queue_io_done(struct spdk_bdev_io *bdev_io);

static inline bool
spdk_bdev_io_is_read_io(struct spdk_bdev_io *bdev_io)
{
	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_NVME_IO:
	case SPDK_BDEV_IO_TYPE_NVME_IO_MD:
		/* Bit 1 (0x2) set for read operation */
		if (bdev_io->u.nvme_passthru.cmd.opc & SPDK_NVME_OPC_READ) {
			return true;
		} else {
			return false;
		}
	case SPDK_BDEV_IO_TYPE_READ:
		return true;
	case SPDK_BDEV_IO_TYPE_ZCOPY:
		/* Populate to read from disk */
		if (bdev_io->u.bdev.zcopy.populate) {
			return true;
		} else {
			return false;
		}
	default:
		return false;
	}
}

static inline uint64_t
spdk_bdev_io_get_io_size_in_bytes(struct spdk_bdev_io *bdev_io)
{
	uint32_t blocklen = spdk_bdev_io_get_block_size(bdev_io);

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_NVME_IO:
	case SPDK_BDEV_IO_TYPE_NVME_IO_MD:
		return bdev_io->u.nvme_passthru.nbytes;
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
		return bdev_io->u.bdev.num_blocks * blocklen;
	case SPDK_BDEV_IO_TYPE_ZCOPY:
		/* Track the data in the start phase only */
		if (bdev_io->u.bdev.zcopy.start) {
			return bdev_io->u.bdev.num_blocks * blocklen;
		} else {
			return 0;
		}
	default:
		return 0;
	}
}

#endif /* SPDK_INTERNAL_BDEV_QOS_MODULE_H */
