/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

/** \file
 * Block device reservations public interface
 */

#ifndef SPDK_BDEV_RESERVATIONS_H
#define SPDK_BDEV_RESERVATIONS_H

#include "spdk/stdinc.h"
#include "spdk/bdev.h"

#ifdef __cplusplus
extern "C" {
#endif

struct spdk_bdev_reservation_caps {
	/** supports persist through power loss */
	bool persist;

	/** supports write exclusive */
	bool write_exclusive;

	/** supports exclusive access */
	bool exclusive_access;

	/** supports write exclusive - registrants only */
	bool write_exclusive_reg_only;

	/** supports exclusive access - registrants only */
	bool exclusive_access_reg_only;

	/** supports write exclusive - all registrants */
	bool write_exclusive_all_reg;

	/** supports exclusive access - all registrants */
	bool exclusive_access_all_reg;

	/** supports ignore existing key */
	bool ignore_existing_key;
};

/**
 * Change persist through power loss state for
 *  Reservation Register command
 */
enum spdk_bdev_reservation_register_cptpl {
	SPDK_BDEV_RESERVATION_PTPL_NO_CHANGES               = 0x0,
	SPDK_BDEV_RESERVATION_PTPL_CLEAR_POWER_ON           = 0x2,
	SPDK_BDEV_RESERVATION_PTPL_PERSIST_POWER_LOSS       = 0x3,
};

/**
 * Registration action for Reservation Register command
 */
enum spdk_bdev_reservation_register_action {
	SPDK_BDEV_RESERVATION_REGISTER_KEY          = 0x0,
	SPDK_BDEV_RESERVATION_UNREGISTER_KEY        = 0x1,
	SPDK_BDEV_RESERVATION_REPLACE_KEY           = 0x2,
};

enum spdk_bdev_reservation_type {
	SPDK_BDEV_NO_RESERVATION			= 0x0,

	/* Write Exclusive Reservation */
	SPDK_BDEV_RESERVATION_WRITE_EXCLUSIVE		= 0x1,

	/* Exclusive Access Reservation */
	SPDK_BDEV_RESERVATION_EXCLUSIVE_ACCESS		= 0x2,

	/* Write Exclusive - Registrants Only Reservation */
	SPDK_BDEV_RESERVATION_WRITE_EXCLUSIVE_REG_ONLY	= 0x3,

	/* Exclusive Access - Registrants Only Reservation */
	SPDK_BDEV_RESERVATION_EXCLUSIVE_ACCESS_REG_ONLY	= 0x4,

	/* Write Exclusive - All Registrants Reservation */
	SPDK_BDEV_RESERVATION_WRITE_EXCLUSIVE_ALL_REGS	= 0x5,

	/* Exclusive Access - All Registrants Reservation */
	SPDK_BDEV_RESERVATION_EXCLUSIVE_ACCESS_ALL_REGS	= 0x6,
};

/**
 * Reservation Acquire action
 */
enum spdk_bdev_reservation_acquire_action {
	SPDK_BDEV_RESERVATION_ACQUIRE               = 0x0,
	SPDK_BDEV_RESERVATION_PREEMPT               = 0x1,
	SPDK_BDEV_RESERVATION_PREEMPT_ABORT         = 0x2,
};


/**
 * Reservation Release action
 */
enum spdk_bdev_reservation_release_action {
	SPDK_BDEV_RESERVATION_RELEASE               = 0x0,
	SPDK_BDEV_RESERVATION_CLEAR                 = 0x1,
};

struct spdk_bdev_registered_ctrlr_data {
	/** controller id */
	uint16_t cntlid;
	/** reservation status */
	bool holds_reservation;
	/** reservation key */
	uint64_t rkey;
	union {
		/** 64-bit host identifier */
		uint64_t hostid;

		/** 128-bit host identifier */
		uint8_t hostid_ext[16];
	};
};

struct spdk_bdev_reservation_status_data {
	/** persist through power loss state */
	uint8_t ptpls;
	/** number of registered controllers */
	uint16_t regctl;
	/** reservation action generation counter */
	uint32_t gen;
	/** host ID is extended */
	bool hostid_is_ext;
	/** reservation type */
	enum spdk_bdev_reservation_type rtype;
	/** controller data */
	struct spdk_bdev_registered_ctrlr_data ctrlr_data[];
};

/**
 * Submit a reservation register request to the block device
 *
 * \ingroup bdev_io_submit_functions
 *
 * \param desc Block device descriptor.
 * \param ch I/O channel. Obtained by calling spdk_bdev_get_io_channel().
 * \param crkey Current reservation key
 * \param nrkey New reservation key
 * \param ignore_key '1' the current reservation key check is disabled
 * \param action Specifies the registration action
 * \param cptpl Change the Persist Through Power Loss state
 * \param cb Called when the request is complete
 * \param cb_arg Argument passed to the cb
 *
 * \return 0 on success. On success, the callback will always
 * be called (even if the request ultimately failed). Return
 * negated errno on failure, in which case the callback will not be called.
 *   * -ENOMEM - spdk_bdev_io buffer cannot be allocated
 *   * -ENOTSUP - the bdev does not support reservation register.
 */
int spdk_bdev_reservation_register(struct spdk_bdev_desc *desc,
				   struct spdk_io_channel *ch,
				   uint64_t crkey,
				   uint64_t nrkey,
				   bool ignore_key,
				   enum spdk_bdev_reservation_register_action action,
				   enum spdk_bdev_reservation_register_cptpl cptpl,
				   spdk_bdev_io_completion_cb cb,
				   void *cb_arg);

/**
 * Submit a reservation acquire request to the block device
 *
 * \ingroup bdev_io_submit_functions
 *
 * \param desc Block device descriptor.
 * \param ch I/O channel. Obtained by calling spdk_bdev_get_io_channel().
 * \param crkey Current reservation key
 * \param prkey Preemt reservation key
 * \param ignore_key '1' the current reservation key check is disabled
 * \param action Specifies the reservation action
 * \param type Reservation type for the namespace.
 * \param cb Called when the request is complete
 * \param cb_arg Argument passed to the cb
 *
 * \return 0 on success. On success, the callback will always
 * be called (even if the request ultimately failed). Return
 * negated errno on failure, in which case the callback will not be called.
 *   * -ENOMEM - spdk_bdev_io buffer cannot be allocated
 *   * -ENOTSUP - the bdev does not support reservation register.
 */
int spdk_bdev_reservation_acquire(struct spdk_bdev_desc *desc,
				  struct spdk_io_channel *ch,
				  uint64_t crkey,
				  uint64_t prkey,
				  bool ignore_key,
				  enum spdk_bdev_reservation_acquire_action action,
				  enum spdk_bdev_reservation_type type,
				  spdk_bdev_io_completion_cb cb,
				  void *cb_arg);

/**
 * Submit a reservation release request to the block device
 *
 * \ingroup bdev_io_submit_functions
 *
 * \param desc Block device descriptor.
 * \param ch I/O channel. Obtained by calling spdk_bdev_get_io_channel().
 * \param crkey Current reservation key
 * \param ignore_key '1' the current reservation key check is disabled
 * \param action Specifies the reservation release action
 * \param type Reservation type for the namespace.
 * \param cb Called when the request is complete
 * \param cb_arg Argument passed to the cb
 *
 * \return 0 on success. On success, the callback will always
 * be called (even if the request ultimately failed). Return
 * negated errno on failure, in which case the callback will not be called.
 *   * -ENOMEM - spdk_bdev_io buffer cannot be allocated
 *   * -ENOTSUP - the bdev does not support reservation register.
 */
int spdk_bdev_reservation_release(struct spdk_bdev_desc *desc,
				  struct spdk_io_channel *ch,
				  uint64_t crkey, bool ignore_key,
				  enum spdk_bdev_reservation_release_action action,
				  enum spdk_bdev_reservation_type type,
				  spdk_bdev_io_completion_cb cb,
				  void *cb_arg);

/**
 * Submit a reservation report to the block device
 *
 * \ingroup bdev_io_submit_functions
 *
 * \param desc Block device descriptor.
 * \param ch I/O channel. Obtained by calling spdk_bdev_get_io_channel().
 * \param status_data Virtual address pointer for reservation status data
 * \param Length bytes for reservation status data structure
 * \param cb Called when the request is complete
 * \param cb_arg Argument passed to the cb
 *
 * \return 0 on success. On success, the callback will always
 * be called (even if the request ultimately failed). Return
 * negated errno on failure, in which case the callback will not be called.
 *   * -ENOMEM - spdk_bdev_io buffer cannot be allocated
 *   * -ENOTSUP - the bdev does not support reservation register.
 */
int spdk_bdev_reservation_report(struct spdk_bdev_desc *desc,
				 struct spdk_io_channel *ch,
				 struct spdk_bdev_reservation_status_data *status_data,
				 uint32_t len,
				 spdk_bdev_io_completion_cb cb,
				 void *cb_arg);

/**
 * Get the reservation capabilities of the block device
 *
 * \param bdev Block Device
 *
 * \return Reference to the reservation capabilities of block device
 */
const struct spdk_bdev_reservation_caps *spdk_bdev_get_reservation_caps(struct spdk_bdev *bdev);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_BDEV_RESERVATIONS_H */
