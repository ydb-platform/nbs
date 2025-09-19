/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES.
 *   All rights reserved.
 */

/** \file
 * Block Device Group Interface
 *
 * For information on how to write a bdev group, see @ref bdev_group.
 */

#ifndef SPDK_BDEV_GROUP_H
#define SPDK_BDEV_GROUP_H

#include "spdk/stdinc.h"
#include "spdk/bdev.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * \brief Handle to an opened SPDK group of block devices.
 */
struct spdk_bdev_group;

/**
 * Construct a group of the block devices.
 *
 * \param group_name desired group name.
 *
 * \return spdk_bdev_group. The new group.
 */
struct spdk_bdev_group *spdk_bdev_group_create(const char *group_name);

/**
 * Add block device to the group.
 *
 * \param group Group to add the block device to.
 * \param bdev_name Name of the block device to add.
 * \param cb_fn Callback function to be called when the adding is complete.
 * \param cb_arg Argument to be supplied to cb_fn.
 */
void spdk_bdev_group_add_bdev(struct spdk_bdev_group *group, const char *bdev_name,
			      void (*cb_fn)(void *cb_arg, int status),
			      void *cb_arg);

/**
 * Remove block device from the group.
 *
 * \param group Group to remove the block device from.
 * \param bdev_name Name of the block device to remove.
 * \param cb_fn Callback function to be called when the removal is complete.
 * \param cb_arg Argument to be supplied to cb_fn.
 */
void spdk_bdev_group_remove_bdev(struct spdk_bdev_group *group,
				 const char *bdev_name,
				 void (*cb_fn)(void *cb_arg, int status),
				 void *cb_arg);

/**
 * Call the provided function for each block device in the group.
 *
 * \param group Group to enumerate.
 * \param cb_fn Callback function to be called upon each block device in the group.
 * \param cb_arg Argument to be supplied to cb_fn.
 *
 * Note: the enumeration continues while the cb_fn returns 0.
 *
 * \return 0 if operation is successful, or suitable errno value one of the
 * callback returned otherwise.
 */
int spdk_bdev_group_for_each_bdev(struct spdk_bdev_group *group, void *cb_arg,
				  int (*cb_fn)(void *cb_arg, struct spdk_bdev_group *group, struct spdk_bdev *bdev));

/**
 * Get group name
 *
 * \param group Group of interest.
 *
 * \return group name
 */
const char *spdk_bdev_group_get_name(struct spdk_bdev_group *group);

/**
 * Get group's QoS rate limits
 *
 * \param group Group of interest.
 * \param limits Pointer to the QoS rate limits array which holding the limits.
 *
 * \return group name
 */
void spdk_bdev_group_get_qos_rate_limits(struct spdk_bdev_group *group, uint64_t *limits);

/**
 * Set group's QoS rate limits
 *
 * \param group Group of interest.
 * \param limits Pointer to the QoS rate limits array which holding the limits.
 * \param cb_fn Callback function to be called when the set is complete.
 * \param cb_arg Argument to be supplied to cb_fn.
 */
void spdk_bdev_group_set_qos_rate_limits(struct spdk_bdev_group *group, const uint64_t *limits,
		void (*cb_fn)(void *cb_arg, int status),
		void *cb_arg);

/**
 * Destroy the group of the block devices.
 *
 * \param group The group to operate on.
 * \param cb_fn Callback function to be called when the destoy is complete.
 * \param cb_arg Argument to be supplied to cb_fn.
 */
void spdk_bdev_group_destroy(struct spdk_bdev_group *group,
			     void (*cb_fn)(void *cb_arg, int status),
			     void *cb_arg);

/**
 * Find group by name.
 *
 * \param group_name Name of the group to find.
 *
 * \return spdk_bdev_group. The group.
 */
struct spdk_bdev_group *spdk_bdev_group_get_by_name(const char *group_name);

struct spdk_bdev_group_desc;

/**
 * Open a group for safe system operations.
 *
 * \param group_name Group name to open.
 * \param desc output parameter for the descriptor when operation is successful.
 * \return 0 if operation is successful, suitable errno value otherwise.
 */
int spdk_bdev_group_open(const char *group_name,
			 struct spdk_bdev_group_desc **desc);

/**
 * Close a previously opened bdev group.
 *
 * \param desc Bdev group descriptor to close.
 */
void spdk_bdev_group_close(struct spdk_bdev_group_desc *desc);

/**
 * Get the bdev group associated with a descriptor.
 *
 * \param desc Open bdev group descriptor
 * \return bdev group associated with the descriptor.
 */
struct spdk_bdev_group *spdk_bdev_group_desc_get_bdev_group(
	struct spdk_bdev_group_desc *desc);

/**
 * Call the provided function for each block device group.
 *
 * \param cb_fn Callback function to be called upon each block device group.
 * \param cb_arg Argument to be supplied to cb_fn.
 *
 * Note: the enumeration continues while the cb_fn returns 0.
 *
 * \return 0 if operation is successful, or suitable errno value one of the
 * callback returned otherwise.
 */
int spdk_for_each_bdev_group(void *cb_arg, int (*cb_fn)(void *cb_arg,
			     struct spdk_bdev_group *group));

/**
 * Get the full configuration options for the registered bdev group modules and created groups.
 *
 * \param w pointer to a JSON write context where the configuration will be written.
 */
void
spdk_bdev_group_subsystem_config_json(struct spdk_json_write_ctx *w);

struct spdk_bdev_io_stat;

typedef void (*spdk_bdev_group_get_device_stat_cb)(struct spdk_bdev_group *group,
		struct spdk_bdev_io_stat *stat, void *cb_arg, int rc);

/**
 * Return I/O statistics for this bdev group. All the required information
 * will be passed via the callback function.
 *
 * \param group Group to query.
 * \param stat Structure for aggregating collected statistics.  Passed argument to cb.
 * \param cb Called when this operation completes.
 * \param cb_arg Argument passed to callback function.
 */
void spdk_bdev_group_get_device_stat(struct spdk_bdev_group *group,
				     struct spdk_bdev_io_stat *stat,
				     spdk_bdev_group_get_device_stat_cb cb,
				     void *cb_arg);

/**
 * Return I/O statistics for this channel.
 *
 * \param group Group to query.
 * \param ch I/O channel.
 * \param stat The per-channel statistics.
 */
void spdk_bdev_group_get_io_stat(struct spdk_bdev_group *group, struct spdk_io_channel *ch,
				 struct spdk_bdev_io_stat *stat);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_BDEV_GROUP_H */
