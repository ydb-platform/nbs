/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#ifndef SPDK_INTERNAL_BDEV_H
#define SPDK_INTERNAL_BDEV_H

#include "spdk/stdinc.h"
#include "spdk/bdev_module.h"

/**
 * Abort the specified I/O if it is queued in the specified list.
 *
 * \param queue The list to traverse.
 * \param bio_to_abort The I/O to abort.
 *
 * \return true if aborted successfully, or false otherwise.
 */
bool spdk_bdev_abort_queued_io(bdev_io_tailq_t *queue, struct spdk_bdev_io *bio_to_abort);

struct spdk_bdev_channel;

/**
 * Abort any I/O queued in the specified list and submitted on the specified channel.
 *
 * \param queue The list to traverse.
 * \param ch The bdev channel on which target I/Os were submitted.
 */
void spdk_bdev_abort_all_queued_io(bdev_io_tailq_t *queue,
				   struct spdk_bdev_channel *bdev_ch);

/**
 * Unblock any I/O queued in the specified list and submitted on the specified channel.
 *
 * \param queue The list to traverse.
 * \param ch The bdev channel on which target I/Os were submitted.
 */
void spdk_bdev_unblock_all_queued_io(bdev_io_tailq_t *queue,
				     struct spdk_bdev_channel *bdev_ch);

#endif /* SPDK_INTERNAL_BDEV_H */
