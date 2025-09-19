/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#ifndef SPDK_VFSDEV_PASSTHRU_H
#define SPDK_VFSDEV_PASSTHRU_H

#include "spdk/stdinc.h"

#include "spdk/fsdev.h"
#include "spdk/fsdev_module.h"

/**
 * Create new pass through fsdev.
 *
 * \param fsdev_name Fsdev on which pass through vfsdev will be created.
 * \param vfsdev_name Name of the pass through fsdev.
 * \return 0 on success, other on failure.
 */
int fsdev_passthru_external_create(const char *fsdev_name, const char *vfsdev_name);

/**
 * Delete passthru fsdev.
 *
 * \param fsdev_name Name of the pass through fsdev to delete.
 * \param cb_fn Function to call after deletion.
 * \param cb_arg Argument to pass to cb_fn.
 */
void fsdev_passthru_external_delete(const char *fsdev_name, spdk_fsdev_unregister_cb cb_fn,
				    void *cb_arg);

#endif /* SPDK_VFSDEV_PASSTHRU_H */
