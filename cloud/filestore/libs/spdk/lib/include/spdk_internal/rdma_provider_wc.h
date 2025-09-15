/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES.
 *   All rights reserved.
 */

#ifndef SPDK_RDMA_PROVIDER_WC_H
#define SPDK_RDMA_PROVIDER_WC_H

#include "spdk/stdinc.h"
#include "spdk/dif.h"

/* SPDK_RDMA_PROVIDER_WC_SIG_ERR is a fake status that corresponds to MLX5_CQE_SIG_ERR.
 * Use INT32_MAX to not overlap with the existing enum ibv_wc_status as possible as we can.
 */
enum {
	SPDK_RDMA_PROVIDER_WC_SIG_ERR = INT32_MAX,
};

#endif /* SPDK_RDMA_PROVIDER_WC_H */
