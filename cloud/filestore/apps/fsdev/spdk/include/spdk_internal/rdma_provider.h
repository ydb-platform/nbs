/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) Mellanox Technologies LTD. All rights reserved.
 *   Copyright (c) 2021-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#ifndef SPDK_RDMA_H
#define SPDK_RDMA_H

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include "spdk/dma.h"
#include "spdk/json.h"

#include "spdk_internal/rdma_provider_wc.h"

/* rxe driver vendor_id has been changed from 0 to 0XFFFFFF in 0184afd15a141d7ce24c32c0d86a1e3ba6bc0eb3 */
#define SPDK_RDMA_PROVIDER_RXE_VENDOR_ID_OLD 0
#define SPDK_RDMA_PROVIDER_RXE_VENDOR_ID_NEW 0XFFFFFF

struct spdk_rdma_provider_wr_stats {
	/* Total number of submitted requests */
	uint64_t num_submitted_wrs;
	/* Total number of doorbell updates */
	uint64_t doorbell_updates;
};

struct spdk_rdma_provider_qp_stats {
	struct spdk_rdma_provider_wr_stats send;
	struct spdk_rdma_provider_wr_stats recv;
	uint64_t accel_sequences_executed;
};

struct spdk_rdma_provider_qp_init_attr {
	void				*qp_context;
	struct spdk_rdma_provider_cq	*cq;
	struct spdk_rdma_provider_srq	*srq;
	struct ibv_qp_cap		cap;
	struct ibv_pd			*pd;
	struct spdk_rdma_provider_qp_stats *stats;
	spdk_memory_domain_transfer_data_cb domain_transfer;
};

struct spdk_rdma_provider_send_wr_list {
	struct ibv_send_wr	*first;
	struct ibv_send_wr	*last;
};

struct spdk_rdma_provider_recv_wr_list {
	struct ibv_recv_wr	*first;
	struct ibv_recv_wr	*last;
};

struct spdk_rdma_provider_qp {
	struct ibv_qp *qp;
	struct rdma_cm_id *cm_id;
	struct spdk_memory_domain *domain;
	struct spdk_rdma_provider_send_wr_list send_wrs;
	struct spdk_rdma_provider_recv_wr_list recv_wrs;
	struct spdk_rdma_provider_qp_stats *stats;
	bool shared_stats;
};

struct spdk_rdma_provider_srq_init_attr {
	struct ibv_pd *pd;
	struct spdk_rdma_provider_wr_stats *stats;
	struct ibv_srq_init_attr srq_init_attr;
};

struct spdk_rdma_provider_srq {
	struct ibv_srq *srq;
	struct spdk_rdma_provider_recv_wr_list recv_wrs;
	struct spdk_rdma_provider_wr_stats *stats;
	bool shared_stats;
};

struct spdk_rdma_provider_cq_init_attr {
	int				cqe;
	int				comp_vector;
	void			       *cq_context;
	struct ibv_comp_channel	       *comp_channel;
	struct ibv_pd		       *pd;
};

struct spdk_rdma_provider_cq {
	struct ibv_cq *cq;
};

struct spdk_rdma_provider_memory_translation_ctx {
	void *addr;
	size_t length;
	uint32_t lkey;
	uint32_t rkey;
};

struct spdk_rdma_provider_opts {
	/**
	 * Size of this structure in bytes
	 */
	size_t opts_size;
	/**
	 * Support HW offloads on network QP. If set, memory domain created for qpair contains a pointer to the qpair,
	 * Qpair and CQ might be created with extended size
	 */
	bool support_offload_on_qp;
};

/**
 * Create RDMA SRQ
 *
 * \param init_attr Pointer to SRQ init attr
 * \return pointer to srq on success or NULL on failure. errno is updated in failure case.
 */
struct spdk_rdma_provider_srq *spdk_rdma_provider_srq_create(
	struct spdk_rdma_provider_srq_init_attr *init_attr);

/**
 * Destroy RDMA SRQ
 *
 * \param rdma_srq Pointer to SRQ
 * \return 0 on success, errno on failure
 */
int spdk_rdma_provider_srq_destroy(struct spdk_rdma_provider_srq *rdma_srq);

/**
 * Append the given recv wr structure to the SRQ's outstanding recv list.
 * This function accepts either a single Work Request or the first WR in a linked list.
 *
 * \param rdma_srq Pointer to SRQ
 * \param first pointer to the first Work Request
 * \return true if there were no outstanding WRs before, false otherwise
 */
bool spdk_rdma_provider_srq_queue_recv_wrs(struct spdk_rdma_provider_srq *rdma_srq,
		struct ibv_recv_wr *first);

/**
 * Submit all queued receive Work Request
 *
 * \param rdma_srq Pointer to SRQ
 * \param bad_wr Stores a pointer to the first failed WR if this function return nonzero value
 * \return 0 on success, errno on failure
 */
int spdk_rdma_provider_srq_flush_recv_wrs(struct spdk_rdma_provider_srq *rdma_srq,
		struct ibv_recv_wr **bad_wr);

/**
 * Create RDMA provider specific qpair
 *
 * \param cm_id Pointer to RDMA_CM cm_id
 * \param qp_attr Pointer to qpair init attributes
 * \return Pointer to a newly created qpair on success or NULL on failure
 */
struct spdk_rdma_provider_qp *spdk_rdma_provider_qp_create(struct rdma_cm_id *cm_id,
		struct spdk_rdma_provider_qp_init_attr *qp_attr);

/**
 * Accept a connection request. Called by the passive side (NVMEoF target)
 *
 * \param spdk_rdma_qp Pointer to SPDK RDMA qpair
 * \param conn_param Optional information needed to establish the connection
 * \return 0 on success, errno on failure
 */
int spdk_rdma_provider_qp_accept(struct spdk_rdma_provider_qp *spdk_rdma_qp,
				 struct rdma_conn_param *conn_param);

/**
 * Complete the connection process, must be called by the active
 * side (NVMEoF initiator) upon receipt RDMA_CM_EVENT_CONNECT_RESPONSE
 *
 * \param spdk_rdma_qp Pointer to SPDK RDMA qpair
 * \return 0 on success, errno on failure
 */
int spdk_rdma_provider_qp_complete_connect(struct spdk_rdma_provider_qp *spdk_rdma_qp);

/**
 * Destroy RDMA provider specific qpair
 *
 * \param spdk_rdma_qp Pointer to SPDK RDMA qpair to be destroyed
 */
void spdk_rdma_provider_qp_destroy(struct spdk_rdma_provider_qp *spdk_rdma_qp);

/**
 * Disconnect a connection and transition associated qpair to error state.
 * Generates RDMA_CM_EVENT_DISCONNECTED on both connection sides
 *
 * \param spdk_rdma_qp Pointer to qpair to be disconnected
 */
int spdk_rdma_provider_qp_disconnect(struct spdk_rdma_provider_qp *spdk_rdma_qp);

/**
 * Append the given send wr structure to the qpair's outstanding sends list.
 * This function accepts either a single Work Request or the first WR in a linked list.
 *
 * \param spdk_rdma_qp Pointer to SPDK RDMA qpair
 * \param first Pointer to the first Work Request
 * \return true if there were no outstanding WRs before, false otherwise
 */
bool spdk_rdma_provider_qp_queue_send_wrs(struct spdk_rdma_provider_qp *spdk_rdma_qp,
		struct ibv_send_wr *first);

/**
 * Submit all queued send Work Request
 *
 * \param spdk_rdma_qp Pointer to SPDK RDMA qpair
 * \param bad_wr Stores a pointer to the first failed WR if this function return nonzero value
 * \return 0 on success, errno on failure
 */
int spdk_rdma_provider_qp_flush_send_wrs(struct spdk_rdma_provider_qp *spdk_rdma_qp,
		struct ibv_send_wr **bad_wr);

/**
 * Append the given recv wr structure to the qpair's outstanding recv list.
 * This function accepts either a single Work Request or the first WR in a linked list.
 *
 * \param spdk_rdma_qp Pointer to SPDK RDMA qpair
 * \param first Pointer to the first Work Request
 * \return true if there were no outstanding WRs before, false otherwise
 */
bool spdk_rdma_provider_qp_queue_recv_wrs(struct spdk_rdma_provider_qp *spdk_rdma_qp,
		struct ibv_recv_wr *first);

/**
 * Submit all queued recv Work Request
 * \param spdk_rdma_qp Pointer to SPDK RDMA qpair
 * \param bad_wr Stores a pointer to the first failed WR if this function return nonzero value
 * \return 0 on success, errno on failure
 */
int spdk_rdma_provider_qp_flush_recv_wrs(struct spdk_rdma_provider_qp *spdk_rdma_qp,
		struct ibv_recv_wr **bad_wr);

/**
 * Create RDMA provider specific CQ
 *
 * \param cq_attr Pointer to CQ init attributes
 * \return Pointer to a newly created CQ on success or NULL on failure
 */
struct spdk_rdma_provider_cq *spdk_rdma_provider_cq_create(struct spdk_rdma_provider_cq_init_attr
		*cq_attr);

/**
 * Destroy RDMA provider specific CQ
 *
 * \param rdma_cq Pointer to SPDK RDMA CQ to be destroyed
 */
void spdk_rdma_provider_cq_destroy(struct spdk_rdma_provider_cq *rdma_cq);

/**
 * Resize Completion Queue
 *
 * \param rdma_cq CQ to be resized
 * \param cqe New CQ size
 * \return 0 on succes, errno on failure
 */
int spdk_rdma_provider_cq_resize(struct spdk_rdma_provider_cq *rdma_cq, int cqe);

/**
 * Poll Completion Queue, save up to \b num_entries into \b wc array
 *
 * If returned wc->status is SPDK_RDMA_PROVIDER_IBV_WC_SIG_ERR, mkey which caused
 * the error is stored in wc->wr_id and err_type is stored in wc->vendor_err.
 * err_type defined in the SPDK DIF library is used to avoid extra conversion.
 *
 * \param cq Completion Queue
 * \param num_entries Maximum number of completions to be polled
 * \param wc Array of work completions to be filled by this function
 * \return number of polled completions on succes, negated errno on failure
 */
int spdk_rdma_provider_cq_poll(struct spdk_rdma_provider_cq *rdma_cq, int num_entries,
			       struct ibv_wc *wc);

/**
 * Check whether RDMA provider supports accel sequences
 *
 * return true if accel sequence is supported
 */
bool spdk_rdma_provider_accel_sequence_supported(void);

/**
 * Get a reference of the memory key
 *
 * \param mkey Memory key
 */
void spdk_rdma_provider_memory_key_get_ref(void *mkey);

/**
 * Put a reference of the memory key
 *
 * The memory key is released once the count reaches 0.
 *
 * \param mkey Memory key
 */
void spdk_rdma_provider_memory_key_put_ref(void *mkey);

/**
 * Get key of the specified memory key.
 *
 * \param mkey Memory key
 *
 * return key for this memory key.
 */
uint32_t spdk_rdma_provider_memory_key_get_key(void *mkey);

/**
 * Set the options for the rdma_provide library.
 *
 * \param opts options to set
 * \return 0 on success.
 * \return -EINVAL if the options are invalid.
 */
int spdk_rdma_provider_set_opts(const struct spdk_rdma_provider_opts *opts);

/**
 * Get the options for the rdma_provide library.
 *
 * \param opts Output parameter for options.
 * \param opts_size sizeof(*opts)
 */
int spdk_rdma_provider_get_opts(struct spdk_rdma_provider_opts *opts, size_t opts_size);

/**
 * Get RDMA provider configuration
 *
 * \param w pointer to a JSON write context where the configuration will be written.
 */
void spdk_rdma_provider_subsystem_config_json(struct spdk_json_write_ctx *w);

#endif /* SPDK_RDMA_H */
