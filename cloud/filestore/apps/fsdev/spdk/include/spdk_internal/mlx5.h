/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2022-2025, NVIDIA CORPORATION & AFFILIATES.
 *   All rights reserved.
 */

#ifndef SPDK_MLX5_H
#define SPDK_MLX5_H

#include <infiniband/mlx5dv.h>
#include <rdma/rdma_cma.h>

#include "spdk/tree.h"

#define SPDK_MLX5_DRIVER_NAME "mlx5"
#define SPDK_MLX5_VENDOR_ID_MELLANOX 0x2c9
#define SPDK_MLX5_DEV_MAX_NAME_LEN 64

/* API for low level PRM based mlx5 driver implementation. Some terminology:
 * PRM - Programming Reference Manual
 * QP - Queue Pair
 * SQ - Submission Queue
 * CQ - Completion Queue
 * WQE - Work Queue Element
 * WQEBB - Work Queue Element Build Block (64 bytes)
 * CQE - Completion Queue Entry
 * BSF - Byte Stream Format - part of UMR WQ which describes specific data properties such as encryption or signature
 * UMR - User Memory Region
 * DEK - Data Encryption Key
 */

#define SPDK_MLX5_VENDOR_ID_MELLANOX 0x2c9

struct spdk_mlx5_crypto_keytag;

enum {
	/** Error Completion Event - generate CQE on error for every CTRL segment, even one without CQ_UPDATE bit.
	 * Don't generate CQE in other cases. Default behaviour */
	SPDK_MLX5_WQE_CTRL_CE_CQ_ECE			= 3 << 2,
	/** Do not generate IBV_WC_WR_FLUSH_ERR for non-signaled CTRL segments. Completions are generated only for
	 * signaled (CQ_UPDATE) CTRL segments and the first error */
	SPDK_MLX5_WQE_CTRL_CE_CQ_NO_FLUSH_ERROR		= 1 << 2,
	/** Always generate CQE for CTRL segment WQE */
	SPDK_MLX5_WQE_CTRL_CE_CQ_UPDATE			= MLX5_WQE_CTRL_CQ_UPDATE,
	SPDK_MLX5_WQE_CTRL_CE_MASK			= 3 << 2,
	SPDK_MLX5_WQE_CTRL_SOLICITED			= MLX5_WQE_CTRL_SOLICITED,
	/** WQE starts execution only after all previous Read/Atomic WQEs complete */
	SPDK_MLX5_WQE_CTRL_FENCE			= MLX5_WQE_CTRL_FENCE,
	/** WQE starts execution after all local WQEs (memory operation, gather) complete */
	SPDK_MLX5_WQE_CTRL_INITIATOR_SMALL_FENCE	= MLX5_WQE_CTRL_INITIATOR_SMALL_FENCE,
	/** WQE starts execution only after all previous WQEs complete */
	SPDK_MLX5_WQE_CTRL_STRONG_ORDERING		= 3 << 5,
};

/*
 * SPDK_MLX5_CQE_SYNDROME_SIGERR is a fake syndrome that corresponds to opcode
 * MLX5_CQE_SIG_ERR. It is added to avoid growing spdk_mlx5_cq_completion.
 *
 * The size of the syndrome field in the HW CQE is 8 bits. So, the new syndrome
 * cannot overlap with the HW syndromes.
 */
enum {
	SPDK_MLX5_CQE_SYNDROME_SIGERR = 1 << 8,
};

struct spdk_mlx5_crypto_dek_create_attr {
	/* Data Encryption Key in binary form */
	char *dek;
	/* Length of the dek */
	size_t dek_len;
	/* LBA is located in upper part of a tweak */
	bool tweak_upper_lba;
};

struct spdk_mlx5_cq;
struct spdk_mlx5_qp;

struct spdk_mlx5_cq_attr {
	uint32_t cqe_cnt;
	uint32_t cqe_size;
	void *cq_context;
	struct ibv_comp_channel *comp_channel;
	int comp_vector;
};

struct spdk_mlx5_qp_attr {
	struct ibv_qp_cap cap;
	struct spdk_mlx5_srq *srq;
	void *qp_context;
	bool sigall;
	/* If set then CQ_UPDATE will be cleared for every ctrl WQE and only last ctrl WQE before ringing the doorbell
	 * will be updated with CQ_UPDATE flag */
	bool siglast;
	bool aes_xts_inc_64;
};

struct spdk_mlx5_cq_completion {
	union {
		uint64_t wr_id;
		uint32_t mkey; /* applicable if status == MLX5_CQE_SYNDROME_SIGERR */
	};
	int status;
};

enum {
	SPDK_MLX5_SIGERR_CQE_SYNDROME_REFTAG = 1 << 11,
	SPDK_MLX5_SIGERR_CQE_SYNDROME_APPTAG = 1 << 12,
	SPDK_MLX5_SIGERR_CQE_SYNDROME_GUARD = 1 << 13,

	SPDK_MLX5_SIGERR_CQE_SIG_TYPE_BLOCK = 0,
	SPDK_MLX5_SIGERR_CQE_SIG_TYPE_TRASACTION = 1,
};

union spdk_mlx5_cq_error {
	struct spdk_mlx5_sig_err {
		uint16_t syndrome;
		uint64_t expected;
		uint64_t actual;
		uint64_t offset;
		uint8_t sig_type;
		uint8_t domain;
	} sigerr;
};

struct spdk_mlx5_mkey_pool;

enum spdk_mlx5_mkey_pool_flags {
	SPDK_MLX5_MKEY_POOL_FLAG_CRYPTO = 1 << 0,
	SPDK_MLX5_MKEY_POOL_FLAG_SIGNATURE = 1 << 1,
	/* Max number of pools of different types */
	SPDK_MLX5_MKEY_POOL_FLAG_COUNT = 3,
};

struct spdk_mlx5_mkey_pool_param {
	uint32_t mkey_count;
	uint32_t cache_per_thread;
	/* enum spdk_mlx5_mkey_pool_flags */
	uint32_t flags;
};

struct spdk_mlx5_psv_pool_obj;

struct spdk_mlx5_mkey_pool_obj {
	uint32_t mkey;
	uint32_t ref_count;
	struct spdk_mlx5_mkey_pool *pool;
	struct spdk_mlx5_psv_pool_obj *psv;
	RB_ENTRY(spdk_mlx5_mkey_pool_obj) node;
	struct {
		uint32_t sigerr_count;
		bool sigerr;
	} sig;
};

struct spdk_mlx5_psv_pool;

struct spdk_mlx5_psv_pool_param {
	uint32_t psv_count;
	uint32_t cache_per_thread;
	struct spdk_rdma_utils_mem_map *map;
};

struct spdk_mlx5_psv_pool_obj {
	uint32_t psv_index;
	struct {
		uint32_t error : 1;
		uint32_t reserved : 31;
	} bits;
	/* mlx5 engine requires DMAable memory, use this member to copy user's crc value since we don't know which
	 * memory it is in */
	uint32_t crc;
	uint32_t crc_lkey;
	struct spdk_mlx5_psv_pool *pool;
};

struct spdk_mlx5_umr_attr {
	struct ibv_sge *sge;
	uint32_t mkey; /* User Memory Region key to configure */
	uint32_t umr_len;
	uint16_t sge_count;
};

enum spdk_mlx5_encryption_order {
	SPDK_MLX5_ENCRYPTION_ORDER_ENCRYPTED_WIRE_SIGNATURE    = 0x0,
	SPDK_MLX5_ENCRYPTION_ORDER_ENCRYPTED_MEMORY_SIGNATURE  = 0x1,
	SPDK_MLX5_ENCRYPTION_ORDER_ENCRYPTED_RAW_WIRE          = 0x2,
	SPDK_MLX5_ENCRYPTION_ORDER_ENCRYPTED_RAW_MEMORY        = 0x3,
};

enum spdk_mlx5_block_size_selector {
	SPDK_MLX5_BLOCK_SIZE_SELECTOR_RESERVED	= 0,
	SPDK_MLX5_BLOCK_SIZE_SELECTOR_512	= 1,
	SPDK_MLX5_BLOCK_SIZE_SELECTOR_520	= 2,
	SPDK_MLX5_BLOCK_SIZE_SELECTOR_4096	= 3,
	SPDK_MLX5_BLOCK_SIZE_SELECTOR_4160	= 4,
};

enum spdk_mlx5_crypto_key_tweak_mode {
	SPDK_MLX5_CRYPTO_KEY_TWEAK_MODE_SIMPLE_LBA_BE	= 0,
	SPDK_MLX5_CRYPTO_KEY_TWEAK_MODE_SIMPLE_LBA_LE	= 1,
	SPDK_MLX5_CRYPTO_KEY_TWEAK_MODE_UPPER_LBA_BE	= 2,
	SPDK_MLX5_CRYPTO_KEY_TWEAK_MODE_UPPER_LBA_LE	= 3
};

struct spdk_mlx5_crypto_dek_data {
	/** low level devx obj id which represents the DEK */
	uint32_t dek_obj_id;
	/** Crypto key tweak mode */
	enum spdk_mlx5_crypto_key_tweak_mode tweak_mode;
};

struct spdk_mlx5_umr_crypto_attr {
	uint8_t enc_order; /* see \ref enum spdk_mlx5_encryption_order */
	uint8_t bs_selector; /* see \ref enum spdk_mlx5_block_size_selector */
	uint8_t tweak_mode; /* see \ref enum spdk_mlx5_crypto_key_tweak_mode */
	/* Low level ID of the Data Encryption Key */
	uint32_t dek_obj_id;
	uint64_t xts_iv;
	uint64_t keytag; /* Must match DEK's keytag or 0 */
};

/* Persistent Signature Value (PSV) is used to contain a calculated signature value for a single signature
 * along with some meta-data, such as error flags and status flags */
struct spdk_mlx5_psv {
	struct mlx5dv_devx_obj *devx_obj;
	uint32_t index;
};

enum spdk_mlx5_umr_sig_domain {
	SPDK_MLX5_UMR_SIG_DOMAIN_MEMORY,
	SPDK_MLX5_UMR_SIG_DOMAIN_WIRE
};

struct spdk_mlx5_umr_trans_sig_attr {
	uint32_t seed;
	/* Index of the PSV used by this UMR */
	uint32_t psv_index;
	enum spdk_mlx5_umr_sig_domain domain;
	/* Number of sigerr completions received on the UMR */
	uint32_t sigerr_count;
	/* Number of bytes covered by this UMR */
	uint32_t raw_data_size;
	bool init; /* Set to true on the first UMR to initialize signature with its default values */
	bool check_gen; /* Set to true for the last UMR to generate signature */
};

enum spdk_mlx5_sig_t10dif_flags {
	SPDK_MLX5_SIG_T10DIF_FLAGS_REF_REMAP = 1 << 0,
	SPDK_MLX5_SIG_T10DIF_FLAGS_APP_ESCAPE = 1 << 1,
	SPDK_MLX5_SIG_T10DIF_FLAGS_APP_REF_ESCAPE = 1 << 2,
};

struct spdk_mlx5_sig_t10dif {
	uint32_t ref_tag;
	uint16_t app_tag;
	uint16_t apptag_mask;
	uint16_t flags;
};

enum spdk_mlx5_sig_type {
	SPDK_MLX5_SIG_TYPE_NONE = 0,
	SPDK_MLX5_SIG_TYPE_T10DIF,
};

struct spdk_mlx5_sig_block_domain {
	uint8_t sig_type;
	uint8_t bs_selector;
	uint32_t psv_index;
	union {
		struct spdk_mlx5_sig_t10dif dif;
	} sig;
};

enum spdk_mlx5_sig_mask {
	SPDK_MLX5_SIG_MASK_T10DIF_GUARD = 0xc0,
	SPDK_MLX5_SIG_MASK_T10DIF_APPTAG = 0x30,
	SPDK_MLX5_SIG_MASK_T10DIF_REFTAG = 0x0f,
};

struct spdk_mlx5_umr_block_sig_attr {
	struct spdk_mlx5_sig_block_domain mem;
	struct spdk_mlx5_sig_block_domain wire;
	uint32_t flags;
	/* Number of sigerr completions receivd on the UMR */
	uint32_t sigerr_count;
	uint8_t check_mask;
	uint8_t copy_mask;
};

struct spdk_mlx5_device_crypto_caps {
	uint8_t wrapped_crypto_operational : 1;
	uint8_t wrapped_crypto_going_to_commissioning : 1;
	uint8_t wrapped_import_method_aes_xts : 1;
	uint8_t single_block_le_tweak : 1;
	uint8_t multi_block_be_tweak : 1;
	uint8_t multi_block_le_tweak : 1;
	uint8_t tweak_inc_64 : 1;
	uint8_t large_mtu_tweak : 1;
};

struct spdk_mlx5_device_caps {
	/* Content of this structure is valid only if crypto_supported is 1 */
	struct spdk_mlx5_device_crypto_caps crypto;
	uint8_t crypto_supported : 1;
	uint8_t crc32c_supported : 1;
};

/**
 * Query device capabilities
 *
 * \param context Context of a device to query
 * \param caps Device capabilities
 * \return 0 on success, negated errno on failure.
 */
int spdk_mlx5_device_query_caps(struct ibv_context *context, struct spdk_mlx5_device_caps *caps);

/**
 * Create Completion Queue
 *
 * \note: CQ and all associated qpairs must be accessed in scope of a single thread
 * \note: CQ size must be enough to hold completions of all connected qpairs
 *
 * \param pd Protection Domain
 * \param cq_attr Attributes to be used to create CQ
 * \param cq_out Pointer created CQ
 * \return 0 on success, negated errno on failure. \b cq_out is set only on success result
 */
int spdk_mlx5_cq_create(struct ibv_pd *pd, struct spdk_mlx5_cq_attr *cq_attr,
			struct spdk_mlx5_cq **cq_out);

/**
 * Destroy Completion Queue
 *
 * \param cq CQ created with \ref spdk_mlx5_cq_create
 */
int spdk_mlx5_cq_destroy(struct spdk_mlx5_cq *cq);

/**
 * Create Shared Receive Queue
 *
 * \param pd Protection Domain
 * \param srq_attr Attributes to be used to create SRQ
 * \param srq_out Pointer created SRQ
 * \return 0 on success, negated errno on failure. \b srq_out is set only on success result
 */
int spdk_mlx5_srq_create(struct ibv_pd *pd, struct ibv_srq_init_attr *srq_attr,
			 struct spdk_mlx5_srq **srq_out);

/**
 * Destroy Shared Receive Queue
 *
 * \param srq SRQ created with \ref spdk_mlx5_srq_create
 * \return 0 on success, negated errno on failure.
 */
int spdk_mlx5_srq_destroy(struct spdk_mlx5_srq *srq);

/**
 * Create qpair suitable for RDMA operations
 *
 * \param pd Protection Domain
 * \param cq Completion Queue to bind QP to
 * \param qp_attr Attributes to be used to create QP
 * \param qp_out Pointer created QP
 * \return 0 on success, negated errno on failure. \b qp_out is set only on success result
 */
int spdk_mlx5_qp_create(struct ibv_pd *pd, struct spdk_mlx5_cq *cq,
			struct spdk_mlx5_qp_attr *qp_attr, struct spdk_mlx5_qp **qp_out);

/**
 * Connect QP as loopback
 *
 * \param qp QP created with \ref spdk_mlx5_qp_create
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_qp_connect_loopback(struct spdk_mlx5_qp *qp);

/**
 * Connect QP using RDMA CM
 *
 * \param qp QP created with \ref spdk_mlx5_qp_create
 * \param cm_id RDMA CM id to be used to connect QP
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_qp_connect_cm(struct spdk_mlx5_qp *qp, struct rdma_cm_id *cm_id);

/**
 * Modify the attributes of QP qp with the attributes in attr according to the mask attr_mask
 *
 * \param qp QP to be modified
 * \param attr QP attributed to be used to modify QP
 * \param attr_mask specifies the QP attributes to be modified
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_qp_modify(struct spdk_mlx5_qp *qp, struct ibv_qp_attr *attr, int attr_mask);

/**
 * Changes internal qpair state to error causing all unprocessed Work Requests to be completed with IBV_WC_WR_FLUSH_ERR
 * status code.
 *
 * \param qp qpair pointer
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_qp_set_error_state(struct spdk_mlx5_qp *qp);

/**
 * Get original verbs qp
 *
 * \param qp mlx5 qp
 * \return Pointer to the underlying ibv_verbs qp
 */
struct ibv_qp *spdk_mlx5_qp_get_verbs_qp(struct spdk_mlx5_qp *qp);

/**
 * Destroy qpair
 *
 * \param qp QP created with \ref spdk_mlx5_qp_create
 */
void spdk_mlx5_qp_destroy(struct spdk_mlx5_qp *qp);

/**
 * Poll Completion Queue, save up to \b max_completions into \b comp array
 *
 * \param cq Completion Queue
 * \param comp Array of completions to be filled by this function
 * \param err_info Array of errors to be filled only if error occurred
 * \param max_completions
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_cq_poll_completions(struct spdk_mlx5_cq *cq,
				  struct spdk_mlx5_cq_completion *comp,
				  union spdk_mlx5_cq_error *err, int max_completions);

/**
 * Poll Completion Queue, save up to \b num_entries into \b wc array
 *
 * \param cq Completion Queue
 * \param num_entries Maximum number of completions to be polled
 * \param wc Array of work completions to be filled by this function
 * \return
 */
int spdk_mlx5_cq_poll_wc(struct spdk_mlx5_cq *cq, int num_entries, struct ibv_wc *wc);

/**
 * Resize Completion Queue
 *
 * \param cq Completion Queue to be resized
 * \param cqe New size of Completion Queue
 * \return 0 on success, errno on failure
 */
int spdk_mlx5_cq_resize(struct spdk_mlx5_cq *cq, int cqe);

/**
 * Ring Send Queue doorbell, submits all previously posted WQEs to HW
 *
 * \param qp qpair pointer
 */
void spdk_mlx5_qp_complete_send(struct spdk_mlx5_qp *qp);

/**
 * Submit RDMA_WRITE operations on the qpair
 *
 * \param qp qpair pointer
 * \param sge Memory layout of the local data to be written
 * \param sge_count Number of \b sge entries
 * \param dstaddr Remote address to write \b sge to
 * \param rkey Remote memory key
 * \param wrid wrid which is returned in the CQE
 * \param flags SPDK_MLX5_WQE_CTRL_CE_CQ_UPDATE to have a signaled completion; Any of SPDK_MLX5_WQE_CTRL_FENCE* or 0
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_qp_rdma_write(struct spdk_mlx5_qp *qp, struct ibv_sge *sge, uint32_t sge_count,
			    uint64_t dstaddr, uint32_t rkey, uint64_t wrid, uint32_t flags);

/**
 * Submit RDMA_WRITE operations on the qpair
 *
 * \param qp qpair pointer
 * \param sge Memory layout of the local buffers for reading remote data
 * \param sge_count Number of \b sge entries
 * \param dstaddr Remote address to read into \b sge
 * \param rkey Remote memory key
 * \param wrid wrid which is returned in the CQE
 * \param flags SPDK_MLX5_WQE_CTRL_CE_CQ_UPDATE to have a signaled completion; Any of SPDK_MLX5_WQE_CTRL_FENCE* or 0
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_qp_rdma_read(struct spdk_mlx5_qp *qp, struct ibv_sge *sge, uint32_t sge_count,
			   uint64_t dstaddr, uint32_t rkey, uint64_t wrid, uint32_t flags);

/**
 * Write a Send WR to the SQ of the given QP
 *
 * \param qp QP where WR will be written
 * \param sge SGE array that represents a source for Send
 * \param num_sge Size of the sge array
 * \param wrid Id, that is returned in the Work Completion when WR is executed
 * \param flags Flags from enum ibv_send_flags
 * \return 0 on success, errno on failure
 */
int spdk_mlx5_qp_send(struct spdk_mlx5_qp *qp, struct ibv_sge *sge, uint32_t num_sge,
		      uint64_t wrid, uint32_t flags);

/**
 * Write a Send with invalidate WR to the SQ of the given QP
 *
 * \param qp QP where WR will be written
 * \param sge SGE array that represents a source for Send
 * \param num_sge Size of the sge array
 * \param invalidate_rkey Remote Key that will be invalidated on the remote host
 * \param wrid Id, that is returned in the Work Completion when WR is executed
 * \param flags Flags from enum ibv_send_flags
 * \return 0 on success, errno on failure
 */
int spdk_mlx5_qp_send_inv(struct spdk_mlx5_qp *qp, struct ibv_sge *sge, uint32_t num_sge,
			  uint32_t invalidate_rkey, uint64_t wrid, uint32_t flags);

/**
 * Write a Receive WR to the RQ of the given QP
 *
 * \param qp QP where WR will be written
 * \param sge SGE array that represents a destination for Receive
 * \param num_sge Size of the sge array
 * \param wrid Id, that is returned in the Work Completion when WR is executed
 * \return 0 on success, errno on failure
 */
int spdk_mlx5_qp_recv(struct spdk_mlx5_qp *qp, struct ibv_sge *sge, uint32_t num_sge,
		      uint64_t wrid);

/**
 * Update receive doorbell record for the given QP to start executing WRs written by spdk_mlx5_qp_recv()
 *
 * \param qp QP where doorbell record will be updated
 */
void spdk_mlx5_qp_complete_recv(struct spdk_mlx5_qp *qp);

/**
 * Write a Receive WR to the SRQ
 *
 * \param srq SRQ where WR will be queued
 * \param sge SGE array that represents a destination for Receive
 * \param num_sge Size of the sge array
 * \param wrid Id, that is returned in the Work Completion when WR is executed
 * \return 0 on success, errno on failure
 */
int spdk_mlx5_srq_recv(struct spdk_mlx5_srq *srq, struct ibv_sge *sge, uint32_t num_sge,
		       uint64_t wrid);

/**
 * Update receive doorbell record for the given SRQ to start executing WRs written by spdk_mlx5_srq_recv()
 *
 * \param srq SRQ where doorbell record will be updated
 */
void spdk_mlx5_srq_complete_recv(struct spdk_mlx5_srq *srq);

/**
 * Configure User Memory Region obtained using \ref spdk_mlx5_mkey_pool_get_bulk with crypto capabilities.
 *
 * Besides crypto capabilities, it allows to gather memory chunks into virtually contig (from the NIC point of view)
 * memory space with start address 0. The user must ensure that \b qp's capacity is enough to perform this operation.
 * It only works if the UMR pool was created with crypto capabilities.
 *
 * \param qp Qpair to be used for UMR configuration. If RDMA operation which references this UMR is used on the same \b qp
 * then it is not necessary to wait for the UMR configuration to complete. Instead, first RDMA operation after UMR
 * configuration must have flag SPDK_MLX5_WQE_CTRL_INITIATOR_SMALL_FENCE set to 1
 * \param umr_attr Common UMR attributes, describe memory layout
 * \param crypto_attr Crypto UMR attributes
 * \param wr_id wrid which is returned in the CQE
 * \param flags SPDK_MLX5_WQE_CTRL_CE_CQ_UPDATE to have a signaled completion; Any of SPDK_MLX5_WQE_CTRL_FENCE* or 0
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_umr_configure_crypto(struct spdk_mlx5_qp *qp, struct spdk_mlx5_umr_attr *umr_attr,
				   struct spdk_mlx5_umr_crypto_attr *crypto_attr, uint64_t wr_id, uint32_t flags);

/**
 * Configure User Memory Region obtained using \ref spdk_mlx5_mkey_pool_get_bulk.
 *
 * It allows to gather memory chunks into virtually contig (from the NIC point of view) memory space with
 * start address 0. The user must ensure that \b qp's capacity is enough to perform this operation.
 * It only works if the UMR pool was created without crypto capabilities.
 *
 * \param qp Qpair to be used for UMR configuration. If RDMA operation which references this UMR is used on the same \b qp
 * then it is not necessary to wait for the UMR configuration to complete. Instead, first RDMA operation after UMR
 * configuration must have flag SPDK_MLX5_WQE_CTRL_INITIATOR_SMALL_FENCE set to 1
 * \param umr_attr Common UMR attributes, describe memory layout
 * \param wr_id wrid which is returned in the CQE
 * \param flags SPDK_MLX5_WQE_CTRL_CE_CQ_UPDATE to have a signaled completion; Any of SPDK_MLX5_WQE_CTRL_FENCE* or 0
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_umr_configure(struct spdk_mlx5_qp *qp, struct spdk_mlx5_umr_attr *umr_attr,
			    uint64_t wr_id, uint32_t flags);

/**
 * Create a PSV to be used for signature operations
 *
 * \param pd Protection Domain PSV belongs to
 * \return 0 on success, negated errno on failure
 */
struct spdk_mlx5_psv *spdk_mlx5_create_psv(struct ibv_pd *pd);

/**
 * Destroy PSV
 *
 * \param psv PSV pointer
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_destroy_psv(struct spdk_mlx5_psv *psv);

/**
 * Once a signature error happens on PSV, it's state can be re-initialized via a special SET_PSV WQE
 *
 * \param qp qp to be used to re-initialize PSV after error
 * \param psv_index index of the PSV object
 * \param transient_signature Intermediate signature state
 * \param wrid wrid which is returned in the CQE
 * \param flags SPDK_MLX5_WQE_CTRL_CE_CQ_UPDATE to have a signaled completion or 0
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_qp_set_psv(struct spdk_mlx5_qp *qp, uint32_t psv_index,
			 uint64_t transient_signature, uint64_t wr_id, uint32_t flags);

/**
 * Configure User Memory Region obtained using \ref spdk_mlx5_mkey_pool_get_bulk with CRC32C capabilities.
 *
 * Besides signature capabilities, it allows to gather memory chunks into virtually contig (from the NIC point of view)
 * memory space with start address 0. The user must ensure that \b qp's capacity is enough to perform this operation.
 *
 * \param qp Qpair to be used for UMR configuration. If RDMA operation which references this UMR is used on the same \b qp
 * then it is not necessary to wait for the UMR configuration to complete. Instead, first RDMA operation after UMR
 * configuration must have flag SPDK_MLX5_WQE_CTRL_INITIATOR_SMALL_FENCE set to 1
 * \param umr_attr Common UMR attributes, describe memory layout
 * \param sig_attr Signature UMR attributes
 * \param wr_id wrid which is returned in the CQE
 * \param flags SPDK_MLX5_WQE_CTRL_CE_CQ_UPDATE to have a signaled completion; Any of SPDK_MLX5_WQE_CTRL_FENCE* or 0
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_umr_configure_trans_sig(struct spdk_mlx5_qp *qp, struct spdk_mlx5_umr_attr *umr_attr,
				      struct spdk_mlx5_umr_trans_sig_attr *sig_attr, uint64_t wr_id,
				      uint32_t flags);

/**
 * Configure User Memory Region obtained using \ref spdk_mlx5_mkey_pool_get_bulk with crypto and CRC32C capabilities.
 *
 * Besides crypto and signature capabilities, it allows to gather memory chunks into virtually contig (from the NIC point of view)
 * memory space with start address 0. The user must ensure that \b qp's capacity is enough to perform this operation.
 *
 * \param qp Qpair to be used for UMR configuration. If RDMA operation which references this UMR is used on the same \b qp
 * then it is not necessary to wait for the UMR configuration to complete. Instead, first RDMA operation after UMR
 * configuration must have flag SPDK_MLX5_WQE_CTRL_INITIATOR_SMALL_FENCE set to 1
 * \param umr_attr Common UMR attributes, describe memory layout
 * \param sig_attr Signature UMR attributes
 * \param crypto_attr Crypto UMR attributes
 * \param wr_id wrid which is returned in the CQE
 * \param flags SPDK_MLX5_WQE_CTRL_CE_CQ_UPDATE to have a signaled completion; Any of SPDK_MLX5_WQE_CTRL_FENCE* or 0
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_umr_configure_trans_sig_crypto(struct spdk_mlx5_qp *qp,
		struct spdk_mlx5_umr_attr *umr_attr,
		struct spdk_mlx5_umr_trans_sig_attr *sig_attr,
		struct spdk_mlx5_umr_crypto_attr *crypto_attr,
		uint64_t wr_id, uint32_t flags);

/**
 * Configure User Memory Region obtained using \ref spdk_mlx5_mkey_pool_get_bulk with T10 DIF capabilities.
 *
 * Besides signature capabilities, it allows to gather memory chunks into virtually contig (from the NIC point of view)
 * memory space with start address 0. The user must ensure that \b qp's capacity is enough to perform this operation.
 *
 * \param qp Qpair to be used for UMR configuration. If RDMA operation which references this UMR is used on the same \b qp
 * then it is not necessary to wait for the UMR configuration to complete. Instead, first RDMA operation after UMR
 * configuration must have flag SPDK_MLX5_WQE_CTRL_INITIATOR_SMALL_FENCE set to 1
 * \param umr_attr Common UMR attributes, describe memory layout
 * \param sig_attr Signature UMR attributes
 * \param wr_id wrid which is returned in the CQE
 * \param flags SPDK_MLX5_WQE_CTRL_CE_CQ_UPDATE to have a signaled completion; Any of SPDK_MLX5_WQE_CTRL_FENCE* or 0
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_umr_configure_block_sig(struct spdk_mlx5_qp *qp, struct spdk_mlx5_umr_attr *umr_attr,
				      struct spdk_mlx5_umr_block_sig_attr *sig_attr, uint64_t wr_id,
				      uint32_t flags);

/**
 * Return a NULL terminated array of devices which support crypto operation on Nvidia NICs
 *
 * \param dev_num The size of the array or 0
 * \return Array of contexts. This array must be released with \b spdk_mlx5_crypto_devs_release
 */
struct ibv_context **spdk_mlx5_crypto_devs_get(int *dev_num);

/**
 * Releases array of devices allocated by \b spdk_mlx5_crypto_devs_get
 *
 * \param rdma_devs Array of device to be released
 */
void spdk_mlx5_crypto_devs_release(struct ibv_context **rdma_devs);

/**
 * Create a keytag which contains DEKs per each crypto device in the system
 *
 * \param attr Crypto attributes
 * \param out Keytag
 * \return 0 on success, negated errno of failure
 */
int spdk_mlx5_crypto_keytag_create(struct spdk_mlx5_crypto_dek_create_attr *attr,
				   struct spdk_mlx5_crypto_keytag **out);

/**
 * Destroy a keytag created using \b spdk_mlx5_crypto_keytag_create
 *
 * \param keytag Keytag pointer
 */
void spdk_mlx5_crypto_keytag_destroy(struct spdk_mlx5_crypto_keytag *keytag);

/**
 * Get Data Encryption Key data
 *
 * \param keytag Keytag with DEKs
 * \param pd Protection Domain which is going to be used to register UMR.
 * \param dek_obj_id Low level DEK ID, can be used to configure crypto UMR
 * \param data DEK data to be filled by this function
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_crypto_get_dek_data(struct spdk_mlx5_crypto_keytag *keytag, struct ibv_pd *pd,
				  struct spdk_mlx5_crypto_dek_data *data);

/**
 * Specify which devices are allowed to be used for crypto operation.
 *
 * If the user doesn't call this function then all devices which support crypto will be used.
 * This function copies devices names. In order to free allocated memory, the user must call
 * this function with either NULL \b dev_names or with \b devs_count equal 0. This way can also
 * be used to allow all devices.
 *
 * Subsequent calls with non-NULL \b dev_names and non-zero \b devs_count current copied dev_names array.
 *
 * This function is not thread safe.
 *
 * \param dev_names Array of devices names which are allowed to be used for crypto operations
 * \param devs_count Size of \b devs_count array
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_crypto_devs_allow(const char *const dev_names[], size_t devs_count);

/**
 * Creates a pool of memory keys for a given \b PD. If params::flags has SPDK_MLX5_MKEY_POOL_FLAG_CRYPTO enabled,
 * then a device associated with PD must support crypto operations.
 *
 * Can be called several times for different PDs. Has no effect if a pool for \b PD with the same \b flags already exists
 *
 * \param params Parameter of the memory pool
 * \param pd Protection Domain
 * \return 0 on success, errno on failure
 */
int spdk_mlx5_mkey_pool_init(struct spdk_mlx5_mkey_pool_param *params, struct ibv_pd *pd);

/**
 * Destroy mkey pools with the given \b flags and \b pd which was created by \ref spdk_mlx5_mkey_pool_init.
 *
 * The pool reference must be released by \ref spdk_mlx5_mkey_pool_put_ref before calling this function.
 *
 * \param pd Protection Domain
 * \param flags Specifies type of the pool to delete.
 * \return 0 on success, negated errno on failure
 */
int spdk_mlx5_mkey_pool_destroy(uint32_t flags, struct ibv_pd *pd);

/**
 * Get a reference to mkey pool specified by PD, increment internal reference counter.
 *
 * \param pd PD to get a mkey pool for
 * \param flags Required mkey pool flags, see \ref enum spdk_mlx5_mkey_pool_flags
 * \return Pointer to the mkey pool on success or NULL on error
 */
struct spdk_mlx5_mkey_pool *spdk_mlx5_mkey_pool_get_ref(struct ibv_pd *pd, uint32_t flags);

/**
 * Put the mkey pool reference.
 *
 * The pool is NOT destroyed if even reference counter reaches 0
 *
 * \param pool Mkey pool pointer
 */
void spdk_mlx5_mkey_pool_put_ref(struct spdk_mlx5_mkey_pool *pool);

/**
 * Get several mkeys from the pool
 *
 * \param pool mkey pool
 * \param mkeys array of mkey pointers to be filled by this function
 * \param mkeys_count number of mkeys to get from the pool
 * \return 0 on success, errno on failure
 */
int spdk_mlx5_mkey_pool_get_bulk(struct spdk_mlx5_mkey_pool *pool,
				 struct spdk_mlx5_mkey_pool_obj **mkeys, uint32_t mkeys_count);

/**
 * Return mkeys to the pool
 *
 * \param pool mkey pool
 * \param mkeys array of mkey pointers to be returned to the pool
 * \param mkeys_count number of mkeys to be returned to the pool
 */
void spdk_mlx5_mkey_pool_put_bulk(struct spdk_mlx5_mkey_pool *pool,
				  struct spdk_mlx5_mkey_pool_obj **mkeys, uint32_t mkeys_count);

/**
 * Get an mkey from the pool.
 *
 * \param pool mkey pool
 * \return Pointer to an mkey on success, NULL if the pool is empty
 */
struct spdk_mlx5_mkey_pool_obj *spdk_mlx5_mkey_pool_get(struct spdk_mlx5_mkey_pool *pool);

/**
 * Return the mkey to the pool
 *
 * \param pool mkey pool
 * \param mkey mkey to return to the pool
 */
void spdk_mlx5_mkey_pool_put(struct spdk_mlx5_mkey_pool *pool,
			     struct spdk_mlx5_mkey_pool_obj *mkey);

/**
 * Increment reference count of the mkey.
 *
 * The mkey is not returned to the pool until its reference counter reaches 0.
 *
 * \param mkey the mkey pool object
 */
void spdk_mlx5_mkey_pool_obj_get_ref(struct spdk_mlx5_mkey_pool_obj *mkey);

/**
 * Decrement the reference count of the mkey.
 *
 * The mkey is not returned to the pool until its reference counter reaches 0.
 * If the reference count is 0, the mkey object is returned to the pool. This function is an alternative to
 * \ref spdk_mlx5_mkey_pool_put
 *
 * \param mkey the mkey pool object
 */
void spdk_mlx5_mkey_pool_obj_put_ref(struct spdk_mlx5_mkey_pool_obj *mkey);

/**
 * Find mkey object by mkey ID
 *
 * \param ch mkey pool channel
 * \param mkey_id mkey ID
 * \return Pointer to mkey object or NULL
 */
struct spdk_mlx5_mkey_pool_obj *spdk_mlx5_mkey_pool_find_mkey_by_id(void *ch, uint32_t mkey_id);

/**
 * Associate PSV with an mkey to defer the PSV's release until the corresponding mkey is released.
 *
 * This function will also write NULL to the PSV pointer pointer to by ppsv,
 * to avoid double free.
 *
 * \param mkey the mkey pool object
 * \param ppsv the PSV pool object
 */
static inline void
spdk_mlx5_mkey_pool_obj_set_psv(struct spdk_mlx5_mkey_pool_obj *mkey,
				struct spdk_mlx5_psv_pool_obj **ppsv)
{
	struct spdk_mlx5_psv_pool_obj *psv = *ppsv;

	if (psv == NULL) {
		return;
	}

	*ppsv = NULL;

	assert(mkey->psv == NULL);

	mkey->psv = psv;
}

/**
 * Create a pool of PSVs for a given PD.
 *
 * Can be called several times for different PDs.
 *
 * \param params Parameter of the memory pool
 * \param pd Protection Domain
 * \return Pointer to the PSV pool on success or NULL on error
 */
struct spdk_mlx5_psv_pool *spdk_mlx5_psv_pool_create(struct spdk_mlx5_psv_pool_param *params,
		struct ibv_pd *pd);

/**
 * Destroy a pool of PSVs which was created by \ref spdk_mlx5_psv_pool_create.
 *
 * \param pool Pointer to the PSV pool to destroy.
 */
void spdk_mlx5_psv_pool_destroy(struct spdk_mlx5_psv_pool *pool);

/**
 * Get a PSV from the pool.
 *
 * \param pool PSV pool
 * \return Pointer to a PSV on success, NULL if the pool is empty
 */
struct spdk_mlx5_psv_pool_obj *spdk_mlx5_psv_pool_get(struct spdk_mlx5_psv_pool *pool);

/**
 * Return the PSV to the pool.
 *
 * This function will also write NULL to the PSV pointer pointed to by ppsv,
 * to not reuse a PSV pointer after returned.
 *
 * \param ppsv PSV to return to the pool
 */
void spdk_mlx5_psv_pool_put(struct spdk_mlx5_psv_pool_obj **ppsv);

/**
 * Notify the mlx5 library that a module which can handle UMR configuration is registered or unregistered
 *
 * \param registered True if the module is registered, false otherwise
 */
void spdk_mlx5_umr_implementer_register(bool registered);

/**
 * Check whether a module which can handle UMR configuration is registered or not
 *
 * \return True of the UMR implementer is registered, false otherwise
 */
bool spdk_mlx5_umr_implementer_is_registered(void);

#endif /* SPDK_MLX5_H */
