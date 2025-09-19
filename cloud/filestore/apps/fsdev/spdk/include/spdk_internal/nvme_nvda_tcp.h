/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation. All rights reserved.
 *   Copyright (c) 2020 Mellanox Technologies LTD. All rights reserved.
 *   Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#ifndef SPDK_INTERNAL_NVME_TCP_H
#define SPDK_INTERNAL_NVME_TCP_H

#include "spdk/likely.h"
#include "spdk/dif.h"

#include "sgl.h"

#define SPDK_CRC32C_XOR				0xffffffffUL
#define SPDK_NVME_TCP_DIGEST_LEN		4
#define SPDK_NVME_TCP_DIGEST_ALIGNMENT		4
#define SPDK_NVME_TCP_QPAIR_EXIT_TIMEOUT	30
#define SPDK_NVMF_TCP_RECV_BUF_SIZE_FACTOR	8
#define SPDK_NVME_TCP_IN_CAPSULE_DATA_MAX_SIZE	8192u
/*
 * Maximum number of SGL elements.
 */
#define NVME_TCP_MAX_SGL_DESCRIPTORS	(16)

#define MAKE_DIGEST_WORD(BUF, CRC32C) \
        (   ((*((uint8_t *)(BUF)+0)) = (uint8_t)((uint32_t)(CRC32C) >> 0)), \
            ((*((uint8_t *)(BUF)+1)) = (uint8_t)((uint32_t)(CRC32C) >> 8)), \
            ((*((uint8_t *)(BUF)+2)) = (uint8_t)((uint32_t)(CRC32C) >> 16)), \
            ((*((uint8_t *)(BUF)+3)) = (uint8_t)((uint32_t)(CRC32C) >> 24)))

#define MATCH_DIGEST_WORD(BUF, CRC32C) \
        (    ((((uint32_t) *((uint8_t *)(BUF)+0)) << 0)         \
            | (((uint32_t) *((uint8_t *)(BUF)+1)) << 8)         \
            | (((uint32_t) *((uint8_t *)(BUF)+2)) << 16)        \
            | (((uint32_t) *((uint8_t *)(BUF)+3)) << 24))       \
            == (CRC32C))

#define DGET32(B)                                                               \
        (((  (uint32_t) *((uint8_t *)(B)+0)) << 0)                              \
         | (((uint32_t) *((uint8_t *)(B)+1)) << 8)                              \
         | (((uint32_t) *((uint8_t *)(B)+2)) << 16)                             \
         | (((uint32_t) *((uint8_t *)(B)+3)) << 24))

#define DSET32(B,D)                                                             \
        (((*((uint8_t *)(B)+0)) = (uint8_t)((uint32_t)(D) >> 0)),               \
         ((*((uint8_t *)(B)+1)) = (uint8_t)((uint32_t)(D) >> 8)),               \
         ((*((uint8_t *)(B)+2)) = (uint8_t)((uint32_t)(D) >> 16)),              \
         ((*((uint8_t *)(B)+3)) = (uint8_t)((uint32_t)(D) >> 24)))

struct nvme_tcp_req;
struct nvme_tcp_qpair;

typedef void (*nvme_tcp_qpair_xfer_complete_cb)(struct nvme_tcp_qpair *, struct nvme_tcp_req *);

/* Max possible size of CPDA, this limits max size of PDU to be sent.
 * ICREQ and H2C_TERM are located in TCP qpair */
#define NVME_NVDA_TCP_CPDA_BYTES_MAX 128

struct nvme_tcp_pdu {
	union {
		/* to hold error pdu data */
		uint8_t					raw[NVME_NVDA_TCP_CPDA_BYTES_MAX];
		struct spdk_nvme_tcp_common_pdu_hdr	common;
		struct spdk_nvme_tcp_term_req_hdr	term_req;
		struct spdk_nvme_tcp_cmd		capsule_cmd;
		struct spdk_nvme_tcp_h2c_data_hdr	h2c_data;
		struct spdk_nvme_tcp_ic_resp		ic_resp;
		struct spdk_nvme_tcp_rsp		capsule_resp;
		struct spdk_nvme_tcp_c2h_data_hdr	c2h_data;
		struct spdk_nvme_tcp_r2t_hdr		r2t;
	} hdr;
	TAILQ_ENTRY(nvme_tcp_pdu)			tailq;
	TAILQ_ENTRY(nvme_tcp_pdu)			link;
	nvme_tcp_qpair_xfer_complete_cb			cb_fn;
	struct nvme_tcp_req				*req; /* data tied to a tcp request */

	/* Points to nvme_tcp_req iov. To be removed later */
	struct iovec					*iovs;
	uint32_t					data_len;
	uint32_t					rw_offset;
	uint32_t					data_digest_crc32;
	uint8_t						data_digest[SPDK_NVME_TCP_DIGEST_LEN];
	union {
		struct {
			uint8_t has_hdgst: 1;
			uint8_t ddgst_enable: 1;
			uint8_t has_mkeys: 1;
			uint8_t has_capsule: 1;
			uint8_t ddigest_offset: 3;
		};
		uint8_t u_raw;
	};
	uint8_t						data_iovcnt;
	/* Total iovcn, includes data_iovcnt, header and ddigest iovs */
	uint8_t						iovcnt;

	uint8_t						ch_valid_bytes;
	uint8_t						psh_valid_bytes;
	uint8_t						psh_len;
	uint8_t						padding_len;
	/* Used to track write of capsule */
	uint8_t						capsule_len;
	uint8_t						capsule_offset;
	/* XXX 7 bytes hole, try to pack */
#ifdef DEBUG
	void						*curr_list;
#endif
	uint32_t					mkeys[NVME_TCP_MAX_SGL_DESCRIPTORS];
};

enum nvme_tcp_pdu_recv_state {
	/* Ready to wait for PDU */
	NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY,

	/* Active tqpair waiting for any PDU common header */
	NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_CH,

	/* Active tqpair waiting for any PDU specific header */
	NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PSH,

	/* Active tqpair waiting for a tcp request, only use in target side */
	NVME_TCP_PDU_RECV_STATE_AWAIT_REQ,

	/* Active tqpair waiting for payload */
	NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PAYLOAD,

	/* Active tqpair waiting for all outstanding PDUs to complete */
	NVME_TCP_PDU_RECV_STATE_QUIESCING,

	/* Active tqpair does not wait for payload */
	NVME_TCP_PDU_RECV_STATE_ERROR,
};

enum nvme_tcp_error_codes {
	NVME_TCP_PDU_IN_PROGRESS        = 0,
	NVME_TCP_CONNECTION_FATAL       = -1,
	NVME_TCP_PDU_FATAL              = -2,
};

enum nvme_tcp_qpair_state {
	NVME_TCP_QPAIR_STATE_INVALID = 0,
	NVME_TCP_QPAIR_STATE_CONNECTING = 1,
	NVME_TCP_QPAIR_STATE_SOCK_CONNECT_FAIL = 2,
	NVME_TCP_QPAIR_STATE_SOCK_CONNECTED = 3,
	NVME_TCP_QPAIR_STATE_ICREQ_SEND = 4,
	NVME_TCP_QPAIR_STATE_ICRESP_WAIT = 5,
	NVME_TCP_QPAIR_STATE_ICRESP_RECEIVED = 6,
	NVME_TCP_QPAIR_STATE_FABRIC_CONNECT_SEND = 7,
	NVME_TCP_QPAIR_STATE_FABRIC_CONNECT_POLL = 8,
	NVME_TCP_QPAIR_STATE_RUNNING = 9,
	NVME_TCP_QPAIR_STATE_EXITING = 10,
	NVME_TCP_QPAIR_STATE_EXITED = 11
};

static const bool g_nvme_tcp_hdgst[] = {
	[SPDK_NVME_TCP_PDU_TYPE_IC_REQ]         = false,
	[SPDK_NVME_TCP_PDU_TYPE_IC_RESP]        = false,
	[SPDK_NVME_TCP_PDU_TYPE_H2C_TERM_REQ]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_C2H_TERM_REQ]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_CAPSULE_CMD]    = true,
	[SPDK_NVME_TCP_PDU_TYPE_CAPSULE_RESP]   = true,
	[SPDK_NVME_TCP_PDU_TYPE_H2C_DATA]       = true,
	[SPDK_NVME_TCP_PDU_TYPE_C2H_DATA]       = true,
	[SPDK_NVME_TCP_PDU_TYPE_R2T]            = true
};

static const bool g_nvme_tcp_ddgst[] = {
	[SPDK_NVME_TCP_PDU_TYPE_IC_REQ]         = false,
	[SPDK_NVME_TCP_PDU_TYPE_IC_RESP]        = false,
	[SPDK_NVME_TCP_PDU_TYPE_H2C_TERM_REQ]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_C2H_TERM_REQ]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_CAPSULE_CMD]    = true,
	[SPDK_NVME_TCP_PDU_TYPE_CAPSULE_RESP]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_H2C_DATA]       = true,
	[SPDK_NVME_TCP_PDU_TYPE_C2H_DATA]       = true,
	[SPDK_NVME_TCP_PDU_TYPE_R2T]            = false
};

static uint32_t
nvme_tcp_pdu_calc_header_digest(struct nvme_tcp_pdu *pdu)
{
	uint32_t crc32c;
	uint32_t hlen = pdu->hdr.common.hlen;

	crc32c = spdk_crc32c_update(&pdu->hdr.raw, hlen, ~0);
	crc32c = crc32c ^ SPDK_CRC32C_XOR;
	return crc32c;
}

static void
nvme_tcp_pdu_calc_psh_len(struct nvme_tcp_pdu *pdu, bool hdgst_enable)
{
	uint8_t psh_len, pdo, padding_len;

	psh_len = pdu->hdr.common.hlen;

	if (hdgst_enable && g_nvme_tcp_hdgst[pdu->hdr.common.pdu_type]) {
		pdu->has_hdgst = 1;
		psh_len += SPDK_NVME_TCP_DIGEST_LEN;
	}
	if (pdu->hdr.common.plen > psh_len) {
		switch (pdu->hdr.common.pdu_type) {
		case SPDK_NVME_TCP_PDU_TYPE_CAPSULE_CMD:
		case SPDK_NVME_TCP_PDU_TYPE_H2C_DATA:
		case SPDK_NVME_TCP_PDU_TYPE_C2H_DATA:
			pdo = pdu->hdr.common.pdo;
			padding_len = pdo - psh_len;
			if (padding_len > 0) {
				psh_len = pdo;
			}
			break;
		default:
			/* There is no padding for other PDU types */
			break;
		}
	}

	psh_len -= sizeof(struct spdk_nvme_tcp_common_pdu_hdr);
	pdu->psh_len = psh_len;
}

#endif /* SPDK_INTERNAL_NVME_TCP_H */
