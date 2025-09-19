/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#include "vfsdev_passthru.h"
#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"

/* Structure to hold the parameters for this RPC method. */
struct rpc_fsdev_passthru_create {
	char *base_fsdev_name;
	char *name;
};

/* Free the allocated memory resource after the RPC handling. */
static void
free_rpc_fsdev_passthru_create(struct rpc_fsdev_passthru_create *r)
{
	free(r->base_fsdev_name);
	free(r->name);
}

/* Structure to decode the input parameters for this RPC method. */
static const struct spdk_json_object_decoder rpc_fsdev_passthru_create_decoders[] = {
	{"base_fsdev_name", offsetof(struct rpc_fsdev_passthru_create, base_fsdev_name), spdk_json_decode_string},
	{"name", offsetof(struct rpc_fsdev_passthru_create, name), spdk_json_decode_string},
};

/* Decode the parameters for this RPC method and properly construct the passthru
 * device. Error status returned in the failed cases.
 */
static void
rpc_fsdev_passthru_create(struct spdk_jsonrpc_request *request,
			  const struct spdk_json_val *params)
{
	struct rpc_fsdev_passthru_create req = {NULL};
	struct spdk_json_write_ctx *w;
	int rc;

	if (spdk_json_decode_object(params, rpc_fsdev_passthru_create_decoders,
				    SPDK_COUNTOF(rpc_fsdev_passthru_create_decoders),
				    &req)) {
		SPDK_DEBUGLOG(vfsdev_passthru, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = fsdev_passthru_external_create(req.base_fsdev_name, req.name);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, req.name);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_fsdev_passthru_create(&req);
}
SPDK_RPC_REGISTER("fsdev_passthru_ext_create", rpc_fsdev_passthru_create, SPDK_RPC_RUNTIME)

struct rpc_fsdev_passthru_delete {
	char *name;
};

static void
free_rpc_fsdev_passthru_delete(struct rpc_fsdev_passthru_delete *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_fsdev_passthru_delete_decoders[] = {
	{"name", offsetof(struct rpc_fsdev_passthru_delete, name), spdk_json_decode_string},
};

static void
rpc_fsdev_passthru_delete_cb(void *cb_arg, int fsdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (fsdeverrno == 0) {
		spdk_jsonrpc_send_bool_response(request, true);
	} else {
		spdk_jsonrpc_send_error_response(request, fsdeverrno, spdk_strerror(-fsdeverrno));

	}
}

static void
rpc_fsdev_passthru_delete(struct spdk_jsonrpc_request *request,
			  const struct spdk_json_val *params)
{
	struct rpc_fsdev_passthru_delete req = {NULL};
	struct spdk_fsdev *fsdev;

	if (spdk_json_decode_object(params, rpc_fsdev_passthru_delete_decoders,
				    SPDK_COUNTOF(rpc_fsdev_passthru_delete_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	fsdev_passthru_external_delete(req.name, rpc_fsdev_passthru_delete_cb, request);

cleanup:
	free_rpc_fsdev_passthru_delete(&req);
}
SPDK_RPC_REGISTER("fsdev_passthru_ext_delete", rpc_fsdev_passthru_delete, SPDK_RPC_RUNTIME)
