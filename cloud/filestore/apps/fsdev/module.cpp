#include "bootstrap.h"
#include "spdk/fsdev_module.h"
#include "spdk/log.h"
#include "spdk/rpc.h"
#include "spdk/string.h"

#include <util/system/compiler.h>

namespace NCloud::NFileStore::NFsdev {

struct TFsdevState
{
    TBootstrap* Bootstrap;
    struct spdk_fsdev_module FsdevModule;
};

extern struct TFsdevState gFsdevState;

static int ModuleInit()
{
    try {
        auto bootstrap = std::make_unique<TBootstrap>();
        bootstrap->Init();
        bootstrap->Start();

        gFsdevState.Bootstrap = bootstrap.release();
    } catch (...) {
        SPDK_ERRLOG("FilestoreFsdev init failed\n");
        return -EINVAL;
    }

    return 0;
}

static void ModuleFini()
{
    TBootstrapPtr bootstrap(gFsdevState.Bootstrap);
    gFsdevState.Bootstrap = nullptr;
    bootstrap->Stop();
}

static int ConfigJson(struct spdk_json_write_ctx* w)
{
    Y_UNUSED(w);
    return 0;
}

static int GetCtxSize()
{
    return 0;
}

struct rpc_filestore_create
{
    char* name;
};

static const struct spdk_json_object_decoder rpc_filestore_create_decoders[] = {
    {"name", offsetof(struct rpc_filestore_create, name), spdk_json_decode_string, false},
};

static void FsdevRpcFilestoreCreate(
    struct spdk_jsonrpc_request* request,
    const struct spdk_json_val* params)
{
    Y_UNUSED(params);
    auto* bootstrap = static_cast<TBootstrap*>(gFsdevState.Bootstrap);
    if (!bootstrap) {
        spdk_jsonrpc_send_error_response(
            request,
            SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
            spdk_strerror(-EINVAL));
    }

    rpc_filestore_create req = {};

	if (spdk_json_decode_object(params, rpc_filestore_create_decoders,
				    SPDK_COUNTOF(rpc_filestore_create_decoders),
				    &req)) {
		SPDK_ERRLOG("spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
						 "spdk_json_decode_object failed");

		free(req.name);
		return;
	}

    bootstrap->RpcFilestoreCreate(req.name);

    free(req.name);
    spdk_jsonrpc_send_bool_response(request, true);
}

static void FsdevRpcFilestoreDelete(
    struct spdk_jsonrpc_request* request,
    const struct spdk_json_val* params)
{
    Y_UNUSED(params);
    auto* bootstrap = static_cast<TBootstrap*>(gFsdevState.Bootstrap);
    if (!bootstrap) {
        spdk_jsonrpc_send_error_response(
            request,
            SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
            spdk_strerror(-EINVAL));
    }

    bootstrap->RpcFilestoreDelete();

    spdk_jsonrpc_send_bool_response(request, true);
}

///////////////////////////////////////////////////////////////////////////////
// Global filestore fsdev module state

struct TFsdevState gFsdevState = {
    .Bootstrap = nullptr,
    .FsdevModule = {
        .module_init = ModuleInit,
        .module_fini = ModuleFini,
        .config_json = ConfigJson,

        .name = "filestore_fsdev",

        .get_ctx_size = GetCtxSize,
    }};

}   // namespace NCloud::NFileStore::NFsdev

////////////////////////////////////////////////////////////////////////////////
// Exported API

using namespace NCloud::NFileStore::NFsdev;

extern "C" {

void hello()
{}

SPDK_FSDEV_MODULE_REGISTER(filestore_fsdev, &gFsdevState.FsdevModule);
SPDK_RPC_REGISTER(
    "fsdev_filestore_create",
    FsdevRpcFilestoreCreate,
    SPDK_RPC_RUNTIME);
SPDK_RPC_REGISTER(
    "fsdev_filestore_delete",
    FsdevRpcFilestoreDelete,
    SPDK_RPC_RUNTIME);

}   // extern "C"
