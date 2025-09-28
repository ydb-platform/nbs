#include "bootstrap.h"

#include "spdk/fsdev_module.h"
#include "spdk/log.h"

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

}   // extern "C"
