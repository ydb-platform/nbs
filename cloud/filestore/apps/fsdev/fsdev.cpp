#include "spdk/fsdev_module.h"
#include "spdk/log.h"

#include <util/system/compiler.h>

#include <memory>

namespace NCloud::NFileStore::NFsdev {

struct TFsdevModule
{
};

struct TFsdevState
{
    TFsdevModule* FsdevModule;
    struct spdk_fsdev_module SpdkFsdevModule;
};

extern struct TFsdevState gFsdevState;

static int ModuleInit()
{
    SPDK_NOTICELOG("xxx ModuleInit\n");
    auto fsdevModule = std::make_unique<TFsdevModule>();
    gFsdevState.FsdevModule = fsdevModule.release();
    return 0;
}

static void ModuleFini()
{
    SPDK_NOTICELOG("xxx ModuleFini\n");
    std::unique_ptr<TFsdevModule> fsdevModule(gFsdevState.FsdevModule);
    gFsdevState.FsdevModule = nullptr;
}

static int ConfigJson(struct spdk_json_write_ctx* w)
{
    Y_UNUSED(w);
    SPDK_NOTICELOG("xxx ConfigJson\n");
    return 0;
}

static int GetCtxSize()
{
    SPDK_NOTICELOG("xxx GetCtxSize\n");
    return 0;
}

///////////////////////////////////////////////////////////////////////////////
// Global filestore fsdev module state

struct TFsdevState gFsdevState = {
    .FsdevModule = nullptr,
    .SpdkFsdevModule = {
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

SPDK_FSDEV_MODULE_REGISTER(filestore_fsdev, &gFsdevState.SpdkFsdevModule);

}   // extern "C"
