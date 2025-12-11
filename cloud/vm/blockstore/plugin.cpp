/*
 * Blockstore plugin for external C code (QEMU)
 */

#include "bootstrap.h"

#include <cloud/vm/blockstore/lib/plugin.h>

#include <cloud/blockstore/libs/client/session.h>

#include <cloud/storage/core/libs/version/version.h>

namespace NCloud::NBlockStore::NPlugin {

////////////////////////////////////////////////////////////////////////////////
// Exported plugin interface

int MountVolume(
    BlockPlugin* plugin,
    const char* volumeName,
    const char* mountToken,
    BlockPlugin_Volume* volume)
{
    if (!plugin || !plugin->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!volume) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    BlockPlugin_MountOpts opts = {};
    opts.volume_name = volumeName;
    opts.mount_token = mountToken;

    auto* bootstrap = static_cast<TBootstrap*>(plugin->state);
    return bootstrap->GetPlugin()->MountVolume(
        bootstrap->GetMountConfig(opts.volume_name),
        bootstrap->FillSessionConfig(&opts),
        volume);
}

int MountVolumeAsync(
    BlockPlugin* plugin,
    BlockPlugin_MountOpts* opts,
    BlockPlugin_Volume* volume,
    BlockPlugin_Completion* completion)
{
    if (!plugin || !plugin->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!opts) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!volume) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!completion) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    auto* bootstrap = static_cast<TBootstrap*>(plugin->state);
    return bootstrap->GetPlugin()->MountVolumeAsync(
        bootstrap->GetMountConfig(opts->volume_name),
        bootstrap->FillSessionConfig(opts),
        volume,
        completion);
}

int MountVolumeSync(
    BlockPlugin* plugin,
    BlockPlugin_MountOpts* opts,
    BlockPlugin_Volume* volume)
{
    if (!plugin || !plugin->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!opts) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!volume) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    auto* bootstrap = static_cast<TBootstrap*>(plugin->state);
    return bootstrap->GetPlugin()->MountVolume(
        bootstrap->GetMountConfig(opts->volume_name),
        bootstrap->FillSessionConfig(opts),
        volume);
}

int UnmountVolume(BlockPlugin* plugin, BlockPlugin_Volume* volume)
{
    if (!plugin || !plugin->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!volume || !volume->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    auto* bootstrap = static_cast<TBootstrap*>(plugin->state);
    return bootstrap->GetPlugin()->UnmountVolume(volume);
}

int UnmountVolumeAsync(
    BlockPlugin* plugin,
    BlockPlugin_Volume* volume,
    BlockPlugin_Completion* completion)
{
    if (!plugin || !plugin->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!volume || !volume->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!completion) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    auto* bootstrap = static_cast<TBootstrap*>(plugin->state);
    return bootstrap->GetPlugin()->UnmountVolumeAsync(volume, completion);
}

int SubmitRequest(
    BlockPlugin* plugin,
    BlockPlugin_Volume* volume,
    BlockPlugin_Request* req,
    BlockPlugin_Completion* comp)
{
    if (!plugin || !plugin->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!volume || !volume->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!req) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!comp) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    auto* bootstrap = static_cast<TBootstrap*>(plugin->state);
    return bootstrap->GetPlugin()->SubmitRequest(volume, req, comp);
}

int GetCounters(
    struct BlockPlugin* plugin,
    BlockPlugin_GetCountersCallback callback,
    void* opaque)
{
    if (!plugin || !plugin->state) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    if (!callback) {
        return BLOCK_PLUGIN_E_ARGUMENT;
    }

    auto* bootstrap = static_cast<TBootstrap*>(plugin->state);
    callback(bootstrap->GetPlugin()->GetCountersJson().data(), opaque);
    return BLOCK_PLUGIN_E_OK;
}

///////////////////////////////////////////////////////////////////////////////
// Global plugin state
// Currently we only need to support 1 plugin instance per dll

BlockPlugin gBlockPlugin = {
    .magic = BLOCK_PLUGIN_MAGIC,
    .version_major = BLOCK_PLUGIN_API_VERSION_MAJOR,
    .version_minor = BLOCK_PLUGIN_API_VERSION_MINOR,

    .state = nullptr,

    .mount = MountVolume,
    .umount = UnmountVolume,
    .submit_request = SubmitRequest,
    .get_dynamic_counters = GetCounters,
    .mount_async = MountVolumeAsync,
    .umount_async = UnmountVolumeAsync,
    .mount_sync = MountVolumeSync,
};

ui64 gBlockPluginRefCount = 0;   // thread-safety not required

}   // namespace NCloud::NBlockStore::NPlugin

////////////////////////////////////////////////////////////////////////////////
// Exported API

using namespace NCloud::NBlockStore::NPlugin;

extern "C" {

BlockPlugin* BlockPlugin_GetPlugin(BlockPluginHost* host, const char* options)
{
    if (!host || host->magic != BLOCK_PLUGIN_HOST_MAGIC ||
        host->version_major !=
            BLOCK_PLUGIN_API_VERSION_MAJOR   // require major versions to match
        || !host->complete_request || !host->log_message)
    {
        return nullptr;
    }

    if (++gBlockPluginRefCount == 1) {
        try {
            auto bootstrap = std::make_unique<TBootstrap>(host, options);
            bootstrap->Init();
            bootstrap->Start();

            gBlockPlugin.state = bootstrap.release();
        } catch (...) {
            host->log_message(host, CurrentExceptionMessage().data());
            return nullptr;
        }
    }

    return &gBlockPlugin;
}

void BlockPlugin_PutPlugin(BlockPlugin* plugin)
{
    Y_ABORT_UNLESS(plugin == &gBlockPlugin);

    if (--gBlockPluginRefCount == 0) {
        TBootstrapPtr bootstrap(static_cast<TBootstrap*>(gBlockPlugin.state));
        gBlockPlugin.state = nullptr;
        bootstrap->Stop();
    }
}

const char* BlockPlugin_GetVersion()
{
    return NCloud::GetFullVersionString().data();
}

}   // extern "C"
