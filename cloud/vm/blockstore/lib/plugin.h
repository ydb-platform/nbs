#pragma once

#include "public.h"

#include <cloud/vm/api/blockstore-plugin.h>

#include <cloud/blockstore/config/plugin.pb.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/nbd/public.h>
#include <cloud/blockstore/libs/throttling/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NPlugin {

////////////////////////////////////////////////////////////////////////////////

struct IPlugin: public IIncompleteRequestProvider
{
    virtual ~IPlugin() = default;

    virtual int MountVolume(
        const NProto::TPluginMountConfig& mountConfig,
        const NClient::TSessionConfig& sessionConfig,
        BlockPlugin_Volume* volume) = 0;

    virtual int MountVolumeAsync(
        const NProto::TPluginMountConfig& mountConfig,
        const NClient::TSessionConfig& sessionConfig,
        BlockPlugin_Volume* volume,
        BlockPlugin_Completion* comp) = 0;

    virtual int UnmountVolume(BlockPlugin_Volume* volume) = 0;

    virtual int UnmountVolumeAsync(
        BlockPlugin_Volume* volume,
        BlockPlugin_Completion* comp) = 0;

    virtual int SubmitRequest(
        BlockPlugin_Volume* volume,
        BlockPlugin_Request* req,
        BlockPlugin_Completion* comp) = 0;

    virtual TString GetCountersJson() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IPluginPtr CreatePlugin(
    BlockPluginHost* host,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr clientStats,
    IThrottlerPtr throttler,
    NClient::IClientPtr client,
    NBD::IClientPtr nbdClient,
    NClient::TClientAppConfigPtr config);

}   // namespace NCloud::NBlockStore::NPlugin
