#pragma once

#include "aliased_volumes.h"
#include "app_context.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>

#include <library/cpp/logger/log.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TCompareDataActionRunner
{
private:
    TLog& Log;
    TAppContext& AppContext;
    const TAliasedVolumes& AliasedVolumes;
    NClient::TClientAppConfigPtr ClientConfig;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    IClientFactory& ClientFactory;
    TTestContext& TestContext;

public:
    TCompareDataActionRunner(
        TLog& log,
        TAppContext& appContext,
        const TAliasedVolumes& aliasedVolumes,
        NClient::TClientAppConfigPtr clientConfig,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        IClientFactory& clientFactory,
        TTestContext& testContext);

public:
    int Run(const NProto::TActionGraph::TCompareDataAction& action);

private:
    void ReadBlocks(
        const TString& checkpointId,
        ui32 blockIndex,
        ui32 requestSize,
        TVector<char>* buffer);

    void ReadBlocks(
        const NProto::TActionGraph::TSource& source,
        ui32 blockIndex,
        ui32 requestSize,
        TVector<char>* buffer);
};

}   // namespace NCloud::NBlockStore::NLoadTest
