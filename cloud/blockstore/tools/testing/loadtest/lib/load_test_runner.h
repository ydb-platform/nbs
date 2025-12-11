#pragma once

#include "public.h"

#include "aliased_volumes.h"
#include "app_context.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <cloud/storage/core/libs/endpoints/iface/public.h>

#include <library/cpp/logger/log.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TLoadTestRunner
{
private:
    TLog& Log;
    TAppContext& AppContext;
    TAliasedVolumes& AliasedVolumes;
    NClient::TClientAppConfigPtr ClientConfig;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    IClientFactory& ClientFactory;
    TTestContext& TestContext;
    IEndpointStoragePtr EndpointStorage;
    std::unique_ptr<TTempDir> EndpointsDir;

    TString EndpointSocketPath;

public:
    TLoadTestRunner(
        TLog& log,
        TAppContext& appContext,
        TAliasedVolumes& aliasedVolumes,
        NClient::TClientAppConfigPtr clientConfig,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        IClientFactory& clientFactory,
        TTestContext& testContext);

public:
    int Run(
        const NProto::TLoadTest& test,
        const TVector<TTestContext*> dependencies);

private:
    static TVector<ui32> SuccessOnError(const NProto::TLoadTest& test);

    static void FillLatency(
        const TLatencyHistogram& hist,
        NProto::TLatency& latency);

    void SetupTest(
        const NProto::TLoadTest& test,
        const TVector<TTestContext*>& dependencies,
        const TVector<ui32>& successOnError);

    NProto::ETestStatus RunTest(
        const NProto::TLoadTest& test,
        const TVector<ui32>& successOnError);

    void TeardownTest(
        const NProto::TLoadTest& test,
        const TVector<ui32>& successOnError);
};

}   // namespace NCloud::NBlockStore::NLoadTest
