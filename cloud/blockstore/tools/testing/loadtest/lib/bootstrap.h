#pragma once

#include "public.h"

#include "client_factory.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/nbd/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/validation/public.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/thread/lfstack.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TSpdkParts
{
    NSpdk::ISpdkEnvPtr Env;
    std::function<void(TLog& log)> LogInitializer;
};

struct TModuleFactories
{
    std::function<TSpdkParts(NSpdk::TSpdkEnvConfigPtr config)> SpdkFactory;
};

////////////////////////////////////////////////////////////////////////////////

class TBootstrap final
    : public IClientFactory
{
private:
    const TOptionsPtr Options;
    const std::shared_ptr<TModuleFactories> ModuleFactories;

    NClient::TClientAppConfigPtr ClientConfig;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    TLog GrpcLog;
    TLog SpdkLog;
    IMonitoringServicePtr Monitoring;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    IServerStatsPtr ClientStats;
    NSpdk::ISpdkEnvPtr Spdk;
    std::function<void(TLog& log)> SpdkLogInitializer;

    TLockFreeStack<IStartablePtr> Clients;

public:
    TBootstrap(
        TOptionsPtr options,
        std::shared_ptr<TModuleFactories> moduleFactories);
    ~TBootstrap() final;

    void Init();

    void Start();
    void Stop();

    TOptionsPtr GetOptions()
    {
        return Options;
    }

    NClient::TClientAppConfigPtr GetClientConfig()
    {
        return ClientConfig;
    }

    ITimerPtr GetTimer()
    {
        return Timer;
    }

    ISchedulerPtr GetScheduler()
    {
        return Scheduler;
    }

    ILoggingServicePtr GetLogging()
    {
        return Logging;
    }

    IRequestStatsPtr GetRequestStats()
    {
        return RequestStats;
    }

    IVolumeStatsPtr GetVolumeStats()
    {
        return VolumeStats;
    }

    NClient::IBlockStoreValidationClientPtr CreateValidationClient(
        IBlockStorePtr client,
        IValidationCallbackPtr callback,
        TString loggingTag,
        TBlockRange64 validationRange) override;

    IBlockDigestCalculatorPtr CreateDigestCalculator();

    IBlockStorePtr CreateClient(
        TVector<ui32> nonretriableErrorCodes,
        const TString& clientId) override;

    IBlockStorePtr CreateAndStartFilesystemClient() override;

    IBlockStorePtr CreateEndpointDataClient(
        NProto::EClientIpcType ipcType,
        const TString& socketPath,
        const TString& clientId,
        TVector<ui32> nonretriableErrorCodes) override;

    IBlockStorePtr CreateThrottlingClient(
        IBlockStorePtr client,
        NProto::TClientPerformanceProfile performanceProfile) override;

    const TString& GetEndpointStorageDir() const override;

private:
    void InitLWTrace();

    void InitClientConfig();

    NClient::IClientPtr CreateAndStartGrpcClient(TString clientId = {});
    NBD::IClientPtr CreateAndStartNbdClient(TString clientId = {});
    NRdma::IClientPtr CreateAndStartRdmaClient(TString clientId = {});

    IBlockStorePtr CreateDurableDataClient(
        IBlockStorePtr dataClient,
        TVector<ui32> nonretriableErrorCodes);
    NClient::TClientAppConfigPtr CreateClientConfig(
        const NClient::TClientAppConfigPtr& config,
        TString clientId);
};

}   // namespace NCloud::NBlockStore::NLoadTest
