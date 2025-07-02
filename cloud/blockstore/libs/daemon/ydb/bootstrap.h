#include "public.h"

#include <cloud/blockstore/libs/daemon/common/bootstrap.h>
#include <cloud/blockstore/libs/kms/iface/public.h>
#include <cloud/blockstore/libs/logbroker/iface/public.h>
#include <cloud/blockstore/libs/notify/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/root_kms/iface/public.h>
#include <cloud/blockstore/libs/ydbstats/public.h>

#include <cloud/storage/core/config/grpc_client.pb.h>
#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/iam/iface/public.h>
#include <cloud/storage/core/libs/opentelemetry/iface/public.h>

#include <contrib/ydb/core/driver_lib/run/factories.h>

namespace NCloud::NBlockStore::NProto {
    class TGrpcClientConfig;
    class TRootKmsConfig;
}   // namespace NCloud::NBlockStore::NProto

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TSpdkParts
{
    NSpdk::ISpdkEnvPtr Env;
    NVhost::TVhostCallbacks VhostCallbacks;
    std::function<void(TLog& log)> LogInitializer;
};

struct TServerModuleFactories
{
    std::function<NLogbroker::IServicePtr(
        NLogbroker::TLogbrokerConfigPtr config,
        ILoggingServicePtr logging)> LogbrokerServiceFactory;

    std::function<NIamClient::IIamTokenClientPtr(
        NIamClient::TIamClientConfigPtr config,
        ILoggingServicePtr logging,
        ISchedulerPtr scheduler,
        ITimerPtr timer)> IamClientFactory;

    std::function<IComputeClientPtr(
        NProto::TGrpcClientConfig config,
        ILoggingServicePtr logging)> ComputeClientFactory;

    std::function<IKmsClientPtr(
        NProto::TGrpcClientConfig config,
        ILoggingServicePtr logging)> KmsClientFactory;

    std::function<IRootKmsClientPtr(
        const NProto::TRootKmsConfig& config,
        ILoggingServicePtr logging)> RootKmsClientFactory;

    std::function<ITraceServiceClientPtr(
        const ::NCloud::NProto::TGrpcClientConfig& config,
        ILoggingServicePtr logging)>
        TraceServiceClientFactory;

    std::function<TSpdkParts(NSpdk::TSpdkEnvConfigPtr config)> SpdkFactory;

    std::function<NRdma::IServerPtr(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        NRdma::TServerConfigPtr config)> RdmaServerFactory;

    std::function<NRdma::IClientPtr(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        NRdma::TClientConfigPtr config)> RdmaClientFactory;
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrapYdb final
    : public TBootstrapBase
{
private:
    std::shared_ptr<NKikimr::TModuleFactories> ModuleFactories;
    std::shared_ptr<TServerModuleFactories> ServerModuleFactories;

    TConfigInitializerYdbPtr Configs;

    IActorSystemPtr ActorSystem;
    IAsyncLoggerPtr AsyncLogger;
    IStatsAggregatorPtr StatsAggregator;
    IClientPercentileCalculatorPtr ClientPercentiles;
    NYdbStats::IYdbVolumesStatsUploaderPtr StatsUploader;
    NYdbStats::IYdbStoragePtr YdbStorage;
    ITraceSerializerPtr TraceSerializer;
    NLogbroker::IServicePtr LogbrokerService;
    NNotify::IServicePtr NotifyService;
    NCloud::NStorage::IStatsFetcherPtr StatsFetcher;
    NIamClient::IIamTokenClientPtr IamTokenClient;
    IComputeClientPtr ComputeClient;
    IKmsClientPtr KmsClient;
    IRootKmsClientPtr RootKmsClient;
    ITraceServiceClientPtr TraceServiceClient;
    std::function<void(TLog& log)> SpdkLogInitializer;

public:
    TBootstrapYdb(
        std::shared_ptr<NKikimr::TModuleFactories> moduleFactories,
        std::shared_ptr<TServerModuleFactories> serverModuleFactories,
        IDeviceHandlerFactoryPtr deviceHandlerFactory);
    ~TBootstrapYdb() override;

    TProgramShouldContinue& GetShouldContinue() override;

protected:
    TConfigInitializerCommonPtr InitConfigs(int argc, char** argv) override;

    IStartable* GetActorSystem() override;
    IStartable* GetAsyncLogger() override;
    IStartable* GetStatsAggregator() override;
    IStartable* GetClientPercentiles() override;
    IStartable* GetStatsUploader() override;
    IStartable* GetYdbStorage() override;
    ITraceSerializerPtr GetTraceSerializer() override;
    IStartable* GetLogbrokerService() override;
    IStartable* GetNotifyService() override;
    IStartable* GetStatsFetcher() override;
    IStartable* GetIamTokenClient() override;
    IStartable* GetComputeClient() override;
    IStartable* GetKmsClient() override;
    IStartable* GetRootKmsClient() override;

    ITraceServiceClientPtr GetTraceServiceClient() override;

    void InitSpdk() override;
    void InitRdmaClient() override;
    void InitRdmaServer() override;
    void InitKikimrService() override;
    void InitAuthService() override;
    void InitRdmaRequestServer() override;

    void WarmupBSGroupConnections() override;

    void SetupCellsManager() override;

private:
    void InitConfigs();
};

}   // namespace NCloud::NBlockStore::NServer
