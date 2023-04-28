#include "public.h"

#include <cloud/blockstore/libs/daemon/common/bootstrap.h>
#include <cloud/blockstore/libs/logbroker/iface/public.h>
#include <cloud/blockstore/libs/notify/public.h>
#include <cloud/blockstore/libs/ydbstats/public.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/iam/iface/public.h>

#include <ydb/core/driver_lib/run/factories.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

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

};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrapYdb final
    : public TBootstrapBase
{
private:
    std::shared_ptr<NKikimr::TModuleFactories> ModuleFactories;
    std::shared_ptr<TServerModuleFactories> ServerModuleFactories;

    TConfigInitializerYdbPtr Configs;

    ITaskQueuePtr SubmissionQueue;
    ITaskQueuePtr CompletionQueue;
    IActorSystemPtr ActorSystem;
    IAsyncLoggerPtr AsyncLogger;
    IStatsAggregatorPtr StatsAggregator;
    IClientPercentileCalculatorPtr ClientPercentiles;
    NYdbStats::IYdbVolumesStatsUploaderPtr StatsUploader;
    NYdbStats::IYdbStoragePtr YdbStorage;
    ITraceSerializerPtr TraceSerializer;
    NLogbroker::IServicePtr LogbrokerService;
    NNotify::IServicePtr NotifyService;
    ICgroupStatsFetcherPtr CgroupStatsFetcher;
    NIamClient::IIamTokenClientPtr IamTokenClient;

public:
    TBootstrapYdb(
        std::shared_ptr<NKikimr::TModuleFactories> moduleFactories,
        std::shared_ptr<TServerModuleFactories> serverModuleFactories,
        IDeviceHandlerFactoryPtr deviceHandlerFactory);
    ~TBootstrapYdb();

    TProgramShouldContinue& GetShouldContinue() override;

protected:
    TConfigInitializerCommonPtr InitConfigs(int argc, char** argv) override;

    IStartable* GetSubmissionQueue() override;
    IStartable* GetCompletionQueue() override;
    IStartable* GetActorSystem() override;
    IStartable* GetAsyncLogger() override;
    IStartable* GetStatsAggregator() override;
    IStartable* GetClientPercentiles() override;
    IStartable* GetStatsUploader() override;
    IStartable* GetYdbStorage() override;
    IStartable* GetTraceSerializer() override;
    IStartable* GetLogbrokerService() override;
    IStartable* GetNotifyService() override;
    IStartable* GetCgroupStatsFetcher() override;
    IStartable* GetIamTokenClient() override;

    void InitKikimrService() override;
    void InitAuthService() override;

private:
    void InitConfigs();
};

}   // namespace NCloud::NBlockStore::NServer
