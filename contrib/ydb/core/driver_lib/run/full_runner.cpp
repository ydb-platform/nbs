#include "run.h"
#include "kikimr_services_initializers.h"


namespace NKikimr {

TIntrusivePtr<TServiceInitializersList> TKikimrRunner::CreateServiceInitializersList(
    const TKikimrRunConfig& runConfig,
    const TBasicKikimrServicesMask& serviceMask) {
    using namespace NKikimrServicesInitializers;
    const TServiceInitializerFactories tabletServices{
        .LocalService = [](const TKikimrRunConfig& config) -> IServiceInitializer* {
            return new TLocalServiceInitializer(config);
        },
        .BlobCache = [](const TKikimrRunConfig& config) -> IServiceInitializer* {
            return new TBlobCacheInitializer(config);
        },
        .CompPriorities = [](const TKikimrRunConfig& config) -> IServiceInitializer* {
            return new TCompPrioritiesInitializer(config);
        },
        .CompositeConveyor = [](const TKikimrRunConfig& config) -> IServiceInitializer* {
            return new TCompositeConveyorInitializer(config);
        },
        .GeneralCachePortionsMetadata = [](const TKikimrRunConfig& config) -> IServiceInitializer* {
            return new TGeneralCachePortionsMetadataInitializer(config);
        },
        .GeneralCacheColumnData = [](const TKikimrRunConfig& config) -> IServiceInitializer* {
            return new TGeneralCacheColumnDataInitializer(config);
        },
        .OverloadManager = [](const TKikimrRunConfig& config) -> IServiceInitializer* {
            return new TOverloadManagerInitializer(config);
        },
    };
    return CreateServiceInitializersList(runConfig, serviceMask, tabletServices);
}

TIntrusivePtr<TKikimrRunner> TKikimrRunner::CreateKikimrRunner(
        const TKikimrRunConfig& runConfig,
        std::shared_ptr<TModuleFactories> factories) {
    TIntrusivePtr<TKikimrRunner> runner(new TKikimrRunner(factories));
    runner->InitializeAllocator(runConfig);
    runner->InitializeRegistries(runConfig);
    runner->InitializeMonitoring(runConfig);
    runner->InitializeControlBoard(runConfig);
    runner->InitializeAppData(runConfig);
    runner->InitializeLogSettings(runConfig);
    TIntrusivePtr<TServiceInitializersList> sil(runner->CreateServiceInitializersList(runConfig, runConfig.ServicesMask));
    runner->InitializeActorSystem(runConfig, sil, runConfig.ServicesMask);
    runner->InitializeMonitoringLogin(runConfig);
    runner->InitializeKqpController(runConfig);
    runner->InitializeGracefulShutdown(runConfig);
    runner->InitializeGRpc(runConfig);
    runner->InitializePlugins(runConfig);
    return runner;
}

} // namespace NKikimr
