#include "test_env.h"

#include <cloud/blockstore/libs/notify/iface/notify.h>

namespace NCloud::NBlockStore::NStorage::NDiskRegistryTest {

////////////////////////////////////////////////////////////////////////////////

NProto::TStorageServiceConfig CreateDefaultStorageConfig()
{
    NProto::TStorageServiceConfig configProto;
    configProto.SetMaxDisksInPlacementGroup(3);
    configProto.SetBrokenDiskDestructionDelay(5000);
    configProto.SetNonReplicatedAgentMinTimeout(15000);
    configProto.SetNonReplicatedAgentMaxTimeout(15000);
    configProto.SetAgentRequestTimeout(5000);
    configProto.SetNonReplicatedInfraTimeout(TDuration::Days(1).MilliSeconds());
    configProto.SetNonReplicatedMigrationStartAllowed(true);
    configProto.SetMirroredMigrationStartAllowed(true);
    configProto.SetAllocationUnitNonReplicatedSSD(10);
    configProto.SetDiskRegistryVolumeConfigUpdatePeriod(5000);
    configProto.SetCachedAcquireRequestLifetime(1000 * 60 * 60);
    configProto.SetDiskRegistryAlwaysAllocatesLocalDisks(true);

    return configProto;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::TTestActorRuntime> TTestRuntimeBuilder::Build()
{
    if (!NotifyService) {
        NotifyService = NNotify::CreateServiceStub();
    }

    if (!StorageConfig) {
        StorageConfig = std::make_shared<TStorageConfig>(
            CreateDefaultStorageConfig(),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );
    }

    if (!DiagnosticsConfig) {
        DiagnosticsConfig = std::make_shared<TDiagnosticsConfig>();
    }

    if (!LogbrokerService) {
        LogbrokerService = NLogbroker::CreateServiceStub();
    }

    auto runtime = std::make_unique<NActors::TTestBasicRuntime>(
        std::max(DiskAgents.size(), 1UL));

    runtime->AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    // for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
    //    runtime->SetLogPriority(i, NActors::NLog::PRI_DEBUG);
    // }
    // runtime->SetLogPriority(NActors::NLog::InvalidComponent, NActors::NLog::PRI_DEBUG);
    runtime->SetLogPriority(
        TBlockStoreComponents::DISK_REGISTRY,
        NActors::NLog::PRI_DEBUG
    );

    runtime->SetLogPriority(
        TBlockStoreComponents::DISK_REGISTRY_WORKER,
        NActors::NLog::PRI_DEBUG
    );

    int nodeIdx = 0;
    for (auto* diskAgent: DiskAgents) {
        const auto nodeId = runtime->GetNodeId(nodeIdx);

        runtime->AddLocalService(
            MakeDiskAgentServiceId(nodeId),
            NActors::TActorSetupCmd(
                diskAgent,
                NActors::TMailboxType::Simple,
                0
            ),
            nodeIdx
        );

        ++nodeIdx;
    }

    runtime->AddLocalService(
        MakeSSProxyServiceId(),
        NActors::TActorSetupCmd(
            new TSSProxyMock(),
            NActors::TMailboxType::Simple,
            0
        )
    );

    runtime->AddLocalService(
        MakeStorageServiceId(),
        NActors::TActorSetupCmd(
            new TFakeStorageService(),
            NActors::TMailboxType::Simple,
            0
        )
    );

    runtime->AddLocalService(
        MakeVolumeProxyServiceId(),
        NActors::TActorSetupCmd(
            new TFakeVolumeProxy(),
            NActors::TMailboxType::Simple,
            0
        )
    );

    NKikimr::SetupTabletServices(*runtime);

    std::unique_ptr<NKikimr::TTabletStorageInfo> tabletInfo(
        NKikimr::CreateTestTabletInfo(
            TestTabletId,
            NKikimr::TTabletTypes::BlockStoreDiskRegistry
        )
    );

    auto storageConfig = StorageConfig;
    auto diagnosticsConfig = DiagnosticsConfig;
    auto logbrokerService = LogbrokerService;
    auto notifyService = NotifyService;
    auto logging = Logging;

    auto createFunc =
        [=] (const NActors::TActorId& owner, NKikimr::TTabletStorageInfo* info) {
            auto tablet = CreateDiskRegistry(
                owner,
                logging,
                info,
                storageConfig,
                diagnosticsConfig,
                logbrokerService,
                notifyService);
            return tablet.release();
        };

    auto actorId = NKikimr::CreateTestBootstrapper(
        *runtime,
        tabletInfo.release(),
        createFunc
    );

    runtime->EnableScheduleForActor(actorId);

    return runtime;
}

TTestRuntimeBuilder& TTestRuntimeBuilder::WithAgents(
    std::initializer_list<NActors::IActor*> agents)
{
    DiskAgents.assign(agents.begin(), agents.end());
    return *this;
}

TTestRuntimeBuilder& TTestRuntimeBuilder::WithAgents(
    const TVector<NProto::TAgentConfig>& configs)
{
    DiskAgents.clear();
    for (const auto& config: configs) {
        DiskAgents.push_back(CreateTestDiskAgent(config));
    }

    return *this;
}

TTestRuntimeBuilder& TTestRuntimeBuilder::With(
    NProto::TStorageServiceConfig config)
{
    StorageConfig = std::make_shared<TStorageConfig>(
        config,
        std::make_shared<NFeatures::TFeaturesConfig>(
            NCloud::NProto::TFeaturesConfig())
    );
    return *this;
}

TTestRuntimeBuilder& TTestRuntimeBuilder::With(TStorageConfigPtr config)
{
    StorageConfig = std::move(config);
    return *this;
}

TTestRuntimeBuilder& TTestRuntimeBuilder::With(NLogbroker::IServicePtr service)
{
    LogbrokerService = std::move(service);
    return *this;
}

TTestRuntimeBuilder& TTestRuntimeBuilder::With(NNotify::IServicePtr service)
{
    NotifyService = std::move(service);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void WaitForSecureErase(
    NActors::TTestActorRuntime& runtime,
    size_t deviceCount)
{
    using NActors::TDispatchOptions;
    using NActors::TTestActorRuntimeBase;
    using NActors::IEventHandle;

    size_t cleanDevices = 0;

    TTestActorRuntimeBase::TEventObserver prev;

    prev = runtime.SetObserverFunc(
        [&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvDiskRegistryPrivate::EvSecureEraseResponse) {
                auto* msg = event->Get<TEvDiskRegistryPrivate::TEvSecureEraseResponse>();

                cleanDevices += msg->CleanDevices;
            }

            return prev(event);
        });

    runtime.AdvanceCurrentTime(TDuration::Seconds(5));

    TDispatchOptions options;
    options.CustomFinalCondition = [&] {
        return cleanDevices >= deviceCount;
    };

    runtime.DispatchEvents(options);
    runtime.SetObserverFunc(prev);
}

void WaitForSecureErase(
    NActors::TTestActorRuntime& runtime,
    TVector<NProto::TAgentConfig> agents)
{
    size_t n = 0;
    for (auto& agent: agents) {
        n += agent.DevicesSize();
    }

    if (n) {
        WaitForSecureErase(runtime, n);
    }
}

void RegisterAgent(NActors::TTestActorRuntime& runtime, int nodeIdx)
{
    auto sender = runtime.AllocateEdgeActor();
    auto nodeId = runtime.GetNodeId(nodeIdx);

    auto request = std::make_unique<TEvRegisterAgent>();

    runtime.Send(new NActors::IEventHandle(
        MakeDiskAgentServiceId(nodeId),
        sender,
        request.release()));

    runtime.DispatchEvents({}, TDuration::Seconds(1));
}

void WaitForAgent(NActors::TTestActorRuntime& runtime, int nodeIdx)
{
    auto sender = runtime.AllocateEdgeActor(nodeIdx);
    auto nodeId = runtime.GetNodeId(nodeIdx);

    auto request = std::make_unique<TEvDiskAgent::TEvWaitReadyRequest>();

    auto* ev = new NActors::IEventHandle(
        MakeDiskAgentServiceId(nodeId),
        sender,
        request.release());

    runtime.Send(ev, nodeIdx);

    TAutoPtr<NActors::IEventHandle> handle;
    runtime.GrabEdgeEventRethrow<TEvDiskAgent::TEvWaitReadyResponse>(
        handle, WaitTimeout);
    UNIT_ASSERT(handle);
}

void WaitForAgents(NActors::TTestActorRuntime& runtime, int agentsCount)
{
    for (int nodeIdx = 0; nodeIdx != agentsCount; ++nodeIdx) {
        WaitForAgent(runtime, nodeIdx);
    }
}

void RegisterAgents(NActors::TTestActorRuntime& runtime, int agentsCount)
{
    for (int nodeIdx = 0; nodeIdx != agentsCount; ++nodeIdx) {
        RegisterAgent(runtime, nodeIdx);
    }
}

void RegisterAndWaitForAgents(
    NActors::TTestActorRuntime& runtime,
    const TVector<NProto::TAgentConfig>& agents)
{
    for (size_t i = 0; i != agents.size(); ++i) {
        RegisterAndWaitForAgent(runtime, static_cast<int>(i), agents[i].DevicesSize());
    }
}

void RegisterAndWaitForAgent(
    NActors::TTestActorRuntime& runtime,
    int nodeIdx,
    size_t deviceCount)
{
    using NActors::TDispatchOptions;
    using NActors::TTestActorRuntimeBase;
    using NActors::IEventHandle;

    TTestActorRuntimeBase::TEventObserver prev;
    size_t cleanDevices = 0;

    if (deviceCount) {
        prev = runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvDiskRegistryPrivate::EvSecureEraseResponse) {
                    auto* msg = event->Get<TEvDiskRegistryPrivate::TEvSecureEraseResponse>();

                    cleanDevices += msg->CleanDevices;
                }

                return prev(event);
            });
    }

    auto sender = runtime.AllocateEdgeActor(nodeIdx);
    auto nodeId = runtime.GetNodeId(nodeIdx);

    runtime.Send(new NActors::IEventHandle(
        MakeDiskAgentServiceId(nodeId),
        sender,
        new TEvRegisterAgent()));

    runtime.DispatchEvents({}, TDuration::MilliSeconds(10));

    auto request = std::make_unique<TEvDiskAgent::TEvWaitReadyRequest>();

    auto* ev = new NActors::IEventHandle(
        MakeDiskAgentServiceId(nodeId),
        sender,
        request.release());

    runtime.Send(ev, nodeIdx);

    TAutoPtr<NActors::IEventHandle> handle;
    runtime.GrabEdgeEventRethrow<TEvDiskAgent::TEvWaitReadyResponse>(
        handle, WaitTimeout);
    UNIT_ASSERT(handle);

    if (deviceCount) {
        runtime.AdvanceCurrentTime(TDuration::Seconds(6));

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return cleanDevices >= deviceCount;
        };

        runtime.DispatchEvents(options);
        runtime.SetObserverFunc(prev);
    }
}

void KillAgent(NActors::TTestActorRuntime& runtime, int nodeIdx)
{
    using namespace NActors;

    auto sender = runtime.AllocateEdgeActor();
    auto nodeId = runtime.GetNodeId(nodeIdx);

    auto request = std::make_unique<TEvents::TEvPoisonPill>();

    runtime.Send(new IEventHandle(
        MakeDiskAgentServiceId(nodeId),
        sender,
        request.release()));
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistryTest
