#include "disk_registry_proxy.h"


#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/config/storage.pb.h>

#include <cloud/blockstore/libs/kikimr/helpers.h>

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_mock.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TDuration WaitTimeout = TDuration::Seconds(5);
const ui64 HiveId = MakeDefaultHiveID(0);
const ui64 TestDiskRegistryTabletId = MakeTabletID(0, HiveId, 1);
const ui64 DefaultOwner = 42;
const ui64 DeadOwner = 100;

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryMock final
    : public TActor<TDiskRegistryMock>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    TDiskRegistryMock(
            const TActorId& owner,
            TTabletStorageInfo* info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, owner, nullptr)
    {}

private:
    void DefaultSignalTabletActive(const TActorContext&) override
    {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext& ctx) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
    }

    void OnDetach(const TActorContext& ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(
        TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx) override
    {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void Enqueue(STFUNC_SIG) override
    {
        Y_ABORT("Unexpected event %x", ev->GetTypeRewrite());
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

            HFunc(TEvDiskRegistry::TEvAllocateDiskRequest, HandleAllocateDisk);
            HFunc(TEvDiskRegistry::TEvDeallocateDiskRequest, HandleDeallocateDisk);

            HFunc(TEvDiskRegistry::TEvAcquireDiskRequest, HandleAcquireDisk);
            HFunc(TEvDiskRegistry::TEvReleaseDiskRequest, HandleReleaseDisk);

            // IgnoreFunc(TEvents::TEvPoisonPill);
            HFunc(TEvents::TEvPoisonPill, HandlePoison);
            IgnoreFunc(TEvents::TEvWakeup);

            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    HandleUnexpectedEvent(
                        ev,
                        TBlockStoreComponents::DISK_REGISTRY_PROXY,
                        __PRETTY_FUNCTION__);
                }
        }
    }

    void HandlePoison(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
    }

    void HandleAllocateDisk(
        const TEvDiskRegistry::TEvAllocateDiskRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto response = std::make_unique<TEvDiskRegistry::TEvAllocateDiskResponse>();

        auto device = response->Record.AddDevices();
        device->SetDeviceName("test");

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleAcquireDisk(
        const TEvDiskRegistry::TEvAcquireDiskRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto response = std::make_unique<TEvDiskRegistry::TEvAcquireDiskResponse>();

        auto device = response->Record.AddDevices();
        device->SetDeviceName("test");

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleDeallocateDisk(
        const TEvDiskRegistry::TEvDeallocateDiskRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        NCloud::Reply(ctx, *ev,
            std::make_unique<TEvDiskRegistry::TEvDeallocateDiskResponse>());
    }

    void HandleReleaseDisk(
        const TEvDiskRegistry::TEvReleaseDiskRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        NCloud::Reply(ctx, *ev,
            std::make_unique<TEvDiskRegistry::TEvReleaseDiskResponse>());
    }

}; // TDiskRegistryMock

////////////////////////////////////////////////////////////////////////////////

struct THiveProxyMockConfig
{
    int RetryCount = 0;
};

class THiveProxyMock final
    : public TActor<THiveProxyMock>
{
private:
    const THiveProxyMockConfig Config;

    int RequestCount = 0;

public:
    explicit THiveProxyMock(THiveProxyMockConfig config)
        : TActor(&TThis::StateWork)
        , Config(std::move(config))
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHiveProxy::TEvLookupTabletRequest, HandleLookupTablet);
            HFunc(TEvHiveProxy::TEvCreateTabletRequest, HandleCreateTablet);
        }
    }

    void HandleLookupTablet(
        const TEvHiveProxy::TEvLookupTabletRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        ++RequestCount;

        if (Config.RetryCount > RequestCount) {
            return;
        }

        if (msg->Owner == DeadOwner) {
            auto response = std::make_unique<TEvHiveProxy::TEvLookupTabletResponse>(
                MakeError(E_FAIL));

            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }

        auto response = std::make_unique<TEvHiveProxy::TEvLookupTabletResponse>(
            TestDiskRegistryTabletId);

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleCreateTablet(
        const TEvHiveProxy::TEvCreateTabletRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto response = std::make_unique<TEvHiveProxy::TEvCreateTabletResponse>(
            TestDiskRegistryTabletId);

        NCloud::Reply(ctx, *ev, std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

void BootDiskRegistryMock(TTestActorRuntime& runtime)
{
    std::unique_ptr<TTabletStorageInfo> tabletInfo(CreateTestTabletInfo(
            TestDiskRegistryTabletId,
            TTabletTypes::BlockStoreDiskRegistry));

    auto createFunc =
        [=] (const TActorId& owner, TTabletStorageInfo* info) {
            return new TDiskRegistryMock(owner, info);
        };

    auto actorId = CreateTestBootstrapper(runtime, tabletInfo.release(), createFunc);
    runtime.EnableScheduleForActor(actorId);

    {
        TDispatchOptions options;
        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1)};
        runtime.DispatchEvents(options);
    }
}

void RebootDiskRegistryMock(TTestActorRuntime& runtime)
{
    auto sender = runtime.AllocateEdgeActor(0);

    TVector<ui64> tablets = { TestDiskRegistryTabletId };
    auto guard = CreateTabletScheduledEventsGuard(
        tablets,
        runtime,
        sender);

    RebootTablet(runtime, TestDiskRegistryTabletId, sender);
}

struct TTestRuntimeBuilder
{
    THiveProxyMockConfig HiveProxyMockConfig;
    ui64 OwnerId = DefaultOwner;
    NProto::TStorageServiceConfig StorageConfig;
    TVector<TSSProxyMock::TPoolDescr> Pools;
    TTestActorRuntimeBase::TEventObserver StartObserver;

    TTestRuntimeBuilder& With(THiveProxyMockConfig config)
    {
        HiveProxyMockConfig = std::move(config);
        return *this;
    }

    TTestRuntimeBuilder& WithOwnerId(ui64 ownerId)
    {
        OwnerId = ownerId;
        return *this;
    }

    TTestRuntimeBuilder& WithPoolKind(TString kind)
    {
        StorageConfig.SetSSDSystemChannelPoolKind(kind);
        StorageConfig.SetSSDLogChannelPoolKind(kind);
        StorageConfig.SetSSDIndexChannelPoolKind(kind);
        return *this;
    }

    TTestRuntimeBuilder& WithPoolKinds(
        TString sysKind,
        TString logKind,
        TString indexKind)
    {
        StorageConfig.SetSSDSystemChannelPoolKind(sysKind);
        StorageConfig.SetSSDLogChannelPoolKind(logKind);
        StorageConfig.SetSSDIndexChannelPoolKind(indexKind);
        return *this;
    }

    TTestRuntimeBuilder& WithStoragePools(TVector<TSSProxyMock::TPoolDescr> pools)
    {
        Pools = std::move(pools);
        return *this;
    }

    TTestRuntimeBuilder& WithObserver(TTestActorRuntimeBase::TEventObserver observer)
    {
        StartObserver = observer;
        return *this;
    }

    std::unique_ptr<TTestActorRuntime> Build(bool initializeDRProxy = true)
    {
        auto runtime = std::make_unique<TTestBasicRuntime>(1);

        runtime->AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        runtime->SetLogPriority(TBlockStoreComponents::DISK_REGISTRY_PROXY, NLog::PRI_DEBUG);
        runtime->SetLogPriority(TBlockStoreComponents::SS_PROXY, NLog::PRI_DEBUG);

        // for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
        //    runtime->SetLogPriority(i, NLog::PRI_DEBUG);
        // }
        // runtime->SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);

        runtime->SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        if (StartObserver) {
            runtime->SetObserverFunc(StartObserver);
        }

        runtime->AddLocalService(
            MakeHiveProxyServiceId(),
            TActorSetupCmd(
                new THiveProxyMock(HiveProxyMockConfig),
                TMailboxType::Simple,
                0));

        if (initializeDRProxy) {
            InitializeDiskRegistryProxy(*runtime, {});
        }

        return runtime;
    }

    void InitializeDiskRegistryProxy(
        TTestActorRuntime& runtime,
        NProto::TDiskRegistryProxyConfig proxyConfigProto)
    {
        auto storageConfig = std::make_shared<TStorageConfig>(
            StorageConfig,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig()));

        proxyConfigProto.SetOwner(OwnerId);

        auto proxyConfig = std::make_shared<TDiskRegistryProxyConfig>(
            std::move(proxyConfigProto));

        auto drp = CreateDiskRegistryProxy(storageConfig, proxyConfig);

        runtime.AddLocalService(
            MakeDiskRegistryProxyServiceId(),
            TActorSetupCmd(drp.release(), TMailboxType::Simple, 0));

        runtime.AddLocalService(
            MakeSSProxyServiceId(),
            TActorSetupCmd(new TSSProxyMock(Pools), TMailboxType::Simple, 0));

        SetupTabletServices(runtime);

        if (OwnerId) {
            BootDiskRegistryMock(runtime);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryClient
{
private:
    TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    TActorId Sender;

public:
    TDiskRegistryClient(TTestActorRuntime& runtime, ui32 nodeIdx = 0)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , Sender(runtime.AllocateEdgeActor(nodeIdx))
    {}

    template <typename TRequest>
    void SendRequest(
        const TActorId& recipient,
        std::unique_ptr<TRequest> request)
    {
        auto* ev = new IEventHandle(
            recipient,
            Sender,
            request.release());

        Runtime.Send(ev, NodeIdx);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    auto CreateAllocateDiskRequest(const TString& diskId, ui64 diskSize)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvAllocateDiskRequest>();

        request->Record.SetDiskId(diskId);
        request->Record.SetBlockSize(4_KB);
        request->Record.SetBlocksCount(diskSize / 4_KB);

        return request;
    }

    auto CreateDeallocateDiskRequest(
        const TString& diskId,
        bool sync = false)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvDeallocateDiskRequest>();

        request->Record.SetDiskId(diskId);
        request->Record.SetSync(sync);

        return request;
    }

    auto CreateAcquireDiskRequest(const TString& diskId, const TString& clientId)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvAcquireDiskRequest>();

        request->Record.SetDiskId(diskId);
        request->Record.MutableHeaders()->SetClientId(clientId);

        return request;
    }

    auto CreateReleaseDiskRequest(const TString& diskId, const TString& clientId)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvReleaseDiskRequest>();

        request->Record.SetDiskId(diskId);
        request->Record.MutableHeaders()->SetClientId(clientId);

        return request;
    }

    auto CreateSubscribeRequest(const TActorId& subscriber)
    {
        auto request = std::make_unique<TEvDiskRegistryProxy::TEvSubscribeRequest>(
            subscriber);

        return request;
    }

    auto CreateUnsubscribeRequest(const TActorId& subscriber)
    {
        auto request = std::make_unique<TEvDiskRegistryProxy::TEvUnsubscribeRequest>(
            subscriber);

        return request;
    }

    auto CreateReassignRequest(TString sysKind, TString logKind, TString indexKind)
    {
        auto request = std::make_unique<TEvDiskRegistryProxy::TEvReassignRequest>(
            std::move(sysKind),
            std::move(logKind),
            std::move(indexKind)
        );

        return request;
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(MakeDiskRegistryProxyServiceId(), std::move(request));     \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()            \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>();                        \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(MakeDiskRegistryProxyServiceId(), std::move(request));     \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_DISK_REGISTRY_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvDiskRegistry)
    BLOCKSTORE_DISK_REGISTRY_PROXY_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvDiskRegistryProxy)

#undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryProxyTest)
{
    Y_UNIT_TEST(ShouldAllocateDisk)
    {
        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient client(*runtime);

        auto response = client.AllocateDisk("disk-id", 10_GB);

        UNIT_ASSERT_VALUES_EQUAL(1, response->Record.DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL("test", response->Record.GetDevices(0).GetDeviceName());

        client.DeallocateDisk("disk-id", true);
    }

    Y_UNIT_TEST(ShouldAcquireDisk)
    {
        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient client(*runtime);

        auto response = client.AcquireDisk("disk-id", "session-id");

        UNIT_ASSERT_VALUES_EQUAL(1, response->Record.DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL("test", response->Record.GetDevices(0).GetDeviceName());

        client.ReleaseDisk("disk-id", "session-id");
    }

    Y_UNIT_TEST(ShouldRestoreConnection)
    {
        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient client(*runtime);

        client.AllocateDisk("disk-1", 10_GB);

        RebootDiskRegistryMock(*runtime);

        client.DeallocateDisk("disk-1", true);
        client.AllocateDisk("disk-2", 10_GB);
    }

    Y_UNIT_TEST(ShouldRespondWithoutOwner)
    {
        auto runtime = TTestRuntimeBuilder()
            .WithOwnerId(0)
            .Build();

        TDiskRegistryClient client(*runtime);

        client.SendAllocateDiskRequest("disk-1", 10_GB);
        auto response = client.RecvAllocateDiskResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldHandleCreateTabletError)
    {
        auto runtime = TTestRuntimeBuilder()
            .WithOwnerId(DeadOwner)
            .Build();

        TDiskRegistryClient client(*runtime);

        client.SendAllocateDiskRequest("disk-1", 10_GB);
        auto response = client.RecvAllocateDiskResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldNotCreateDrIfNoSuitableStoragePool)
    {
        auto runtime = TTestRuntimeBuilder()
            .WithPoolKind("ssd")
            .WithOwnerId(DeadOwner)
            .WithStoragePools({{"hdd-pool", "hdd"}})
            .Build();

        TDiskRegistryClient client(*runtime);

        client.SendAllocateDiskRequest("disk-1", 10_GB);
        auto response = client.RecvAllocateDiskResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldHandleConnectionLost)
    {
        auto builder = TTestRuntimeBuilder();
        auto runtime = builder.Build(false);

        int lookupSent = 0;
        std::unique_ptr<IEventHandle> lookupResponseEvent;
        auto baseFilter = runtime->SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvLookupTabletRequest:
                        lookupSent++;
                        break;
                    case TEvHiveProxy::EvLookupTabletResponse:
                        UNIT_ASSERT_VALUES_EQUAL(1, lookupSent);
                        lookupResponseEvent.reset(event.Release());
                        return true;
                    default:
                        break;
                }
                return false;
            });

        UNIT_ASSERT(!lookupResponseEvent);
        builder.InitializeDiskRegistryProxy(*runtime, {});
        // TEvHiveProxy::EvLookupTabletRequest should be sent on the start of
        // the actor.
        UNIT_ASSERT_VALUES_EQUAL(1, lookupSent);
        UNIT_ASSERT(lookupResponseEvent);

        TDiskRegistryClient client(*runtime);
        auto subscriber = runtime->AllocateEdgeActor();

        {
            auto response = client.Subscribe(subscriber);
            // DR is not discovered until TEvHiveProxy::TEvLookupTabletResponse
            // is not received.
            UNIT_ASSERT(!response->Discovered);
        }

        runtime->SetEventFilter(baseFilter);
        runtime->SendAsync(lookupResponseEvent.release());
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = client.Subscribe(subscriber);
            UNIT_ASSERT(response->Discovered);
        }

        // Allocate disk to connect DRProxy with DR.
        client.AllocateDisk("disk-1", 10_GB);

        RebootDiskRegistryMock(*runtime);

        using TNotification = TEvDiskRegistryProxy::TEvConnectionLost;
        TAutoPtr<IEventHandle> handle;
        runtime->GrabEdgeEventRethrow<TNotification>(handle, WaitTimeout);

        UNIT_ASSERT(handle);
        std::unique_ptr<TNotification> notification(
            handle->Release<TNotification>().Release());

        UNIT_ASSERT(SUCCEEDED(notification->GetStatus()));

        client.Unsubscribe(subscriber);
    }

    Y_UNIT_TEST(ShouldHandleLookupTimeout)
    {
        THiveProxyMockConfig config;
        config.RetryCount = 5;

        auto runtime = TTestRuntimeBuilder()
            .With(config)
            .Build();

        int lookupCount = 1;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvHiveProxy::EvLookupTabletRequest) {
                    ++lookupCount;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TDiskRegistryClient client(*runtime);

        {
            client.SendAllocateDiskRequest("disk-1", 10_GB);
            auto response = client.RecvAllocateDiskResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        for (int i = 0; i != config.RetryCount - 1; ++i) {
            runtime->AdvanceCurrentTime(TDuration::Seconds(60));
            runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        }

        UNIT_ASSERT_VALUES_EQUAL(config.RetryCount, lookupCount);

        client.AllocateDisk("disk-1", 10_GB);
        runtime->AdvanceCurrentTime(TDuration::Seconds(60));
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ShouldHandleExpiredLookup)
    {
        THiveProxyMockConfig config;
        config.RetryCount = 2;

        auto runtime = TTestRuntimeBuilder()
            .With(config)
            .Build();

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvHiveProxy::EvLookupTabletRequest) {
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TDiskRegistryClient client(*runtime);

        {
            client.SendAllocateDiskRequest("disk-1", 10_GB);
            auto response = client.RecvAllocateDiskResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        // wait for 1st timeout
        runtime->AdvanceCurrentTime(TDuration::Seconds(60));
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // wait for 2nd timeout
        runtime->AdvanceCurrentTime(TDuration::Seconds(60));
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // send expired response
        runtime->AdvanceCurrentTime(TDuration::Seconds(30));
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto sender = runtime->AllocateEdgeActor();

        runtime->Send(new IEventHandle(
            MakeDiskRegistryProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvLookupTabletResponse(MakeError(E_FAIL))),
            0,
            true);

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        // wait for 3th timeout
        runtime->AdvanceCurrentTime(TDuration::Seconds(30));
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        client.AllocateDisk("disk-1", 10_GB);
        runtime->AdvanceCurrentTime(TDuration::Seconds(60));
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ShouldCancelActiveRequests)
    {
        auto runtime = TTestRuntimeBuilder().Build();

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvDiskRegistry::EvAllocateDiskResponse) {
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TDiskRegistryClient client(*runtime);

        client.SendAllocateDiskRequest("disk-id", 10_GB);

        // wait for allocate request
        {
            TDispatchOptions options;

            options.FinalEvents = {
                TDispatchOptions::TFinalEventCondition(
                    TEvDiskRegistry::EvAllocateDiskRequest)
            };

            runtime->DispatchEvents(options, TDuration::Seconds(1));
        }

        RebootDiskRegistryMock(*runtime);

        auto response = client.RecvAllocateDiskResponse();

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            "DiskRegistry tablet not available",
            response->GetErrorReason());

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        client.AllocateDisk("disk-id", 10_GB);
    }

    Y_UNIT_TEST(ShouldCreateDRWithProperStoragePool)
    {
        TVector<TString> bindings;

        TTestActorRuntimeBase::TEventObserver ob = [&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvHiveProxy::EvCreateTabletRequest) {
                const auto* msg = event->Get<TEvHiveProxy::TEvCreateTabletRequest>();
                for (const auto& ch: msg->Request.GetBindedChannels()) {
                    bindings.push_back(ch.GetStoragePoolName());
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        auto runtime = TTestRuntimeBuilder()
            .WithPoolKind("ssd")
            .WithOwnerId(DeadOwner)
            .WithStoragePools({{"hdd-pool", "hdd"}, {"ssd-pool", "ssd"}})
            .WithObserver(ob)
            .Build();

        TDiskRegistryClient client(*runtime);

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            std::all_of(bindings.begin(), bindings.end(), [&] (const auto& s) {
                return s == "ssd-pool";
            }));
    }

    Y_UNIT_TEST(ShouldReassignDRToDesiredKind)
    {
        TVector<TString> bindings;

        TTestActorRuntimeBase::TEventObserver ob = [&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvHiveProxy::EvCreateTabletRequest) {
                const auto* msg = event->Get<TEvHiveProxy::TEvCreateTabletRequest>();
                for (const auto& ch: msg->Request.GetBindedChannels()) {
                    bindings.push_back(ch.GetStoragePoolName());
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        auto runtime = TTestRuntimeBuilder()
            .WithPoolKind("ssd")
            .WithOwnerId(DeadOwner)
            .WithStoragePools({{"hdd-pool", "hdd"}, {"ssd-pool", "ssd"}})
            .WithObserver(ob)
            .Build();

        TDiskRegistryClient client(*runtime);

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            std::all_of(bindings.begin(), bindings.end(), [&] (const auto& s) {
                return s == "ssd-pool";
            }));

        bindings.clear();

        client.Reassign("hdd", "hdd", "hdd");

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            std::all_of(bindings.begin(), bindings.end(), [&] (const auto& s) {
                return s == "hdd-pool";
            }));
    }

    Y_UNIT_TEST(ShouldFailReassignIfNoPoolForKind)
    {
        TVector<TString> bindings;

        TTestActorRuntimeBase::TEventObserver ob = [&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvHiveProxy::EvCreateTabletRequest) {
                const auto* msg = event->Get<TEvHiveProxy::TEvCreateTabletRequest>();
                for (const auto& ch: msg->Request.GetBindedChannels()) {
                    bindings.push_back(ch.GetStoragePoolName());
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        auto runtime = TTestRuntimeBuilder()
            .WithPoolKind("ssd")
            .WithOwnerId(DeadOwner)
            .WithStoragePools({{"hdd-pool", "hdd"}, {"ssd-pool", "ssd"}})
            .WithObserver(ob)
            .Build();

        TDiskRegistryClient client(*runtime);

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            std::all_of(bindings.begin(), bindings.end(), [&] (const auto& s) {
                return s == "ssd-pool";
            }));

        bindings.clear();

        client.SendReassignRequest("nrd", "ssd", "hdd");
        auto response = client.RecvReassignResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldChooseDefaultChannelsForReassignIfAbsent)
    {
        TVector<TString> bindings;

        TTestActorRuntimeBase::TEventObserver ob = [&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvHiveProxy::EvCreateTabletRequest) {
                const auto* msg = event->Get<TEvHiveProxy::TEvCreateTabletRequest>();
                for (const auto& ch: msg->Request.GetBindedChannels()) {
                    bindings.push_back(ch.GetStoragePoolName());
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        auto runtime = TTestRuntimeBuilder()
            .WithPoolKind("ssd")
            .WithOwnerId(DeadOwner)
            .WithStoragePools({{"hdd-pool", "hdd"}, {"ssd-pool", "ssd"}})
            .WithObserver(ob)
            .Build();

        TDiskRegistryClient client(*runtime);

        for (const auto& ch: bindings) {
            UNIT_ASSERT_VALUES_EQUAL("ssd-pool", ch);
        }

        bindings.clear();

        client.Reassign("hdd", "", "hdd");

        UNIT_ASSERT_VALUES_EQUAL("hdd-pool", bindings[0]);
        UNIT_ASSERT_VALUES_EQUAL("ssd-pool", bindings[1]);
        UNIT_ASSERT_VALUES_EQUAL("hdd-pool", bindings[2]);
    }

    Y_UNIT_TEST(ShouldNotStartReassignIfAnotherReassignIsInProgress)
    {
        auto runtime = TTestRuntimeBuilder()
            .WithPoolKind("ssd")
            .WithOwnerId(DefaultOwner)
            .WithStoragePools({{"ssd-pool", "ssd"}})
            .Build();

        TDiskRegistryClient client(*runtime);

        client.SendReassignRequest("hdd", "hdd", "hdd");

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvHiveProxy::EvCreateTabletRequest) {
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        client.SendReassignRequest("hdd", "hdd", "hdd");
        auto response = client.RecvReassignResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldHandleDiskRegistryTabletIdInConfig)
    {
        auto builder = TTestRuntimeBuilder();
        auto runtime = builder.Build(false);

        auto baseFilter = runtime->SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvLookupTabletRequest:
                        UNIT_ASSERT_C(
                            false,
                            "Disk Registry tablet id should be received from "
                            "config");
                        break;
                }
                return false;
            });

        NProto::TDiskRegistryProxyConfig proxyConfigProto;
        proxyConfigProto.SetDiskRegistryTabletId(TestDiskRegistryTabletId);
        builder.InitializeDiskRegistryProxy(
            *runtime,
            std::move(proxyConfigProto));

        TDiskRegistryClient client(*runtime);
        auto subscriber = runtime->AllocateEdgeActor();

        {
            auto response = client.Subscribe(subscriber);
            UNIT_ASSERT(response->Discovered);
        }

        client.AllocateDisk("disk-1", 10_GB);
        client.Unsubscribe(subscriber);
    }

    Y_UNIT_TEST(ShouldHandleRejectFromHive)
    {
        auto builder = TTestRuntimeBuilder();
        auto runtime = builder.Build(false);

        bool successfulLookup = false;
        auto baseFilter = runtime->SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvLookupTabletRequest: {
                        static int RequestCount = 0;
                        if (++RequestCount > 2) {
                            successfulLookup = true;
                            return false;
                        }

                        auto response = std::make_unique<
                            TEvHiveProxy::TEvLookupTabletResponse>(
                            MakeError(E_REJECTED));
                        runtime.Send(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0,   // flags
                            event->Cookie));
                        return true;
                    }
                }
                return false;
            });

        NProto::TDiskRegistryProxyConfig proxyConfigProto;
        proxyConfigProto.SetRetryLookupTimeout(50);   // 50 ms.
        builder.InitializeDiskRegistryProxy(
            *runtime,
            std::move(proxyConfigProto));

        while (!successfulLookup) {
            runtime->AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        }

        TDiskRegistryClient client(*runtime);
        auto subscriber = runtime->AllocateEdgeActor();

        {
            auto response = client.Subscribe(subscriber);
            UNIT_ASSERT(response->Discovered);
        }

        client.AllocateDisk("disk-1", 10_GB);
        client.Unsubscribe(subscriber);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
