#include "dr_proxy.h"

#include <cloud/filestore/libs/storage/dr_proxy/api/service.h>
#include <cloud/filestore/libs/storage/testlib/helpers.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/volume.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/base/tabletid.h>
#include <contrib/ydb/core/tablet_flat/tablet_flat_executed.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>
#include <contrib/ydb/library/actors/core/event_pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/string/join.h>

#include <functional>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

using TEvDiskRegistry = NBlockStore::NStorage::TEvDiskRegistry;
using TEvHiveProxy = NCloud::NStorage::TEvHiveProxy;
using NCloud::NStorage::MakeHiveProxyServiceId;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DiskAgentPort = 9042;

struct TFakeDiskRegistryState
{
    NProto::TError Error;
    NBlockStore::NProto::TAllocateDiskResponse AllocateResponse;
    NBlockStore::NProto::TDescribeDiskResponse DescribeResponse;

    NBlockStore::NProto::TAllocateDiskRequest Allocate;
    NBlockStore::NProto::TDescribeDiskRequest Describe;
    NBlockStore::NProto::TMarkDiskForCleanupRequest MarkForCleanup;
    NBlockStore::NProto::TDeallocateDiskRequest Deallocate;
    NBlockStore::NProto::TMarkReplacementDeviceRequest MarkReplacement;
    NBlockStore::NProto::TFinishMigrationRequest FinishMigration;
    NBlockStore::NProto::TReplaceDeviceRequest ReplaceDevice;
};

class TFakeDiskRegistry final
    : public TActor<TFakeDiskRegistry>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
private:
    const std::shared_ptr<TFakeDiskRegistryState> State;

public:
    TFakeDiskRegistry(
            const TActorId& owner,
            TTabletStorageInfo* info,
            std::shared_ptr<TFakeDiskRegistryState> state)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, owner, nullptr)
        , State(std::move(state))
    {}

private:
    void DefaultSignalTabletActive(const TActorContext&) override
    {}

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
        Y_ABORT("unexpected event %u", ev->GetTypeRewrite());
    }

    template <typename TResponse, typename TRequestPtr>
    void Reply(const TActorContext& ctx, const TRequestPtr& ev)
    {
        NCloud::Reply(ctx, *ev, std::make_unique<TResponse>(State->Error));
    }

    void HandleAllocateDisk(
        const TEvDiskRegistry::TEvAllocateDiskRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        State->Allocate = ev->Get()->Record;
        auto response =
            std::make_unique<TEvDiskRegistry::TEvAllocateDiskResponse>(
                State->AllocateResponse);
        *response->Record.MutableError() = State->Error;
        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleDescribeDisk(
        const TEvDiskRegistry::TEvDescribeDiskRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        State->Describe = ev->Get()->Record;
        auto response =
            std::make_unique<TEvDiskRegistry::TEvDescribeDiskResponse>(
                State->DescribeResponse);
        *response->Record.MutableError() = State->Error;
        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleMarkDiskForCleanup(
        const TEvDiskRegistry::TEvMarkDiskForCleanupRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        State->MarkForCleanup = ev->Get()->Record;
        Reply<TEvDiskRegistry::TEvMarkDiskForCleanupResponse>(ctx, ev);
    }

    void HandleDeallocateDisk(
        const TEvDiskRegistry::TEvDeallocateDiskRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        State->Deallocate = ev->Get()->Record;
        Reply<TEvDiskRegistry::TEvDeallocateDiskResponse>(ctx, ev);
    }

    void HandleMarkReplacementDevice(
        const TEvDiskRegistry::TEvMarkReplacementDeviceRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        State->MarkReplacement = ev->Get()->Record;
        Reply<TEvDiskRegistry::TEvMarkReplacementDeviceResponse>(ctx, ev);
    }

    void HandleFinishMigration(
        const TEvDiskRegistry::TEvFinishMigrationRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        State->FinishMigration = ev->Get()->Record;
        Reply<TEvDiskRegistry::TEvFinishMigrationResponse>(ctx, ev);
    }

    void HandleReplaceDevice(
        const TEvDiskRegistry::TEvReplaceDeviceRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        State->ReplaceDevice = ev->Get()->Record;
        Reply<TEvDiskRegistry::TEvReplaceDeviceResponse>(ctx, ev);
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
            HFunc(TEvDiskRegistry::TEvDescribeDiskRequest, HandleDescribeDisk);
            HFunc(
                TEvDiskRegistry::TEvMarkDiskForCleanupRequest,
                HandleMarkDiskForCleanup);
            HFunc(
                TEvDiskRegistry::TEvDeallocateDiskRequest,
                HandleDeallocateDisk);
            HFunc(
                TEvDiskRegistry::TEvMarkReplacementDeviceRequest,
                HandleMarkReplacementDevice);
            HFunc(
                TEvDiskRegistry::TEvFinishMigrationRequest,
                HandleFinishMigration);
            HFunc(
                TEvDiskRegistry::TEvReplaceDeviceRequest,
                HandleReplaceDevice);

            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    Y_ABORT("unexpected event %u", ev->GetTypeRewrite());
                }
        }
    }
};

ui64 BootFakeDiskRegistry(
    TTestEnv& env,
    ui32 nodeIdx,
    std::shared_ptr<TFakeDiskRegistryState> state)
{
    auto& runtime = env.GetRuntime();
    const ui64 tabletId = MakeTabletID(false /* fromHive */, 1);

    auto bootstrapper = CreateTestBootstrapper(
        runtime,
        CreateTestTabletInfo(tabletId, TTabletTypes::BlockStoreDiskRegistry),
        [state = std::move(state)] (
            const TActorId& owner,
            TTabletStorageInfo* info)
        {
            return new TFakeDiskRegistry(owner, info, state);
        },
        nodeIdx);
    runtime.EnableScheduleForActor(bootstrapper);
    runtime.DispatchEvents(TDispatchOptions{
        .FinalEvents = {
            TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1)}});

    return tabletId;
}

////////////////////////////////////////////////////////////////////////////////

struct TFakeHiveProxyState
{
    NProto::TError Error;
    ui64 TabletId = 0;
    bool Hang = false;

    ui32 Lookups = 0;
    ui64 HiveId = 0;
    ui64 Owner = 0;
    ui64 OwnerIdx = 0;
};

class TFakeHiveProxy final
    : public TActor<TFakeHiveProxy>
{
private:
    const std::shared_ptr<TFakeHiveProxyState> State;

public:
    explicit TFakeHiveProxy(std::shared_ptr<TFakeHiveProxyState> state)
        : TActor(&TThis::StateWork)
        , State(std::move(state))
    {}

private:
    void HandleLookupTablet(
        const TEvHiveProxy::TEvLookupTabletRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        ++State->Lookups;
        State->HiveId = msg->HiveId;
        State->Owner = msg->Owner;
        State->OwnerIdx = msg->OwnerIdx;

        if (State->Hang) {
            return;
        }

        using TResponse = TEvHiveProxy::TEvLookupTabletResponse;
        NCloud::Reply(
            ctx,
            *ev,
            HasError(State->Error)
                ? std::make_unique<TResponse>(State->Error)
                : std::make_unique<TResponse>(State->TabletId));
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHiveProxy::TEvLookupTabletRequest, HandleLookupTablet);

            default:
                Y_ABORT("unexpected event %u", ev->GetTypeRewrite());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TStorageConfig DefaultConfig()
{
    NProto::TStorageConfig config;
    // Give up on a dead tablet fast.
    config.SetPipeClientRetryCount(1);
    config.SetPipeClientMinRetryTime(10);   // ms
    config.SetPipeClientMaxRetryTime(10);   // ms
    return config;
}

struct TDRProxyEnv
{
    TTestEnv Env;
    std::shared_ptr<TFakeDiskRegistryState> DiskRegistry =
        std::make_shared<TFakeDiskRegistryState>();
    std::shared_ptr<TFakeHiveProxyState> HiveProxy =
        std::make_shared<TFakeHiveProxyState>();
    TStorageConfigPtr Config;
    ui32 NodeIdx = 0;
    ui64 TabletId = 0;
    TActorId Sender;

    explicit TDRProxyEnv(
        bool withDiskRegistry = true,
        NProto::TStorageConfig config = DefaultConfig())
    {
        NodeIdx = Env.AddDynamicNode();
        auto& runtime = Env.GetRuntime();
        Sender = runtime.AllocateEdgeActor(NodeIdx);

        if (withDiskRegistry) {
            TabletId = BootFakeDiskRegistry(Env, NodeIdx, DiskRegistry);
            if (config.GetFastShardDROwner()) {
                HiveProxy->TabletId = TabletId;
                Register(
                    MakeHiveProxyServiceId(),
                    std::make_unique<TFakeHiveProxy>(HiveProxy));
            } else {
                config.SetFastShardDRTabletId(TabletId);
            }
        }

        Config = CreateTestStorageConfig(std::move(config));
        Register(MakeFileStoreDeviceRegistryProxyId(), CreateDRProxy(Config));
    }

    void Register(const TActorId& serviceId, IActorPtr actor)
    {
        auto& runtime = Env.GetRuntime();
        const auto actorId = runtime.Register(actor.release(), NodeIdx);
        runtime.EnableScheduleForActor(actorId);
        runtime.RegisterService(serviceId, actorId, NodeIdx);
    }

    template <typename TRequest>
    void Send(std::unique_ptr<TRequest> request)
    {
        Env.GetRuntime().Send(
            new IEventHandle(
                MakeFileStoreDeviceRegistryProxyId(),
                Sender,
                request.release()),
            NodeIdx);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> Recv()
    {
        TAutoPtr<IEventHandle> handle;
        Env.GetRuntime().GrabEdgeEventRethrow<TResponse>(handle);
        return std::unique_ptr<TResponse>(
            handle->Release<TResponse>().Release());
    }

    template <typename TResponse, typename TRequest>
    std::unique_ptr<TResponse> Execute(std::unique_ptr<TRequest> request)
    {
        Send(std::move(request));
        return Recv<TResponse>();
    }

    void WaitFor(std::function<bool()> condition)
    {
        Env.GetRuntime().DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = std::move(condition)});
    }
};

NBlockStore::NProto::TDeviceConfig Device(
    const TString& uuid,
    const TString& host)
{
    NBlockStore::NProto::TDeviceConfig device;
    device.SetDeviceUUID(uuid);
    device.MutableJournalledEndpoint()->SetHost(host);
    device.MutableJournalledEndpoint()->SetPort(DiskAgentPort);
    return device;
}

template <typename TRecord>
void FillLayout(TRecord& record)
{
    *record.AddDevices() = Device("uuid-1", "agent-1");
    *record.AddReplicas()->AddDevices() = Device("uuid-2", "agent-2");
    *record.AddReplicas()->AddDevices() = Device("uuid-3", "agent-3");

    auto* migration = record.AddMigrations();
    migration->SetSourceDeviceId("uuid-2");
    *migration->MutableTargetDevice() = Device("uuid-4", "agent-4");

    record.AddDeviceReplacementUUIDs("uuid-3");
    if constexpr (requires { record.AddUnavailableDeviceUUIDs("uuid-2"); }) {
        record.AddUnavailableDeviceUUIDs("uuid-2");
    }
}

void CheckLayout(
    const TEvDeviceService::TDeviceLayout& layout,
    const TVector<TString>& unavailable)
{
    UNIT_ASSERT_VALUES_EQUAL(3, layout.Replicas.size());
    for (ui32 i = 0; i < 3; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(1, layout.Replicas[i].size());
        const auto& device = layout.Replicas[i][0];
        UNIT_ASSERT_VALUES_EQUAL("uuid-" + ToString(i + 1), device.GetDeviceId());
        UNIT_ASSERT_VALUES_EQUAL("agent-" + ToString(i + 1), device.GetHost());
        UNIT_ASSERT_VALUES_EQUAL(DiskAgentPort, device.GetPort());
    }

    UNIT_ASSERT_VALUES_EQUAL(1, layout.Migrations.size());
    UNIT_ASSERT_VALUES_EQUAL("uuid-2", layout.Migrations[0].SourceUUID);
    UNIT_ASSERT_VALUES_EQUAL("uuid-4", layout.Migrations[0].Target.GetDeviceId());
    UNIT_ASSERT_VALUES_EQUAL("agent-4", layout.Migrations[0].Target.GetHost());
    UNIT_ASSERT_VALUES_EQUAL(DiskAgentPort, layout.Migrations[0].Target.GetPort());

    UNIT_ASSERT_VALUES_EQUAL(1, layout.ReplacementDeviceUUIDs.size());
    UNIT_ASSERT_VALUES_EQUAL("uuid-3", layout.ReplacementDeviceUUIDs[0]);
    UNIT_ASSERT_VALUES_EQUAL(
        JoinSeq(",", unavailable),
        JoinSeq(",", layout.UnavailableDeviceUUIDs));
}

void CheckWireFormat(
    const google::protobuf::Descriptor* blockstore,
    const google::protobuf::Descriptor* filestore)
{
    for (int i = 0; i < filestore->field_count(); ++i) {
        const auto* f = filestore->field(i);
        const auto* b = blockstore->FindFieldByNumber(f->number());
        UNIT_ASSERT_C(b, f->full_name());
        UNIT_ASSERT_VALUES_EQUAL_C(int(b->type()), int(f->type()), f->full_name());
        UNIT_ASSERT_VALUES_EQUAL_C(
            b->is_repeated(),
            f->is_repeated(),
            f->full_name());
        if (f->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
            CheckWireFormat(b->message_type(), f->message_type());
        }
    }
}

TString SerializeEvent(const IEventBase& event)
{
    TAllocChunkSerializer serializer;
    UNIT_ASSERT(event.SerializeToArcadiaStream(&serializer));
    return serializer.Release(event.CreateSerializationInfo())->GetString();
}

template <typename TEvent>
std::unique_ptr<TEvent> LoadEvent(const TString& bytes)
{
    TIntrusivePtr<TEventSerializedData> data =
        new TEventSerializedData(bytes, {});
    return std::unique_ptr<TEvent>(TEvent::Load(data.Get()));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDRProxyTest)
{
    using TEvAllocate = TEvDeviceService::TEvAllocateDevicesRequest;
    using TEvAllocateResponse = TEvDeviceService::TEvAllocateDevicesResponse;
    using TEvDescribe = TEvDeviceService::TEvDescribeDevicesRequest;
    using TEvDescribeResponse = TEvDeviceService::TEvDescribeDevicesResponse;
    using TEvMark = TEvDeviceService::TEvMarkForCleanupRequest;
    using TEvMarkResponse = TEvDeviceService::TEvMarkForCleanupResponse;

    Y_UNIT_TEST(ShouldAllocateDevices)
    {
        TDRProxyEnv env;
        FillLayout(env.DiskRegistry->AllocateResponse);

        auto response = env.Execute<TEvAllocateResponse>(
            std::make_unique<TEvAllocate>("fs", 42, "cloud", "folder", 3, 100, "fastshard"));
        UNIT_ASSERT_C(!HasError(response->GetError()), response->GetError());
        CheckLayout(response->Layout, {"uuid-2"});

        const auto& request = env.DiskRegistry->Allocate;
        UNIT_ASSERT_VALUES_EQUAL("fs", request.GetDiskId());
        // TODO(issue-7373): restore when DR supports OwnerVolumeTabletId
        // UNIT_ASSERT_VALUES_EQUAL(42, request.GetOwnerVolumeTabletId());
        UNIT_ASSERT_VALUES_EQUAL("cloud", request.GetCloudId());
        UNIT_ASSERT_VALUES_EQUAL("folder", request.GetFolderId());
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, request.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(100, request.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(2, request.GetReplicaCount());
        UNIT_ASSERT_EQUAL(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            request.GetStorageMediaKind());
        UNIT_ASSERT_VALUES_EQUAL("fastshard", request.GetPoolName());
    }

    Y_UNIT_TEST(ShouldMapDeviceCountToMediaKind)
    {
        TDRProxyEnv env;

        env.Execute<TEvAllocateResponse>(
            std::make_unique<TEvAllocate>("fs", 42, "", "", 1, 100, "fastshard"));
        UNIT_ASSERT_VALUES_EQUAL(0, env.DiskRegistry->Allocate.GetReplicaCount());
        UNIT_ASSERT_EQUAL(
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            env.DiskRegistry->Allocate.GetStorageMediaKind());

        env.Execute<TEvAllocateResponse>(
            std::make_unique<TEvAllocate>("fs", 42, "", "", 2, 100, "fastshard"));
        UNIT_ASSERT_VALUES_EQUAL(1, env.DiskRegistry->Allocate.GetReplicaCount());
        UNIT_ASSERT_EQUAL(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2,
            env.DiskRegistry->Allocate.GetStorageMediaKind());

        auto response = env.Execute<TEvAllocateResponse>(
            std::make_unique<TEvAllocate>("other", 42, "", "", 0, 100, "fastshard"));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("fs", env.DiskRegistry->Allocate.GetDiskId());

        response = env.Execute<TEvAllocateResponse>(
            std::make_unique<TEvAllocate>("other", 42, "", "", 4, 100, "fastshard"));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("fs", env.DiskRegistry->Allocate.GetDiskId());
    }

    Y_UNIT_TEST(ShouldDescribeDevices)
    {
        TDRProxyEnv env;
        FillLayout(env.DiskRegistry->DescribeResponse);

        auto response =
            env.Execute<TEvDeviceService::TEvDescribeDevicesResponse>(
                std::make_unique<TEvDeviceService::TEvDescribeDevicesRequest>(
                    "fs"));
        UNIT_ASSERT_C(!HasError(response->GetError()), response->GetError());
        CheckLayout(response->Layout, {});
        UNIT_ASSERT_VALUES_EQUAL("fs", env.DiskRegistry->Describe.GetDiskId());
    }

    Y_UNIT_TEST(ShouldMapLifecycleRequests)
    {
        TDRProxyEnv env;
        auto& dr = *env.DiskRegistry;

        env.Execute<TEvDeviceService::TEvMarkForCleanupResponse>(
            std::make_unique<TEvDeviceService::TEvMarkForCleanupRequest>(
                "fs", 42));
        UNIT_ASSERT_VALUES_EQUAL("fs", dr.MarkForCleanup.GetDiskId());
        // TODO(issue-7373): restore when DR supports OwnerVolumeTabletId
        // UNIT_ASSERT_VALUES_EQUAL(42, dr.MarkForCleanup.GetOwnerVolumeTabletId());

        env.Execute<TEvDeviceService::TEvDeallocateDevicesResponse>(
            std::make_unique<TEvDeviceService::TEvDeallocateDevicesRequest>(
                "fs", 42));
        UNIT_ASSERT_VALUES_EQUAL("fs", dr.Deallocate.GetDiskId());
        // TODO(issue-7373): restore when DR supports OwnerVolumeTabletId
        // UNIT_ASSERT_VALUES_EQUAL(42, dr.Deallocate.GetOwnerVolumeTabletId());
        UNIT_ASSERT(!dr.Deallocate.GetSync());

        env.Execute<TEvDeviceService::TEvFinishRepairResponse>(
            std::make_unique<TEvDeviceService::TEvFinishRepairRequest>(
                "fs", "uuid-3"));
        UNIT_ASSERT_VALUES_EQUAL("fs", dr.MarkReplacement.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3", dr.MarkReplacement.GetDeviceId());
        UNIT_ASSERT(!dr.MarkReplacement.GetIsReplacement());
    }

    Y_UNIT_TEST(ShouldMapReplicaRequests)
    {
        TDRProxyEnv env;
        auto& dr = *env.DiskRegistry;

        env.Execute<TEvDeviceService::TEvFinishMigrationResponse>(
            std::make_unique<TEvDeviceService::TEvFinishMigrationRequest>(
                "fs", 3, 1, "uuid-2", "uuid-4"));
        UNIT_ASSERT_VALUES_EQUAL("fs/1", dr.FinishMigration.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(1, dr.FinishMigration.MigrationsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2",
            dr.FinishMigration.GetMigrations(0).GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-4",
            dr.FinishMigration.GetMigrations(0).GetTargetDeviceId());

        env.Execute<TEvDeviceService::TEvReplaceDeviceResponse>(
            std::make_unique<TEvDeviceService::TEvReplaceDeviceRequest>(
                "fs", 3, 1, "uuid-2"));
        UNIT_ASSERT_VALUES_EQUAL("fs/1", dr.ReplaceDevice.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-2", dr.ReplaceDevice.GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("", dr.ReplaceDevice.GetDeviceReplacementUUID());

        // A single-device shard is a plain disk, not a replica.
        env.Execute<TEvDeviceService::TEvFinishMigrationResponse>(
            std::make_unique<TEvDeviceService::TEvFinishMigrationRequest>(
                "fs", 1, 0, "uuid-1", "uuid-4"));
        UNIT_ASSERT_VALUES_EQUAL("fs", dr.FinishMigration.GetDiskId());
        env.Execute<TEvDeviceService::TEvReplaceDeviceResponse>(
            std::make_unique<TEvDeviceService::TEvReplaceDeviceRequest>(
                "fs", 1, 0, "uuid-1"));
        UNIT_ASSERT_VALUES_EQUAL("fs", dr.ReplaceDevice.GetDiskId());
    }

    Y_UNIT_TEST(ShouldPassDiskRegistryErrors)
    {
        TDRProxyEnv env;
        env.DiskRegistry->Error = MakeError(E_NOT_FOUND, "no such disk");

        auto allocate = env.Execute<TEvAllocateResponse>(
            std::make_unique<TEvAllocate>("fs", 42, "", "", 3, 100, "fastshard"));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, allocate->GetStatus());
        UNIT_ASSERT(allocate->Layout.Replicas.empty());

        auto describe =
            env.Execute<TEvDeviceService::TEvDescribeDevicesResponse>(
                std::make_unique<TEvDeviceService::TEvDescribeDevicesRequest>(
                    "fs"));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, describe->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("no such disk", describe->GetErrorReason());
        UNIT_ASSERT(describe->Layout.Replicas.empty());

        auto mark = env.Execute<TEvDeviceService::TEvMarkForCleanupResponse>(
            std::make_unique<TEvDeviceService::TEvMarkForCleanupRequest>(
                "fs", 42));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, mark->GetStatus());

        auto deallocate =
            env.Execute<TEvDeviceService::TEvDeallocateDevicesResponse>(
                std::make_unique<TEvDeviceService::TEvDeallocateDevicesRequest>(
                    "fs", 42));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, deallocate->GetStatus());

        auto repair = env.Execute<TEvDeviceService::TEvFinishRepairResponse>(
            std::make_unique<TEvDeviceService::TEvFinishRepairRequest>(
                "fs", "uuid"));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, repair->GetStatus());

        auto migration =
            env.Execute<TEvDeviceService::TEvFinishMigrationResponse>(
                std::make_unique<TEvDeviceService::TEvFinishMigrationRequest>(
                    "fs", 1, 0, "source", "target"));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, migration->GetStatus());

        auto replace = env.Execute<TEvDeviceService::TEvReplaceDeviceResponse>(
            std::make_unique<TEvDeviceService::TEvReplaceDeviceRequest>(
                "fs", 1, 0, "uuid"));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, replace->GetStatus());
    }

    Y_UNIT_TEST(ShouldReplayRequestsQueuedWhileConnecting)
    {
        TDRProxyEnv env;
        auto& dr = *env.DiskRegistry;

        // The requests reach the proxy before its pipe to the disk registry
        // is up and leave for the tablet only after that.
        bool connected = false;
        ui32 queued = 0;
        env.Env.GetRuntime().SetEventFilter([&] (auto&, auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvTabletPipe::EvClientConnected: {
                    using TEvConnected = TEvTabletPipe::TEvClientConnected;
                    if (event->template Get<TEvConnected>()->TabletId ==
                        env.TabletId)
                    {
                        connected = true;
                    }
                    break;
                }
                case TEvDeviceService::EvDescribeDevicesRequest:
                case TEvDeviceService::EvMarkForCleanupRequest:
                    UNIT_ASSERT(!connected);
                    ++queued;
                    break;
                case TEvDiskRegistry::EvDescribeDiskRequest:
                case TEvDiskRegistry::EvMarkDiskForCleanupRequest:
                    UNIT_ASSERT(connected);
                    break;
            }
            return false;
        });

        env.Send(std::make_unique<TEvDescribe>("fs-1"));
        env.Send(std::make_unique<TEvMark>("fs-2", 42));

        auto describe = env.Recv<TEvDescribeResponse>();
        UNIT_ASSERT_C(!HasError(describe->GetError()), describe->GetError());
        auto mark = env.Recv<TEvMarkResponse>();
        UNIT_ASSERT_C(!HasError(mark->GetError()), mark->GetError());

        UNIT_ASSERT_VALUES_EQUAL("fs-1", dr.Describe.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("fs-2", dr.MarkForCleanup.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(2, queued);
    }

    Y_UNIT_TEST(ShouldCancelInflightRequestsWhenPipeBreaks)
    {
        TDRProxyEnv env;
        auto& runtime = env.Env.GetRuntime();

        auto response = env.Execute<TEvDescribeResponse>(
            std::make_unique<TEvDescribe>("fs"));
        UNIT_ASSERT_C(!HasError(response->GetError()), response->GetError());

        // Keep the next one in flight.
        bool drop = true;
        bool dropped = false;
        runtime.SetEventFilter([&] (auto&, auto& event) {
            if (drop && event->GetTypeRewrite() ==
                TEvDiskRegistry::EvDescribeDiskRequest)
            {
                dropped = true;
                return true;
            }
            return false;
        });
        env.Send(std::make_unique<TEvDescribe>("fs"));
        env.WaitFor([&] { return dropped; });

        RebootTablet(runtime, env.TabletId, env.Sender, env.NodeIdx);
        response = env.Recv<TEvDescribeResponse>();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());

        // Reconnected on its own after the delay.
        drop = false;
        runtime.AdvanceCurrentTime(env.Config->GetPipeClientMinRetryTime());
        response = env.Execute<TEvDescribeResponse>(
            std::make_unique<TEvDescribe>("fs"));
        UNIT_ASSERT_C(!HasError(response->GetError()), response->GetError());
    }

    Y_UNIT_TEST(ShouldLookupDiskRegistryInHive)
    {
        auto config = DefaultConfig();
        config.SetTenantHiveTabletId(777);
        config.SetFastShardDROwner(16045690984503103501ULL);
        config.SetFastShardDROwnerIdx(2);
        TDRProxyEnv env(true /* withDiskRegistry */, std::move(config));

        auto response = env.Execute<TEvDescribeResponse>(
            std::make_unique<TEvDescribe>("fs"));
        UNIT_ASSERT_C(!HasError(response->GetError()), response->GetError());
        UNIT_ASSERT_VALUES_EQUAL("fs", env.DiskRegistry->Describe.GetDiskId());

        const auto& hive = *env.HiveProxy;
        UNIT_ASSERT_VALUES_EQUAL(777, hive.HiveId);
        UNIT_ASSERT_VALUES_EQUAL(16045690984503103501ULL, hive.Owner);
        UNIT_ASSERT_VALUES_EQUAL(2, hive.OwnerIdx);
    }

    Y_UNIT_TEST(ShouldRetryLookupOnTimeout)
    {
        auto config = DefaultConfig();
        config.SetFastShardDROwner(1);
        config.SetFastShardDROwnerIdx(1);
        TDRProxyEnv env(true /* withDiskRegistry */, std::move(config));
        auto& runtime = env.Env.GetRuntime();
        auto& hive = *env.HiveProxy;

        hive.Hang = true;
        env.Send(std::make_unique<TEvDescribe>("fs"));
        env.WaitFor([&] { return hive.Lookups == 1; });

        hive.Hang = false;
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        auto describe = env.Recv<TEvDescribeResponse>();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, describe->GetStatus());
        UNIT_ASSERT_STRING_CONTAINS(describe->GetErrorReason(), "timed out");

        // Looked up again after the reconnect delay.
        runtime.AdvanceCurrentTime(env.Config->GetPipeClientMinRetryTime());
        env.WaitFor([&] { return hive.Lookups == 2; });
        describe = env.Execute<TEvDescribeResponse>(
            std::make_unique<TEvDescribe>("fs"));
        UNIT_ASSERT_C(!HasError(describe->GetError()), describe->GetError());
    }

    Y_UNIT_TEST(ShouldRejectRequestsWhenLookupFails)
    {
        auto config = DefaultConfig();
        config.SetFastShardDROwner(1);
        config.SetFastShardDROwnerIdx(1);
        TDRProxyEnv env(true /* withDiskRegistry */, std::move(config));
        auto& runtime = env.Env.GetRuntime();
        auto& hive = *env.HiveProxy;

        hive.Error = MakeError(E_NOT_FOUND, "no such tablet");
        env.Send(std::make_unique<TEvDescribe>("fs"));
        env.Send(std::make_unique<TEvMark>("fs", 42));

        auto describe = env.Recv<TEvDescribeResponse>();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, describe->GetStatus());
        UNIT_ASSERT_STRING_CONTAINS(
            describe->GetErrorReason(),
            "no such tablet");
        auto mark = env.Recv<TEvMarkResponse>();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, mark->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("", env.DiskRegistry->Describe.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(1, hive.Lookups);

        // Looked up again after the reconnect delay.
        hive.Error = {};
        runtime.AdvanceCurrentTime(env.Config->GetPipeClientMinRetryTime());
        env.WaitFor([&] { return hive.Lookups == 2; });
        describe = env.Execute<TEvDescribeResponse>(
            std::make_unique<TEvDescribe>("fs"));
        UNIT_ASSERT_C(!HasError(describe->GetError()), describe->GetError());
        UNIT_ASSERT_VALUES_EQUAL("fs", env.DiskRegistry->Describe.GetDiskId());
    }

    Y_UNIT_TEST(ShouldRejectRequestsWithoutDiskRegistry)
    {
        TDRProxyEnv env(false /* withDiskRegistry */);

        auto allocate = env.Execute<TEvAllocateResponse>(
            std::make_unique<TEvAllocate>("fs", 42, "", "", 3, 100, "fastshard"));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, allocate->GetStatus());

        auto describe =
            env.Execute<TEvDeviceService::TEvDescribeDevicesResponse>(
                std::make_unique<TEvDeviceService::TEvDescribeDevicesRequest>(
                    "fs"));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, describe->GetStatus());
    }

    Y_UNIT_TEST(ShouldRejectRequestsWhenDiskRegistryIsDown)
    {
        auto config = DefaultConfig();
        config.SetFastShardDRTabletId(MakeTabletID(false, 2));
        TDRProxyEnv env(false /* withDiskRegistry */, std::move(config));

        // Queued for the connection that never comes, then cancelled.
        env.Send(std::make_unique<TEvAllocate>("fs", 42, "", "", 3, 100, "fastshard"));
        env.Send(std::make_unique<TEvDescribe>("fs"));

        auto allocate = env.Recv<TEvAllocateResponse>();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            allocate->GetStatus(),
            allocate->GetErrorReason());
        auto describe = env.Recv<TEvDescribeResponse>();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, describe->GetStatus());
    }

    Y_UNIT_TEST(ShouldMatchReallocateDiskWireFormat)
    {
        using TEvVolume = NBlockStore::NStorage::TEvVolume;

        CheckWireFormat(
            NBlockStore::NProto::TReallocateDiskRequest::descriptor(),
            NProtoPrivate::TLayoutChangedRequest::descriptor());
        CheckWireFormat(
            NBlockStore::NProto::TReallocateDiskResponse::descriptor(),
            NProtoPrivate::TLayoutChangedResponse::descriptor());

        TEvVolume::TEvReallocateDiskRequest sent;
        sent.Record.SetDiskId("fs");
        sent.Record.MutableHeaders()->SetTraceId("trace");
        sent.Record.MutableHeaders()->SetClientId("dr");
        const TString bytes = SerializeEvent(sent);

        // Undeclared headers survive as unknown fields.
        auto received =
            LoadEvent<TEvDeviceService::TEvLayoutChangedRequest>(bytes);
        UNIT_ASSERT_VALUES_EQUAL(sent.Type(), received->Type());
        UNIT_ASSERT_VALUES_EQUAL("fs", received->Record.GetFileSystemId());

        auto reemitted = LoadEvent<TEvVolume::TEvReallocateDiskRequest>(
            SerializeEvent(*received));
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(
            sent.Record,
            reemitted->Record));

        TEvDeviceService::TEvLayoutChangedResponse answer(
            MakeError(E_REJECTED, "busy"));
        const TString answerBytes = SerializeEvent(answer);

        auto read = LoadEvent<TEvVolume::TEvReallocateDiskResponse>(answerBytes);
        UNIT_ASSERT_VALUES_EQUAL(answer.Type(), read->Type());
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, read->Record.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("busy", read->Record.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(answerBytes, SerializeEvent(*read));
    }
}

}   // namespace NCloud::NFileStore::NStorage
