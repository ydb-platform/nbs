#pragma once

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/libs/notify/public.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/pending_request.h>
#include <cloud/blockstore/libs/storage/core/volume_label.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_actor.h>
#include <cloud/blockstore/libs/storage/testlib/common_properties.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_mock.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/core/protos/bind_channel_storage_pool.pb.h>
#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

#include <google/protobuf/util/json_util.h>

#include <atomic>

namespace NCloud::NBlockStore::NStorage::NDiskRegistryTest {

////////////////////////////////////////////////////////////////////////////////

const TDuration WaitTimeout = TDuration::Seconds(5);

const ui64 HiveId = NKikimr::MakeDefaultHiveID(0);
const ui64 TestTabletId  = NKikimr::MakeTabletID(0, HiveId, 1);

constexpr ui32 DefaultBlockSize = 512;
constexpr ui32 DefaultLogicalBlockSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

enum ETestEvents
{
    EvRegisterAgent = EventSpaceBegin(NKikimr::TEvents::ES_USERSPACE + 1),
};

struct TEvRegisterAgent
    : public NActors::TEventBase<TEvRegisterAgent, ETestEvents::EvRegisterAgent>
{
    DEFINE_SIMPLE_NONLOCAL_EVENT(TEvRegisterAgent, "TEvRegisterAgent");
};

////////////////////////////////////////////////////////////////////////////////

struct TByUUID
{
    const TString& operator () (const NProto::TDeviceConfig& device) const {
        return device.GetDeviceUUID();
    }
};

////////////////////////////////////////////////////////////////////////////////

void WaitForSecureErase(
    NActors::TTestActorRuntime& runtime,
    size_t deviceCount);

void WaitForSecureErase(
    NActors::TTestActorRuntime& runtime,
    TVector<NProto::TAgentConfig> agents);

void WaitForAgent(NActors::TTestActorRuntime& runtime, int nodeIdx);
void WaitForAgents(NActors::TTestActorRuntime& runtime, int agentsCount);

void RegisterAndWaitForAgent(
    NActors::TTestActorRuntime& runtime,
    int nodeIdx,
    size_t deviceCount);

void RegisterAndWaitForAgents(
    NActors::TTestActorRuntime& runtime,
    const TVector<NProto::TAgentConfig>& agents);

void RegisterAgent(NActors::TTestActorRuntime& runtime, int nodeIdx);
void RegisterAgents(NActors::TTestActorRuntime& runtime, int agentsCount);

void KillAgent(NActors::TTestActorRuntime& runtime, int nodeIdx);

NProto::TStorageServiceConfig CreateDefaultStorageConfig();

////////////////////////////////////////////////////////////////////////////////

class TTestDiskAgent final
    : public NActors::TActorBootstrapped<TTestDiskAgent>
{
private:
    NProto::TAgentConfig Config;
    TDeque<TPendingRequest> PendingRequests;
    bool Ready = false;

public:
    explicit TTestDiskAgent(NProto::TAgentConfig config)
        : Config(std::move(config))
    {
    }

    void Bootstrap(const NActors::TActorContext& ctx)
    {
        Config.SetNodeId(ctx.SelfID.NodeId());

        Become(&TThis::StateWork);
    }

    std::function<bool(
        const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr&,
        const NActors::TActorContext&)> HandleAcquireDevicesImpl;

    std::function<void(
        const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr&,
        const NActors::TActorContext&)> HandleReleaseDevicesImpl;

    std::function<bool(
        const TEvDiskAgent::TEvSecureEraseDeviceRequest::TPtr&,
        const NActors::TActorContext&)> HandleSecureEraseDeviceImpl;

private:
    void RegisterAgent(const NActors::TActorContext& ctx)
    {
        NKikimr::NTabletPipe::TClientConfig pipeConfig(
            NKikimr::NTabletPipe::TClientRetryPolicy::WithRetries());

        auto pipe = ctx.Register(
            NKikimr::NTabletPipe::CreateClient(
                ctx.SelfID,
                TestTabletId,
                pipeConfig
            )
        );

        auto request =
            std::make_unique<TEvDiskRegistry::TEvRegisterAgentRequest>();

        request->Record.MutableAgentConfig()->CopyFrom(Config);

        NKikimr::NTabletPipe::SendData(ctx, pipe, request.release());
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            IgnoreFunc(NKikimr::TEvTabletPipe::TEvClientConnected);
            IgnoreFunc(NKikimr::TEvTabletPipe::TEvClientDestroyed);
            IgnoreFunc(NKikimr::TEvTabletPipe::TEvServerConnected);

            HFunc(TEvDiskRegistry::TEvRegisterAgentResponse, HandleRegisterAgent);

            HFunc(TEvDiskAgent::TEvWaitReadyRequest, HandleWaitReady)
            HFunc(TEvDiskAgent::TEvAcquireDevicesRequest, HandleAcquireDevices);
            HFunc(TEvDiskAgent::TEvReleaseDevicesRequest, HandleReleaseDevices);
            HFunc(TEvDiskAgent::TEvSecureEraseDeviceRequest, HandleSecureEraseDevice);
            HFunc(TEvDiskAgent::TEvEnableAgentDeviceRequest, HandleEnableAgentDevice);

            HFunc(TEvRegisterAgent, HandleRegisterAgent)

            default:
                //HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_REGISTRY);
                Y_ABORT("Unexpected event %x", ev->GetTypeRewrite());
        }
    }

    void HandlePoisonPill(
        const NKikimr::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Die(ctx);
    }

    void HandleRegisterAgent(
        const TEvRegisterAgent::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);
        RegisterAgent(ctx);
    }

    void HandleRegisterAgent(
        const TEvDiskRegistry::TEvRegisterAgentResponse::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Ready = true;

        SendPendingRequests(ctx, PendingRequests);
    }

    void HandleWaitReady(
        const TEvDiskAgent::TEvWaitReadyRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        if (!Ready) {
            PendingRequests.emplace_back(NActors::IEventHandlePtr(ev.Release()), nullptr);
            return;
        }

        auto response = std::make_unique<TEvDiskAgent::TEvWaitReadyResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleAcquireDevices(
        const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        if (HandleAcquireDevicesImpl && HandleAcquireDevicesImpl(ev, ctx)) {
            return;
        }

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAcquireDevicesResponse>());
    }

    void HandleReleaseDevices(
        const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        if (HandleReleaseDevicesImpl) {
            HandleReleaseDevicesImpl(ev, ctx);
        }

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvReleaseDevicesResponse>());
    }

    void HandleSecureEraseDevice(
        const TEvDiskAgent::TEvSecureEraseDeviceRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        if (HandleSecureEraseDeviceImpl && HandleSecureEraseDeviceImpl(ev, ctx)) {
            return;
        }

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvSecureEraseDeviceResponse>());
    }

    void HandleEnableAgentDevice(
        const TEvDiskAgent::TEvEnableAgentDeviceRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvEnableAgentDeviceResponse>());
    }
};

inline TTestDiskAgent* CreateTestDiskAgent(NProto::TAgentConfig config)
{
    return new TTestDiskAgent(std::move(config));
}

inline TTestDiskAgent* CreateBrokenTestDiskAgent(NProto::TAgentConfig config)
{
    auto agent = new TTestDiskAgent(std::move(config));

    agent->HandleAcquireDevicesImpl = [] (
        const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAcquireDevicesResponse>(
                MakeError(E_BS_INVALID_SESSION)));
        return true;
    };

    return agent;
}

inline TTestDiskAgent* CreateSuspendedTestDiskAgent(
    NProto::TAgentConfig config,
    std::shared_ptr<std::atomic<bool>> active)
{
    auto agent = new TTestDiskAgent(std::move(config));

    agent->HandleAcquireDevicesImpl = [active] (
        const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr&,
        const NActors::TActorContext&) mutable
    {
        return !active->load();
    };

    return agent;
}

struct TCreateDeviceConfigParams
{
    TString Name;
    TString Id;
    TString SerialNum;
    TString Rack = "the-rack";
    ui64 TotalSize = 10_GB;
    ui32 BlockSize = DefaultBlockSize;
};

inline NProto::TDeviceConfig CreateDeviceConfig(
    TCreateDeviceConfigParams params)
{
    NProto::TDeviceConfig device;

    device.SetDeviceName(std::move(params.Name));
    device.SetDeviceUUID(params.Id);
    device.SetSerialNumber(std::move(params.SerialNum));
    device.SetRack(std::move(params.Rack));
    device.SetBlocksCount(params.TotalSize / params.BlockSize);
    device.SetBlockSize(params.BlockSize);
    device.SetTransportId(params.Id);
    device.MutableRdmaEndpoint()->SetHost(std::move(params.Id));
    device.MutableRdmaEndpoint()->SetPort(10020);

    return device;
}

inline NProto::TDeviceConfig Device(
    TString name,
    TString uuid,
    TString rack = "the-rack",
    ui64 totalSize = 10_GB,
    ui32 blockSize = DefaultBlockSize)
{
    return CreateDeviceConfig({
        .Name = std::move(name),
        .Id = std::move(uuid),
        .Rack = std::move(rack),
        .TotalSize = totalSize,
        .BlockSize = blockSize
    });
}

inline auto CreateAgentConfig(
    const TString& agentId,
    std::initializer_list<NProto::TDeviceConfig> devices)
{
    NProto::TAgentConfig config;
    config.SetAgentId(agentId);
    for (auto& device: devices) {
        *config.AddDevices() = device;
    }

    return config;
}

inline auto CreateRegistryConfig(
    ui32 version,
    const TVector<NProto::TAgentConfig>& agents,
    const TVector<NProto::TDeviceOverride>& deviceOverrides = {})
{
    NProto::TDiskRegistryConfig config;
    config.SetVersion(version);

    for (const auto& agent: agents) {
        *config.AddKnownAgents() = agent;
    }

    for (const auto& deviceOverride: deviceOverrides) {
        *config.AddDeviceOverrides() = deviceOverride;
    }

    return config;
}

inline auto CreateRegistryConfig(const TVector<NProto::TAgentConfig>& agents)
{
    return CreateRegistryConfig(0, agents);
}

////////////////////////////////////////////////////////////////////////////////

class TFakeStorageService final
    : public NActors::TActor<TFakeStorageService>
{
public:
    TFakeStorageService()
        : TActor(&TThis::StateWork)
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvService::TEvDestroyVolumeRequest, HandleDestroyVolumeRequest);

            default:
                HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
        }
    }

    void HandleDestroyVolumeRequest(
        const TEvService::TEvDestroyVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto response = std::make_unique<TEvService::TEvDestroyVolumeResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFakeVolumeProxy final
    : public NActors::TActor<TFakeVolumeProxy>
{
public:
    TFakeVolumeProxy()
        : TActor(&TThis::StateWork)
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvVolume::TEvReallocateDiskRequest, HandleReallocateDiskRequest);

            default:
                HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
        }
    }

    void HandleReallocateDiskRequest(
        const TEvVolume::TEvReallocateDiskRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto response = std::make_unique<TEvVolume::TEvReallocateDiskResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestRuntimeBuilder
{
    TVector<NActors::IActor*> DiskAgents;
    TStorageConfigPtr StorageConfig;
    TDiagnosticsConfigPtr DiagnosticsConfig;
    NLogbroker::IServicePtr LogbrokerService;
    NNotify::IServicePtr NotifyService;
    ILoggingServicePtr Logging = CreateLoggingService("console");

    std::unique_ptr<NActors::TTestActorRuntime> Build();

    TTestRuntimeBuilder& WithAgents(std::initializer_list<NActors::IActor*> agents);
    TTestRuntimeBuilder& WithAgents(const TVector<NProto::TAgentConfig>& configs);
    TTestRuntimeBuilder& With(NProto::TStorageServiceConfig config);
    TTestRuntimeBuilder& With(TStorageConfigPtr config);
    TTestRuntimeBuilder& With(NLogbroker::IServicePtr service);
    TTestRuntimeBuilder& With(NNotify::IServicePtr service);
};

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryClient
{
private:
    NActors::TTestActorRuntime& Runtime;
    ui32 NodeIdx = 0;
    ui64 TabletId;

    const NActors::TActorId Sender;
    NActors::TActorId PipeClient;

public:
    TDiskRegistryClient(
            NActors::TTestActorRuntime& runtime,
            ui32 nodeIdx = 0,
            ui64 tabletId = TestTabletId)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , TabletId(tabletId)
        , Sender(runtime.AllocateEdgeActor(NodeIdx))
    {
        PipeClient = Runtime.ConnectToPipe(
            TabletId,
            Sender,
            NodeIdx,
            NKikimr::GetPipeConfigWithRetries());
    }

    ~TDiskRegistryClient()
    {
        auto* ev = new NActors::IEventHandle(
            PipeClient,
            Sender,
            new NKikimr::TEvents::TEvPoisonPill());

        Runtime.Send(ev, NodeIdx);
    }

    void RebootTablet()
    {
        TVector<ui64> tablets = { TabletId };
        auto guard = NKikimr::CreateTabletScheduledEventsGuard(
            tablets,
            Runtime,
            Sender);

        NKikimr::RebootTablet(Runtime, TabletId, Sender);

        PipeClient = Runtime.ConnectToPipe(
            TabletId,
            Sender,
            NodeIdx,
            NKikimr::GetPipeConfigWithRetries());
    }

    bool Exists(const TString& id)
    {
        SendDescribeDiskRequest(id);
        auto response = RecvDescribeDiskResponse();
        const auto status = response->GetStatus();

        UNIT_ASSERT(status == S_OK || status == E_NOT_FOUND);

        return status == S_OK;
    }

    template <typename TRequest>
    void SendRequest(std::unique_ptr<TRequest> request, ui64 cookie = 0)
    {
        Runtime.SendToPipe(
            PipeClient,
            Sender,
            request.release(),
            NodeIdx,
            cookie
        );
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    auto CreateWaitReadyRequest()
    {
        return std::make_unique<TEvDiskRegistry::TEvWaitReadyRequest>();
    }

    auto CreateRegisterAgentRequest(NProto::TAgentConfig config)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvRegisterAgentRequest>();

        request->Record.MutableAgentConfig()->Swap(&config);

        return request;
    }

    auto CreateAllocateDiskRequest(
        const TString& diskId,
        ui64 diskSize,
        ui32 blockSize = DefaultLogicalBlockSize,
        TString placementGroupId = {},
        ui32 placementPartitionIndex = 0,
        TString cloudId = "yc.nbs",
        TString folderId = "yc.nbs.tests",
        ui32 replicaCount = 0,
        NProto::EStorageMediaKind mediaKind =
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvAllocateDiskRequest>();

        request->Record.SetDiskId(diskId);
        request->Record.SetCloudId(cloudId);
        request->Record.SetFolderId(folderId);
        request->Record.SetBlockSize(blockSize);
        request->Record.SetBlocksCount(diskSize / blockSize);
        request->Record.SetPlacementGroupId(std::move(placementGroupId));
        request->Record.SetPlacementPartitionIndex(placementPartitionIndex);
        request->Record.SetReplicaCount(replicaCount);
        request->Record.SetStorageMediaKind(mediaKind);

        return request;
    }

    auto CreateDeallocateDiskRequest(const TString& diskId, bool sync = false)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvDeallocateDiskRequest>();

        request->Record.SetDiskId(diskId);
        request->Record.SetSync(sync);

        return request;
    }

    auto CreateAllocateCheckpointRequest(
        const TString& sourceDiskId,
        const TString& checkpointId)
    {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvAllocateCheckpointRequest>();

        request->Record.SetSourceDiskId(sourceDiskId);
        request->Record.SetCheckpointId(checkpointId);

        return request;
    }

    auto CreateDeallocateCheckpointRequest(
        const TString& sourceDiskId,
        const TString& checkpointId)
    {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvDeallocateCheckpointRequest>();

        request->Record.SetSourceDiskId(sourceDiskId);
        request->Record.SetCheckpointId(checkpointId);

        return request;
    }

    auto CreateGetCheckpointDataStateRequest(
        const TString& sourceDiskId,
        const TString& checkpointId)
    {
        auto request = std::make_unique<
            TEvDiskRegistry::TEvGetCheckpointDataStateRequest>();

        request->Record.SetSourceDiskId(sourceDiskId);
        request->Record.SetCheckpointId(checkpointId);

        return request;
    }

    auto CreateSetCheckpointDataStateRequest(
        const TString& sourceDiskId,
        const TString& checkpointId,
        NProto::ECheckpointState newState)
    {
        auto request = std::make_unique<
            TEvDiskRegistry::TEvSetCheckpointDataStateRequest>();

        request->Record.SetSourceDiskId(sourceDiskId);
        request->Record.SetCheckpointId(checkpointId);
        request->Record.SetCheckpointState(newState);

        return request;
    }

    auto CreateGetAgentNodeIdRequest(
        const TString& agentId)
    {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvGetAgentNodeIdRequest>();

        request->Record.SetAgentId(agentId);

        return request;
    }

    auto CreateAcquireDiskRequest(
        const TString& diskId,
        const TString& clientId,
        const NProto::EVolumeAccessMode accessMode = NProto::VOLUME_ACCESS_READ_WRITE,
        const ui64 mountSeqNumber = 0,
        const ui32 volumeGeneration = 0)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvAcquireDiskRequest>();

        request->Record.SetDiskId(diskId);
        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetAccessMode(accessMode);
        request->Record.SetMountSeqNumber(mountSeqNumber);
        request->Record.SetVolumeGeneration(volumeGeneration);

        return request;
    }

    auto CreateReleaseDiskRequest(
        const TString& diskId,
        const TString& clientId,
        const ui32 volumeGeneration = 0)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvReleaseDiskRequest>();

        request->Record.SetDiskId(diskId);
        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetVolumeGeneration(volumeGeneration);

        return request;
    }

    auto CreateDescribeDiskRequest(const TString& diskId)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvDescribeDiskRequest>();

        request->Record.SetDiskId(diskId);

        return request;
    }

    auto CreateUpdateConfigRequest(
        NProto::TDiskRegistryConfig config,
        bool ignoreVersion = false)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvUpdateConfigRequest>();
        auto& r = request->Record;
        *r.MutableConfig() = std::move(config);

        r.SetIgnoreVersion(ignoreVersion);

        return request;
    }

    auto CreateSetWritableStateRequest(bool value = false)
    {
        auto request = std::make_unique<
            TEvDiskRegistry::TEvSetWritableStateRequest>();
        request->Record.SetState(value);

        return request;
    }

    auto CreatePublishDiskStatesRequest()
    {
        return std::make_unique<TEvDiskRegistryPrivate::TEvPublishDiskStatesRequest>();
    }

    auto CreateListBrokenDisksRequest()
    {
        return std::make_unique<TEvDiskRegistryPrivate::TEvListBrokenDisksRequest>();
    }

    auto CreateListDisksToNotifyRequest()
    {
        return std::make_unique<TEvDiskRegistryPrivate::TEvListDisksToNotifyRequest>();
    }

    auto CreateDescribeConfigRequest()
    {
        return std::make_unique<TEvDiskRegistry::TEvDescribeConfigRequest>();
    }

    auto CreateCleanupDisksRequest()
    {
        return std::make_unique<TEvDiskRegistryPrivate::TEvCleanupDisksRequest>();
    }

    auto CreateSecureEraseRequest(
        TVector<NProto::TDeviceConfig> devices,
        TDuration requestTimeout = {})
    {
        return std::make_unique<TEvDiskRegistryPrivate::TEvSecureEraseRequest>(
            TString{},
            std::move(devices),
            requestTimeout);
    }

    auto CreateCleanupDevicesRequest(TVector<TString> devices)
    {
        return std::make_unique<TEvDiskRegistryPrivate::TEvCleanupDevicesRequest>(
            std::move(devices));
    }

    auto CreateRemoveDiskSessionRequest(
        TString diskId,
        TString clientId,
        TVector<TAgentReleaseDevicesCachedRequest> sentRequests)
    {
        return std::make_unique<
            TEvDiskRegistryPrivate::TEvRemoveDiskSessionRequest>(
            std::move(diskId),
            std::move(clientId),
            std::move(sentRequests));
    }

    auto CreateUpdateAgentStatsRequest(NProto::TAgentStats stats)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvUpdateAgentStatsRequest>();
        *request->Record.MutableAgentStats() = std::move(stats);

        return request;
    }

    auto CreateCreatePlacementGroupRequest(
        TString groupId,
        NProto::EPlacementStrategy placementStrategy,
        ui32 placementPartitionCount)
    {
        auto request = std::make_unique<TEvService::TEvCreatePlacementGroupRequest>();
        request->Record.SetGroupId(std::move(groupId));
        request->Record.SetPlacementStrategy(placementStrategy);
        request->Record.SetPlacementPartitionCount(placementPartitionCount);
        return request;
    }

    auto CreateUpdatePlacementGroupSettingsRequest(
        TString groupId,
        ui32 configVersion,
        NProto::TPlacementGroupSettings settings)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvUpdatePlacementGroupSettingsRequest>();
        request->Record.SetGroupId(std::move(groupId));
        request->Record.SetConfigVersion(configVersion);
        *request->Record.MutableSettings() = std::move(settings);

        return request;
    }

    auto CreateDestroyPlacementGroupRequest(TString groupId)
    {
        auto request = std::make_unique<TEvService::TEvDestroyPlacementGroupRequest>();
        request->Record.SetGroupId(std::move(groupId));

        return request;
    }

    auto CreateAlterPlacementGroupMembershipRequest(
        TString groupId,
        ui32 configVersion,
        TVector<TString> disksToAdd,
        TVector<TString> disksToRemove)
    {
        auto request = std::make_unique<TEvService::TEvAlterPlacementGroupMembershipRequest>();
        request->Record.SetGroupId(std::move(groupId));
        request->Record.SetConfigVersion(configVersion);
        for (auto& d: disksToAdd) {
            *request->Record.AddDisksToAdd() = std::move(d);
        }
        for (auto& d: disksToRemove) {
            *request->Record.AddDisksToRemove() = std::move(d);
        }

        return request;
    }

    auto CreateListPlacementGroupsRequest()
    {
        return std::make_unique<TEvService::TEvListPlacementGroupsRequest>();
    }

    auto CreateDescribePlacementGroupRequest(TString groupId)
    {
        auto request = std::make_unique<TEvService::TEvDescribePlacementGroupRequest>();
        request->Record.SetGroupId(std::move(groupId));

        return request;
    }

    auto CreateReplaceDeviceRequest(TString diskId, TString deviceId)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvReplaceDeviceRequest>();
        request->Record.SetDiskId(std::move(diskId));
        request->Record.SetDeviceUUID(std::move(deviceId));

        return request;
    }

    auto CreateCmsActionRequest(TVector<NProto::TAction> requests)
    {
        auto request = std::make_unique<TEvService::TEvCmsActionRequest>();
        for (const auto& r: requests) {
            auto& cmsAction = *request->Record.MutableActions()->Add();
            cmsAction = std::move(r);
        }
        return request;
    }

    auto CreateChangeAgentStateRequest(
        TString agentId,
        NProto::EAgentState state)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvChangeAgentStateRequest>();
        request->Record.SetAgentId(std::move(agentId));
        request->Record.SetAgentState(state);

        return request;
    }

    auto CreateChangeDeviceStateRequest(
        TString deviceId,
        NProto::EDeviceState state)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvChangeDeviceStateRequest>();
        request->Record.SetDeviceUUID(std::move(deviceId));
        request->Record.SetDeviceState(state);
        request->Record.SetReason("test");

        return request;
    }

    auto CreateBackupDiskRegistryStateRequest(bool localDB)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvBackupDiskRegistryStateRequest>();
        request->Record.SetBackupLocalDB(localDB);

        return request;
    }

    auto CreateRestoreDiskRegistryStateRequest(
        NProto::TDiskRegistryStateBackup backup,
        bool force)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvRestoreDiskRegistryStateRequest>();
        request->Record.MutableBackup()->Swap(&backup);
        request->Record.SetForce(force);

        return request;
    }

    auto CreateFinishMigrationRequest(
        const TString& diskId,
        const TString& sourceId,
        const TString& targetId)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvFinishMigrationRequest>();

        request->Record.SetDiskId(diskId);
        auto& m = *request->Record.AddMigrations();
        m.SetSourceDeviceId(sourceId);
        m.SetTargetDeviceId(targetId);

        return request;
    }

    auto CreateMarkDiskForCleanupRequest(const TString& diskId)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvMarkDiskForCleanupRequest>();
        request->Record.SetDiskId(diskId);
        return request;
    }

    auto CreateSetUserIdRequest(const TString& diskId, const TString& userId)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvSetUserIdRequest>();
        request->Record.SetDiskId(diskId);
        request->Record.SetUserId(userId);
        return request;
    }

    auto CreateQueryAvailableStorageRequest(
        TVector<TString> agentIds,
        TString poolName,
        NProto::EStoragePoolKind poolKind)
    {
        auto request = std::make_unique<TEvService::TEvQueryAvailableStorageRequest>();

        request->Record.SetStoragePoolName(poolName);
        request->Record.SetStoragePoolKind(poolKind);
        request->Record.MutableAgentIds()->Assign(
            agentIds.begin(),
            agentIds.end());

        return request;
    }

    auto CreateResumeDeviceRequest(
        TString agentId,
        TString path,
        bool dryRun = false)
    {
        auto request =
            std::make_unique<TEvService::TEvResumeDeviceRequest>();

        request->Record.SetAgentId(std::move(agentId));
        request->Record.SetPath(std::move(path));
        request->Record.SetDryRun(dryRun);

        return request;
    }

    auto CreateUpdateDiskBlockSizeRequest(
        TString diskId,
        ui32 blockSize,
        bool force)
    {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvUpdateDiskBlockSizeRequest>();

        request->Record.SetDiskId(std::move(diskId));
        request->Record.SetBlockSize(blockSize);
        request->Record.SetForce(force);

        return request;
    }

    auto CreateSuspendDeviceRequest(TString deviceId)
    {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvSuspendDeviceRequest>();

        request->Record.SetDeviceId(std::move(deviceId));

        return request;
    }

    auto CreateRestoreDiskRegistryStateRequest(const TString& backupJson)
    {
        auto request = std::make_unique<
            TEvDiskRegistry::TEvRestoreDiskRegistryStateRequest>();

        google::protobuf::util::JsonStringToMessage(
            backupJson,
            &request->Record);

        return request;
    }

    auto CreateCreateDiskFromDevicesRequest(
        const TString& diskId,
        ui32 blockSize,
        TVector<NProto::TDeviceConfig> devices,
        bool force = false)
    {
        auto request = std::make_unique<
            TEvDiskRegistry::TEvCreateDiskFromDevicesRequest>();

        request->Record.MutableVolumeConfig()->SetDiskId(diskId);
        request->Record.MutableVolumeConfig()->SetBlockSize(blockSize);

        request->Record.SetForce(force);
        request->Record.MutableDevices()->Assign(devices.begin(), devices.end());

        return request;
    }

    auto CreateStartForceMigrationRequest(
        const TString& sourceDiskId,
        const TString& sourceDeviceId,
        const TString& targetDeviceId)
    {
        auto request = std::make_unique<
            TEvDiskRegistry::TEvStartForceMigrationRequest>();

        request->Record.SetSourceDiskId(sourceDiskId);
        request->Record.SetSourceDeviceId(sourceDeviceId);
        request->Record.SetTargetDeviceId(targetDeviceId);

        return request;
    }

    auto CreateAddLaggingDevicesRequest(
        const TString& diskId,
        TVector<NProto::TLaggingDevice> laggingDevices)
    {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvAddLaggingDevicesRequest>();
        request->Record.SetDiskId(diskId);
        for (auto& laggingDevice: laggingDevices) {
            *request->Record.AddLaggingDevices() = std::move(laggingDevice);
        }

        return request;
    }

    auto CreateUpdateDiskRegistryAgentListParamsRequest(
        const TVector<TString>& agentIds,
        TDuration newNonReplicatedAgentMinTimeoutMs,
        TDuration newNonReplicatedAgentMaxTimeoutMs,
        TDuration timeout)
    {
        auto request = std::make_unique<TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsRequest>();
        auto& params = *request->Record.MutableParams();

        params.MutableAgentIds()->Assign(agentIds.begin(), agentIds.end());
        params.SetNewNonReplicatedAgentMinTimeoutMs(newNonReplicatedAgentMinTimeoutMs.MilliSeconds());
        params.SetNewNonReplicatedAgentMaxTimeoutMs(newNonReplicatedAgentMaxTimeoutMs.MilliSeconds());
        params.SetTimeoutMs(timeout.MilliSeconds());

        return request;
    }

    auto CreateDisableAgentRequest(const TString& agentId) {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvDisableAgentRequest>();
        request->Record.SetAgentId(agentId);

        return request;
    }

    auto CreateGetClusterCapacityRequest() {
        auto request = std::make_unique<TEvDiskRegistry::TEvGetClusterCapacityRequest>();

        return request;
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(std::move(request));                                       \
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
        SendRequest(std::move(request));                                       \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_DISK_REGISTRY_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvDiskRegistry)
    BLOCKSTORE_DISK_REGISTRY_REQUESTS_FWD_SERVICE(BLOCKSTORE_DECLARE_METHOD, TEvService)
    BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_METHOD, TEvDiskRegistryPrivate)

#undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistryTest
