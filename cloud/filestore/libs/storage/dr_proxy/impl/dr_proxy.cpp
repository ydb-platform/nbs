#include "dr_proxy.h"

#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/dr_proxy/api/service.h>

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/volume.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>
#include <cloud/storage/core/libs/kikimr/tenant.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

using TEvDiskRegistry = NBlockStore::NStorage::TEvDiskRegistry;
using TEvVolume = NBlockStore::NStorage::TEvVolume;
using TEvHiveProxy = NCloud::NStorage::TEvHiveProxy;
using NCloud::NStorage::MakeHiveProxyServiceId;

namespace {

////////////////////////////////////////////////////////////////////////////////

static_assert(
    static_cast<int>(TEvDeviceService::EvLayoutChangedRequest) ==
        static_cast<int>(TEvVolume::EvReallocateDiskRequest),
    "LayoutChangedRequest must be a wire duplicate of ReallocateDiskRequest");
static_assert(
    static_cast<int>(TEvDeviceService::EvLayoutChangedResponse) ==
        static_cast<int>(TEvVolume::EvReallocateDiskResponse),
    "LayoutChangedResponse must be a wire duplicate of ReallocateDiskResponse");

////////////////////////////////////////////////////////////////////////////////

TResultOrError<NProto::EStorageMediaKind> MediaKindForDeviceCount(
    ui32 deviceCount)
{
    switch (deviceCount) {
        case 1: return NProto::STORAGE_MEDIA_SSD_NONREPLICATED;
        case 2: return NProto::STORAGE_MEDIA_SSD_MIRROR2;
        case 3: return NProto::STORAGE_MEDIA_SSD_MIRROR3;
    }

    return MakeError(
        E_ARGUMENT,
        TStringBuilder() << "unsupported device count: " << deviceCount);
}

TString ReplicaDiskId(
    const TString& fileSystemId,
    ui32 deviceCount,
    ui32 replicaIndex)
{
    if (deviceCount < 2) {
        return fileSystemId;
    }
    return TStringBuilder() << fileSystemId << "/" << replicaIndex;
}

////////////////////////////////////////////////////////////////////////////////

class TDRProxyActor final
    : public NActors::TActorBootstrapped<TDRProxyActor>
{
private:
    enum EConnectionState
    {
        DISCONNECTED,
        RESOLVING,
        CONNECTING,
        CONNECTED,
    };

    const TStorageConfigPtr Config;
    ui64 DiskRegistryTabletId;

    EConnectionState State = DISCONNECTED;
    NActors::TActorId TabletClientId;
    TDeque<NActors::IEventHandlePtr> PendingRequests;

    // Cookie of the last request or reconnect timer issued; a lookup shares
    // it with its timeout timer. A late answer or an expired timer carries
    // an older one.
    ui64 RequestId = 0;
    THashMap<ui64, NActors::IEventHandlePtr> ActiveRequests;

public:
    TDRProxyActor(TStorageConfigPtr config);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void Connect(const NActors::TActorContext& ctx);

    void LookupTablet(const NActors::TActorContext& ctx);
    void HandleLookupTabletResponse(
        const TEvHiveProxy::TEvLookupTabletResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void CreateClient(const NActors::TActorContext& ctx);
    void HandleClientConnected(
        NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);
    void StartConnection(const NActors::TActorContext& ctx);
    void HandleClientDestroyed(
        NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);
    // Ends the waiting requests with the error and connects again later.
    void OnConnectionError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void ScheduleWakeup(const NActors::TActorContext& ctx, TDuration delay);
    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void PostponeRequest(NActors::IEventHandlePtr ev);
    void ProcessPendingRequests();

    template <typename TRequest>
    void ForwardRequest(
        const NActors::TActorContext& ctx,
        NActors::IEventHandlePtr request,
        std::unique_ptr<TRequest> diskRegistryRequest);

    NActors::IEventHandlePtr GetRequestByCookie(ui64 cookie);

    // False for an event that is not a device service request.
    bool ReplyError(
        const NActors::TActorContext& ctx,
        const NActors::IEventHandle& request,
        const NProto::TError& error);

    void CancelRequests(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    template <typename TResponse, typename TRecord>
    void Reply(
        const NActors::TActorContext& ctx,
        ui64 cookie,
        const TRecord& record);

    template <typename TResponse, typename TRecord>
    void ReplyNoFields(
        const NActors::TActorContext& ctx,
        ui64 cookie,
        const TRecord& record);

    NProtoPrivate::TStorageDevice ToStorageDevice(
        const NBlockStore::NProto::TDeviceConfig& device) const;

    FILESTORE_DEVICE_SERVICE_REQUESTS(
        FILESTORE_IMPLEMENT_REQUEST,
        TEvDeviceService)

    void HandleAllocateDiskResponse(
        const TEvDiskRegistry::TEvAllocateDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleDescribeDiskResponse(
        const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleMarkDiskForCleanupResponse(
        const TEvDiskRegistry::TEvMarkDiskForCleanupResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleDeallocateDiskResponse(
        const TEvDiskRegistry::TEvDeallocateDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleMarkReplacementDeviceResponse(
        const TEvDiskRegistry::TEvMarkReplacementDeviceResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleFinishMigrationResponse(
        const TEvDiskRegistry::TEvFinishMigrationResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleReplaceDeviceResponse(
        const TEvDiskRegistry::TEvReplaceDeviceResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    STFUNC(StateWork);
    STFUNC(StateBroken);
};

////////////////////////////////////////////////////////////////////////////////

TDRProxyActor::TDRProxyActor(TStorageConfigPtr config)
    : Config(std::move(config))
    , DiskRegistryTabletId(Config->GetFastShardDRTabletId())
{}

void TDRProxyActor::Bootstrap(const TActorContext& ctx)
{
    NProto::TError error;
    if (!Config->GetFastShardDRTabletId() &&
        !Config->GetFastShardDROwner())
    {
        error = MakeError(
            E_INVALID_STATE,
            "disk registry is not configured");
    }

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::DR_PROXY,
            "DR proxy is not operational: %s",
            FormatError(error).c_str());

        Become(&TThis::StateBroken);
        return;
    }

    Become(&TThis::StateWork);
    Connect(ctx);
}

void TDRProxyActor::Connect(const TActorContext& ctx)
{
    if (DiskRegistryTabletId) {
        CreateClient(ctx);
    } else {
        LookupTablet(ctx);
    }
}

void TDRProxyActor::LookupTablet(const TActorContext& ctx)
{
    ui64 hiveTabletId = Config->GetTenantHiveTabletId();
    if (!hiveTabletId) {
        hiveTabletId = NCloud::NStorage::GetHiveTabletId(ctx);
    }

    NCloud::Send<TEvHiveProxy::TEvLookupTabletRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        ++RequestId,
        hiveTabletId,
        Config->GetFastShardDROwner(),
        Config->GetFastShardDROwnerIdx());

    ScheduleWakeup(ctx, Config->GetFastShardDRLookupTimeout());
    State = RESOLVING;

    LOG_INFO(
        ctx,
        TFileStoreComponents::DR_PROXY,
        "Looking up disk registry %lu:%lu in hive %lu",
        Config->GetFastShardDROwner(),
        Config->GetFastShardDROwnerIdx(),
        hiveTabletId);
}

void TDRProxyActor::HandleLookupTabletResponse(
    const TEvHiveProxy::TEvLookupTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Cookie != RequestId) {
        LOG_INFO(ctx, TFileStoreComponents::DR_PROXY,
            "ignoring expired tablet lookup response: "
            "cookie %lu, request id %lu",
            ev->Cookie,
            RequestId);
        return;
    }

    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        auto error = MakeError(
            E_REJECTED,
            TStringBuilder() << "cannot find disk registry tablet: "
                << FormatError(msg->GetError()));

        LOG_ERROR(
            ctx,
            TFileStoreComponents::DR_PROXY,
            "%s",
            FormatError(error).c_str());
        OnConnectionError(ctx, error);
        return;
    }

    DiskRegistryTabletId = msg->TabletId;
    CreateClient(ctx);
}

void TDRProxyActor::CreateClient(const TActorContext& ctx)
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = Config->GetPipeClientRetryCount(),
        .MinRetryTime = Config->GetPipeClientMinRetryTime(),
        .MaxRetryTime = Config->GetPipeClientMaxRetryTime()
    };

    TabletClientId = ctx.Register(NTabletPipe::CreateClient(
        ctx.SelfID,
        DiskRegistryTabletId,
        clientConfig));
    State = CONNECTING;

    LOG_INFO(ctx, TFileStoreComponents::DR_PROXY,
        "Connecting to disk registry tablet: %lu",
        DiskRegistryTabletId);
}

void TDRProxyActor::HandleClientConnected(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (msg->ClientId != TabletClientId) {
        LOG_INFO(ctx, TFileStoreComponents::DR_PROXY,
            "ignoring expired tablet pipe connection: "
            "cookie %lu, request id %lu",
            ev->Cookie,
            RequestId);

        return;
    }

    if (msg->Status == NKikimrProto::OK) {
        StartConnection(ctx);
        return;
    }

    auto error = MakeError(
        E_REJECTED,
        TStringBuilder() << "cannot connect to disk registry "
            << msg->TabletId << ": "
            << NKikimrProto::EReplyStatus_Name(msg->Status));

    LOG_ERROR(
        ctx,
        TFileStoreComponents::DR_PROXY,
        "%s",
        FormatError(error).c_str());

    OnConnectionError(ctx, error);
}

void TDRProxyActor::StartConnection(const TActorContext& ctx)
{
    LOG_INFO(ctx, TFileStoreComponents::DR_PROXY,
        "Connected to disk registry %lu",
        DiskRegistryTabletId);

    State = CONNECTED;
    ProcessPendingRequests();
}

void TDRProxyActor::HandleClientDestroyed(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Get()->ClientId != TabletClientId) {
        return;   // a client already given up on
    }

    LOG_WARN(
        ctx,
        TFileStoreComponents::DR_PROXY,
        "Connection to disk registry %lu broken",
        DiskRegistryTabletId);
    OnConnectionError(
        ctx,
        MakeError(E_REJECTED, "disk registry connection broken"));
}

void TDRProxyActor::OnConnectionError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    ++RequestId;
    State = DISCONNECTED;
    if (TabletClientId) {
        NTabletPipe::CloseClient(ctx, TabletClientId);
        TabletClientId = {};
    }

    CancelRequests(ctx, error);
    ScheduleWakeup(ctx, Config->GetPipeClientMinRetryTime());
}

void TDRProxyActor::ScheduleWakeup(const TActorContext& ctx, TDuration delay)
{
    ctx.Schedule(
        delay,
        std::make_unique<IEventHandle>(
            ctx.SelfID,
            ctx.SelfID,
            new TEvents::TEvWakeup(),
            0,   // flags
            RequestId));
}

void TDRProxyActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Cookie != RequestId) {
        LOG_INFO(ctx, TFileStoreComponents::DR_PROXY,
            "ignoring expired wakeup: cookie %lu, request id %lu",
            ev->Cookie,
            RequestId);
        return;
    }

    switch (State) {
        case RESOLVING: {
            auto error = MakeError(
                E_REJECTED,
                "disk registry hive lookup timed out");

            LOG_ERROR(
                ctx,
                TFileStoreComponents::DR_PROXY,
                "%s",
                FormatError(error).c_str());

            OnConnectionError(ctx, error);
            break;
        }

        case DISCONNECTED:
            Connect(ctx);
            break;

        case CONNECTING:
        case CONNECTED:
            break;   // the lookup timeout after a successful lookup
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDRProxyActor::PostponeRequest(IEventHandlePtr ev)
{
    PendingRequests.emplace_back(std::move(ev));
}

void TDRProxyActor::ProcessPendingRequests()
{
    auto requests = std::move(PendingRequests);
    PendingRequests.clear();

    for (auto& request: requests) {
        TAutoPtr<IEventHandle> handle(request.release());
        Receive(handle);
    }
}

template <typename TRequest>
void TDRProxyActor::ForwardRequest(
    const TActorContext& ctx,
    IEventHandlePtr request,
    std::unique_ptr<TRequest> diskRegistryRequest)
{
    const ui64 cookie = ++RequestId;

    auto event = std::make_unique<IEventHandle>(
        SelfId(),
        SelfId(),
        diskRegistryRequest.release(),
        0,   // flags
        cookie);

    NCloud::PipeSend(ctx, TabletClientId, std::move(event));
    ActiveRequests.emplace(cookie, std::move(request));
}

IEventHandlePtr TDRProxyActor::GetRequestByCookie(ui64 cookie)
{
    auto it = ActiveRequests.find(cookie);
    if (it == ActiveRequests.end()) {
        return nullptr;
    }

    auto request = std::move(it->second);
    ActiveRequests.erase(it);
    return request;
}

bool TDRProxyActor::ReplyError(
    const TActorContext& ctx,
    const IEventHandle& request,
    const NProto::TError& error)
{
    switch (request.GetTypeRewrite()) {
#define FILESTORE_REPLY_WITH_ERROR(name, ...)                                  \
        case TEvDeviceService::Ev##name##Request: {                            \
            NCloud::Reply(                                                     \
                ctx,                                                           \
                request,                                                       \
                std::make_unique<TEvDeviceService::TEv##name##Response>(       \
                    error));                                                   \
            return true;                                                       \
        }                                                                      \
// FILESTORE_REPLY_WITH_ERROR

        FILESTORE_DEVICE_SERVICE_REQUESTS(FILESTORE_REPLY_WITH_ERROR)

#undef FILESTORE_REPLY_WITH_ERROR

        default:
            return false;
    }
}

void TDRProxyActor::CancelRequests(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    const auto pending = std::move(PendingRequests);
    PendingRequests.clear();
    for (const auto& request: pending) {
        ReplyError(ctx, *request, error);
    }

    const auto active = std::move(ActiveRequests);
    ActiveRequests.clear();
    for (const auto& [cookie, request]: active) {
        ReplyError(ctx, *request, error);
    }
}

template <typename TResponse, typename TRecord>
void TDRProxyActor::Reply(
    const TActorContext& ctx,
    ui64 cookie,
    const TRecord& record)
{
    const auto request = GetRequestByCookie(cookie);
    if (!request) {
        return;
    }

    if (HasError(record.GetError())) {
        NCloud::Reply(
            ctx,
            *request,
            std::make_unique<TResponse>(record.GetError()));
        return;
    }

    TEvDeviceService::TDeviceLayout layout;

    auto& main = layout.Replicas.emplace_back();
    for (const auto& device: record.GetDevices()) {
        main.push_back(ToStorageDevice(device));
    }
    for (const auto& replica: record.GetReplicas()) {
        auto& devices = layout.Replicas.emplace_back();
        for (const auto& device: replica.GetDevices()) {
            devices.push_back(ToStorageDevice(device));
        }
    }

    for (const auto& migration: record.GetMigrations()) {
        layout.Migrations.push_back({
            .SourceUUID = migration.GetSourceDeviceId(),
            .Target = ToStorageDevice(migration.GetTargetDevice()),
        });
    }

    layout.ReplacementDeviceUUIDs.assign(
        record.GetDeviceReplacementUUIDs().begin(),
        record.GetDeviceReplacementUUIDs().end());

    // Only the AllocateDisk response reports unavailable devices.
    if constexpr (requires { record.GetUnavailableDeviceUUIDs(); }) {
        layout.UnavailableDeviceUUIDs.assign(
            record.GetUnavailableDeviceUUIDs().begin(),
            record.GetUnavailableDeviceUUIDs().end());
    }

    NCloud::Reply(ctx, *request, std::make_unique<TResponse>(std::move(layout)));
}

template <typename TResponse, typename TRecord>
void TDRProxyActor::ReplyNoFields(
    const TActorContext& ctx,
    ui64 cookie,
    const TRecord& record)
{
    if (const auto request = GetRequestByCookie(cookie)) {
        NCloud::Reply(
            ctx,
            *request,
            std::make_unique<TResponse>(record.GetError()));
    }
}

NProtoPrivate::TStorageDevice TDRProxyActor::ToStorageDevice(
    const NBlockStore::NProto::TDeviceConfig& device) const
{
    NProtoPrivate::TStorageDevice storageDevice;
    storageDevice.SetHost(device.GetJournalledEndpoint().GetHost());
    storageDevice.SetPort(device.GetJournalledEndpoint().GetPort());
    storageDevice.SetDeviceId(device.GetDeviceUUID());
    return storageDevice;
}

////////////////////////////////////////////////////////////////////////////////

void TDRProxyActor::HandleAllocateDevices(
    const TEvDeviceService::TEvAllocateDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto mediaKind = MediaKindForDeviceCount(msg->DeviceCount);
    if (HasError(mediaKind)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDeviceService::TEvAllocateDevicesResponse>(
                mediaKind.GetError()));
        return;
    }

    if (State != CONNECTED) {
        PostponeRequest(IEventHandlePtr(ev.Release()));
        return;
    }

    auto request = std::make_unique<TEvDiskRegistry::TEvAllocateDiskRequest>();
    auto& record = request->Record;
    record.SetDiskId(msg->FileSystemId);
    // TODO(issue-7373): restore when DR supports OwnerVolumeTabletId
    // record.SetOwnerVolumeTabletId(msg->TabletId);
    record.SetCloudId(msg->CloudId);
    record.SetFolderId(msg->FolderId);
    record.SetBlockSize(DefaultBlockSize);
    record.SetBlocksCount(msg->DeviceBlocksCount);
    record.SetReplicaCount(msg->DeviceCount - 1);
    record.SetStorageMediaKind(mediaKind.GetResult());
    record.SetPoolName(msg->DevicePoolName);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDRProxyActor::HandleDescribeDevices(
    const TEvDeviceService::TEvDescribeDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (State != CONNECTED) {
        PostponeRequest(IEventHandlePtr(ev.Release()));
        return;
    }

    auto request = std::make_unique<TEvDiskRegistry::TEvDescribeDiskRequest>();
    request->Record.SetDiskId(ev->Get()->FileSystemId);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDRProxyActor::HandleMarkForCleanup(
    const TEvDeviceService::TEvMarkForCleanupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (State != CONNECTED) {
        PostponeRequest(IEventHandlePtr(ev.Release()));
        return;
    }

    auto request =
        std::make_unique<TEvDiskRegistry::TEvMarkDiskForCleanupRequest>();
    request->Record.SetDiskId(msg->FileSystemId);
    // TODO(issue-7373): restore when DR supports OwnerVolumeTabletId
    // request->Record.SetOwnerVolumeTabletId(msg->TabletId);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDRProxyActor::HandleDeallocateDevices(
    const TEvDeviceService::TEvDeallocateDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (State != CONNECTED) {
        PostponeRequest(IEventHandlePtr(ev.Release()));
        return;
    }

    auto request =
        std::make_unique<TEvDiskRegistry::TEvDeallocateDiskRequest>();
    request->Record.SetDiskId(msg->FileSystemId);
    // TODO(issue-7373): restore when DR supports OwnerVolumeTabletId
    // request->Record.SetOwnerVolumeTabletId(msg->TabletId);
    request->Record.SetSync(false);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDRProxyActor::HandleFinishRepair(
    const TEvDeviceService::TEvFinishRepairRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (State != CONNECTED) {
        PostponeRequest(IEventHandlePtr(ev.Release()));
        return;
    }

    auto request =
        std::make_unique<TEvDiskRegistry::TEvMarkReplacementDeviceRequest>();
    request->Record.SetDiskId(msg->FileSystemId);
    request->Record.SetDeviceId(msg->DeviceUUID);
    request->Record.SetIsReplacement(false);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDRProxyActor::HandleFinishMigration(
    const TEvDeviceService::TEvFinishMigrationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (State != CONNECTED) {
        PostponeRequest(IEventHandlePtr(ev.Release()));
        return;
    }

    auto request =
        std::make_unique<TEvDiskRegistry::TEvFinishMigrationRequest>();
    request->Record.SetDiskId(
        ReplicaDiskId(msg->FileSystemId, msg->DeviceCount, msg->ReplicaIndex));
    auto* migration = request->Record.AddMigrations();
    migration->SetSourceDeviceId(msg->SourceUUID);
    migration->SetTargetDeviceId(msg->TargetUUID);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDRProxyActor::HandleReplaceDevice(
    const TEvDeviceService::TEvReplaceDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (State != CONNECTED) {
        PostponeRequest(IEventHandlePtr(ev.Release()));
        return;
    }

    auto request = std::make_unique<TEvDiskRegistry::TEvReplaceDeviceRequest>();
    request->Record.SetDiskId(
        ReplicaDiskId(msg->FileSystemId, msg->DeviceCount, msg->ReplicaIndex));
    request->Record.SetDeviceUUID(msg->DeviceUUID);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TDRProxyActor::HandleAllocateDiskResponse(
    const TEvDiskRegistry::TEvAllocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Reply<TEvDeviceService::TEvAllocateDevicesResponse>(
        ctx,
        ev->Cookie,
        ev->Get()->Record);
}

void TDRProxyActor::HandleDescribeDiskResponse(
    const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Reply<TEvDeviceService::TEvDescribeDevicesResponse>(
        ctx,
        ev->Cookie,
        ev->Get()->Record);
}

void TDRProxyActor::HandleMarkDiskForCleanupResponse(
    const TEvDiskRegistry::TEvMarkDiskForCleanupResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDeviceService::TEvMarkForCleanupResponse>(
        ctx,
        ev->Cookie,
        ev->Get()->Record);
}

void TDRProxyActor::HandleDeallocateDiskResponse(
    const TEvDiskRegistry::TEvDeallocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDeviceService::TEvDeallocateDevicesResponse>(
        ctx,
        ev->Cookie,
        ev->Get()->Record);
}

void TDRProxyActor::HandleMarkReplacementDeviceResponse(
    const TEvDiskRegistry::TEvMarkReplacementDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDeviceService::TEvFinishRepairResponse>(
        ctx,
        ev->Cookie,
        ev->Get()->Record);
}

void TDRProxyActor::HandleFinishMigrationResponse(
    const TEvDiskRegistry::TEvFinishMigrationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDeviceService::TEvFinishMigrationResponse>(
        ctx,
        ev->Cookie,
        ev->Get()->Record);
}

void TDRProxyActor::HandleReplaceDeviceResponse(
    const TEvDiskRegistry::TEvReplaceDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDeviceService::TEvReplaceDeviceResponse>(
        ctx,
        ev->Cookie,
        ev->Get()->Record);
}

void TDRProxyActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    CancelRequests(ctx, MakeError(E_REJECTED, "DR proxy is stopping"));
    if (TabletClientId) {
        NTabletPipe::CloseClient(ctx, TabletClientId);
    }

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDRProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        FILESTORE_DEVICE_SERVICE_REQUESTS(
            FILESTORE_HANDLE_REQUEST,
            TEvDeviceService)

        HFunc(
            TEvDiskRegistry::TEvAllocateDiskResponse,
            HandleAllocateDiskResponse);
        HFunc(
            TEvDiskRegistry::TEvDescribeDiskResponse,
            HandleDescribeDiskResponse);
        HFunc(
            TEvDiskRegistry::TEvMarkDiskForCleanupResponse,
            HandleMarkDiskForCleanupResponse);
        HFunc(
            TEvDiskRegistry::TEvDeallocateDiskResponse,
            HandleDeallocateDiskResponse);
        HFunc(
            TEvDiskRegistry::TEvMarkReplacementDeviceResponse,
            HandleMarkReplacementDeviceResponse);
        HFunc(
            TEvDiskRegistry::TEvFinishMigrationResponse,
            HandleFinishMigrationResponse);
        HFunc(
            TEvDiskRegistry::TEvReplaceDeviceResponse,
            HandleReplaceDeviceResponse);

        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(
            TEvHiveProxy::TEvLookupTabletResponse,
            HandleLookupTabletResponse);
        HFunc(TEvTabletPipe::TEvClientConnected, HandleClientConnected);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleClientDestroyed);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::DR_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TDRProxyActor::StateBroken)
{
    static const NProto::TError error = MakeError(E_INVALID_STATE, "DR Proxy is broken");

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            if (!ReplyError(ActorContext(), *ev, error)) {
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::DR_PROXY,
                    __PRETTY_FUNCTION__);
            }

            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDRProxy(TStorageConfigPtr config)
{
    return std::make_unique<TDRProxyActor>(std::move(config));
}

}   // namespace NCloud::NFileStore::NStorage
