#include "disk_registry_proxy.h"

#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/disk_registry_proxy/api/service.h>

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

#include <util/generic/hash.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

using NBlockStore::NStorage::TEvDiskRegistry;
using NBlockStore::NStorage::TEvVolume;
using NCloud::NStorage::MakeHiveProxyServiceId;
using NCloud::NStorage::TEvHiveProxy;

namespace {

////////////////////////////////////////////////////////////////////////////////

static_assert(
    static_cast<int>(TEvDiskRegistryProxy::EvLayoutChangedRequest) ==
        static_cast<int>(TEvVolume::EvReallocateDiskRequest),
    "LayoutChangedRequest must be a wire duplicate of ReallocateDiskRequest");
static_assert(
    static_cast<int>(TEvDiskRegistryProxy::EvLayoutChangedResponse) ==
        static_cast<int>(TEvVolume::EvReallocateDiskResponse),
    "LayoutChangedResponse must be a wire duplicate of ReallocateDiskResponse");

////////////////////////////////////////////////////////////////////////////////

TString DiskId(
    const TString& fileSystemId,
    const std::optional<ui32>& replicaIndex)
{
    if (!replicaIndex) {
        return fileSystemId;
    }

    return TStringBuilder() << fileSystemId << "/" << *replicaIndex;
}

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryProxyActor final
    : public NActors::TActorBootstrapped<TDiskRegistryProxyActor>
{
private:
    const TStorageConfigPtr Config;
    ui64 DiskRegistryTabletId = 0;

    NActors::TActorId TabletClientId;

    ui64 RequestId = 0;
    THashMap<ui64, NActors::IEventHandlePtr> ActiveRequests;

public:
    TDiskRegistryProxyActor(TStorageConfigPtr config);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void LookupTablet(const NActors::TActorContext& ctx);
    void HandleLookupTabletResponse(
        const TEvHiveProxy::TEvLookupTabletResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ScheduleWakeup(const NActors::TActorContext& ctx, TDuration delay);
    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void CreateClient(const NActors::TActorContext& ctx);

    void HandleClientConnected(
        NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleClientDestroyed(
        NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);
    // Ends the waiting requests with the error and connects again later.
    void OnConnectionError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);
    void CancelActiveRequests(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);
    bool ReplyError(
        const NActors::TActorContext& ctx,
        const NActors::IEventHandle& request,
        NProto::TError error);

    template <typename TRequest>
    void ForwardRequest(
        const NActors::TActorContext& ctx,
        NActors::IEventHandlePtr request,
        std::unique_ptr<TRequest> diskRegistryRequest);

    NActors::IEventHandlePtr GetActiveRequest(ui64 cookie);

    template <typename TResponse, typename TRecord>
    void Reply(
        const NActors::TActorContext& ctx,
        ui64 cookie,
        TRecord&& record);

    template <typename TResponse, typename TRecord>
    void ReplyNoFields(
        const NActors::TActorContext& ctx,
        ui64 cookie,
        const TRecord& record);

    NProtoPrivate::TStorageDevice ToStorageDevice(
        NBlockStore::NProto::TDeviceConfig&& device) const;

    FILESTORE_DISK_REGISTRY_PROXY_REQUESTS(
        FILESTORE_IMPLEMENT_REQUEST,
        TEvDiskRegistryProxy)

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

    STFUNC(StateLookup);
    STFUNC(StateWork);
    STFUNC(StateBroken);
};

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryProxyActor::TDiskRegistryProxyActor(TStorageConfigPtr config)
    : Config(std::move(config))
    , DiskRegistryTabletId(Config->GetFastShardDiskRegistryTabletId())
{}

void TDiskRegistryProxyActor::Bootstrap(const TActorContext& ctx)
{
    if (!DiskRegistryTabletId && !Config->GetFastShardDiskRegistryOwner()) {
        auto error = MakeError(
            E_INVALID_STATE,
            "disk registry is not configured");

        LOG_ERROR(
            ctx,
            TFileStoreComponents::DISK_REGISTRY_PROXY,
            "Disk registry proxy is not operational: %s",
            FormatError(error).c_str());

        Become(&TThis::StateBroken);
        return;
    }

    if (DiskRegistryTabletId) {
        CreateClient(ctx);
        Become(&TThis::StateWork);
        return;
    }

    LookupTablet(ctx);
    Become(&TThis::StateLookup);
}

void TDiskRegistryProxyActor::LookupTablet(const TActorContext& ctx)
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
        Config->GetFastShardDiskRegistryOwner(),
        Config->GetFastShardDiskRegistryOwnerIdx());

    ScheduleWakeup(ctx, Config->GetFastShardDiskRegistryLookupTimeout());

    LOG_INFO(
        ctx,
        TFileStoreComponents::DISK_REGISTRY_PROXY,
        "Looking up disk registry %lu:%lu in hive %lu",
        Config->GetFastShardDiskRegistryOwner(),
        Config->GetFastShardDiskRegistryOwnerIdx(),
        hiveTabletId);
}

void TDiskRegistryProxyActor::HandleLookupTabletResponse(
    const TEvHiveProxy::TEvLookupTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Cookie != RequestId) {
        LOG_INFO(ctx, TFileStoreComponents::DISK_REGISTRY_PROXY,
            "ignoring expired tablet lookup response: "
            "cookie %lu, request id %lu",
            ev->Cookie,
            RequestId);
        return;
    }

    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        // The scheduled wakeup looks the tablet up again.
        LOG_ERROR(
            ctx,
            TFileStoreComponents::DISK_REGISTRY_PROXY,
            "Cannot find the disk registry tablet: %s",
            FormatError(msg->GetError()).c_str());
        return;
    }

    DiskRegistryTabletId = msg->TabletId;
    CreateClient(ctx);
    Become(&TThis::StateWork);
}

void TDiskRegistryProxyActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_ERROR(
        ctx,
        TFileStoreComponents::DISK_REGISTRY_PROXY,
        "Disk registry lookup timed out");

    LookupTablet(ctx);
}

void TDiskRegistryProxyActor::CreateClient(const TActorContext& ctx)
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

    LOG_INFO(ctx, TFileStoreComponents::DISK_REGISTRY_PROXY,
        "Connecting to disk registry tablet: %lu",
        DiskRegistryTabletId);
}

void TDiskRegistryProxyActor::HandleClientConnected(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (msg->ClientId != TabletClientId) {
        LOG_INFO(ctx, TFileStoreComponents::DISK_REGISTRY_PROXY,
            "ignoring an expired tablet pipe %s",
            ToString(msg->ClientId).c_str());

        return;
    }

    if (msg->Status != NKikimrProto::OK) {
        auto error = MakeError(
            E_REJECTED,
            TStringBuilder() << "failed to connect to disk registry "
                << msg->TabletId << ": "
                << NKikimrProto::EReplyStatus_Name(msg->Status));

        LOG_ERROR(
            ctx,
            TFileStoreComponents::DISK_REGISTRY_PROXY,
            "%s",
            FormatError(error).c_str());

        OnConnectionError(ctx, error);
    }
}

void TDiskRegistryProxyActor::HandleClientDestroyed(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Get()->ClientId != TabletClientId) {
        return;   // a client already given up on
    }

    LOG_ERROR(
        ctx,
        TFileStoreComponents::DISK_REGISTRY_PROXY,
        "Connection to disk registry %lu broken: %s",
        DiskRegistryTabletId,
        ev->Get()->ToString().c_str());

    OnConnectionError(
        ctx,
        MakeError(E_REJECTED, "disk registry connection broken"));
}

void TDiskRegistryProxyActor::OnConnectionError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    ++RequestId;
    if (TabletClientId) {
        NTabletPipe::CloseClient(ctx, TabletClientId);
        TabletClientId = {};
    }

    CancelActiveRequests(ctx, error);
}

void TDiskRegistryProxyActor::ScheduleWakeup(
    const TActorContext& ctx,
    TDuration delay)
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

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
void TDiskRegistryProxyActor::ForwardRequest(
    const TActorContext& ctx,
    IEventHandlePtr request,
    std::unique_ptr<TRequest> diskRegistryRequest)
{
    if (!TabletClientId) {
        CreateClient(ctx);
    }

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

IEventHandlePtr TDiskRegistryProxyActor::GetActiveRequest(ui64 cookie)
{
    auto it = ActiveRequests.find(cookie);
    if (it == ActiveRequests.end()) {
        return nullptr;
    }

    auto request = std::move(it->second);
    ActiveRequests.erase(it);
    return request;
}

bool TDiskRegistryProxyActor::ReplyError(
    const TActorContext& ctx,
    const IEventHandle& request,
    NProto::TError error)
{
    switch (request.GetTypeRewrite()) {
#define FILESTORE_REPLY_WITH_ERROR(name, ...)                                  \
        case TEvDiskRegistryProxy::Ev##name##Request: {                        \
            NCloud::Reply(                                                     \
                ctx,                                                           \
                request,                                                       \
                std::make_unique<TEvDiskRegistryProxy::TEv##name##Response>(   \
                    std::move(error)));                                        \
            return true;                                                       \
        }                                                                      \
// FILESTORE_REPLY_WITH_ERROR

        FILESTORE_DISK_REGISTRY_PROXY_REQUESTS(FILESTORE_REPLY_WITH_ERROR)

#undef FILESTORE_REPLY_WITH_ERROR

        default:
            return false;
    }
}

void TDiskRegistryProxyActor::CancelActiveRequests(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    const auto active = std::move(ActiveRequests);
    ActiveRequests.clear();
    for (const auto& [cookie, request]: active) {
        ReplyError(ctx, *request, error);
    }
}

template <typename TResponse, typename TRecord>
void TDiskRegistryProxyActor::Reply(
    const TActorContext& ctx,
    ui64 cookie,
    TRecord&& record)
{
    const auto request = GetActiveRequest(cookie);
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

    TEvDiskRegistryProxy::TDeviceLayout layout;

    auto& main = layout.Replicas.emplace_back();
    for (auto& device: *record.MutableDevices()) {
        main.push_back(ToStorageDevice(std::move(device)));
    }
    for (auto& replica: *record.MutableReplicas()) {
        auto& devices = layout.Replicas.emplace_back();
        for (auto& device: *replica.MutableDevices()) {
            devices.push_back(ToStorageDevice(std::move(device)));
        }
    }

    for (auto& migration: *record.MutableMigrations()) {
        layout.Migrations.push_back({
            .SourceUUID = std::move(*migration.MutableSourceDeviceId()),
            .Target = ToStorageDevice(std::move(
                *migration.MutableTargetDevice())),
        });
    }

    for (auto& uuid: *record.MutableDeviceReplacementUUIDs()) {
        layout.ReplacementDeviceUUIDs.push_back(std::move(uuid));
    }

    // Only the AllocateDisk response reports unavailable devices.
    if constexpr (requires { record.MutableUnavailableDeviceUUIDs(); }) {
        for (auto& uuid: *record.MutableUnavailableDeviceUUIDs()) {
            layout.UnavailableDeviceUUIDs.push_back(std::move(uuid));
        }
    }

    NCloud::Reply(
        ctx,
        *request,
        std::make_unique<TResponse>(std::move(layout)));
}

template <typename TResponse, typename TRecord>
void TDiskRegistryProxyActor::ReplyNoFields(
    const TActorContext& ctx,
    ui64 cookie,
    const TRecord& record)
{
    if (const auto request = GetActiveRequest(cookie)) {
        NCloud::Reply(
            ctx,
            *request,
            std::make_unique<TResponse>(record.GetError()));
    }
}

NProtoPrivate::TStorageDevice TDiskRegistryProxyActor::ToStorageDevice(
    NBlockStore::NProto::TDeviceConfig&& device) const
{
    auto& endpoint = *device.MutableJournalledEndpoint();

    NProtoPrivate::TStorageDevice storageDevice;
    storageDevice.SetHost(std::move(*endpoint.MutableHost()));
    storageDevice.SetPort(endpoint.GetPort());
    storageDevice.SetDeviceId(std::move(*device.MutableDeviceUUID()));
    return storageDevice;
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryProxyActor::HandleAllocateDevices(
    const TEvDiskRegistryProxy::TEvAllocateDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto request = std::make_unique<TEvDiskRegistry::TEvAllocateDiskRequest>();
    auto& record = request->Record;
    record.SetDiskId(msg->FileSystemId);
    // TODO(#7373): restore when DiskRegistry supports ExternalVolumeTabletId
    // record.SetExternalVolumeTabletId(msg->TabletId);
    record.SetCloudId(msg->CloudId);
    record.SetFolderId(msg->FolderId);
    record.SetBlockSize(DefaultBlockSize);
    record.SetBlocksCount(msg->DeviceBlocksCount);
    record.SetReplicaCount(msg->ReplicaCount);
    record.SetStorageMediaKind(msg->MediaKind);
    record.SetPoolName(msg->DevicePoolName);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDiskRegistryProxyActor::HandleDescribeDevices(
    const TEvDiskRegistryProxy::TEvDescribeDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskRegistry::TEvDescribeDiskRequest>();
    request->Record.SetDiskId(ev->Get()->FileSystemId);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDiskRegistryProxyActor::HandleMarkForCleanup(
    const TEvDiskRegistryProxy::TEvMarkForCleanupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto request =
        std::make_unique<TEvDiskRegistry::TEvMarkDiskForCleanupRequest>();
    request->Record.SetDiskId(msg->FileSystemId);
    // TODO(#7373): restore when DiskRegistry supports ExternalVolumeTabletId
    // request->Record.SetExternalVolumeTabletId(msg->TabletId);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDiskRegistryProxyActor::HandleDeallocateDevices(
    const TEvDiskRegistryProxy::TEvDeallocateDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto request =
        std::make_unique<TEvDiskRegistry::TEvDeallocateDiskRequest>();
    request->Record.SetDiskId(msg->FileSystemId);
    // TODO(#7373): restore when DiskRegistry supports ExternalVolumeTabletId
    // request->Record.SetExternalVolumeTabletId(msg->TabletId);
    request->Record.SetSync(false);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDiskRegistryProxyActor::HandleFinishRepair(
    const TEvDiskRegistryProxy::TEvFinishRepairRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto request =
        std::make_unique<TEvDiskRegistry::TEvMarkReplacementDeviceRequest>();
    request->Record.SetDiskId(msg->FileSystemId);
    request->Record.SetDeviceId(msg->DeviceUUID);
    request->Record.SetIsReplacement(false);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDiskRegistryProxyActor::HandleFinishMigration(
    const TEvDiskRegistryProxy::TEvFinishMigrationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto request =
        std::make_unique<TEvDiskRegistry::TEvFinishMigrationRequest>();
    request->Record.SetDiskId(DiskId(msg->FileSystemId, msg->ReplicaIndex));

    auto* migration = request->Record.AddMigrations();
    migration->SetSourceDeviceId(msg->SourceUUID);
    migration->SetTargetDeviceId(msg->TargetUUID);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

void TDiskRegistryProxyActor::HandleReplaceDevice(
    const TEvDiskRegistryProxy::TEvReplaceDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto request = std::make_unique<TEvDiskRegistry::TEvReplaceDeviceRequest>();
    request->Record.SetDiskId(DiskId(msg->FileSystemId, msg->ReplicaIndex));
    request->Record.SetDeviceUUID(msg->DeviceUUID);

    ForwardRequest(ctx, IEventHandlePtr(ev.Release()), std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryProxyActor::HandleAllocateDiskResponse(
    const TEvDiskRegistry::TEvAllocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Reply<TEvDiskRegistryProxy::TEvAllocateDevicesResponse>(
        ctx,
        ev->Cookie,
        std::move(ev->Get()->Record));
}

void TDiskRegistryProxyActor::HandleDescribeDiskResponse(
    const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Reply<TEvDiskRegistryProxy::TEvDescribeDevicesResponse>(
        ctx,
        ev->Cookie,
        std::move(ev->Get()->Record));
}

void TDiskRegistryProxyActor::HandleMarkDiskForCleanupResponse(
    const TEvDiskRegistry::TEvMarkDiskForCleanupResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDiskRegistryProxy::TEvMarkForCleanupResponse>(
        ctx,
        ev->Cookie,
        std::move(ev->Get()->Record));
}

void TDiskRegistryProxyActor::HandleDeallocateDiskResponse(
    const TEvDiskRegistry::TEvDeallocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDiskRegistryProxy::TEvDeallocateDevicesResponse>(
        ctx,
        ev->Cookie,
        std::move(ev->Get()->Record));
}

void TDiskRegistryProxyActor::HandleMarkReplacementDeviceResponse(
    const TEvDiskRegistry::TEvMarkReplacementDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDiskRegistryProxy::TEvFinishRepairResponse>(
        ctx,
        ev->Cookie,
        std::move(ev->Get()->Record));
}

void TDiskRegistryProxyActor::HandleFinishMigrationResponse(
    const TEvDiskRegistry::TEvFinishMigrationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDiskRegistryProxy::TEvFinishMigrationResponse>(
        ctx,
        ev->Cookie,
        std::move(ev->Get()->Record));
}

void TDiskRegistryProxyActor::HandleReplaceDeviceResponse(
    const TEvDiskRegistry::TEvReplaceDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyNoFields<TEvDiskRegistryProxy::TEvReplaceDeviceResponse>(
        ctx,
        ev->Cookie,
        std::move(ev->Get()->Record));
}

void TDiskRegistryProxyActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    OnConnectionError(
        ctx,
        MakeError(E_REJECTED, "disk registry proxy is stopping"));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDiskRegistryProxyActor::StateLookup)
{
    static const NProto::TError error =
        MakeError(E_REJECTED, "disk registry is not available yet");

    switch (ev->GetTypeRewrite()) {
        HFunc(
           TEvHiveProxy::TEvLookupTabletResponse,
           HandleLookupTabletResponse);

        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            if (!ReplyError(ActorContext(), *ev, error)) {
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::DISK_REGISTRY_PROXY,
                    __PRETTY_FUNCTION__);
            }

            break;
    }
}

STFUNC(TDiskRegistryProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        FILESTORE_DISK_REGISTRY_PROXY_REQUESTS(
            FILESTORE_HANDLE_REQUEST,
            TEvDiskRegistryProxy)

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

        HFunc(TEvTabletPipe::TEvClientConnected, HandleClientConnected);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleClientDestroyed);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvents::TEvWakeup);
        IgnoreFunc(TEvHiveProxy::TEvLookupTabletResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::DISK_REGISTRY_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TDiskRegistryProxyActor::StateBroken)
{
    static const NProto::TError error =
        MakeError(E_INVALID_STATE, "disk registry proxy is not configured");

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            if (!ReplyError(ActorContext(), *ev, error)) {
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::DISK_REGISTRY_PROXY,
                    __PRETTY_FUNCTION__);
            }

            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDiskRegistryProxy(TStorageConfigPtr config)
{
    return std::make_unique<TDiskRegistryProxyActor>(std::move(config));
}

}   // namespace NCloud::NFileStore::NStorage
