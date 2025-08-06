#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/common/media.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void OutputDevice(const NProto::TDeviceConfig& device, TStringBuilder& result)
{
    result << "("
        << device.GetDeviceUUID() << " "
        << device.GetBlocksCount() << " "
        << "(" << device.GetUnadjustedBlockCount() << ") "
        << device.GetBlockSize()
    << ")";
}

void OutputDevices(const auto& devices, TStringBuilder& result)
{
    result << "[";
    if (!devices.empty()) {
        result << " ";
    }
    for (const auto& device: devices) {
        OutputDevice(device, result);
        result << " ";
    }
    result << "]";
}

TString GetReplicaDiskId(const TString& diskId, ui32 i)
{
    return TStringBuilder() << diskId << "/" << i;
}

}   // namespace

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleAllocateDisk(
    const TEvDiskRegistry::TEvAllocateDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AllocateDisk);

    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received AllocateDisk request: DiskId=%s, BlockSize=%u"
        ", BlocksCount=%lu, ReplicaCount=%u, CloudId=%s, FolderId=%s"
        ", PlacementGroupId=%s, PlacementPartitionIndex=%u",
        TabletID(),
        msg->Record.GetDiskId().c_str(),
        msg->Record.GetBlockSize(),
        msg->Record.GetBlocksCount(),
        msg->Record.GetReplicaCount(),
        msg->Record.GetCloudId().c_str(),
        msg->Record.GetFolderId().c_str(),
        msg->Record.GetPlacementGroupId().c_str(),
        msg->Record.GetPlacementPartitionIndex());

    Y_DEBUG_ABORT_UNLESS(
        msg->Record.GetStorageMediaKind() != NProto::STORAGE_MEDIA_DEFAULT);

    ExecuteTx<TAddDisk>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        msg->Record.GetCloudId(),
        msg->Record.GetFolderId(),
        msg->Record.GetPlacementGroupId(),
        msg->Record.GetPlacementPartitionIndex(),
        msg->Record.GetBlockSize(),
        msg->Record.GetBlocksCount(),
        msg->Record.GetReplicaCount(),
        TVector<TString> {
            msg->Record.GetAgentIds().begin(),
            msg->Record.GetAgentIds().end()
        },
        msg->Record.GetPoolName(),
        msg->Record.GetStorageMediaKind());
}

bool TDiskRegistryActor::PrepareAddDisk(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddDisk& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteAddDisk(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddDisk& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);

    TDiskRegistryState::TAllocateDiskResult result{};

    args.Error = State->AllocateDisk(
        ctx.Now(),
        db,
        TDiskRegistryState::TAllocateDiskParams {
            .DiskId = args.DiskId,
            .CloudId = args.CloudId,
            .FolderId = args.FolderId,
            .PlacementGroupId = args.PlacementGroupId,
            .PlacementPartitionIndex = args.PlacementPartitionIndex,
            .BlockSize = args.BlockSize,
            .BlocksCount = args.BlocksCount,
            .ReplicaCount = args.ReplicaCount,
            .AgentIds = args.AgentIds,
            .PoolName = args.PoolName,
            .MediaKind = args.MediaKind
        },
        &result);

    if (args.Error.GetCode() == E_BS_DISK_ALLOCATION_FAILED &&
        IsDiskRegistryLocalMediaKind(args.MediaKind) &&
        State->CanAllocateLocalDiskAfterSecureErase(
            args.AgentIds,
            args.PoolName,
            args.BlocksCount * args.BlockSize))
    {
        // Note: DM uses this specific error message to identify this case.
        // Update createEmptyDiskTask simultaneously if you change it.
        args.Error = MakeError(
            E_TRY_AGAIN,
            "Unable to allocate local disk: secure erase has not finished yet");
    }

    args.Devices = std::move(result.Devices);
    args.DeviceMigrations = std::move(result.Migrations);
    args.Replicas = std::move(result.Replicas);
    args.DeviceReplacementUUIDs = std::move(result.DeviceReplacementIds);
    args.LaggingDevices = std::move(result.LaggingDevices);
    args.IOMode = result.IOMode;
    args.IOModeTs = result.IOModeTs;
    args.MuteIOErrors = result.MuteIOErrors;
}

void TDiskRegistryActor::CompleteAddDisk(
    const TActorContext& ctx,
    TTxDiskRegistry::TAddDisk& args)
{
    auto response = std::make_unique<TEvDiskRegistry::TEvAllocateDiskResponse>();

    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] AddDisk error: %s. DiskId=%s",
            TabletID(),
            FormatError(args.Error).c_str(),
            args.DiskId.Quote().c_str());

        *response->Record.MutableError() = std::move(args.Error);
    } else {
        TStringBuilder devices;
        OutputDevices(args.Devices, devices);

        TStringBuilder replicas;
        replicas << "[";
        if (!args.Replicas.empty()) {
            replicas << " ";
        }
        for (const auto& replica: args.Replicas) {
            OutputDevices(replica, replicas);
        }
        replicas << "]";

        TStringBuilder migrations;
        migrations << "[";
        if (!args.DeviceMigrations.empty()) {
            migrations << " ";
        }
        for (const auto& migration: args.DeviceMigrations) {
            migrations << migration.GetSourceDeviceId() << " -> ";
            OutputDevice(migration.GetTargetDevice(), migrations);
            migrations << " ";
        }
        migrations << "]";

        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] AddDisk success. DiskId=%s Devices=%s Replicas=%s"
            " Migrations=%s",
            TabletID(),
            args.DiskId.Quote().c_str(),
            devices.c_str(),
            replicas.c_str(),
            migrations.c_str()
        );

        auto onDevice = [&] (NProto::TDeviceConfig& d, ui32 blockSize) {
            if (ToLogicalBlocks(d, blockSize)) {
                return true;
            }

            TStringBuilder error;
            error << "CompleteAddDisk: ToLogicalBlocks failed, device: "
                << d.GetDeviceUUID().Quote().c_str();
            LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY, error);

            *response->Record.MutableError() = MakeError(E_FAIL, error);
            NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

            return false;
        };

        for (auto& device: args.Devices) {
            if (!onDevice(device, args.BlockSize)) {
                return;
            }

            *response->Record.AddDevices() = std::move(device);
        }
        for (auto& m: args.DeviceMigrations) {
            if (!onDevice(*m.MutableTargetDevice(), args.BlockSize)) {
                return;
            }

            *response->Record.AddMigrations() = std::move(m);
        }
        for (auto& replica: args.Replicas) {
            auto* r = response->Record.AddReplicas();

            for (auto& device: replica) {
                if (!onDevice(device, args.BlockSize)) {
                    return;
                }

                *r->AddDevices() = std::move(device);
            }
        }

        for (auto& deviceId: args.DeviceReplacementUUIDs) {
            *response->Record.AddDeviceReplacementUUIDs() = std::move(deviceId);
        }

        for (auto& laggingDevice: args.LaggingDevices) {
            *response->Record.AddRemovedLaggingDevices() =
                std::move(laggingDevice.Device);
        }

        response->Record.SetIOMode(args.IOMode);
        response->Record.SetIOModeTs(args.IOModeTs.MicroSeconds());
        response->Record.SetMuteIOErrors(args.MuteIOErrors);

        if (args.ReplicaCount) {
            for (size_t i = 0; i < args.ReplicaCount + 1; ++i) {
                auto replicaId = GetReplicaDiskId(args.DiskId, i);
                auto unavailableDevicesForDisk =
                    State->GetUnavailableDevicesForDisk(replicaId);

                response->Record.MutableUnavailableDeviceUUIDs()->Add(
                    std::make_move_iterator(unavailableDevicesForDisk.begin()),
                    std::make_move_iterator(unavailableDevicesForDisk.end()));
            }
        } else {
            auto unavailableDevicesForDisk =
                State->GetUnavailableDevicesForDisk(args.DiskId);
            response->Record.MutableUnavailableDeviceUUIDs()->Add(
                std::make_move_iterator(unavailableDevicesForDisk.begin()),
                std::make_move_iterator(unavailableDevicesForDisk.end()));
        }
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    DestroyBrokenDisks(ctx);
    NotifyUsers(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleDeallocateDisk(
    const TEvDiskRegistry::TEvDeallocateDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(DeallocateDisk);

    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received DeallocateDisk request: DiskId=%s Sync=%d",
        TabletID(),
        msg->Record.GetDiskId().c_str(),
        msg->Record.GetSync());

    const auto& diskId = msg->Record.GetDiskId();

    if (msg->Record.GetSync() && State->HasPendingCleanup(diskId)) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Postpone DeallocateDisk response. DiskId=%s",
            TabletID(), diskId.c_str());

        AddPendingDeallocation(ctx, diskId, std::move(requestInfo));

        return;
    }

    ExecuteTx<TRemoveDisk>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        msg->Record.GetSync());
}

bool TDiskRegistryActor::PrepareRemoveDisk(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemoveDisk& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteRemoveDisk(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemoveDisk& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->DeallocateDisk(db, args.DiskId);
}

void TDiskRegistryActor::CompleteRemoveDisk(
    const TActorContext& ctx,
    TTxDiskRegistry::TRemoveDisk& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "RemoveDisk error: %s. DiskId=%s",
            FormatError(args.Error).c_str(),
            args.DiskId.Quote().c_str());
    } else {
        OnDiskDeallocated(args.DiskId);
    }

    auto response = std::make_unique<TEvDiskRegistry::TEvDeallocateDiskResponse>();
    *response->Record.MutableError() = args.Error;

    if (HasError(args.Error) || args.Error.GetCode() == S_ALREADY || !args.Sync) {
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    } else {
        AddPendingDeallocation(ctx, args.DiskId, args.RequestInfo);
    }

    SecureErase(ctx);
    NotifyUsers(ctx);
}

void TDiskRegistryActor::AddPendingDeallocation(
    const NActors::TActorContext& ctx,
    const TString& diskId,
    TRequestInfoPtr requestInfo)
{
    auto& requestInfos = PendingDiskDeallocationRequests[diskId];

    if (requestInfos.size() > Config->GetMaxNonReplicatedDiskDeallocationRequests()) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "Too many pending deallocation requests (%lu) for disk %s. "
            "Reject all requests.",
            requestInfos.size(),
            diskId.Quote().c_str());

        ReplyToPendingDeallocations(ctx, requestInfos, MakeError(E_REJECTED));
    }

    requestInfos.emplace_back(std::move(requestInfo));
}

void TDiskRegistryActor::ReplyToPendingDeallocations(
    const NActors::TActorContext& ctx,
    TVector<TRequestInfoPtr>& requestInfos,
    NProto::TError error)
{
    for (auto& requestInfo: requestInfos) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvDiskRegistry::TEvDeallocateDiskResponse>(
                error));
    }
    requestInfos.clear();
}

void TDiskRegistryActor::ReplyToPendingDeallocations(
    const NActors::TActorContext& ctx,
    const TString& diskId)
{
    auto it = PendingDiskDeallocationRequests.find(diskId);
    if (it == PendingDiskDeallocationRequests.end()) {
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Reply to pending deallocation requests. DiskId=%s PendingRquests=%d",
        diskId.Quote().c_str(),
        static_cast<int>(it->second.size()));

    ReplyToPendingDeallocations(ctx, it->second, MakeError(S_OK));

    PendingDiskDeallocationRequests.erase(it);
}

}   // namespace NCloud::NBlockStore::NStorage
