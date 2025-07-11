#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>
#include <cloud/storage/core/libs/common/media.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using MessageDifferencer = google::protobuf::util::MessageDifferencer;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GetSize(const TDevices& devs)
{
    ui64 s = 0;
    for (const auto& d: devs) {
        s += d.GetBlockSize() * d.GetBlocksCount();
    }

    return s;
}

ui64 GetBlocks(const NKikimrBlockStore::TVolumeConfig& config)
{
    Y_ABORT_UNLESS(config.PartitionsSize() == 1);
    return config.GetPartitions(0).GetBlockCount();
}

////////////////////////////////////////////////////////////////////////////////

bool ValidateDevices(
    const TActorContext& ctx,
    const TString& logTitle,
    const TString& label,
    const TDevices& oldDevs,
    const TDevices& newDevs,
    bool checkDeviceId)
{
    bool ok = true;

    auto newDeviceIt = newDevs.begin();
    auto oldDeviceIt = oldDevs.begin();
    while (oldDeviceIt != oldDevs.end()) {
        if (newDeviceIt == newDevs.end()) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s %s: got less devices than previously existed"
                ", old device count: %lu, new device count: %lu",
                logTitle.c_str(),
                label.c_str(),
                oldDevs.size(),
                newDevs.size());

            ok = false;
            break;
        }

        if (checkDeviceId &&
                newDeviceIt->GetDeviceUUID() != oldDeviceIt->GetDeviceUUID())
        {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s %s: device %u id changed: %s -> %s",
                logTitle.c_str(),
                label.c_str(),
                std::distance(newDevs.begin(), newDeviceIt),
                oldDeviceIt->GetDeviceUUID().Quote().c_str(),
                newDeviceIt->GetDeviceUUID().Quote().c_str());
        }

        if (newDeviceIt->GetBlocksCount() != oldDeviceIt->GetBlocksCount()) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s %s: device block count changed: %s: %lu -> %lu",
                logTitle.c_str(),
                label.c_str(),
                oldDeviceIt->GetDeviceUUID().Quote().c_str(),
                oldDeviceIt->GetBlocksCount(),
                newDeviceIt->GetBlocksCount());

            ok = false;
        }

        if (newDeviceIt->GetBlockSize() != oldDeviceIt->GetBlockSize()) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s %s: device block size changed: %s: %u -> %u",
                logTitle.c_str(),
                label.c_str(),
                oldDeviceIt->GetDeviceUUID().Quote().c_str(),
                oldDeviceIt->GetBlockSize(),
                newDeviceIt->GetBlockSize());

            ok = false;
        }

        ++oldDeviceIt;
        ++newDeviceIt;
    }

    return ok;
}

std::unique_ptr<MessageDifferencer> CreateLiteReallocationDifferencer()
{
    std::array descriptors{
        NProto::TVolumeMeta::GetDescriptor()->FindFieldByName("IOModeTs"),
        NProto::TDeviceConfig::GetDescriptor()->FindFieldByName("State"),
        NProto::TDeviceConfig::GetDescriptor()->FindFieldByName("StateTs"),
        NProto::TDeviceConfig::GetDescriptor()->FindFieldByName("StateMessage"),
        // These are two fields that will change during disk agent blue-green
        // deploy.
        NProto::TDeviceConfig::GetDescriptor()->FindFieldByName("NodeId"),
        NProto::TRdmaEndpoint::GetDescriptor()->FindFieldByName("Port")};

    if (size_t index = FindIndex(descriptors, nullptr); index != NPOS) {
        ReportFieldDescriptorNotFound(
            TStringBuilder() << "Lite reallocation is impossible. Descriptor #"
                             << index << " is nullptr.");
        return nullptr;
    }

    auto diff = std::make_unique<MessageDifferencer>();
    for (const auto* descriptor: descriptors) {
        diff->IgnoreField(descriptor);
    }
    diff->set_float_comparison(
        MessageDifferencer::FloatComparison::APPROXIMATE);
    diff->set_message_field_comparison(
        MessageDifferencer::MessageFieldComparison::EQUAL);
    return diff;
}

NProto::TVolumeMeta CreateNewMeta(
    const NProto::TVolumeMeta& oldMeta,
    TTxVolume::TUpdateDevices& args)
{
    auto newMeta = oldMeta;
    *newMeta.MutableDevices() = std::move(args.Devices);
    *newMeta.MutableMigrations() = std::move(args.Migrations);
    newMeta.ClearReplicas();
    for (auto& devices: args.Replicas) {
        auto* replica = newMeta.AddReplicas();
        *replica->MutableDevices() = std::move(devices);
    }
    newMeta.ClearFreshDeviceIds();
    for (auto& freshDeviceId: args.FreshDeviceIds) {
        *newMeta.AddFreshDeviceIds() = std::move(freshDeviceId);
    }
    newMeta.SetIOMode(args.IOMode);
    newMeta.SetIOModeTs(args.IOModeTs.MicroSeconds());
    newMeta.SetMuteIOErrors(args.MuteIOErrors);
    UpdateLaggingDevicesAfterMetaUpdate(newMeta, args.RemovedLaggingDeviceIds);

    Sort(args.UnavailableDeviceIds);

    newMeta.MutableUnavailableDeviceIds()->Assign(
        std::make_move_iterator(args.UnavailableDeviceIds.begin()),
        std::make_move_iterator(args.UnavailableDeviceIds.end()));

    return newMeta;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

const NKikimrBlockStore::TVolumeConfig& TVolumeActor::GetNewestConfig() const
{
    if (UpdateVolumeConfigInProgress) {
        return UnfinishedUpdateVolumeConfig.Record.GetVolumeConfig();
    }

    Y_ABORT_UNLESS(State);
    return State->GetMeta().GetVolumeConfig();
}

////////////////////////////////////////////////////////////////////////////////

NProto::TAllocateDiskRequest TVolumeActor::MakeAllocateDiskRequest() const
{
    const auto& config = GetNewestConfig();
    const auto blocks = GetBlocks(config);

    NProto::TAllocateDiskRequest request;

    request.SetDiskId(config.GetDiskId());
    request.SetCloudId(config.GetCloudId());
    request.SetFolderId(config.GetFolderId());
    request.SetBlockSize(config.GetBlockSize());
    request.SetBlocksCount(blocks);
    request.SetPlacementGroupId(config.GetPlacementGroupId());
    request.SetPlacementPartitionIndex(config.GetPlacementPartitionIndex());
    request.MutableAgentIds()->CopyFrom(config.GetAgentIds());

    const auto mediaKind = GetNewestConfig().GetStorageMediaKind();
    if (mediaKind == NProto::STORAGE_MEDIA_SSD_MIRROR2) {
        request.SetReplicaCount(Config->GetMirror2DiskReplicaCount());
    } else if (mediaKind == NProto::STORAGE_MEDIA_SSD_MIRROR3) {
        request.SetReplicaCount(Config->GetMirror3DiskReplicaCount());
    }

    request.SetStorageMediaKind(static_cast<NProto::EStorageMediaKind>(mediaKind));
    request.SetPoolName(config.GetStoragePoolName());

    return request;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::AllocateDisk(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskRegistry::TEvAllocateDiskRequest>();
    request->Record = MakeAllocateDiskRequest();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s AllocateDiskRequest: %s",
        LogTitle.GetWithTime().c_str(),
        request->Record.Utf8DebugString().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleAllocateDiskIfNeeded(
    const TEvVolumePrivate::TEvAllocateDiskIfNeeded::TPtr& ev,
    const TActorContext& ctx)
{
    if (UpdateVolumeConfigInProgress) {
        return;
    }

    if (HasError(StorageAllocationResult)) {
        return;
    }

    DiskAllocationScheduled = false;

    Y_UNUSED(ev);

    Y_ABORT_UNLESS(State);
    const auto& config = State->GetMeta().GetVolumeConfig();
    const auto blocks = GetBlocks(config);
    auto expectedSize = blocks * config.GetBlockSize();
    auto actualSize = GetSize(State->GetMeta().GetDevices());

    if (expectedSize <= actualSize) {
        if (expectedSize < actualSize) {
            LOG_INFO(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s Attempt to decrease disk size, currentSize=%lu, "
                "expectedSize=%lu",
                LogTitle.GetWithTime().c_str(),
                actualSize,
                expectedSize);
        }

        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Allocating disk, currentSize=%lu, expectedSize=%lu",
        LogTitle.GetWithTime().c_str(),
        actualSize,
        expectedSize);

    AllocateDisk(ctx);
}

void TVolumeActor::ScheduleAllocateDiskIfNeeded(const TActorContext& ctx)
{
    if (State && !DiskAllocationScheduled) {
        if (!State->IsDiskRegistryMediaKind()) {
            return;
        }

        DiskAllocationScheduled = true;

        ctx.Schedule(
            TDuration::Seconds(1),
            new TEvVolumePrivate::TEvAllocateDiskIfNeeded()
        );
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleAllocateDiskError(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (UpdateVolumeConfigInProgress && State) {
        UnfinishedUpdateVolumeConfig.Devices = State->GetMeta().GetDevices();
        UnfinishedUpdateVolumeConfig.Migrations =
            State->GetMeta().GetMigrations();

        UnfinishedUpdateVolumeConfig.Replicas.clear();
        for (auto& r: State->GetMeta().GetReplicas()) {
            UnfinishedUpdateVolumeConfig.Replicas.push_back(r.GetDevices());
        }

        UnfinishedUpdateVolumeConfig.FreshDeviceIds.assign(
            State->GetMeta().GetFreshDeviceIds().begin(),
            State->GetMeta().GetFreshDeviceIds().end());
        UnfinishedUpdateVolumeConfig.UnavailableDeviceIds.assign(
            State->GetMeta().GetUnavailableDeviceIds().begin(),
            State->GetMeta().GetUnavailableDeviceIds().end());
    }

    if (GetErrorKind(error) == EErrorKind::ErrorRetriable) {
        ScheduleAllocateDiskIfNeeded(ctx);
        return;
    }

    const auto mediaKind = static_cast<NProto::EStorageMediaKind>(
        GetNewestConfig().GetStorageMediaKind());
    const bool localDiskAllocationRetry =
        Config->GetLocalDiskAsyncDeallocationEnabled() &&
        error.GetCode() == E_TRY_AGAIN &&
        IsDiskRegistryLocalMediaKind(mediaKind);

    if (error.GetCode() != E_BS_RESOURCE_EXHAUSTED && !localDiskAllocationRetry)
    {
        ReportDiskAllocationFailure(
            TStringBuilder() << "Disk " << GetNewestConfig().GetDiskId().Quote()
                             << ": disk allocation failed");
    }
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Disk allocation failed with error: %s.",
        LogTitle.GetWithTime().c_str(),
        FormatError(error).c_str());

    StorageAllocationResult = std::move(error);
}

void TVolumeActor::HandleAllocateDiskResponse(
    const TEvDiskRegistry::TEvAllocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_DEFER {
        if (UpdateVolumeConfigInProgress) {
            FinishUpdateVolumeConfig(ctx);
        }
    };

    auto* msg = ev->Get();

    if (auto error = msg->Record.GetError(); FAILED(error.GetCode())) {
        HandleAllocateDiskError(ctx, std::move(error));
        return;
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Disk allocation success. %s",
            LogTitle.GetWithTime().c_str(),
            DescribeAllocation(msg->Record).c_str());
    }

    if (!StateLoadFinished) {
        return;
    }

    auto& devices = *msg->Record.MutableDevices();
    auto& migrations = *msg->Record.MutableMigrations();
    TVector<TDevices> replicas;
    TVector<TString> freshDeviceIds;
    TVector<TString> removedLaggingDevices;
    TVector<TString> unavailableDeviceIds;
    for (auto& msgReplica: *msg->Record.MutableReplicas()) {
        replicas.push_back(std::move(*msgReplica.MutableDevices()));
    }
    for (auto& freshDeviceId: *msg->Record.MutableDeviceReplacementUUIDs()) {
        freshDeviceIds.push_back(std::move(freshDeviceId));
    }
    for (auto& removedLaggingDevice:
         *msg->Record.MutableRemovedLaggingDevices())
    {
        removedLaggingDevices.push_back(
            std::move(*removedLaggingDevice.MutableDeviceUUID()));
    }
    for (auto& deviceId: *msg->Record.MutableUnavailableDeviceUUIDs()) {
        unavailableDeviceIds.push_back(std::move(deviceId));
    }

    if (!CheckAllocationResult(ctx, devices, replicas)) {
        return;
    }

    if (UpdateVolumeConfigInProgress) {
        UnfinishedUpdateVolumeConfig.Devices = std::move(devices);
        UnfinishedUpdateVolumeConfig.Migrations = std::move(migrations);
        UnfinishedUpdateVolumeConfig.Replicas = std::move(replicas);
        UnfinishedUpdateVolumeConfig.FreshDeviceIds = std::move(freshDeviceIds);
        UnfinishedUpdateVolumeConfig.RemovedLaggingDeviceIds =
            std::move(removedLaggingDevices);
        UnfinishedUpdateVolumeConfig.UnavailableDeviceIds =
            std::move(unavailableDeviceIds);
    } else {
        ExecuteTx<TUpdateDevices>(
            ctx,
            std::move(devices),
            std::move(migrations),
            std::move(replicas),
            std::move(freshDeviceIds),
            std::move(removedLaggingDevices),
            std::move(unavailableDeviceIds),
            msg->Record.GetIOMode(),
            TInstant::MicroSeconds(msg->Record.GetIOModeTs()),
            msg->Record.GetMuteIOErrors());
    }
}

void TVolumeActor::HandleUpdateDevices(
    const TEvVolumePrivate::TEvUpdateDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (!StateLoadFinished) {
        auto response = std::make_unique<TEvVolumePrivate::TEvUpdateDevicesResponse>(
            MakeError(E_REJECTED, "State load not finished"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (UpdateVolumeConfigInProgress) {
        auto response = std::make_unique<TEvVolumePrivate::TEvUpdateDevicesResponse>(
            MakeError(E_REJECTED, "Update volume config in progress"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (!CheckAllocationResult(ctx, msg->Devices, msg->Replicas)) {
        auto response = std::make_unique<TEvVolumePrivate::TEvUpdateDevicesResponse>(
            MakeError(E_INVALID_STATE, "Bad allocation result"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    ExecuteTx<TUpdateDevices>(
        ctx,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        std::move(msg->Devices),
        std::move(msg->Migrations),
        std::move(msg->Replicas),
        std::move(msg->FreshDeviceIds),
        std::move(msg->RemovedLaggingDevices),
        std::move(msg->UnavailableDeviceIds),
        msg->IOMode,
        msg->IOModeTs,
        msg->MuteIOErrors);
}

bool TVolumeActor::CheckAllocationResult(
    const TActorContext& ctx,
    const TDevices& devices,
    const TVector<TDevices>& replicas)
{
    Y_ABORT_UNLESS(StateLoadFinished);

    if (!State) {
        return true;
    }

    const auto& config = GetNewestConfig();

    const auto blocks = GetBlocks(config);
    auto expectedSize = blocks * config.GetBlockSize();
    auto allocatedSize = GetSize(devices);

    bool ok = ValidateDevices(
        ctx,
        LogTitle.GetWithTime(),
        "MainConfig",
        State->GetMeta().GetDevices(),
        devices,
        true);

    const auto oldReplicaCount = State->GetMeta().ReplicasSize();
    if (replicas.size() < oldReplicaCount) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Got less replicas than previously existed"
            ", old replica count: %lu, new replica count: %lu",
            LogTitle.GetWithTime().c_str(),
            State->GetMeta().ReplicasSize(),
            replicas.size());

        ok = false;
    }

    for (ui32 i = 0; i < Min(replicas.size(), oldReplicaCount); ++i) {
        ok &= ValidateDevices(
            ctx,
            LogTitle.GetWithTime(),
            Sprintf("Replica-%u", i),
            State->GetMeta().GetReplicas(i).GetDevices(),
            replicas[i],
            true);

        ok &= ValidateDevices(
            ctx,
            LogTitle.GetWithTime(),
            Sprintf("ReplicaReference-%u", i),
            devices,
            replicas[i],
            false);

        if (replicas[i].size() > devices.size()) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s Replica-%u: got more devices than main config"
                ", main device count: %lu, replica device count: %lu",
                LogTitle.GetWithTime().c_str(),
                i,
                devices.size(),
                replicas[i].size());

            ok = false;
        }
    }

    if (ok && allocatedSize < expectedSize) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "%s Bad disk allocation result, allocatedSize=%lu, expectedSize=%lu",
            LogTitle.GetWithTime().c_str(),
            allocatedSize,
            expectedSize);

        ok = false;
    }

    if (!ok) {
        ReportDiskAllocationFailure(
            TStringBuilder() << "Disk " << State->GetDiskId().Quote()
                             << ": invalid disk allocation response received");

        if (State->GetAcceptInvalidDiskAllocationResponse()) {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s Accepting invalid disk allocation response",
                LogTitle.GetWithTime().c_str());
        } else {
            ScheduleAllocateDiskIfNeeded(ctx);
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareUpdateDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteUpdateDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateDevices& args)
{
    Y_ABORT_UNLESS(State);
    const auto& oldMeta = State->GetMeta();
    auto newMeta = CreateNewMeta(oldMeta, args);

    Y_DEBUG_ABORT_UNLESS(State->IsDiskRegistryMediaKind());
    if (Config->GetAllowLiteDiskReallocations()) {
        auto differencer = CreateLiteReallocationDifferencer();
        args.LiteReallocation =
            differencer && differencer->Compare(oldMeta, newMeta);
    }

    TVolumeDatabase db(tx.DB);
    if (!args.LiteReallocation) {
        // TODO: reset MigrationIndex here and in UpdateVolumeConfig only if our
        // migration or fresh device lists have changed
        // NBS-1988
        newMeta.SetMigrationIndex(0);

        TVolumeMetaHistoryItem metaHistoryItem{ctx.Now(), newMeta};
        db.WriteMetaHistory(State->GetMetaHistory().size(), metaHistoryItem);
        State->AddMetaHistory(std::move(metaHistoryItem));

        args.ReplacedDevices = GetReplacedDevices(oldMeta, newMeta);
    }

    db.WriteMeta(newMeta);
    State->ResetMeta(std::move(newMeta));
}

void TVolumeActor::CompleteUpdateDevices(
    const TActorContext& ctx,
    TTxVolume::TUpdateDevices& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Devices have been updated. LiteReallocation: %d",
        LogTitle.GetWithTime().c_str(),
        args.LiteReallocation);

    // Hacky way to avoid race condition with "TTxVolume::TUpdateMigrationState"
    // TODO(komarevtsev-d): Remove after proper fix in #3162.
    if (!args.LiteReallocation) {
        State->UpdateMigrationIndexInMeta(0);
    }

    TPoisonCallback onPartitionDestroy =
        [requestInfo =
             args.RequestInfo](const TActorContext& ctx, NProto::TError error)
    {
        if (!requestInfo) {
            return;
        }
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolumePrivate::TEvUpdateDevicesResponse>(
                std::move(error)));
    };

    StopPartitions(ctx, onPartitionDestroy);
    SendVolumeConfigUpdated(ctx);
    StartPartitionsForUse(ctx);
    ResetServicePipes(ctx);
    if (!args.LiteReallocation) {
        // Non-lite reallocation means that new devices could have been added.
        AcquireDiskIfNeeded(ctx);
        // Try to release devices that don't belong to the volume anymore. This
        // task is not critical, and in case of failure, the acquire will become
        // obsolete in some time.
        ReleaseReplacedDevices(ctx, args.ReplacedDevices);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
