#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>

#include <cloud/storage/core/libs/common/media.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool ShouldSkipVolumeHealthNotification(
    const TStorageConfig& config,
    const NProto::TVolumeMeta& meta)
{
    return IsReliableDiskRegistryMediaKind(
               meta.GetConfig().GetStorageMediaKind()) ||
           !config.GetVolumeHealthNotificationEnabled();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendVolumeHealthNotification(
    const TActorContext& ctx,
    const THealthNotification& notification)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Notifying DiskRegistry: volume health = %s (seqNo=%" PRIu64 ")",
        LogTitle.GetWithTime().c_str(),
        notification.Health == NProto::VOLUME_HEALTH_HEALTHY ? "healthy"
                                                             : "unhealthy",
        notification.SeqNo);

    auto request =
        std::make_unique<TEvDiskRegistry::TEvUpdateVolumeHealthRequest>();
    request->Record.SetDiskId(State->GetDiskId());
    request->Record.SetVolumeHealth(notification.Health);
    request->Record.SetSeqNo(notification.SeqNo);
    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TVolumeActor::EnqueueVolumeHealthNotification(
    const TActorContext& ctx,
    NProto::EVolumeHealth health)
{
    const ui64 seqNo = (static_cast<ui64>(Executor()->Generation()) << 32) |
                       (++VolumeHealthLocalSeqNo);
    THealthNotification notification{health, seqNo};
    PendingHealthNotifications.push_back(notification);
    if (PendingHealthNotifications.size() == 1) {
        SendVolumeHealthNotification(ctx, notification);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleBrokenDeviceNotification(
    const TEvNonreplPartitionPrivate::TEvBrokenDeviceNotification::TPtr& ev,
    const TActorContext& ctx)
{
    if (ShouldSkipVolumeHealthNotification(*Config, State->GetMeta())) {
        return;
    }

    auto* msg = ev->Get();
    const auto& uuid = msg->DeviceUUID;
    const auto& ts = msg->BrokenTs;

    if (DeviceUUIDToBrokenAt.contains(uuid)) {
        return;
    }

    LOG_WARN(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Device %s is broken (broken at %s)",
        LogTitle.GetWithTime().c_str(),
        uuid.Quote().c_str(),
        ts.ToString().c_str());

    const bool wasEmpty = DeviceUUIDToBrokenAt.empty();
    DeviceUUIDToBrokenAt[uuid] = ts;
    ExecuteTx<TUpdateBrokenDevice>(ctx, uuid, ts, /*add=*/true);

    if (wasEmpty) {
        EnqueueVolumeHealthNotification(ctx, NProto::VOLUME_HEALTH_UNHEALTHY);
    }
}

void TVolumeActor::HandleDeviceRecoveredNotification(
    const TEvNonreplPartitionPrivate::TEvDeviceRecoveredNotification::TPtr& ev,
    const TActorContext& ctx)
{
    if (ShouldSkipVolumeHealthNotification(*Config, State->GetMeta())) {
        return;
    }

    auto* msg = ev->Get();
    const auto& uuid = msg->DeviceUUID;

    if (!DeviceUUIDToBrokenAt.contains(uuid)) {
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Device %s has recovered",
        LogTitle.GetWithTime().c_str(),
        uuid.Quote().c_str());

    DeviceUUIDToBrokenAt.erase(uuid);
    ExecuteTx<TUpdateBrokenDevice>(ctx, uuid, TInstant::Zero(), /*add=*/false);

    if (DeviceUUIDToBrokenAt.empty()) {
        EnqueueVolumeHealthNotification(ctx, NProto::VOLUME_HEALTH_HEALTHY);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareUpdateBrokenDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateBrokenDevice& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TVolumeActor::ExecuteUpdateBrokenDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateBrokenDevice& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    if (args.Add) {
        db.WriteBrokenDevice(args.DeviceUUID, args.BrokenTs);
    } else {
        db.DeleteBrokenDevice(args.DeviceUUID);
    }
}

void TVolumeActor::CompleteUpdateBrokenDevice(
    const TActorContext& ctx,
    TTxVolume::TUpdateBrokenDevice& args)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s BrokenDevice %s: %s",
        LogTitle.GetWithTime().c_str(),
        args.Add ? "persisted" : "removed",
        args.DeviceUUID.Quote().c_str());
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleUpdateVolumeHealthResponse(
    const TEvDiskRegistry::TEvUpdateVolumeHealthResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(!PendingHealthNotifications.empty());

    const auto& error = ev->Get()->Record.GetError();
    if (HasError(error)) {
        if (GetErrorKind(error) == EErrorKind::ErrorRetriable) {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s UpdateVolumeHealth response error: %s, will retry",
                LogTitle.GetWithTime().c_str(),
                FormatError(error).c_str());
            SendVolumeHealthNotification(
                ctx,
                PendingHealthNotifications.front());
            return;
        }
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s UpdateVolumeHealth response error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());
    }

    PendingHealthNotifications.pop_front();
    if (!PendingHealthNotifications.empty()) {
        SendVolumeHealthNotification(ctx, PendingHealthNotifications.front());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::CleanupStaleBrokenDevices(const TActorContext& ctx)
{
    if (DeviceUUIDToBrokenAt.empty()) {
        return;
    }

    THashSet<TString> currentUUIDs;
    for (const auto* dev: GetAllDevices(State->GetMeta())) {
        currentUUIDs.insert(dev->GetDeviceUUID());
    }
    for (auto it = DeviceUUIDToBrokenAt.begin();
         it != DeviceUUIDToBrokenAt.end();)
    {
        if (!currentUUIDs.contains(it->first)) {
            ExecuteTx<TUpdateBrokenDevice>(
                ctx,
                it->first,
                TInstant::Zero(),
                /*add=*/false);
            DeviceUUIDToBrokenAt.erase(it++);
        } else {
            ++it;
        }
    }

    if (DeviceUUIDToBrokenAt.empty()) {
        EnqueueVolumeHealthNotification(ctx, NProto::VOLUME_HEALTH_HEALTHY);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
