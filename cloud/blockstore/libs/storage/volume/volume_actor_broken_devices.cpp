#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

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

void TVolumeBrokenNotificationSerializer::Send(
    const TActorContext& ctx,
    const TString& diskId,
    const TString& logPrefix,
    bool broken)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Notifying DiskRegistry: volume health = %s",
        logPrefix.c_str(),
        broken ? "unhealthy" : "healthy");

    auto request =
        std::make_unique<TEvDiskRegistry::TEvUpdateVolumeHealthRequest>();
    request->Record.SetDiskId(diskId);
    request->Record.SetVolumeHealth(
        broken ? NProto::VOLUME_HEALTH_UNHEALTHY
               : NProto::VOLUME_HEALTH_HEALTHY);
    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TVolumeBrokenNotificationSerializer::Notify(
    const TActorContext& ctx,
    const TString& diskId,
    const TString& logPrefix,
    bool broken)
{
    PendingNotifications.push_back(broken);
    if (PendingNotifications.size() == 1) {
        Send(ctx, diskId, logPrefix, broken);
    }
}

void TVolumeBrokenNotificationSerializer::OnResponse(
    const TActorContext& ctx,
    const TString& diskId,
    const TString& logPrefix)
{
    Y_ABORT_UNLESS(!PendingNotifications.empty());
    PendingNotifications.pop_front();
    if (!PendingNotifications.empty()) {
        Send(ctx, diskId, logPrefix, PendingNotifications.front());
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
        VolumeBrokenNotificationSerializer.Notify(
            ctx,
            State->GetDiskId(),
            LogTitle.GetWithTime(),
            /*broken=*/true);
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
        VolumeBrokenNotificationSerializer.Notify(
            ctx,
            State->GetDiskId(),
            LogTitle.GetWithTime(),
            /*broken=*/false);
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
    const auto& error = ev->Get()->Record.GetError();
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s UpdateVolumeHealth response error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());
    }

    VolumeBrokenNotificationSerializer.OnResponse(
        ctx,
        State->GetDiskId(),
        LogTitle.GetWithTime());
}

}   // namespace NCloud::NBlockStore::NStorage
