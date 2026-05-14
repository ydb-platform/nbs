#include "volume_actor.h"

#include "actors/volume_health_sync_actor.h"

#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/common/media.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::RegisterVolumeHealthSyncActorIfNeeded(
    const TActorContext& ctx)
{
    if (!Config->GetVolumeHealthNotificationEnabled()) {
        return;
    }

    if (VolumeHealthSyncActorId) {
        return;
    }

    if (!State) {
        return;
    }

    if (IsReliableDiskRegistryMediaKind(
            State->GetMeta().GetConfig().GetStorageMediaKind()))
    {
        return;
    }

    VolumeHealthSyncActorId = NCloud::Register<TVolumeHealthSyncActor>(
        ctx,
        State->GetDiskId(),
        Executor()->Generation(),
        LogTitle.GetChild(GetCycleCount()),
        TBackoffDelayProvider{TDuration::Seconds(1), TDuration::Seconds(30)});
    Actors.insert(VolumeHealthSyncActorId);

    if (DeviceUUIDToBrokenAt.empty()) {
        // ???
        // SendVolumeHealthNotification(ctx, NProto::VOLUME_HEALTH_HEALTHY);
    } else {
        SendVolumeHealthNotification(ctx, NProto::VOLUME_HEALTH_UNHEALTHY);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendVolumeHealthNotification(
    const TActorContext& ctx,
    NProto::EVolumeHealth volumeHealth)
{
    if (!VolumeHealthSyncActorId) {
        return;
    }

    auto request =
        std::make_unique<TEvDiskRegistry::TEvUpdateVolumeHealthRequest>();
    request->Record.SetVolumeHealth(volumeHealth);

    NCloud::Send(ctx, VolumeHealthSyncActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleBrokenDeviceNotification(
    const TEvNonreplPartitionPrivate::TEvBrokenDeviceNotification::TPtr& ev,
    const TActorContext& ctx)
{
    if (!VolumeHealthSyncActorId) {
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
        SendVolumeHealthNotification(ctx, NProto::VOLUME_HEALTH_UNHEALTHY);
    }
}

void TVolumeActor::HandleDeviceRecoveredNotification(
    const TEvNonreplPartitionPrivate::TEvDeviceRecoveredNotification::TPtr& ev,
    const TActorContext& ctx)
{
    if (!VolumeHealthSyncActorId) {
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
        SendVolumeHealthNotification(ctx, NProto::VOLUME_HEALTH_HEALTHY);
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

void TVolumeActor::CleanupStaleBrokenDevices(const TActorContext& ctx)
{
    if (DeviceUUIDToBrokenAt.empty()) {
        return;
    }

    THashMap<TString, TInstant> staleDeviceUUIDToBrokenAt;
    staleDeviceUUIDToBrokenAt.swap(DeviceUUIDToBrokenAt);

    for (const auto* dev: GetAllDevices(State->GetMeta())) {
        auto it = staleDeviceUUIDToBrokenAt.find(dev->GetDeviceUUID());
        if (it != staleDeviceUUIDToBrokenAt.end()) {
            DeviceUUIDToBrokenAt.insert(*it);
            staleDeviceUUIDToBrokenAt.erase(it);
        }
    }

    for (const auto& [uuid, _]: staleDeviceUUIDToBrokenAt) {
        ExecuteTx<TUpdateBrokenDevice>(
            ctx,
            uuid,
            TInstant::Zero(),
            false);   // add
    }

    if (DeviceUUIDToBrokenAt.empty()) {
        SendVolumeHealthNotification(ctx, NProto::VOLUME_HEALTH_HEALTHY);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
