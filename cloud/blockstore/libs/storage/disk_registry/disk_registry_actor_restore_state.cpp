#include "disk_registry_actor.h"

#include "actors/restore_validator_actor.h"

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TOperations = TQueue<std::function<void(TDiskRegistryDatabase&)>>;

void RestoreConfig(
    NProto::TDiskRegistryConfig newConfig,
    NProto::TDiskRegistryConfig currentConfig,
    TOperations& operations)
{
    Y_UNUSED(currentConfig);
    operations.push(
        [newConfig = std::move(newConfig)](TDiskRegistryDatabase& db) {
            db.WriteDiskRegistryConfig(newConfig);
        });
}

void RestoreDirtyDevices(
    TVector<TDirtyDevice> newDirtyDevices,
    TVector<TDirtyDevice> currentDirtyDevices,
    TOperations& operations)
{
    for (auto&& [uuid, diskId]: currentDirtyDevices) {
        operations.push(
            [uuid = std::move(uuid)] (TDiskRegistryDatabase& db) {
                db.DeleteDirtyDevice(uuid);
            });
    }
    for (auto&& dd: newDirtyDevices) {
        operations.push(
            [dd = std::move(dd)] (TDiskRegistryDatabase& db) {
                db.UpdateDirtyDevice(dd.Id, dd.DiskId);
            });
    }
}

void RestoreAgents(
    TVector<NProto::TAgentConfig> newAgents,
    TVector<NProto::TAgentConfig> currentAgents,
    TOperations& operations)
{
    for (auto&& agent: currentAgents) {
        operations.push(
            [agent = std::move(agent)](TDiskRegistryDatabase& db) {
                db.DeleteAgent(agent.GetAgentId());
            });
    }
    for (auto&& agent: newAgents) {
        operations.push(
            [agent = std::move(agent)](TDiskRegistryDatabase& db) {
                db.UpdateAgent(agent);
            });
    }
}

void RestoreDisks(
    TVector<NProto::TDiskConfig> newDisks,
    TVector<NProto::TDiskConfig> currentDisks,
    TOperations& operations)
{
    for (auto&& disk: currentDisks) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.DeleteDisk(disk.GetDiskId());
            });
    }
    for (auto&& disk: newDisks) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.UpdateDisk(disk);
            });
    }
}

void RestorePlacementGroups(
    TVector<NProto::TPlacementGroupConfig> newPlacementGroups,
    TVector<NProto::TPlacementGroupConfig> currentPlacementGroups,
    TOperations& operations)
{
    for (auto&& group: currentPlacementGroups) {
        operations.push(
            [group = std::move(group)](TDiskRegistryDatabase& db) {
                db.DeletePlacementGroup(group.GetGroupId());
            });
    }
    for (auto&& group: newPlacementGroups) {
        operations.push(
            [group = std::move(group)](TDiskRegistryDatabase& db) {
                db.UpdatePlacementGroup(group);
            });
    }
}

void RestoreBrokenDisks(
    TVector<TBrokenDiskInfo> newBrokenDisks,
    TVector<TBrokenDiskInfo> currentBrokenDisks,
    TOperations& operations)
{
    for (auto&& disk: currentBrokenDisks) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.DeleteBrokenDisk(disk.DiskId);
            });
    }
    for (auto&& disk: newBrokenDisks) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.AddBrokenDisk(disk);
            });
    }
}

void RestoreDisksToNotify(
    TVector<TString> newDisksToNotify,
    TVector<TString> currentDisksToNotify,
    TOperations& operations)
{
    for (auto&& disk: currentDisksToNotify) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.DeleteDiskToReallocate(disk);
            });
    }
    for (auto&& disk: newDisksToNotify) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.AddDiskToReallocate(disk);
            });
    }
}

void RestoreDiskStateChanges(
    TVector<TDiskStateUpdate> newDiskStateChanges,
    TVector<TDiskStateUpdate> currentDiskStateChanges,
    TOperations& operations)
{
    for (auto&& disk: currentDiskStateChanges) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.DeleteDiskStateChanges(disk.State.GetDiskId(), disk.SeqNo);
            });
    }
    for (auto&& disk: newDiskStateChanges) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.UpdateDiskState(disk.State, disk.SeqNo);
            });
    }
}

void RestoreLastDiskStateSeqNo(
    ui64 newLastDiskStateSeqNo,
    ui64 currentLastDiskStateSeqNo,
    TOperations& operations)
{
    Y_UNUSED(currentLastDiskStateSeqNo);
    operations.push(
        [newLastDiskStateSeqNo = std::move(newLastDiskStateSeqNo)]
        (TDiskRegistryDatabase& db) {
            db.WriteLastDiskStateSeqNo(newLastDiskStateSeqNo);
        });
}

void RestoreWritableState(
    bool newWritableState,
    bool currentWritableState,
    TOperations& operations)
{
    Y_UNUSED(currentWritableState);
    operations.push(
        [newWritableState = std::move(newWritableState)]
        (TDiskRegistryDatabase& db) {
            db.WriteWritableState(newWritableState);
        });
}

void RestoreDisksToCleanup(
    TVector<TString> newDisksToCleanup,
    TVector<TString> currentDisksToCleanup,
    TOperations& operations)
{
    for (auto&& disk: currentDisksToCleanup) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.DeleteDiskToCleanup(disk);
            });
    }
    for (auto&& disk: newDisksToCleanup) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.AddDiskToCleanup(disk);
            });
    }
}

void RestoreErrorNotifications(
    TVector<TString> newErrorNotifications,
    TVector<TString> currentErrorNotifications,
    TOperations& operations)
{
    for (auto&& disk: currentErrorNotifications) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.DeleteErrorNotification(disk);
            });
    }
    for (auto&& disk: newErrorNotifications) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.AddErrorNotification(disk);
            });
    }
}

void RestoreUserNotifications(
    TVector<NProto::TUserNotification> newUserNotifications,
    TVector<NProto::TUserNotification> currentUserNotifications,
    TOperations& operations)
{
    for (auto&& notif: currentUserNotifications) {
        operations.push(
            [seqNo = notif.GetSeqNo()] (TDiskRegistryDatabase& db)
            {
                db.DeleteUserNotification(seqNo);
            });
    }
    for (auto&& notif: newUserNotifications) {
        operations.push(
            [notif = std::move(notif)] (TDiskRegistryDatabase& db) {
                db.AddUserNotification(notif);
            });
    }
}

void RestoreOutdatedVolumeConfigs(
    TVector<TString> newOutdatedVolumeConfigs,
    TVector<TString> currentOutdatedVolumeConfigs,
    TOperations& operations)
{
    for (auto&& disk: currentOutdatedVolumeConfigs) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.DeleteOutdatedVolumeConfig(disk);
            });
    }
    for (auto&& disk: newOutdatedVolumeConfigs) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.AddOutdatedVolumeConfig(disk);
            });
    }
}

void RestoreSuspendedDevices(
    TVector<NProto::TSuspendedDevice> newSuspendedDevices,
    TVector<NProto::TSuspendedDevice> currentSuspendedDevices,
    TOperations& operations)
{
    for (auto& device: currentSuspendedDevices) {
        operations.push(
            [uuid = device.GetId()](TDiskRegistryDatabase& db) {
                db.DeleteSuspendedDevice(uuid);
            });
    }
    for (auto&& device: newSuspendedDevices) {
        operations.push(
            [device = std::move(device)](TDiskRegistryDatabase& db) {
                db.UpdateSuspendedDevice(device);
            });
    }
}

void RestoreAutomaticallyReplacedDevices(
    TDeque<TAutomaticallyReplacedDeviceInfo> newAutomaticallyReplacedDevices,
    TDeque<TAutomaticallyReplacedDeviceInfo> currentAutomaticallyReplacedDevices,
    TOperations& operations)
{
    for (auto&& device: currentAutomaticallyReplacedDevices) {
        operations.push(
            [device = std::move(device)](TDiskRegistryDatabase& db) {
                db.DeleteAutomaticallyReplacedDevice(device.DeviceId);
            });
    }
    for (auto&& device: newAutomaticallyReplacedDevices) {
        operations.push(
            [device = std::move(device)](TDiskRegistryDatabase& db) {
                db.AddAutomaticallyReplacedDevice(device);
            });
    }
}

void RestoreDiskRegistryAgentListParams(
    THashMap<TString, NProto::TDiskRegistryAgentParams> newDiskRegistryAgentListParams,
    THashMap<TString, NProto::TDiskRegistryAgentParams> currentDiskRegistryAgentListParams,
    TOperations& operations)
{
    for (auto&& disk: currentDiskRegistryAgentListParams) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.DeleteDiskRegistryAgentListParams(disk.first);
            });
    }
    for (auto&& disk: newDiskRegistryAgentListParams) {
        operations.push(
            [disk = std::move(disk)](TDiskRegistryDatabase& db) {
                db.AddDiskRegistryAgentListParams(disk.first, disk.second);
            });
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryStateSnapshot MakeNewLoadState(
    NProto::TDiskRegistryStateBackup&& backup)
{
    auto move = [] (auto& src, auto& dst) {
        dst.reserve(src.size());
        dst.assign(
            std::make_move_iterator(src.begin()),
            std::make_move_iterator(src.end()));
        src.Clear();
    };

    auto transform = [] (auto& src, auto& dst, auto func) {
        dst.resize(src.size());
        for (int i = 0; i < src.size(); ++i) {
            func(src[i], dst[i]);
        }
        src.Clear();
    };

    TDiskRegistryStateSnapshot newLoadState;
    // if new fields are added to TDiskRegistryStateSnapshot
    // there will be a compilation error.
    auto& [
        config,
        dirtyDevices,
        agents,
        disks,
        placementGroups,
        brokenDisks,
        disksToReallocate,
        diskStateChanges,
        lastDiskStateSeqNo,
        writableState,
        disksToCleanup,
        errorNotifications,
        userNotifications,
        outdatedVolumeConfigs,
        suspendedDevices,
        automaticallyReplacedDevices,
        diskRegistryAgentListParams,
        replicasWithRecentlyReplacedDevices
    ] = newLoadState;

    if (backup.DirtyDevicesSize()) {
        transform(
            *backup.MutableDirtyDevices(),
            dirtyDevices,
            [] (auto& src, auto& dst) {
                dst.Id = src.GetId();
                dst.DiskId = src.GetDiskId();
            });
    } else {
        transform(
            *backup.MutableOldDirtyDevices(),
            dirtyDevices,
            [] (auto& src, auto& dst) {
                dst.Id = src;
            });
    }

    move(*backup.MutableAgents(), agents);
    move(*backup.MutableDisks(), disks);
    move(*backup.MutablePlacementGroups(), placementGroups);
    move(*backup.MutableDisksToNotify(), disksToReallocate);
    move(*backup.MutableDisksToCleanup(), disksToCleanup);

    move(*backup.MutableErrorNotifications(), errorNotifications);
    move(*backup.MutableUserNotifications(), userNotifications);
    // Filter out unknown events for future version rollback compatibility
    std::erase_if(userNotifications, [] (const auto& notif) {
            return notif.GetEventCase()
                == NProto::TUserNotification::EventCase::EVENT_NOT_SET;
        });

    move(*backup.MutableOutdatedVolumeConfigs(), outdatedVolumeConfigs);
    move(*backup.MutableSuspendedDevices(), suspendedDevices);

    transform(
        *backup.MutableBrokenDisks(),
        brokenDisks,
        [] (auto& src, auto& dst) {
            dst.DiskId = src.GetDiskId();
            dst.TsToDestroy = TInstant::MicroSeconds(src.GetTsToDestroy());
        });
    transform(
        *backup.MutableDiskStateChanges(),
        diskStateChanges,
        [] (auto& src, auto& dst) {
            if (src.HasState()) {
                dst.State.Swap(src.MutableState());
            }
            dst.SeqNo = src.GetSeqNo();
        });
    transform(
        *backup.MutableAutomaticallyReplacedDevices(),
        automaticallyReplacedDevices,
        [] (auto& src, auto& dst) {
            dst.DeviceId = src.GetDeviceId();
            dst.ReplacementTs = TInstant::MicroSeconds(src.GetReplacementTs());
        });

    diskRegistryAgentListParams.insert(
        backup.MutableDiskRegistryAgentListParams()->begin(),
        backup.MutableDiskRegistryAgentListParams()->end());

    if (backup.HasConfig()) {
        config.Swap(backup.MutableConfig());
    }

    lastDiskStateSeqNo = config.GetLastDiskStateSeqNo();
    writableState = config.GetWritableState();

    return newLoadState;
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleRestoreDiskRegistryState(
    const TEvDiskRegistry::TEvRestoreDiskRegistryStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(RestoreDiskRegistryState);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received RestoreDiskRegistryState request",
        TabletID());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);

    TDiskRegistryStateSnapshot snapshot =
        MakeNewLoadState(std::move(*ev->Get()->Record.MutableBackup()));

    if (ev->Get()->Record.GetForce()) {
        BecomeAux(ctx, STATE_RESTORE);

        ExecuteTx<TRestoreDiskRegistryState>(
            ctx,
            std::move(requestInfo),
            std::move(snapshot));
    } else {
        auto actor = NCloud::Register<NDiskRegistry::TRestoreValidationActor>(
            ctx,
            SelfId(),
            std::move(requestInfo),
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            std::move(snapshot));

        Actors.insert(actor);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleRestoreDiskRegistryValidationResponse(
    const TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Actors.erase(ev->Sender);

    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        auto response = std::make_unique<
            TEvDiskRegistry::TEvRestoreDiskRegistryStateResponse>();
        *response->Record.MutableError() = msg->GetError();
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    BecomeAux(ctx, STATE_RESTORE);

    ExecuteTx<TRestoreDiskRegistryState>(
        ctx,
        msg->RequestInfo,
        std::move(msg->LoadDBState));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareRestoreDiskRegistryState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreDiskRegistryState& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    return LoadState(db, args.CurrentState);
}

void TDiskRegistryActor::ExecuteRestoreDiskRegistryState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreDiskRegistryState& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    db.WriteRestoreState(true);

    InitializeState(args.NewState);
}

void TDiskRegistryActor::CompleteRestoreDiskRegistryState(
    const TActorContext& ctx,
    TTxDiskRegistry::TRestoreDiskRegistryState& args)
{
    TOperations operations;

    auto&& [
        newConfig,
        newDirtyDevices,
        newAgents,
        newDisks,
        newPlacementGroups,
        newBrokenDisks,
        newDisksToNotify,
        newDiskStateChanges,
        newLastDiskStateSeqNo,
        newWritableState,
        newDisksToCleanup,
        newErrorNotifications,
        newUserNotifications,
        newOutdatedVolumeConfigs,
        newSuspendedDevices,
        newAutomaticallyReplacedDevices,
        newDiskRegistryAgentListParams,
        newReplicasWithRecentlyReplacedDevices
    ] = std::move(args.NewState);

    auto&& [
        currentConfig,
        currentDirtyDevices,
        currentAgents,
        currentDisks,
        currentPlacementGroups,
        currentBrokenDisks,
        currentDisksToNotify,
        currentDiskStateChanges,
        currentLastDiskStateSeqNo,
        currentWritableState,
        currentDisksToCleanup,
        currentErrorNotifications,
        currentUserNotifications,
        currentOutdatedVolumeConfigs,
        currentSuspendedDevices,
        currentAutomaticallyReplacedDevices,
        currentDiskRegistryAgentListParams,
        currentReplicasWithRecentlyReplacedDevices
    ] = std::move(args.CurrentState);

    RestoreConfig(
        std::move(newConfig),
        std::move(currentConfig),
        operations);
    RestoreDirtyDevices(
        std::move(newDirtyDevices),
        std::move(currentDirtyDevices),
        operations);
    RestoreAgents(
        std::move(newAgents),
        std::move(currentAgents),
        operations);
    RestoreDisks(
        std::move(newDisks),
        std::move(currentDisks),
        operations);
    RestorePlacementGroups(
        std::move(newPlacementGroups),
        std::move(currentPlacementGroups),
        operations);
    RestoreBrokenDisks(
        std::move(newBrokenDisks),
        std::move(currentBrokenDisks),
        operations);
    RestoreDisksToNotify(
        std::move(newDisksToNotify),
        std::move(currentDisksToNotify),
        operations);
    RestoreDiskStateChanges(
        std::move(newDiskStateChanges),
        std::move(currentDiskStateChanges),
        operations);
    RestoreLastDiskStateSeqNo(
        newLastDiskStateSeqNo,
        currentLastDiskStateSeqNo,
        operations);
    RestoreWritableState(
        newWritableState,
        currentWritableState,
        operations);
    RestoreDisksToCleanup(
        std::move(newDisksToCleanup),
        std::move(currentDisksToCleanup),
        operations);
    RestoreErrorNotifications(
        std::move(newErrorNotifications),
        std::move(currentErrorNotifications),
        operations);
    RestoreUserNotifications(
        std::move(newUserNotifications),
        std::move(currentUserNotifications),
        operations);
    RestoreOutdatedVolumeConfigs(
        std::move(newOutdatedVolumeConfigs),
        std::move(currentOutdatedVolumeConfigs),
        operations);
    RestoreSuspendedDevices(
        std::move(newSuspendedDevices),
        std::move(currentSuspendedDevices),
        operations);
    RestoreAutomaticallyReplacedDevices(
        std::move(newAutomaticallyReplacedDevices),
        std::move(currentAutomaticallyReplacedDevices),
        operations);
    RestoreDiskRegistryAgentListParams(
        std::move(newDiskRegistryAgentListParams),
        std::move(currentDiskRegistryAgentListParams),
        operations);

    auto request = std::make_unique<
        TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartRequest>(
            std::move(args.RequestInfo),
            std::move(operations));

    NCloud::Send(ctx, ctx.SelfID, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleRestoreDiskRegistryPart(
    const TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received RestoreDiskRegistryPart request",
        TabletID());

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TRestoreDiskRegistryPart>(
        ctx,
        std::move(msg->RequestInfo),
        std::move(requestInfo),
        std::move(msg->Operations));
}

void TDiskRegistryActor::HandleRestoreDiskRegistryPartResponse(
    const TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto* msg = ev->Get();

    if (msg->Operations.empty()) {
        NCloud::Reply(
            ctx,
            *msg->RequestInfo,
            std::make_unique<
                TEvDiskRegistry::TEvRestoreDiskRegistryStateResponse>());
    } else {
        auto request = std::make_unique<
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartRequest>(
                std::move(msg->RequestInfo),
                std::move(msg->Operations));

        NCloud::Send(ctx, ctx.SelfID, std::move(request));
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareRestoreDiskRegistryPart(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreDiskRegistryPart& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TDiskRegistryActor::ExecuteRestoreDiskRegistryPart(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreDiskRegistryPart& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    const size_t count = std::min(
        Config->GetDiskRegistrySplitTransactionCounter(),
        static_cast<uint32_t>(args.Operations.size()));

    TDiskRegistryDatabase db(tx.DB);
    for (size_t i = 0; i < count; ++i) {
        auto operation = std::move(args.Operations.front());
        args.Operations.pop();
        operation(db);
    }

    if (args.Operations.empty()) {
        db.WriteRestoreState(false);
    }
}

void TDiskRegistryActor::CompleteRestoreDiskRegistryPart(
    const TActorContext& ctx,
    TTxDiskRegistry::TRestoreDiskRegistryPart& args)
{
    auto response = std::make_unique<
        TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartResponse>(
            args.RequestInfo,
            args.PartRequestInfo,
            std::move(args.Operations));

    NCloud::Reply(ctx, *args.PartRequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
