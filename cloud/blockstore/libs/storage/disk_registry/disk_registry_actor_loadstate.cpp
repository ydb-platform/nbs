#include "disk_registry_actor.h"

#include <contrib/ydb/core/base/appdata.h>

#include <util/generic/algorithm.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool AllSucceeded(std::initializer_list<bool> ls)
{
    auto identity = [] (bool x) {
        return x;
    };

    return std::all_of(std::begin(ls), std::end(ls), identity);
}

// TODO: Remove legacy compatibility in next release
void ProcessUserNotifications(
    const TActorContext& ctx,
    TDiskRegistryDatabase& db,
    TVector<TString>& errorNotifications,
    TVector<NProto::TUserNotification>& userNotifications)
{
    // Filter out unknown events for future version rollback compatibility
    std::erase_if(userNotifications, [] (const auto& notif) {
            return notif.GetEventCase()
                == NProto::TUserNotification::EventCase::EVENT_NOT_SET;
        });

    THashSet<TString> ids(
        errorNotifications.begin(),
        errorNotifications.end(),
        errorNotifications.size());

    auto isObsolete = [&ids, now = ctx.Now()] (const auto& notif) {
        if (notif.GetHasLegacyCopy()
            && !ids.contains(notif.GetDiskError().GetDiskId()))
        {
            return true;
        }

        TDuration age = now - TInstant::MicroSeconds(notif.GetTimestamp());
        // It seems it's feasible to hardcode the threshold in temporary code
        return age >= TDuration::Days(3);
    };

    for (const auto& notif: userNotifications) {
        if (isObsolete(notif)) {
            LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
                "Obsolete user notification deleted: %s",
                (TStringBuilder() << notif).c_str()
            );
            db.DeleteUserNotification(notif.GetSeqNo());
        }
    }
    std::erase_if(userNotifications, isObsolete);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::LoadState(
    TDiskRegistryDatabase& db,
    TDiskRegistryStateSnapshot& args)
{
    return AllSucceeded({
        db.ReadDiskRegistryConfig(args.Config),
        db.ReadDirtyDevices(args.DirtyDevices),
        db.ReadAgents(args.Agents),
        db.ReadDisks(args.Disks),
        db.ReadPlacementGroups(args.PlacementGroups),
        db.ReadBrokenDisks(args.BrokenDisks),
        db.ReadDisksToReallocate(args.DisksToReallocate),
        db.ReadErrorNotifications(args.ErrorNotifications),
        db.ReadUserNotifications(args.UserNotifications),
        db.ReadDiskStateChanges(args.DiskStateChanges),
        db.ReadLastDiskStateSeqNo(args.LastDiskStateSeqNo),
        db.ReadWritableState(args.WritableState),
        db.ReadDisksToCleanup(args.DisksToCleanup),
        db.ReadOutdatedVolumeConfigs(args.OutdatedVolumeConfigs),
        db.ReadSuspendedDevices(args.SuspendedDevices),
        db.ReadAutomaticallyReplacedDevices(args.AutomaticallyReplacedDevices),
        db.ReadDiskRegistryAgentListParams(args.DiskRegistryAgentListParams),
        db.ReadDiskWithRecentlyReplacedDevices(
            args.ReplicasWithRecentlyReplacedDevices),
    });
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TLoadState& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);

    return AllSucceeded({
        LoadState(db, args.Snapshot),
        db.ReadRestoreState(args.RestoreState),
        db.ReadLastBackupTs(args.LastBackupTime)});
}

void TDiskRegistryActor::ExecuteLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TLoadState& args)
{
    TDiskRegistryDatabase db(tx.DB);
    ProcessUserNotifications(
        ctx,
        db,
        args.Snapshot.ErrorNotifications,
        args.Snapshot.UserNotifications);
}

void TDiskRegistryActor::InitializeState(TDiskRegistryStateSnapshot snapshot)
{
    State = std::make_unique<TDiskRegistryState>(
        Logging,
        Config,
        ComponentGroup,
        std::move(snapshot.Config),
        std::move(snapshot.Agents),
        std::move(snapshot.Disks),
        std::move(snapshot.PlacementGroups),
        std::move(snapshot.BrokenDisks),
        std::move(snapshot.DisksToReallocate),
        std::move(snapshot.DiskStateChanges),
        snapshot.LastDiskStateSeqNo,
        std::move(snapshot.DirtyDevices),
        std::move(snapshot.DisksToCleanup),
        std::move(snapshot.ErrorNotifications),
        std::move(snapshot.UserNotifications),
        std::move(snapshot.OutdatedVolumeConfigs),
        std::move(snapshot.SuspendedDevices),
        std::move(snapshot.AutomaticallyReplacedDevices),
        std::move(snapshot.DiskRegistryAgentListParams),
        THashMap<TString, NProto::TReplicaWithRecentlyReplacedDevices>());
}

void TDiskRegistryActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxDiskRegistry::TLoadState& args)
{
    Y_ABORT_UNLESS(CurrentState == STATE_INIT);

    if (args.RestoreState) {
        BecomeAux(ctx, STATE_RESTORE);
    } else if (!args.Snapshot.WritableState) {
        BecomeAux(ctx, STATE_READ_ONLY);
    } else {
        BecomeAux(ctx, STATE_WORK);
    }

    // allow pipes to connect
    SignalTabletActive(ctx);

    // resend pending requests
    SendPendingRequests(ctx, PendingRequests);

    InitializeState(std::move(args.Snapshot));

    if (TDuration timeout = Config->GetNonReplicatedAgentMaxTimeout()) {
        const auto deadline = timeout.ToDeadLine(ctx.Now());

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Schedule the initial agents rejection phase to " << deadline);

        auto request =
            std::make_unique<TEvDiskRegistryPrivate::TEvAgentConnectionLost>();
        ctx.Schedule(deadline, request.release());
    }

    SecureErase(ctx);

    ScheduleCleanup(ctx);

    DestroyBrokenDisks(ctx);

    ReallocateDisks(ctx);

    NotifyUsers(ctx);

    PublishDiskStates(ctx);

    UpdateCounters(ctx);

    StartMigration(ctx);

    UpdateVolumeConfigs(ctx);

    ProcessAutomaticallyReplacedDevices(ctx);

    ScheduleMakeBackup(ctx, args.LastBackupTime);

    ScheduleDiskRegistryAgentListExpiredParamsCleanup(ctx);

    if (auto orphanDevices = State->FindOrphanDevices()) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Found devices without agent and try to remove them: "
            "DeviceUUIDs=%s",
            JoinSeq(" ", orphanDevices).c_str());

        ExecuteTx<TRemoveOrphanDevices>(ctx, std::move(orphanDevices));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
