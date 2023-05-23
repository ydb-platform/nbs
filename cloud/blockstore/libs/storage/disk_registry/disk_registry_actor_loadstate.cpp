#include "disk_registry_actor.h"

#include <ydb/core/base/appdata.h>

#include <util/generic/algorithm.h>

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::LoadState(
    TDiskRegistryDatabase& db,
    TDiskRegistryStateSnapshot& args)
{
    return AllSucceeded({
        db.ReadDiskRegistryConfig(args.Config),
        db.ReadDirtyDevices(args.DirtyDevices),
        db.ReadOldAgents(args.OldAgents),
        db.ReadAgents(args.Agents),
        db.ReadDisks(args.Disks),
        db.ReadPlacementGroups(args.PlacementGroups),
        db.ReadBrokenDisks(args.BrokenDisks),
        db.ReadDisksToNotify(args.DisksToNotify),
        db.ReadErrorNotifications(args.ErrorNotifications),
        db.ReadDiskStateChanges(args.DiskStateChanges),
        db.ReadLastDiskStateSeqNo(args.LastDiskStateSeqNo),
        db.ReadWritableState(args.WritableState),
        db.ReadDisksToCleanup(args.DisksToCleanup),
        db.ReadOutdatedVolumeConfigs(args.OutdatedVolumeConfigs),
        db.ReadSuspendedDevices(args.SuspendedDevices),
        db.ReadAutomaticallyReplacedDevices(args.AutomaticallyReplacedDevices),
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
    // Move OldAgents to Agents

    THashSet<TString> ids;
    for (const auto& agent: args.Snapshot.Agents) {
        ids.insert(agent.GetAgentId());
    }

    TDiskRegistryDatabase db(tx.DB);

    for (auto& agent: args.Snapshot.OldAgents) {
        if (!ids.insert(agent.GetAgentId()).second) {
            continue;
        }

        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "Agent %s:%d moved to new table",
            agent.GetAgentId().c_str(),
            agent.GetNodeId());

        args.Snapshot.Agents.push_back(agent);

        db.UpdateAgent(agent);
    }
}

void TDiskRegistryActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxDiskRegistry::TLoadState& args)
{
    Y_VERIFY(CurrentState == STATE_INIT);

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

    for (const auto& agent: args.Snapshot.Agents) {
        if (agent.GetState() != NProto::AGENT_STATE_UNAVAILABLE) {
            // this event will be scheduled using NonReplicatedAgentMaxTimeout
            ScheduleRejectAgent(ctx, agent.GetAgentId(), 0);
        }
    }

    // initialize state
    State.reset(new TDiskRegistryState(
        Config,
        ComponentGroup,
        std::move(args.Snapshot.Config),
        std::move(args.Snapshot.Agents),
        std::move(args.Snapshot.Disks),
        std::move(args.Snapshot.PlacementGroups),
        std::move(args.Snapshot.BrokenDisks),
        std::move(args.Snapshot.DisksToNotify),
        std::move(args.Snapshot.DiskStateChanges),
        args.Snapshot.LastDiskStateSeqNo,
        std::move(args.Snapshot.DirtyDevices),
        std::move(args.Snapshot.DisksToCleanup),
        std::move(args.Snapshot.ErrorNotifications),
        std::move(args.Snapshot.OutdatedVolumeConfigs),
        std::move(args.Snapshot.SuspendedDevices),
        std::move(args.Snapshot.AutomaticallyReplacedDevices)));

    SecureErase(ctx);

    ScheduleCleanup(ctx);

    DestroyBrokenDisks(ctx);

    NotifyDisks(ctx);

    NotifyUsers(ctx);

    PublishDiskStates(ctx);

    UpdateCounters(ctx);

    StartMigration(ctx);

    UpdateVolumeConfigs(ctx);

    ProcessAutomaticallyReplacedDevices(ctx);

    ScheduleMakeBackup(ctx, args.LastBackupTime);
}

}   // namespace NCloud::NBlockStore::NStorage
