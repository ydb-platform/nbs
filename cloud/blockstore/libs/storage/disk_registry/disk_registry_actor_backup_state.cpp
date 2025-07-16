#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleBackupDiskRegistryState(
    const TEvDiskRegistry::TEvBackupDiskRegistryStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(BackupDiskRegistryState);

    const auto* msg = ev->Get();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received BackupDiskRegistryState request."
        "BackupLocalDB=%s,"
        "BackupFilePath=%s",
        TabletID(),
        msg->Record.GetBackupLocalDB() ? "true" : "false",
        msg->Record.GetBackupFilePath().c_str());

    if (!msg->Record.GetBackupLocalDB()) {
        auto response = std::make_unique<
            TEvDiskRegistry::TEvBackupDiskRegistryStateResponse>();
        *response->Record.MutableBackupFilePath() =
            msg->Record.GetBackupFilePath();
        *response->Record.MutableBackup() =
            State->BackupState();
        response->Record.MutableBackup()->MutableConfig()
            ->SetWritableState(CurrentState != STATE_READ_ONLY);

        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TBackupDiskRegistryState>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetBackupFilePath());
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareBackupDiskRegistryState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TBackupDiskRegistryState& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    return LoadState(db, args.Snapshot);
}

void TDiskRegistryActor::ExecuteBackupDiskRegistryState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TBackupDiskRegistryState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    db.WriteLastBackupTs(TInstant::Now());
}

void TDiskRegistryActor::CompleteBackupDiskRegistryState(
    const TActorContext& ctx,
    TTxDiskRegistry::TBackupDiskRegistryState& args)
{
    auto response = std::make_unique<
        TEvDiskRegistry::TEvBackupDiskRegistryStateResponse>();
    *response->Record.MutableBackupFilePath() = args.BackupFilePath;

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
    ] = args.Snapshot;

    auto copy = [] (auto& src, auto* dst) {
        dst->Reserve(src.size());
        std::copy(
            std::make_move_iterator(src.begin()),
            std::make_move_iterator(src.end()),
            RepeatedFieldBackInserter(dst)
        );
        src.clear();
    };

    auto transform = [] (auto& src, auto* dst, auto func) {
        dst->Reserve(src.size());
        for (auto& x: src) {
            func(x, *dst->Add());
        }
        src.clear();
    };

    auto& backup = *response->Record.MutableBackup();

    for (auto & [uuid, diskId]: dirtyDevices) {
        backup.AddOldDirtyDevices(uuid);

        auto* dd = backup.AddDirtyDevices();
        dd->SetId(uuid);
        dd->SetDiskId(diskId);
    }

    copy(disks, backup.MutableDisks());
    copy(suspendedDevices, backup.MutableSuspendedDevices());

    copy(agents, backup.MutableAgents());

    copy(placementGroups, backup.MutablePlacementGroups());
    copy(disksToReallocate, backup.MutableDisksToNotify());
    copy(disksToCleanup, backup.MutableDisksToCleanup());

    copy(errorNotifications, backup.MutableErrorNotifications());
    copy(userNotifications, backup.MutableUserNotifications());

    copy(outdatedVolumeConfigs, backup.MutableOutdatedVolumeConfigs());

    transform(brokenDisks, backup.MutableBrokenDisks(), [] (auto& src, auto& dst) {
        dst.SetDiskId(src.DiskId);
        dst.SetTsToDestroy(src.TsToDestroy.MicroSeconds());
    });

    transform(diskStateChanges, backup.MutableDiskStateChanges(), [] (auto& src, auto& dst) {
        dst.MutableState()->Swap(&src.State);
        dst.SetSeqNo(src.SeqNo);
    });

    transform(
        automaticallyReplacedDevices,
        backup.MutableAutomaticallyReplacedDevices(),
        [] (auto& src, auto& dst) {
            dst.SetDeviceId(src.DeviceId);
            dst.SetReplacementTs(src.ReplacementTs.MicroSeconds());
        });

    backup.MutableDiskRegistryAgentListParams()->insert(
        diskRegistryAgentListParams.begin(),
        diskRegistryAgentListParams.end());

    backup.MutableConfig()->Swap(&config);
    backup.MutableConfig()->SetLastDiskStateSeqNo(lastDiskStateSeqNo);
    backup.MutableConfig()->SetWritableState(writableState);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
