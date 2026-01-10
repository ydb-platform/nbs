#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCompareDiskRegistryStateWithLocalDb(
    const TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbRequest::TPtr&
        ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(CompareDiskRegistryStateWithLocalDb);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received CompareDiskRegistryStateWithLocalDb request",
        LogTitle.GetWithTime().c_str());

    ExecuteTx<TCompareDiskRegistryStateWithLocalDb>(
        ctx,
        CreateRequestInfo<
            TEvDiskRegistry::TCompareDiskRegistryStateWithLocalDbMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext),
        false);
}

void TDiskRegistryActor::HandleScheduledCompareDiskRegistryStateWithLocalDb(
    const TEvDiskRegistryPrivate::TEvCompareDiskRegistryStateWithLocalDb::TPtr&
        ev,
    const TActorContext& ctx)
{
    ExecuteTx<TCompareDiskRegistryStateWithLocalDb>(
        ctx,
        CreateRequestInfo<
            TEvDiskRegistry::TCompareDiskRegistryStateWithLocalDbMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext),
        true);
}

void TDiskRegistryActor::
    HandleScheduledCompareDiskRegistryStateWithLocalDbReadOnly(
        const TEvDiskRegistryPrivate::TEvCompareDiskRegistryStateWithLocalDb::
            TPtr& ev,
        const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ScheduleCompareDiskRegistryStateWithLocalDb(ctx);
}

bool TDiskRegistryActor::PrepareCompareDiskRegistryStateWithLocalDb(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCompareDiskRegistryStateWithLocalDb& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    return LoadState(db, args.StateArgs);
}

void TDiskRegistryActor::ExecuteCompareDiskRegistryStateWithLocalDb(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCompareDiskRegistryStateWithLocalDb& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    auto dbState = std::make_unique<TDiskRegistryState>(
        Logging,
        Config,
        ComponentGroup,
        std::move(args.StateArgs.Config),
        std::move(args.StateArgs.Agents),
        std::move(args.StateArgs.Disks),
        std::move(args.StateArgs.PlacementGroups),
        std::move(args.StateArgs.BrokenDisks),
        std::move(args.StateArgs.DisksToReallocate),
        std::move(args.StateArgs.DiskStateChanges),
        args.StateArgs.LastDiskStateSeqNo,
        std::move(args.StateArgs.DirtyDevices),
        std::move(args.StateArgs.DisksToCleanup),
        std::move(args.StateArgs.ErrorNotifications),
        std::move(args.StateArgs.UserNotifications),
        std::move(args.StateArgs.OutdatedVolumeConfigs),
        std::move(args.StateArgs.SuspendedDevices),
        std::move(args.StateArgs.AutomaticallyReplacedDevices),
        std::move(args.StateArgs.DiskRegistryAgentListParams));

    google::protobuf::util::MessageDifferencer diff;
    TString report;

    diff.ReportDifferencesToString(&report);
    google::protobuf::util::DefaultFieldComparator comparator;
    comparator.set_float_comparison(
        google::protobuf::util::DefaultFieldComparator::FloatComparison::
            APPROXIMATE);
    diff.set_field_comparator(&comparator);

    diff.IgnoreField(
        NProto::TAgentConfig::descriptor()->FindFieldByName("UnknownDevices"));

    if (!diff.Compare(State->BackupState(), dbState->BackupState())) {
        args.Result.SetDiffers(report);
    }
}

void TDiskRegistryActor::CompleteCompareDiskRegistryStateWithLocalDb(
    const TActorContext& ctx,
    TTxDiskRegistry::TCompareDiskRegistryStateWithLocalDb& args)
{
    auto diff = args.Result.GetDiffers();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s CompareDiskRegistryStateWithLocalDb result: %s",
        LogTitle.GetWithTime().c_str(),
        diff.empty() ? "OK" : diff.c_str());

    if (!args.Result.GetDiffers().empty()) {
        ReportDiskRegistryStateMismatchedLocalDb(args.Result.GetDiffers());
    }

    if (!args.Scheduled) {
        auto response = std::make_unique<
            TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbResponse>(
            std::move(args.Result));

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    ScheduleCompareDiskRegistryStateWithLocalDb(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
