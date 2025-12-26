#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompareActor final: public TActorBootstrapped<TCompareActor>
{
private:
    const TChildLogTitle LogTitle;
    const TActorId Owner;
    TRequestInfoPtr RequestInfo;

    TString DiffReport;

public:
    TCompareActor(
        TChildLogTitle logTitle,
        const TActorId& owner,
        TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateCompare);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleBackupDiskRegistryStateResponse(
        const TEvDiskRegistry::TEvBackupDiskRegistryStateResponse::TPtr& ev,
        const TActorContext& ctx);
};

TCompareActor::TCompareActor(
    TChildLogTitle logTitle,
    const TActorId& owner,
    TRequestInfoPtr requestInfo)
    : LogTitle(std::move(logTitle))
    , Owner(owner)
    , RequestInfo(std::move(requestInfo))
{}

void TCompareActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateCompare);

    auto request =
        std::make_unique<TEvDiskRegistry::TEvBackupDiskRegistryStateRequest>();
    request->Record.SetSource(NProto::BACKUP_DISK_REGISTRY_STATE_SOURCE_BOTH);

    NCloud::Send(ctx, Owner, std::move(request));
}

void TCompareActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<
        TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbResponse>(error);

    response->Record.SetDiffers(DiffReport);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCompareActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TCompareActor::HandleBackupDiskRegistryStateResponse(
    const TEvDiskRegistry::TEvBackupDiskRegistryStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    google::protobuf::util::MessageDifferencer diff;

    DiffReport.clear();
    diff.ReportDifferencesToString(&DiffReport);
    diff.set_report_ignores(false);
    diff.set_report_moves(false);

    google::protobuf::util::DefaultFieldComparator comparator;
    comparator.set_float_comparison(
        google::protobuf::util::DefaultFieldComparator::FloatComparison::
            APPROXIMATE);
    diff.set_field_comparator(&comparator);

    diff.IgnoreField(
        NProto::TAgentConfig::descriptor()->FindFieldByName("UnknownDevices"));
    // diff.IgnoreField(NProto::TDiskRegistryConfig::descriptor()->FindFieldByName("WritableState"));
    if (msg->Record.BackupKind_case() !=
        NProto::TBackupDiskRegistryStateResponse::kBothSourcesBackup)
    {
        ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
    }

    const auto* descriptor = NProto::TDiskRegistryStateBackup::descriptor();
    diff.IgnoreField(descriptor->FindFieldByName("OldDirtyDevices"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("Disks"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("PlacementGroups"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("Agents"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("Sessions"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("DiskStateChanges"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("BrokenDisks"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("DisksToNotify"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("DisksToCleanup"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("ErrorNotifications"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("OutdatedVolumeConfigs"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("OldSuspendedDevices"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("DirtyDevices"));
    diff.TreatAsSmartSet(
        descriptor->FindFieldByName("AutomaticallyReplacedDevices"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("UserNotifications"));
    diff.TreatAsSmartSet(descriptor->FindFieldByName("SuspendedDevices"));

    diff.Compare(
        msg->Record.GetBothSourcesBackup().GetRamBackup(),
        msg->Record.GetBothSourcesBackup().GetLocalDbBackup());

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s CompareDiskRegistryStateWithLocalDb result: %s",
        LogTitle.GetWithTime().c_str(),
        DiffReport.empty() ? "OK" : DiffReport.c_str());

    ReplyAndDie(ctx, {});
}

STFUNC(TCompareActor::StateCompare)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskRegistry::TEvBackupDiskRegistryStateResponse,
            HandleBackupDiskRegistryStateResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCompareDiskRegistryStateWithLocalDb(
    const TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbRequest::TPtr&
        ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(CompareDiskRegistryStateWithLocalDb);

    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received CompareDiskRegistryStateWithLocalDb request",
        LogTitle.GetWithTime().c_str());

    auto actor = NCloud::Register<TCompareActor>(
        ctx,
        LogTitle.GetChildWithTags(GetCycleCount(), {}),
        ctx.SelfID,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext));
    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
