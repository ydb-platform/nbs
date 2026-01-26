#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEnsureStateIntegrityActor final
    : public TActorBootstrapped<TEnsureStateIntegrityActor>
{
private:
    const TChildLogTitle LogTitle;
    const TActorId Owner;
    TRequestInfoPtr RequestInfo;

    TString DiffReport;

public:
    TEnsureStateIntegrityActor(
        TChildLogTitle logTitle,
        const TActorId& owner,
        TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateEnsure);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleBackupDiskRegistryStateResponse(
        const TEvDiskRegistry::TEvBackupDiskRegistryStateResponse::TPtr& ev,
        const TActorContext& ctx);
};

TEnsureStateIntegrityActor::TEnsureStateIntegrityActor(
    TChildLogTitle logTitle,
    const TActorId& owner,
    TRequestInfoPtr requestInfo)
    : LogTitle(std::move(logTitle))
    , Owner(owner)
    , RequestInfo(std::move(requestInfo))
{}

void TEnsureStateIntegrityActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateEnsure);

    auto request =
        std::make_unique<TEvDiskRegistry::TEvBackupDiskRegistryStateRequest>();
    request->Record.SetSource(NProto::BDRSS_MEMORY_AND_LOCAL_DB);

    NCloud::Send(ctx, Owner, std::move(request));
}

void TEnsureStateIntegrityActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (RequestInfo) {
        auto response = std::make_unique<
            TEvDiskRegistry::TEvEnsureDiskRegistryStateIntegrityResponse>(
            error);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TEnsureStateIntegrityActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TEnsureStateIntegrityActor::HandleBackupDiskRegistryStateResponse(
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

    const auto* descriptor = NProto::TDiskRegistryStateBackup::descriptor();
    diff.IgnoreField(descriptor->FindFieldByName("UnknownDevices"));
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

    const bool equal = diff.Compare(
        msg->Record.GetMemoryBackup(),
        msg->Record.GetLocalDBBackup());

    NProto::TError error;
    if (!equal) {
        error = MakeError(
            E_FAIL,
            Sprintf(
                "%s DiskRegistry State With Local DB differs: %s",
                LogTitle.GetWithTime().c_str(),
                DiffReport.Quote().c_str()));
    }

    ReplyAndDie(ctx, error);
}

STFUNC(TEnsureStateIntegrityActor::StateEnsure)
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

void TDiskRegistryActor::HandleEnsureDiskRegistryStateIntegrity(
    const TEvDiskRegistry::TEvEnsureDiskRegistryStateIntegrityRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(EnsureDiskRegistryStateIntegrity);

    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received EnsureDiskRegistryStateIntegrity request",
        LogTitle.GetWithTime().c_str());

    auto actor = NCloud::Register<TEnsureStateIntegrityActor>(
        ctx,
        LogTitle.GetChildWithTags(GetCycleCount(), {}),
        ctx.SelfID,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext));
    Actors.insert(actor);
}

void TDiskRegistryActor::HandleEnsureDiskRegistryStateIntegrityResponse(
    const TEvDiskRegistry::TEvEnsureDiskRegistryStateIntegrityResponse::TPtr&
        ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            msg->GetErrorReason().c_str());
        ReportDiskRegistryStateIntegrityBroken(msg->GetErrorReason());
    }

    ScheduleEnsureDiskRegistryStateIntegrity(ctx);
}

void TDiskRegistryActor::HandleScheduledEnsureDiskRegistryStateIntegrity(
    const TEvDiskRegistryPrivate::TEvEnsureDiskRegistryStateIntegrity::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto actor = NCloud::Register<TEnsureStateIntegrityActor>(
        ctx,
        LogTitle.GetChildWithTags(GetCycleCount(), {}),
        ctx.SelfID,
        CreateRequestInfo(SelfId(), 0, MakeIntrusive<TCallContext>()));
    Actors.insert(actor);
}
}   // namespace NCloud::NBlockStore::NStorage
