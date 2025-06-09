#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NMonitoringUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

class THttpVolumeReallocActor final
    : public TActorBootstrapped<THttpVolumeReallocActor>
{
private:
    const TActorId Owner;
    const ui64 TabletId;
    const TRequestInfoPtr RequestInfo;
    const TString DiskId;

public:
    THttpVolumeReallocActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString diskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void Notify(
        const TActorContext& ctx,
        TString message,
        const EAlertLevel alertLevel);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleInitiateDiskReallocationResponse(
        const TEvDiskRegistryPrivate::TEvInitiateDiskReallocationResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

THttpVolumeReallocActor::THttpVolumeReallocActor(
        const TActorId& owner,
        ui64 tabletId,
        TRequestInfoPtr requestInfo,
        TString diskId)
    : Owner(owner)
    , TabletId(tabletId)
    , RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
{}

void THttpVolumeReallocActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvInitiateDiskReallocationRequest>(
        DiskId);

    NCloud::Send(
        ctx,
        Owner,
        std::move(request));

    Become(&TThis::StateWork);
}

void THttpVolumeReallocActor::Notify(
    const TActorContext& ctx,
    TString message,
    const EAlertLevel alertLevel)
{
    TStringStream out;
    BuildTabletNotifyPageWithRedirect(out, std::move(message), TabletId, alertLevel);

    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out.Str())));
}

void THttpVolumeReallocActor::ReplyAndDie(const TActorContext& ctx, NProto::TError error)
{
    if (SUCCEEDED(error.GetCode())) {
        Notify(ctx, "Operation successfully completed", EAlertLevel::SUCCESS);
    } else {
        Notify(
            ctx,
            TStringBuilder()
                << "failed to reallocate volume "
                << DiskId.Quote()
                << ": " << FormatError(error),
            EAlertLevel::DANGER);
    }

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void THttpVolumeReallocActor::HandleInitiateDiskReallocationResponse(
    const TEvDiskRegistryPrivate::TEvInitiateDiskReallocationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    ReplyAndDie(ctx, response->GetError());
}

void THttpVolumeReallocActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(THttpVolumeReallocActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskRegistryPrivate::TEvInitiateDiskReallocationResponse,
            HandleInitiateDiskReallocationResponse);

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

void TDiskRegistryActor::HandleHttpInfo_VolumeRealloc(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto& diskId = params.Get("DiskID");

    if (!diskId) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "No disk id is given");
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Initiate volume reallocation from monitoring page: volume %s",
        diskId.Quote().data());

    auto actor = NCloud::Register<THttpVolumeReallocActor>(
        ctx,
        SelfId(),
        TabletID(),
        std::move(requestInfo),
        diskId);
    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
