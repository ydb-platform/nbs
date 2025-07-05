#include "volume_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoringUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

class THttpRemoveClientActor final
    : public TActorBootstrapped<THttpRemoveClientActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TString DiskId;
    const TString ClientId;
    const ui64 TabletId;

public:
    THttpRemoveClientActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        TString clientId,
        ui64 tabletId);

    void Bootstrap(const TActorContext& ctx);

private:
    void Notify(
        const TActorContext& ctx,
        const TString& message,
        const EAlertLevel alertLevel);

private:
    STFUNC(StateWork);

    void HandleRemoveClientResponse(
        const TEvVolume::TEvRemoveClientResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

THttpRemoveClientActor::THttpRemoveClientActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        TString clientId,
        ui64 tabletId)
    : RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
    , ClientId(std::move(clientId))
    , TabletId(tabletId)
{}

void THttpRemoveClientActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvRemoveClientRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.MutableHeaders()->SetClientId(ClientId);
    request->Record.SetIsMonRequest(true);

    // TODO: why don't we send to volume actor directly?
    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request));

    Become(&TThis::StateWork);
}

void THttpRemoveClientActor::Notify(
    const TActorContext& ctx,
    const TString& message,
    const EAlertLevel alertLevel)
{
    LWTRACK(
        ResponseSent_Volume,
        RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        RequestInfo->CallContext->RequestId);

    TStringStream out;
    BuildTabletNotifyPageWithRedirect(out, message, TabletId, alertLevel);

    auto response = std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void THttpRemoveClientActor::HandleRemoveClientResponse(
    const TEvVolume::TEvRemoveClientResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    if (SUCCEEDED(response->GetStatus())) {
        Notify(ctx, "Operation successfully completed", EAlertLevel::SUCCESS);
    } else {
        Notify(
            ctx,
            TStringBuilder() << "failed to remove client "
                << ClientId.Quote() << " from volume "
                << DiskId.Quote() << ": "
                << FormatError(response->GetError()),
            EAlertLevel::DANGER);
    }

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(THttpRemoveClientActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvRemoveClientResponse, HandleRemoveClientResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleHttpInfo_RemoveClient(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto diskId = State->GetDiskId();
    const auto clientId = params.Get("ClientId");

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Removing volume client per action from monitoring page: client %s",
        LogTitle.GetWithTime().c_str(),
        clientId.Quote().c_str());

    if (!clientId) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "No client id is given");
        return;
    }

    NCloud::Register<THttpRemoveClientActor>(
        ctx,
        std::move(requestInfo),
        std::move(diskId),
        std::move(clientId),
        TabletID());
}

}   // namespace NCloud::NBlockStore::NStorage
