#include "service_actor.h"
#include "service.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class THttpUnmountVolumeActor final
    : public TActorBootstrapped<THttpUnmountVolumeActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TString DiskId;
    const TString ClientId;
    const TString SessionId;

public:
    THttpUnmountVolumeActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        TString clientId,
        TString sessionId);

    void Bootstrap(const TActorContext& ctx);

private:
    void Notify(
        const TActorContext& ctx,
        TString message,
        const EAlertLevel alertLevel);

private:
    STFUNC(StateWork);

    void HandleUnmountVolumeResponse(
        const TEvService::TEvUnmountVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

THttpUnmountVolumeActor::THttpUnmountVolumeActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        TString clientId,
        TString sessionId)
    : RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
    , ClientId(std::move(clientId))
    , SessionId(std::move(sessionId))
{}

void THttpUnmountVolumeActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvUnmountVolumeRequest>();
    auto& headers = *request->Record.MutableHeaders();
    headers.SetClientId(ClientId);
    headers.MutableInternal()->SetControlSource(NProto::SOURCE_SERVICE_MONITORING);
    request->Record.SetDiskId(DiskId);
    request->Record.SetSessionId(SessionId);

    NCloud::Send(
        ctx,
        MakeStorageServiceId(),
        std::move(request));

    Become(&TThis::StateWork);
}

void THttpUnmountVolumeActor::Notify(
    const TActorContext& ctx,
    TString message,
    const EAlertLevel alertLevel)
{
    TStringStream out;
    BuildNotifyPageWithRedirect(
        out,
        std::move(message),
        TStringBuilder()
            << "../blockstore/service?Volume=" << DiskId << "&action=listclients",
        alertLevel);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        RequestInfo->CallContext->RequestId);

    auto response = std::make_unique<NMon::TEvHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void THttpUnmountVolumeActor::HandleUnmountVolumeResponse(
    const TEvService::TEvUnmountVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    if (SUCCEEDED(response->GetStatus())) {
        Notify(ctx, "Operation successfully completed", EAlertLevel::SUCCESS);
    } else {
        Notify(
            ctx,
            TStringBuilder() << "failed to unmount volume "
                << DiskId.Quote()
                << ": " << FormatError(response->GetError()),
            EAlertLevel::DANGER);
    }

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(THttpUnmountVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvUnmountVolumeResponse, HandleUnmountVolumeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleHttpInfo_Unmount(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto& clientId = params.Get("ClientId");
    if (!clientId) {
        RejectHttpRequest(ctx, *requestInfo, "No client id is given");
        return;
    }

    const auto& diskId = params.Get("Volume");
    if (!diskId) {
        RejectHttpRequest(ctx, *requestInfo, "No Volume is given");
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Unmounting volume per action from monitoring page: volume %s, client %s",
        diskId.Quote().data(),
        clientId.Quote().data());

    auto volume = State.GetVolume(diskId);
    if (!volume) {
        TString error = TStringBuilder()
            << "Failed to unmount volume: volume " << diskId << " not found";

        RejectHttpRequest(ctx, *requestInfo, std::move(error));
        return;
    }

    NCloud::Register<THttpUnmountVolumeActor>(
        ctx,
        std::move(requestInfo),
        diskId,
        clientId,
        volume->SessionId);
}

}   // namespace NCloud::NBlockStore::NStorage
