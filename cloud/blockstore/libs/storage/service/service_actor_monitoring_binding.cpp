#include "service_actor.h"
#include "service.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

using EChangeBindingOp = TEvService::TEvChangeVolumeBindingRequest::EChangeBindingOp;

class THttpVolumeBindingActor final
    : public TActorBootstrapped<THttpVolumeBindingActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TString DiskId;
    const bool Push;

public:
    THttpVolumeBindingActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        bool push);

    void Bootstrap(const TActorContext& ctx);

private:
    void Notify(
        const TActorContext& ctx,
        TString message,
        const EAlertLevel alertLevel);

private:
    STFUNC(StateWork);

    void HandleChangeVolumeBindingResponse(
        const TEvService::TEvChangeVolumeBindingResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

THttpVolumeBindingActor::THttpVolumeBindingActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        bool push)
    : RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
    , Push(push)
{}

void THttpVolumeBindingActor::Bootstrap(const TActorContext& ctx)
{
    EChangeBindingOp action;
    if (Push) {
        action = EChangeBindingOp::RELEASE_TO_HIVE;
    } else {
        action = EChangeBindingOp::ACQUIRE_FROM_HIVE;
    }

    auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
        DiskId,
        action,
        NProto::EPreemptionSource::SOURCE_MANUAL);

    NCloud::Send(
        ctx,
        MakeStorageServiceId(),
        std::move(request));

    Become(&TThis::StateWork);
}

void THttpVolumeBindingActor::Notify(
    const TActorContext& ctx,
    TString message,
    const EAlertLevel alertLevel)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        RequestInfo->CallContext->RequestId);

    TStringStream out;
    BuildNotifyPageWithRedirect(
        out,
        std::move(message),
        TStringBuilder()
            << "../blockstore/service",
        alertLevel);

    auto response = std::make_unique<NMon::TEvHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void THttpVolumeBindingActor::HandleChangeVolumeBindingResponse(
    const TEvService::TEvChangeVolumeBindingResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    if (SUCCEEDED(response->GetStatus())) {
        Notify(ctx, "Operation successfully completed", EAlertLevel::SUCCESS);
    } else {
        Notify(
            ctx,
            TStringBuilder()
                << "failed to change volume binding "
                << DiskId.Quote()
                << ": " << FormatError(response->GetError()),
            EAlertLevel::DANGER);
    }

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(THttpVolumeBindingActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvChangeVolumeBindingResponse, HandleChangeVolumeBindingResponse);

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

void TServiceActor::HandleHttpInfo_VolumeBinding(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto& diskId = params.Get("Volume");
    if (!diskId) {
        RejectHttpRequest(ctx, *requestInfo, "No Volume is given");
        return;
    }

    const auto& actionType = params.Get("type");
    if (!actionType) {
        RejectHttpRequest(ctx, *requestInfo, "No action type is given");
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Changing volume binding per action from monitoring page: volume %s, action %s",
        diskId.Quote().data(),
        actionType.Quote().data());

    auto volume = State.GetVolume(diskId);
    if (!volume) {
        TString error = TStringBuilder()
            << "Failed to preempt volume: volume " << diskId << " not found";

        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE, error.data());

        RejectHttpRequest(ctx, *requestInfo, std::move(error));
        return;
    }

    bool pushRequired = true;

    if (actionType == "Pull") {
        pushRequired = false;
    }

    NCloud::Register<THttpVolumeBindingActor>(
        ctx,
        std::move(requestInfo),
        diskId,
        pushRequired);
}

}   // namespace NCloud::NBlockStore::NStorage
