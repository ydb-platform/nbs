#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume_throttling_manager.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdateVolumeThrottlingConfigActor final
    : public TActorBootstrapped<TUpdateVolumeThrottlingConfigActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const NProto::TUpdateVolumeThrottlingConfigRequest Request;
    const TDuration RequestTimeout;

public:
    TUpdateVolumeThrottlingConfigActor(
        TRequestInfoPtr requestInfo,
        NProto::TUpdateVolumeThrottlingConfigRequest request,
        TDuration requestTimeout);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    void HandleUpdateConfigResponse(
        const TEvVolumeThrottlingManager::
            TEvUpdateVolumeThrottlingConfigResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TUpdateVolumeThrottlingConfigActor::TUpdateVolumeThrottlingConfigActor(
    TRequestInfoPtr requestInfo,
    NProto::TUpdateVolumeThrottlingConfigRequest request,
    TDuration requestTimeout)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , RequestTimeout(requestTimeout)
{}

void TUpdateVolumeThrottlingConfigActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request = std::make_unique<
        TEvVolumeThrottlingManager::TEvUpdateVolumeThrottlingConfigRequest>();

    request->ThrottlingConfig = Request.GetConfig();

    NCloud::Send(
        ctx,
        MakeVolumeThrottlingManagerServiceId(),
        std::move(request),
        RequestInfo->Cookie);

    NCloud::Schedule<TEvents::TEvWakeup>(ctx, RequestTimeout);
}

void TUpdateVolumeThrottlingConfigActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<TEvService::TEvUpdateVolumeThrottlingConfigResponse>(
            error));
    Die(ctx);
}

void TUpdateVolumeThrottlingConfigActor::HandleUpdateConfigResponse(
    const TEvVolumeThrottlingManager::TEvUpdateVolumeThrottlingConfigResponse::
        TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& error = msg->Error;
    if (HasError(error)) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Update throttling config failed: %s",
            FormatError(error).data());
    }

    ReplyAndDie(ctx, error);
}

void TUpdateVolumeThrottlingConfigActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(
        ctx,
        MakeError(E_TIMEOUT, "Throttling config was not applied: timeout hit"));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUpdateVolumeThrottlingConfigActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolumeThrottlingManager::TEvUpdateVolumeThrottlingConfigResponse,
            HandleUpdateConfigResponse);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
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

void TServiceActor::HandleUpdateVolumeThrottlingConfig(
    const TEvService::TEvUpdateVolumeThrottlingConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    if (!Config->GetVolumeThrottlingManagerEnabled()) {
        auto error = MakeError(E_FAIL, "VolumeThrottlingManager is disabled");
        auto response = std::make_unique<
            TEvService::TEvUpdateVolumeThrottlingConfigResponse>(
            std::move(error));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Update Volume Throttling Manager config");

    // Volume Throttling Manager notification period serves as request timeout
    auto requestTimeout =
        Config->GetVolumeThrottlingManagerNotificationPeriodSeconds();

    NCloud::Register<TUpdateVolumeThrottlingConfigActor>(
        ctx,
        std::move(requestInfo),
        msg->Record,
        requestTimeout);
}

}   // namespace NCloud::NBlockStore::NStorage
