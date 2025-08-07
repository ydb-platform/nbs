#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/throttling_manager.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdateThrottlingConfigActor final
    : public TActorBootstrapped<TUpdateThrottlingConfigActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const NProto::TUpdateThrottlingConfigRequest Request;

public:
    TUpdateThrottlingConfigActor(
        TRequestInfoPtr requestInfo,
        NProto::TUpdateThrottlingConfigRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    void HandleUpdateConfigResponse(
        const TEvThrottlingManager::TEvUpdateConfigResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TUpdateThrottlingConfigActor::TUpdateThrottlingConfigActor(
    TRequestInfoPtr requestInfo,
    NProto::TUpdateThrottlingConfigRequest request)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
{}

void TUpdateThrottlingConfigActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request =
        std::make_unique<TEvThrottlingManager::TEvUpdateConfigRequest>();

    request->ThrottlingConfig = Request.GetConfig();

    NCloud::Send(
        ctx,
        MakeThrottlingManagerServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TUpdateThrottlingConfigActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<TEvService::TEvUpdateThrottlingConfigResponse>(error));
    Die(ctx);
}

void TUpdateThrottlingConfigActor::HandleUpdateConfigResponse(
    const TEvThrottlingManager::TEvUpdateConfigResponse::TPtr& ev,
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

void TUpdateThrottlingConfigActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(
        ctx,
        MakeError(E_TIMEOUT, "Throttling config was not applied: timeout hit"));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUpdateThrottlingConfigActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvThrottlingManager::TEvUpdateConfigResponse,
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

void TServiceActor::HandleUpdateThrottlingConfig(
    const TEvService::TEvUpdateThrottlingConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Update Throttling Manager config");

    NCloud::Register<TUpdateThrottlingConfigActor>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

}   // namespace NCloud::NBlockStore::NStorage
