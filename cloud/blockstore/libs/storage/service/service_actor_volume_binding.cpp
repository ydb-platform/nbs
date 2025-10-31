#include "service_actor.h"

#include "service.h"

#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <ydb/core/tablet/tablet_setup.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

using EChangeBindingOp =
    TEvService::TEvChangeVolumeBindingRequest::EChangeBindingOp;

////////////////////////////////////////////////////////////////////////////////

class TDelayChangeBindingActor final
    : public TActorBootstrapped<TDelayChangeBindingActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TActorId SessionActor;
    const TDuration Delay;
    const TString DiskId;
    const EChangeBindingOp Action;
    const NProto::EPreemptionSource Source;

public:
    TDelayChangeBindingActor(
            TRequestInfoPtr requestInfo,
            const TActorId& sessionActor,
            TDuration delay,
            TString diskId,
            EChangeBindingOp action,
            NProto::EPreemptionSource source)
        : RequestInfo(std::move(requestInfo))
        , SessionActor(sessionActor)
        , Delay(delay)
        , DiskId(std::move(diskId))
        , Action(action)
        , Source(source)
    {
    }

    void Bootstrap(const TActorContext& ctx)
    {
        TThis::Become(&TThis::StateWork);
        if (Delay) {
            ctx.Schedule(Delay, new TEvents::TEvWakeup);
        } else {
            SendRequest(ctx);
        }
    }

private:
    void SendRequest(const TActorContext& ctx)
    {
        auto request =
            std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                std::move(RequestInfo->CallContext),
                DiskId,
                Action,
                Source);

        NCloud::SendWithUndeliveryTracking(
            ctx,
            SessionActor,
            std::move(request),
            RequestInfo->Cookie);
    }

private:
    STFUNC(StateWork);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        SendRequest(ctx);
    }

    void HandleUndelivery(
        const TEvService::TEvChangeVolumeBindingRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        auto response =
            std::make_unique<TEvService::TEvChangeVolumeBindingResponse>(
                MakeTabletIsDeadError(E_REJECTED, __LOCATION__),
                DiskId);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));

        Die(ctx);
    }

    void HandleBindingResponse(
        const TEvService::TEvChangeVolumeBindingResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        auto response =
            std::make_unique<TEvService::TEvChangeVolumeBindingResponse>(
                msg->GetError(),
                DiskId);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));

        Die(ctx);
    }

    void HandleMountResponse(
        const TEvServicePrivate::TEvInternalMountVolumeResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        auto response =
            std::make_unique<TEvService::TEvChangeVolumeBindingResponse>(
                msg->GetError(),
                DiskId);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));

        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDelayChangeBindingActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(
            TEvServicePrivate::TEvInternalMountVolumeResponse,
            HandleMountResponse);

        HFunc(
            TEvService::TEvChangeVolumeBindingResponse,
            HandleBindingResponse);

        HFunc(
            TEvService::TEvChangeVolumeBindingRequest,
            HandleUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleChangeVolumeBinding(
    const TEvService::TEvChangeVolumeBindingRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& diskId = GetDiskId(*msg);

    auto replyError = [&] (NProto::TError result)
    {
        using TResponse = TEvService::TEvChangeVolumeBindingResponse;
        auto response = std::make_unique<TResponse>(std::move(result));
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    auto volume = State.GetVolume(diskId);

    if (!volume || !volume->VolumeSessionActor) {
        replyError(
            MakeError(
                E_NOT_FOUND,
                TStringBuilder() << "Volume not mounted: " << diskId.Quote()));
        return;
    }

    if (volume->RebindingIsInflight) {
        replyError(MakeError(E_TRY_AGAIN, "Rebinding is in progress"));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        std::move(msg->CallContext));

    TDuration delayInterval;
    if (msg->Source == NProto::SOURCE_BALANCER) {
        delayInterval = Config->GetBalancerActionDelayInterval();
    }

    NCloud::Register<TDelayChangeBindingActor>(
        ctx,
        std::move(requestInfo),
        volume->VolumeSessionActor,
        delayInterval,
        std::move(msg->DiskId),
        msg->Action,
        msg->Source);

    volume->RebindingIsInflight = true;
}

}   // namespace NCloud::NBlockStore::NStorage
