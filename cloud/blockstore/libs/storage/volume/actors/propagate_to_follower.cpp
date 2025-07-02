#include "propagate_to_follower.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/log.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TPropagateLinkToFollowerActor::TPropagateLinkToFollowerActor(
        TString logPrefix,
        TRequestInfoPtr requestInfo,
        TLeaderFollowerLink link,
        EReason reason)
    : LogPrefix(std::move(logPrefix))
    , RequestInfo(std::move(requestInfo))
    , Link(std::move(link))
    , Reason(reason)
{}

void TPropagateLinkToFollowerActor::Bootstrap(const NActors::TActorContext& ctx)
{
    Become(&TThis::StateWork);

    PersistOnFollower(ctx);
}

void TPropagateLinkToFollowerActor::PersistOnFollower(
    const NActors::TActorContext& ctx)
{
    ++TryCount;
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Propagate link %s %s to follower (try #%lu)",
        LogPrefix.c_str(),
        ToString(Reason).c_str(),
        Link.Describe().c_str(),
        TryCount);

    auto request =
        std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
    request->Record.SetLinkUUID(Link.LinkUUID);
    request->Record.SetDiskId(Link.FollowerDiskId);
    request->Record.SetFollowerShardId(Link.FollowerShardId);
    request->Record.SetLeaderDiskId(Link.LeaderDiskId);
    request->Record.SetLeaderShardId(Link.LeaderShardId);

    switch (Reason) {
        case EReason::Creation: {
            request->Record.SetAction(NProto::ELinkAction::LINK_ACTION_CREATE);
            break;
        }
        case EReason::Destruction: {
            request->Record.SetAction(NProto::ELinkAction::LINK_ACTION_DESTROY);
            break;
        }
    }

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));

    ctx.Schedule(
        DelayProvider.GetDelayAndIncrease(),
        new NActors::TEvents::TEvWakeup());
}

void TPropagateLinkToFollowerActor::HandlePersistedOnFollower(
    const TEvVolume::TEvUpdateLinkOnFollowerResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* message = ev->Get();
    const auto& error = message->GetError();
    const bool hasError = HasError(error);

    LOG_LOG(
        ctx,
        hasError ? NActors::NLog::PRI_ERROR : NActors::NLog::PRI_INFO,
        TBlockStoreComponents::VOLUME,
        "%s Propagated link %s %s to follower: %s",
        LogPrefix.c_str(),
        ToString(Reason).c_str(),
        Link.Describe().c_str(),
        FormatError(error).c_str());

    if (hasError && GetErrorKind(error) == EErrorKind::ErrorRetriable) {
        ctx.Schedule(
            DelayProvider.GetDelayAndIncrease(),
            new NActors::TEvents::TEvWakeup());
        return;
    }

    ReplyAndDie(ctx, message->GetError());
}

void TPropagateLinkToFollowerActor::HandleWakeup(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    PersistOnFollower(ctx);
}

void TPropagateLinkToFollowerActor::ReplyAndDie(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    switch (Reason) {
        case EReason::Creation: {
            auto response =
                std::make_unique<TEvVolumePrivate::TEvLinkOnFollowerCreated>(
                    error,
                    Link);

            NCloud::Reply(ctx, *RequestInfo, std::move(response));
            break;
        }
        case EReason::Destruction: {
            auto response =
                std::make_unique<TEvVolumePrivate::TEvLinkOnFollowerDestroyed>(
                    error,
                    Link);
            NCloud::Reply(ctx, *RequestInfo, std::move(response));
            break;
        }
    }

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TPropagateLinkToFollowerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvUpdateLinkOnFollowerResponse,
            HandlePersistedOnFollower);

        HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
