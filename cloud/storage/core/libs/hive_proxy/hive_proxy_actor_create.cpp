#include "hive_proxy_actor.h"

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::SendNextCreateOrLookupRequest(
    const TActorContext& ctx,
    const TCreateOrLookupRequestQueue& queue)
{
    if (queue.size() != 1) {
        return;
    }
    if (!queue.front().IsLookup) {
        auto hiveRequest = std::make_unique<TEvHive::TEvCreateTablet>();

        const auto* msg =
            queue.front().Event->Get<TEvHiveProxy::TEvCreateTabletRequest>();

        hiveRequest->Record = msg->Request;
        ClientCache->Send(ctx, msg->HiveId, hiveRequest.release(), msg->HiveId);

    } else {
        const auto* msg =
            queue.front().Event->Get<TEvHiveProxy::TEvLookupTabletRequest>();

        auto hiveRequest = std::make_unique<TEvHive::TEvLookupTablet>(
            msg->Owner,
            msg->OwnerIdx);

        ClientCache->Send(ctx, msg->HiveId, hiveRequest.release(), msg->HiveId);
    }
}

void THiveProxyActor::HandleCreateTablet(
    const TEvHiveProxy::TEvCreateTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto key =
        std::make_pair(msg->Request.GetOwner(), msg->Request.GetOwnerIdx());

    auto& queue = HiveStates[msg->HiveId].CreateRequests[key];
    queue.emplace_back(false, ev.Release());

    SendNextCreateOrLookupRequest(ctx, queue);
}

void THiveProxyActor::HandleLookupTablet(
    const TEvHiveProxy::TEvLookupTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto key = std::make_pair(msg->Owner, msg->OwnerIdx);

    auto& queue = HiveStates[msg->HiveId].CreateRequests[key];
    queue.emplace_back(true, ev.Release());

    SendNextCreateOrLookupRequest(ctx, queue);
}

void THiveProxyActor::HandleCreateTabletReply(
    const TEvHive::TEvCreateTabletReply::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto hiveId = ev->Cookie;
    auto& state = HiveStates[hiveId];

    const auto key =
        std::make_pair(msg->Record.GetOwner(), msg->Record.GetOwnerIdx());

    auto status = msg->Record.GetStatus();
    bool isOk =
        (status == NKikimrProto::OK) || (status == NKikimrProto::ALREADY);

    const auto error = MakeError(isOk ? S_OK : E_NOT_FOUND);

    const auto tabletId = msg->Record.GetTabletID();

    auto it = state.CreateRequests.find(key);

    if (it == state.CreateRequests.end()) {
        return;
    }

    TCreateOrLookupRequestQueue& queue = it->second;

    if (queue.empty()) {
        LOG_ERROR_S(ctx, LogComponent, "Got unexpected TEvCreateTabletReply");
        return;
    }

    if (!queue.empty() && !queue.front().IsLookup) {
        auto request = std::move(queue.front());
        queue.pop_front();

        NCloud::Reply(
            ctx,
            request,
            std::make_unique<TEvHiveProxy::TEvCreateTabletResponse>(
                error,
                tabletId));
    }

    while (!queue.empty() && queue.front().IsLookup) {
        auto request = std::move(queue.front());
        queue.pop_front();

        NCloud::Reply(
            ctx,
            request,
            std::make_unique<TEvHiveProxy::TEvLookupTabletResponse>(
                error,
                tabletId));
    }

    if (queue.empty()) {
        state.CreateRequests.erase(it);
    } else {
        SendNextCreateOrLookupRequest(ctx, queue);
    }
}

void THiveProxyActor::HandleTabletCreation(
    const TEvHive::TEvTabletCreationResult::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    LOG_DEBUG_S(ctx, LogComponent, ev->Get()->ToString());
}

}   // namespace NCloud::NStorage
