#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

class TReadNodeRefsActor final: public TActorBootstrapped<TReadNodeRefsActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    NProto::TReadNodeRefsRequest ReadNodeRefsRequest;
    NProto::TReadNodeRefsResponse Response;


    // stats for reporting
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;
public:
    TReadNodeRefsActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadNodeRefsRequest readNodeRefsRequest,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ReadNodeRefs(const TActorContext& ctx);

    void HandleReadNodeRefsResponse(
        const TEvService::TEvReadNodeRefsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx);
    void HandleError(
        const TActorContext& ctx,
        NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TReadNodeRefsActor::TReadNodeRefsActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadNodeRefsRequest readNodeRefsRequest,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog)
    : RequestInfo(std::move(requestInfo))
    , ReadNodeRefsRequest(std::move(readNodeRefsRequest))
    , RequestStats(std::move(requestStats))
    , ProfileLog(std::move(profileLog))
{
}

void TReadNodeRefsActor::Bootstrap(const TActorContext& ctx)
{
    ReadNodeRefs(ctx);
    Become(&TThis::StateWork);
}

void TReadNodeRefsActor::ReadNodeRefs(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvReadNodeRefsRequest>();
    request->Record = ReadNodeRefsRequest;
    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TReadNodeRefsActor::HandleReadNodeRefsResponse(
    const TEvService::TEvReadNodeRefsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, *msg->Record.MutableError());
        return;
    }

    Response = std::move(msg->Record);

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////


STFUNC(TReadNodeRefsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvService::TEvReadNodeRefsResponse,
            HandleReadNodeRefsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReadNodeRefsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TReadNodeRefsActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvReadNodeRefsResponse>();
    response->Record = std::move(Response);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TReadNodeRefsActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvReadNodeRefsResponse>(
        std::move(error));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

}
}   // namespace NCloud::NFileStore::NStorage
