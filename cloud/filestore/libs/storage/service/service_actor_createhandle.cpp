#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateHandleActor final: public TActorBootstrapped<TCreateHandleActor>
{
private:
    // Original request
    const TRequestInfoPtr RequestInfo;
    NProto::TCreateHandleRequest CreateHandleRequest;

    // Filesystem-specific params
    const TString LogTag;

    // Response data
    NProto::TCreateHandleResponse LeaderResponse;
    bool LeaderResponded = false;

    // Stats for reporting
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;

public:
    TCreateHandleActor(
        TRequestInfoPtr requestInfo,
        NProto::TCreateHandleRequest createHandleRequest,
        TString logTag,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CreateHandleInLeader(const TActorContext& ctx);

    void HandleCreateHandleResponse(
        const TEvService::TEvCreateHandleResponse::TPtr& ev,
        const TActorContext& ctx);

    void CreateHandleInFollower(const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TCreateHandleResponse followerResponse);
    void HandleError(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TCreateHandleActor::TCreateHandleActor(
        TRequestInfoPtr requestInfo,
        NProto::TCreateHandleRequest createHandleRequest,
        TString logTag,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog)
    : RequestInfo(std::move(requestInfo))
    , CreateHandleRequest(std::move(createHandleRequest))
    , LogTag(std::move(logTag))
    , RequestStats(std::move(requestStats))
    , ProfileLog(std::move(profileLog))
{
}

void TCreateHandleActor::Bootstrap(const TActorContext& ctx)
{
    CreateHandleInLeader(ctx);
    Become(&TThis::StateWork);
}

void TCreateHandleActor::CreateHandleInLeader(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Executing CreateHandle in leader for %lu, %s, %s",
        LogTag.c_str(),
        CreateHandleRequest.GetNodeId(),
        CreateHandleRequest.GetName().Quote().c_str(),
        CreateHandleRequest.GetFollowerFileSystemId().c_str());

    auto request = std::make_unique<TEvService::TEvCreateHandleRequest>();
    request->Record = CreateHandleRequest;

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TCreateHandleActor::CreateHandleInFollower(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Executing CreateHandle in follower for %lu, %s, %s",
        LogTag.c_str(),
        CreateHandleRequest.GetNodeId(),
        CreateHandleRequest.GetName().Quote().c_str(),
        CreateHandleRequest.GetFollowerFileSystemId().c_str());

    auto request = std::make_unique<TEvService::TEvCreateHandleRequest>();
    request->Record = CreateHandleRequest;
    request->Record.SetFileSystemId(
        LeaderResponse.GetFollowerFileSystemId());
    request->Record.SetNodeId(RootNodeId);
    request->Record.SetName(LeaderResponse.GetFollowerNodeName());
    request->Record.ClearFollowerFileSystemId();
    // E_EXCLUSIVE flag should be unset in order not to get EEXIST from the
    // follower
    const auto exclusiveFlag =
        ProtoFlag(NProto::TCreateHandleRequest::E_EXCLUSIVE);
    request->Record.SetFlags(CreateHandleRequest.GetFlags() & ~exclusiveFlag);

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

////////////////////////////////////////////////////////////////////////////////

void TCreateHandleActor::HandleCreateHandleResponse(
    const TEvService::TEvCreateHandleResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, *msg->Record.MutableError());
        return;
    }

    if (LeaderResponded) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "CreateHandle succeeded in follower: %lu, %lu",
            msg->Record.GetNodeAttr().GetId(),
            msg->Record.GetHandle());

        ReplyAndDie(ctx, std::move(msg->Record));
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "CreateHandle succeeded in leader: %s %s",
        msg->Record.GetFollowerFileSystemId().c_str(),
        msg->Record.GetFollowerNodeName().Quote().c_str());

    LeaderResponded = true;
    LeaderResponse = std::move(msg->Record);
    CreateHandleInFollower(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateHandleActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TCreateHandleActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TCreateHandleResponse followerResponse)
{
    auto response = std::make_unique<TEvService::TEvCreateHandleResponse>();
    response->Record = std::move(followerResponse);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TCreateHandleActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvCreateHandleResponse>(
        std::move(error));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateHandleActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvService::TEvCreateHandleResponse,
            HandleCreateHandleResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleCreateHandle(
    const TEvService::TEvCreateHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->Record.GetName().Empty()) {
        // handle creation by NodeId can be handled directly by the follower
        ForwardRequestToFollower<TEvService::TCreateHandleMethod>(
            ctx,
            ev,
            ExtractShardNo(msg->Record.GetNodeId()));
        return;
    }

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<TEvService::TEvCreateHandleResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }
    const NProto::TFileStore& filestore = session->FileStore;
    const auto& followerId = session->SelectFollower();

    if (!StorageConfig->GetMultiTabletForwardingEnabled() || !followerId) {
        ForwardRequest<TEvService::TCreateHandleMethod>(ctx, ev);
        return;
    }

    msg->Record.SetFollowerFileSystemId(followerId);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "create handle in follower %s",
        msg->Record.ShortDebugString().Quote().c_str());

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        session->MediaKind,
        session->RequestStats,
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(SelfId(), cookie, msg->CallContext);

    auto actor = std::make_unique<TCreateHandleActor>(
        std::move(requestInfo),
        std::move(msg->Record),
        filestore.GetFileSystemId(),
        session->RequestStats,
        ProfileLog);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
