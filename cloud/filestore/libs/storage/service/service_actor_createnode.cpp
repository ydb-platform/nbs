#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <util/generic/guid.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief This actor is used only for creating hard links in multi-tablet mode.
 * First it creates a hardlink in the follower, then it creates a nodeRef in the
 * leader, pointing to the hardlink in the follower.
 */
class TLinkActor final: public TActorBootstrapped<TLinkActor>
{
private:
    // Original request
    const TRequestInfoPtr RequestInfo;
    NProto::TCreateNodeRequest CreateNodeRequest;
    const TString FollowerId;
    const TString FollowerNodeName;

    // Filesystem-specific params
    const TString LogTag;

    // Response data
    bool FollowerResponded = false;
    NProto::TCreateNodeResponse FollowerResponse;

    // Stats for reporting
    IProfileLogPtr ProfileLog;

public:
    TLinkActor(
        TRequestInfoPtr requestInfo,
        NProto::TCreateNodeRequest createNodeRequest,
        TString followerId,
        TString logTag,
        IProfileLogPtr profileLog);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CreateNodeInFollower(const TActorContext& ctx);

    void HandleCreateResponse(
        const TEvService::TEvCreateNodeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleLeaderResponse(
        const TEvService::TEvCreateNodeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleFollowerResponse(
        const TEvService::TEvCreateNodeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TCreateNodeResponse followerResponse);

    void HandleError(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TLinkActor::TLinkActor(
        TRequestInfoPtr requestInfo,
        NProto::TCreateNodeRequest createNodeRequest,
        TString followerId,
        TString logTag,
        IProfileLogPtr profileLog)
    : RequestInfo(std::move(requestInfo))
    , CreateNodeRequest(std::move(createNodeRequest))
    , FollowerId(std::move(followerId))
    , FollowerNodeName(CreateGuidAsString())
    , LogTag(std::move(logTag))
    , ProfileLog(std::move(profileLog))
{}

void TLinkActor::Bootstrap(const TActorContext& ctx)
{
    CreateNodeInFollower(ctx);
    Become(&TThis::StateWork);
}

void TLinkActor::CreateNodeInFollower(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvCreateNodeRequest>();

    request->Record.CopyFrom(CreateNodeRequest);
    // hard links should be located at the root node of the follower
    request->Record.SetFileSystemId(FollowerId);
    request->Record.SetNodeId(RootNodeId);
    // Explicitly pick the follower name to reuse afterwards in the leader
    request->Record.SetName(FollowerNodeName);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Creating node in follower for %lu, %s",
        LogTag.c_str(),
        CreateNodeRequest.GetLink().GetTargetNode(),
        FollowerId.c_str());

    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TLinkActor::HandleFollowerResponse(
    const TEvService::TEvCreateNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Error creating link in follower for %lu, with error %s",
            LogTag.c_str(),
            CreateNodeRequest.GetLink().GetTargetNode(),
            FormatError(msg->GetError()).Quote().c_str());

        HandleError(ctx, msg->GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Link created in follower %s with nodeId %s",
        LogTag.c_str(),
        FollowerId.c_str(),
        msg->Record.GetNode().DebugString().Quote().c_str());

    auto request = std::make_unique<TEvService::TEvCreateNodeRequest>();
    // By setting the follower filesystem id, we let the leader know that he
    // should not verify the target node existence and just create a nodeRef
    CreateNodeRequest.SetFollowerFileSystemId(FollowerId);
    CreateNodeRequest.MutableLink()->SetTargetNode(
        msg->Record.GetNode().GetId());
    CreateNodeRequest.MutableLink()->SetFollowerNodeName(FollowerNodeName);
    FollowerResponse = std::move(msg->Record);

    request->Record = std::move(CreateNodeRequest);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Creating nodeRef in leader for %lu, %s",
        LogTag.c_str(),
        msg->Record.GetNode().GetId(),
        CreateNodeRequest.GetLink().GetTargetNode());

    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());

    // TODO(#1350): add proper mechanism for handling dangling links for the
    // cases, when the hardlink has been created in the follower, but the
    // nodeRef creation in the leader was not completed or failed
}

void TLinkActor::HandleLeaderResponse(
    const TEvService::TEvCreateNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Error creating nodeRef in leader for %lu, with error %s",
            LogTag.c_str(),
            CreateNodeRequest.GetLink().GetTargetNode(),
            FormatError(msg->GetError()).Quote().c_str());

        HandleError(ctx, msg->GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] NodeRef created in leader for %lu",
        LogTag.c_str(),
        msg->Record.GetNode().GetId());

    // TODO(#1350): some attributes from the follower response could be invalid
    // by the time the leader response is received
    msg->Record.MutableNode()->Swap(FollowerResponse.MutableNode());

    ReplyAndDie(ctx, std::move(msg->Record));
}

void TLinkActor::HandleCreateResponse(
    const TEvService::TEvCreateNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (!FollowerResponded) {
        FollowerResponded = true;
        HandleFollowerResponse(ev, ctx);
    } else {
        HandleLeaderResponse(ev, ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLinkActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TLinkActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TCreateNodeResponse followerResponse)
{
    auto response = std::make_unique<TEvService::TEvCreateNodeResponse>();
    response->Record = std::move(followerResponse);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TLinkActor::HandleError(const TActorContext& ctx, NProto::TError error)
{
    auto response =
        std::make_unique<TEvService::TEvCreateNodeResponse>(std::move(error));

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TLinkActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvService::TEvCreateNodeResponse, HandleCreateResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleCreateNode(
    const TEvService::TEvCreateNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<TEvService::TEvCreateNodeResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    if (msg->Record.HasFile()) {
        const auto& followerId = session->SelectFollower();

        if (StorageConfig->GetMultiTabletForwardingEnabled() && followerId) {
            msg->Record.SetFollowerFileSystemId(followerId);
        }
    } else if (msg->Record.HasLink()) {
        auto shardNo = ExtractShardNo(msg->Record.GetLink().GetTargetNode());

        const NProto::TFileStore& filestore = session->FileStore;

        auto [followerId, error] = SelectShard(
            ctx,
            sessionId,
            seqNo,
            "",
            msg->CallContext->RequestId,
            filestore,
            shardNo);
        if (HasError(error)) {
            auto response = std::make_unique<TEvService::TEvCreateNodeResponse>(
                std::move(error));
            return NCloud::Reply(ctx, *ev, std::move(response));
        }
        if (followerId && StorageConfig->GetMultiTabletForwardingEnabled()) {
            // If the target node is located on a shard, start a worker actor
            // to separately increment the link count in the follower and insert
            // the node in the leader.

            auto [cookie, inflight] = CreateInFlightRequest(
                TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
                session->MediaKind,
                session->RequestStats,
                ctx.Now());

            InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

            auto requestInfo =
                CreateRequestInfo(SelfId(), cookie, msg->CallContext);

            auto actor = std::make_unique<TLinkActor>(
                std::move(requestInfo),
                std::move(msg->Record),
                followerId,
                filestore.GetFileSystemId(),
                ProfileLog);

            NCloud::Register(ctx, std::move(actor));

            return;
        }
    }

    ForwardRequest<TEvService::TCreateNodeMethod>(ctx, ev);
}

}   // namespace NCloud::NFileStore::NStorage
