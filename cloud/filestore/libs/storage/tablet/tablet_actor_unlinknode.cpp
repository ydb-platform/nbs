#include "tablet_actor.h"
#include "cloud/filestore/public/api/protos/node.pb.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TUnlinkNodeRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    if (auto error = ValidateNodeName(request.GetName()); HasError(error)) {
        return error;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TUnlinkNodeInFollowerActor final
    : public TActorBootstrapped<TUnlinkNodeInFollowerActor>
{
private:
    const TString LogTag;
    TRequestInfoPtr RequestInfo;
    const TString FollowerId;
    const TString FollowerName;
    const TActorId ParentId;
    NProto::TUnlinkNodeRequest Request;
    TUnlinkNodeInFollowerResult Result;

public:
    TUnlinkNodeInFollowerActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        TString followerId,
        TString followerName,
        const TActorId& parentId,
        NProto::TUnlinkNodeRequest request,
        TUnlinkNodeInFollowerResult result);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequest(const TActorContext& ctx);

    void HandleUnlinkNodeResponse(
        const TEvService::TEvUnlinkNodeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TUnlinkNodeInFollowerActor::TUnlinkNodeInFollowerActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        TString followerId,
        TString followerName,
        const TActorId& parentId,
        NProto::TUnlinkNodeRequest request,
        TUnlinkNodeInFollowerResult result)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , FollowerId(std::move(followerId))
    , FollowerName(std::move(followerName))
    , ParentId(parentId)
    , Request(std::move(request))
    , Result(std::move(result))
{}

void TUnlinkNodeInFollowerActor::Bootstrap(const TActorContext& ctx)
{
    SendRequest(ctx);
    Become(&TThis::StateWork);
}

void TUnlinkNodeInFollowerActor::SendRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvUnlinkNodeRequest>();
    request->Record = std::move(Request);
    request->Record.SetFileSystemId(FollowerId);
    request->Record.SetNodeId(RootNodeId);
    request->Record.SetName(FollowerName);

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending UnlinkNodeRequest to follower %s, %s",
        LogTag.c_str(),
        FollowerId.c_str(),
        FollowerName.c_str());

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release());
}

void TUnlinkNodeInFollowerActor::HandleUnlinkNodeResponse(
    const TEvService::TEvUnlinkNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Follower node unlinking failed for %s, %s with error %s",
            LogTag.c_str(),
            FollowerId.c_str(),
            FollowerName.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Follower node unlinked for %s, %s",
        LogTag.c_str(),
        FollowerId.c_str(),
        FollowerName.c_str());

    ReplyAndDie(ctx, {});
}

void TUnlinkNodeInFollowerActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TUnlinkNodeInFollowerActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (HasError(error)) {
        // TODO(#1350): properly retry node unlinking via the leader fs
        // don't forget to properly handle ENOENT after a retry
        if (auto* x = std::get_if<NProto::TRenameNodeResponse>(&Result)) {
            *x->MutableError() = std::move(error);
        } else if (auto* x = std::get_if<NProto::TUnlinkNodeResponse>(&Result)) {
            *x->MutableError() = std::move(error);
        } else {
            TABLET_VERIFY_C(
                0,
                TStringBuilder() << "bad variant index: " << Result.index());
        }
    }

    using TResponse = TEvIndexTabletPrivate::TEvNodeUnlinkedInFollower;
    ctx.Send(ParentId, std::make_unique<TResponse>(
        std::move(RequestInfo),
        std::move(Result)));

    Die(ctx);
}

STFUNC(TUnlinkNodeInFollowerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvService::TEvUnlinkNodeResponse, HandleUnlinkNodeResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnlinkNode(
    const TEvService::TEvUnlinkNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvUnlinkNodeResponse>(
                std::move(error)));

        return;
    }

    auto* session = AcceptRequest<TEvService::TUnlinkNodeMethod>(ev, ctx, ValidateRequest);
    if (!session) {
        return;
    }

    auto* msg = ev->Get();
    if (const auto* e = session->LookupDupEntry(GetRequestId(msg->Record))) {
        auto response = std::make_unique<TEvService::TEvUnlinkNodeResponse>();
        GetDupCacheEntry(e, response->Record);
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TUnlinkNodeMethod>(*requestInfo);

    ExecuteTx<TUnlinkNode>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnlinkNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnlinkNode& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_DUPTX_SESSION(UnlinkNode, args);

    TIndexTabletDatabase db(tx.DB);

    args.CommitId = GetCurrentCommitId();

    // validate parent node exists
    if (!ReadNode(db, args.ParentNodeId, args.CommitId, args.ParentNode)) {
        return false;   // not ready
    }

    if (!args.ParentNode) {
        args.Error = ErrorInvalidParent(args.ParentNodeId);
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.ParentNode);
    // validate target node exists
    if (!ReadNodeRef(db, args.ParentNodeId, args.CommitId, args.Name, args.ChildRef)) {
        return false;   // not ready
    }

    if (!args.ChildRef) {
        args.Error = ErrorInvalidTarget(args.ParentNodeId, args.Name);
        return true;
    }

    if (!ReadNode(db, args.ChildRef->ChildNodeId, args.CommitId, args.ChildNode)) {
        return false;   // not ready
    }

    if (args.ChildRef->FollowerId) {
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.ChildNode);
    if (args.ChildNode->Attrs.GetType() == NProto::E_DIRECTORY_NODE) {
        TVector<IIndexTabletDatabase::TNodeRef> refs;
        // 1 entry is enough to prevent deletion
        if (!ReadNodeRefs(db, args.ChildRef->ChildNodeId, args.CommitId, {}, refs, 1)) {
            return false;
        }

        if (!refs.empty()) {
            // cannot unlink non empty directory
            args.Error = ErrorIsNotEmpty(args.ParentNodeId);
            return true;
        }

        if (!args.Request.GetUnlinkDirectory()) {
            // should expliciltly unlink directory node
            args.Error = ErrorIsDirectory(args.ParentNodeId);
            return true;
        }
    } else if (args.Request.GetUnlinkDirectory()) {
        args.Error = ErrorIsNotDirectory(args.ParentNodeId);
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_UnlinkNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnlinkNode& args)
{
    FILESTORE_VALIDATE_TX_ERROR(UnlinkNode, args);

    TIndexTabletDatabase db(tx.DB);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "UnlinkNode");
    }

    if (args.ChildRef->FollowerId) {
        UnlinkExternalNode(
            db,
            args.ParentNodeId,
            args.Name,
            args.ChildRef->FollowerId,
            args.ChildRef->FollowerName,
            args.ChildRef->MinCommitId,
            args.CommitId);
    } else {
        UnlinkNode(
            db,
            args.ParentNodeId,
            args.Name,
            *args.ChildNode,
            args.ChildRef->MinCommitId,
            args.CommitId);
    }

    auto* session = FindSession(args.SessionId);
    TABLET_VERIFY(session);

    AddDupCacheEntry(
        db,
        session,
        args.RequestId,
        NProto::TUnlinkNodeResponse{},
        Config->GetDupCacheEntryCount());

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_UnlinkNode(
    const TActorContext& ctx,
    TTxIndexTablet::TUnlinkNode& args)
{
    RemoveTransaction(*args.RequestInfo);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s[%s] UnlinkNode completed (%s)",
        LogTag.c_str(),
        args.SessionId.c_str(),
        FormatError(args.Error).c_str());

    if (!HasError(args.Error)) {
        TABLET_VERIFY(args.ChildRef);

        CommitDupCacheEntry(args.SessionId, args.RequestId);

        if (args.ChildRef->FollowerId) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s Unlinking node in follower upon UnlinkNode: %s, %s",
                LogTag.c_str(),
                args.ChildRef->FollowerId.c_str(),
                args.ChildRef->FollowerName.c_str());

            RegisterUnlinkNodeInFollowerActor(
                ctx,
                args.RequestInfo,
                std::move(args.ChildRef->FollowerId),
                std::move(args.ChildRef->FollowerName),
                args.Request,
                std::move(args.Response));

            return;
        }

        // TODO(#1350): support session events for external nodes
        NProto::TSessionEvent sessionEvent;
        {
            auto* unlinked = sessionEvent.AddNodeUnlinked();
            unlinked->SetParentNodeId(args.ParentNodeId);
            unlinked->SetChildNodeId(args.ChildRef->ChildNodeId);
            unlinked->SetName(args.Name);
        }
        NotifySessionEvent(ctx, sessionEvent);
    }

    auto response =
        std::make_unique<TEvService::TEvUnlinkNodeResponse>(args.Error);
    CompleteResponse<TEvService::TUnlinkNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleNodeUnlinkedInFollower(
    const TEvIndexTabletPrivate::TEvNodeUnlinkedInFollower::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& res = msg->Result;

    if (auto* x = std::get_if<NProto::TRenameNodeResponse>(&res)) {
        auto response = std::make_unique<TEvService::TEvRenameNodeResponse>();
        response->Record = std::move(*x);

        CompleteResponse<TEvService::TRenameNodeMethod>(
            response->Record,
            msg->RequestInfo->CallContext,
            ctx);

        NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
    } else if (auto* x = std::get_if<NProto::TUnlinkNodeResponse>(&res)) {
        auto response = std::make_unique<TEvService::TEvUnlinkNodeResponse>();
        response->Record = std::move(*x);

        CompleteResponse<TEvService::TUnlinkNodeMethod>(
            response->Record,
            msg->RequestInfo->CallContext,
            ctx);

        NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
    } else {
        TABLET_VERIFY_C(
            0,
            TStringBuilder() << "bad variant index: " << res.index());
    }

    WorkerActors.erase(ev->Sender);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RegisterUnlinkNodeInFollowerActor(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    TString followerId,
    TString followerName,
    NProto::TUnlinkNodeRequest request,
    TUnlinkNodeInFollowerResult result)
{
    auto actor = std::make_unique<TUnlinkNodeInFollowerActor>(
        LogTag,
        std::move(requestInfo),
        std::move(followerId),
        std::move(followerName),
        ctx.SelfID,
        std::move(request),
        std::move(result));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
