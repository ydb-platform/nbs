#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
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

class TUnlinkNodeInShardActor final
    : public TActorBootstrapped<TUnlinkNodeInShardActor>
{
private:
    const TString LogTag;
    TRequestInfoPtr RequestInfo;
    const TActorId ParentId;
    const NProtoPrivate::TUnlinkNodeInShardRequest Request;
    const ui64 RequestId;
    const ui64 OpLogEntryId;
    TUnlinkNodeInShardResult Result;
    bool ShouldUnlockUponCompletion;

public:
    TUnlinkNodeInShardActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProtoPrivate::TUnlinkNodeInShardRequest request,
        ui64 requestId,
        ui64 opLogEntryId,
        TUnlinkNodeInShardResult result,
        bool shouldUnlockUponCompletion);

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

TUnlinkNodeInShardActor::TUnlinkNodeInShardActor(
    TString logTag,
    TRequestInfoPtr requestInfo,
    const TActorId& parentId,
    NProtoPrivate::TUnlinkNodeInShardRequest request,
    ui64 requestId,
    ui64 opLogEntryId,
    TUnlinkNodeInShardResult result,
    bool shouldUnlockUponCompletion)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , ParentId(parentId)
    , Request(std::move(request))
    , RequestId(requestId)
    , OpLogEntryId(opLogEntryId)
    , Result(std::move(result))
    , ShouldUnlockUponCompletion(shouldUnlockUponCompletion)
{}

void TUnlinkNodeInShardActor::Bootstrap(const TActorContext& ctx)
{
    SendRequest(ctx);
    Become(&TThis::StateWork);
}

void TUnlinkNodeInShardActor::SendRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvUnlinkNodeRequest>();
    request->Record.MutableHeaders()->CopyFrom(Request.GetHeaders());
    request->Record.SetFileSystemId(Request.GetFileSystemId());
    request->Record.SetNodeId(Request.GetNodeId());
    request->Record.SetName(Request.GetName());
    request->Record.SetUnlinkDirectory(Request.GetUnlinkDirectory());

    request->Record.MutableHeaders()->SetBehaveAsDirectoryTablet(false);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending UnlinkNodeRequest to shard %s, %s",
        LogTag.c_str(),
        Request.GetFileSystemId().c_str(),
        Request.GetName().c_str());

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release());
}

void TUnlinkNodeInShardActor::HandleUnlinkNodeResponse(
    const TEvService::TEvUnlinkNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->GetError().GetCode() == E_FS_NOENT) {
        // NOENT can arrive after a successful operation is retried, it's ok
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Shard node unlinking for %s, %s returned ENOENT %s",
            LogTag.c_str(),
            Request.GetFileSystemId().c_str(),
            Request.GetName().c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        msg->Record.ClearError();
    }

    if (HasError(msg->GetError())) {
        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s Shard node unlinking failed for %s, %s with error %s"
                ", retrying",
                LogTag.c_str(),
                Request.GetFileSystemId().c_str(),
                Request.GetName().c_str(),
                FormatError(msg->GetError()).Quote().c_str());

            SendRequest(ctx);
            return;
        }

        const auto message = Sprintf(
            "Shard node unlinking failed for %s, %s with error %s"
            ", will not retry",
            Request.GetFileSystemId().c_str(),
            Request.GetName().c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        if (ShouldUnlockUponCompletion &&
            msg->GetError().GetCode() == E_FS_NOTEMPTY)
        {
            // The response from shard E_FS_NOTEMPTY is expected for directories
            // and should not be considered erroneous
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s %s",
                LogTag.c_str(),
                message.c_str());
        } else {
            LOG_ERROR(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s %s",
                LogTag.c_str(),
                message.c_str());
            ReportReceivedNodeOpErrorFromShard(message);
        }

        ReplyAndDie(ctx, msg->GetError());

        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Shard node unlinked for %s, %s",
        LogTag.c_str(),
        Request.GetFileSystemId().c_str(),
        Request.GetName().c_str());

    ReplyAndDie(ctx, {});
}

void TUnlinkNodeInShardActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TUnlinkNodeInShardActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (HasError(error)) {
        if (auto* x = std::get_if<NProto::TRenameNodeResponse>(&Result)) {
            *x->MutableError() = std::move(error);
        } else if (auto* x = std::get_if<
                NProtoPrivate::TRenameNodeInDestinationResponse>(&Result))
        {
            *x->MutableError() = std::move(error);
        } else if (auto* x = std::get_if<NProto::TUnlinkNodeResponse>(&Result)) {
            *x->MutableError() = std::move(error);
        } else {
            TABLET_VERIFY_C(
                0,
                TStringBuilder() << "bad variant index: " << Result.index());
        }
    }

    using TResponse = TEvIndexTabletPrivate::TEvNodeUnlinkedInShard;
    ctx.Send(
        ParentId,
        std::make_unique<TResponse>(
            std::move(RequestInfo),
            Request.GetHeaders().GetSessionId(),
            RequestId,
            OpLogEntryId,
            std::move(Result),
            ShouldUnlockUponCompletion,
            Request.GetOriginalRequest()));

    Die(ctx);
}

STFUNC(TUnlinkNodeInShardActor::StateWork)
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

    auto* msg = ev->Get();

    // DupCache isn't needed for Create/UnlinkNode requests in shards
    if (!BehaveAsShard(msg->Record.GetHeaders())) {
        auto* session = AcceptRequest<TEvService::TUnlinkNodeMethod>(ev, ctx, ValidateRequest);
        if (!session) {
            return;
        }

        const auto requestId = GetRequestId(msg->Record);
        if (const auto* e = session->LookupDupEntry(requestId)) {
            auto response = std::make_unique<TEvService::TEvUnlinkNodeResponse>();
            if (GetDupCacheEntry(e, response->Record)) {
                return NCloud::Reply(ctx, *ev, std::move(response));
            }

            session->DropDupEntry(requestId);
        }
    }

    if (!TryLockNodeRef({msg->Record.GetNodeId(), msg->Record.GetName()})) {
        auto response = std::make_unique<TEvService::TEvUnlinkNodeResponse>(
            MakeError(E_REJECTED, TStringBuilder() << "node ref "
                << msg->Record.GetNodeId() << " " << msg->Record.GetName()
                << " is locked for UnlinkNode"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvService::TUnlinkNodeMethod>(*requestInfo);

    ExecuteTx<TUnlinkNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnlinkNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnlinkNode& args)
{
    Y_UNUSED(ctx);

    if (!BehaveAsShard(args.Request.GetHeaders())) {
        FILESTORE_VALIDATE_DUPTX_SESSION(UnlinkNode, args);
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

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

    if (args.ChildRef->IsExternal()) {
        return true;
    }

    // TODO: AccessCheck

    if (!args.ChildNode) {
        auto message = ReportChildNodeIsNull(TStringBuilder()
            << "UnlinkNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return true;
    }

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
            // should explicitly unlink directory node
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

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "UnlinkNode");
    }

    if (args.ChildRef->IsExternal()) {
        // This external node can refer both to a file and a directory. In case
        // of a file the request is not supposed to fail: we can unlink the node
        // in the leader and then unlink the external node in the shard.
        //
        // In case of a directory we can't unlink the node in the leader if it's
        // not empty. So a different approach is used here: this nodeRef is not
        // unlocked and not removed until the confirmation from the shard is
        // received that the node is unlinked.

        if (!args.Request.GetUnlinkDirectory()) {
            UnlinkExternalNode(
                db,
                args.ParentNodeId,
                args.Name,
                args.ChildRef->ShardId,
                args.ChildRef->ShardNodeName,
                args.ChildRef->MinCommitId,
                args.CommitId);
        }

        // OpLogEntryId doesn't have to be a CommitId - it's just convenient to
        // use CommitId here in order not to generate some other unique ui64
        args.OpLogEntry.SetEntryId(args.CommitId);
        auto* shardRequest = args.OpLogEntry.MutableUnlinkNodeInShardRequest();
        shardRequest->MutableHeaders()->CopyFrom(args.Request.GetHeaders());
        shardRequest->SetFileSystemId(args.ChildRef->ShardId);
        shardRequest->SetNodeId(RootNodeId);
        shardRequest->SetName(args.ChildRef->ShardNodeName);
        shardRequest->SetUnlinkDirectory(args.Request.GetUnlinkDirectory());
        shardRequest->MutableOriginalRequest()->CopyFrom(args.Request);

        db.WriteOpLogEntry(args.OpLogEntry);
    } else {
        auto e = UnlinkNode(
            db,
            args.ParentNodeId,
            args.Name,
            *args.ChildNode,
            args.ChildRef->MinCommitId,
            args.CommitId);

        if (HasError(e)) {
            args.Error = std::move(e);
            return;
        }
    }

    if (!BehaveAsShard(args.Request.GetHeaders())) {
        auto* session = FindSession(args.SessionId);
        if (!session) {
            auto message = ReportSessionNotFoundInTx(TStringBuilder()
                << "UnlinkNode: " << args.Request.ShortDebugString());
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
            return;
        }

        AddDupCacheEntry(
            db,
            session,
            args.RequestId,
            NProto::TUnlinkNodeResponse{},
            Config->GetDupCacheEntryCount());
    }

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_UnlinkNode(
    const TActorContext& ctx,
    TTxIndexTablet::TUnlinkNode& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s[%s] UnlinkNode completed (%s)",
        LogTag.c_str(),
        args.SessionId.c_str(),
        FormatError(args.Error).c_str());

    if (args.ParentNodeId != InvalidNodeId) {
        InvalidateNodeCaches(args.ParentNodeId);
    }
    if (args.ChildNode && args.ChildNode->NodeId != InvalidNodeId) {
        InvalidateNodeCaches(args.ChildNode->NodeId);
    }

    if (!HasError(args.Error) && !args.ChildRef) {
        auto message = ReportChildRefIsNull(TStringBuilder()
            << "UnlinkNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
    }

    bool shouldUnlockUponCompletion = true;
    // If the node is external and it's a directory and creation of
    // directories is enabled for this filesystem and the request is to be
    // forwarded to the shard, then the nodeRef is not unlocked here as it
    // is possible that the shard will reject the request. In this case the
    // nodeRef will be unlocked afterwards
    if (HasError(args.Error) || !args.ChildRef->IsExternal() ||
        !args.Request.GetUnlinkDirectory() ||
        // TODO: fix following check
        !Config->GetDirectoryCreationInShardsEnabled())
    {
        UnlockNodeRef({args.ParentNodeId, args.Name});
        shouldUnlockUponCompletion = false;
    }
    if (!HasError(args.Error)) {
        if (args.ChildRef->IsExternal()) {
            LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
                "%s Unlinking node in shard upon UnlinkNode: %s, %s",
                LogTag.c_str(),
                args.ChildRef->ShardId.c_str(),
                args.ChildRef->ShardNodeName.c_str());

            RegisterUnlinkNodeInShardActor(
                ctx,
                args.RequestInfo,
                args.OpLogEntry.GetUnlinkNodeInShardRequest(),
                args.RequestId,
                args.OpLogEntry.GetEntryId(),
                {},
                shouldUnlockUponCompletion);

            return;
        }

        if (!BehaveAsShard(args.Request.GetHeaders())) {
            CommitDupCacheEntry(args.SessionId, args.RequestId);
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

    RemoveTransaction(*args.RequestInfo);
    EnqueueBlobIndexOpIfNeeded(ctx);

    Metrics.UnlinkNode.Update(
        1,
        0,
        ctx.Now() - args.RequestInfo->StartedTs);

    auto response =
        std::make_unique<TEvService::TEvUnlinkNodeResponse>(args.Error);
    CompleteResponse<TEvService::TUnlinkNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CompleteUnlinkNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCompleteUnlinkNode& args)
{
    Y_UNUSED(ctx, tx, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    // TODO(debnath): we should pass commitId from the request, not generate it
    args.CommitId = GetCurrentCommitId();

    // validate parent node exists
    if (!ReadNode(db, args.ParentNodeId, args.CommitId, args.ParentNode)) {
        return false;   // not ready
    }

    if (!args.ParentNode) {
        args.Error = ErrorInvalidParent(args.ParentNodeId);
        return true;
    }

    // validate target node exists
    if (!ReadNodeRef(
            db,
            args.ParentNodeId,
            args.CommitId,
            args.Name,
            args.ChildRef))
    {
        return false;   // not ready
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_CompleteUnlinkNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCompleteUnlinkNode& args)
{
    // If the original response was an error or prepare stage failed, we don't
    // need to do anything
    if (FAILED(args.Response.GetError().GetCode()) ||
        FAILED(args.Error.GetCode()))
    {
        return;
    }

    Y_UNUSED(ctx, tx, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "CompleteUnlinkNode");
    }
    if (!args.ChildRef->IsExternal()) {
        auto message = ReportUnexpectedLocalNode(
            TStringBuilder()
            << "CompleteUnlinkNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Unexpected local node: %s",
            LogTag.c_str(),
            message.c_str());
        return;
    }

    UnlinkExternalNode(
        db,
        args.ParentNodeId,
        args.Name,
        args.ChildRef->ShardId,
        args.ChildRef->ShardNodeName,
        args.ChildRef->MinCommitId,
        args.CommitId);

    db.DeleteOpLogEntry(args.OpLogEntryId);
}

void TIndexTabletActor::CompleteTx_CompleteUnlinkNode(
    const TActorContext& ctx,
    TTxIndexTablet::TCompleteUnlinkNode& args)
{
    Y_UNUSED(ctx, args);
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s[%s] CompleteUnlinkNode completed (%s)",
        LogTag.c_str(),
        args.SessionId.c_str(),
        FormatError(args.Error).c_str());

    if (args.ParentNodeId != InvalidNodeId) {
        InvalidateNodeCaches(args.ParentNodeId);
    }
    if (args.ChildNode && args.ChildNode->NodeId != InvalidNodeId) {
        InvalidateNodeCaches(args.ChildNode->NodeId);
    }

    if (!HasError(args.Error)) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Unlinking node in shard upon CompleteUnlinkNode: %s, %s",
            LogTag.c_str(),
            args.ChildRef->ShardId.c_str(),
            args.ChildRef->ShardNodeName.c_str());

        UnlockNodeRef({args.ParentNodeId, args.Name});
    }

    RemoveTransaction(*args.RequestInfo);
    EnqueueBlobIndexOpIfNeeded(ctx);

    Metrics.UnlinkNode.Update(1, 0, ctx.Now() - args.RequestInfo->StartedTs);

    auto response = std::make_unique<TEvService::TEvUnlinkNodeResponse>(
        FAILED(args.Error.GetCode()) ? args.Error : args.Response.GetError());
    CompleteResponse<TEvService::TUnlinkNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleNodeUnlinkedInShard(
    const TEvIndexTabletPrivate::TEvNodeUnlinkedInShard::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& res = msg->Result;

    if (msg->RequestInfo) {
        RemoveTransaction(*msg->RequestInfo);
        CommitDupCacheEntry(msg->SessionId, msg->RequestId);

        if (auto* x = std::get_if<NProto::TRenameNodeResponse>(&res)) {
            auto response =
                std::make_unique<TEvService::TEvRenameNodeResponse>();
            response->Record = std::move(*x);

            CompleteResponse<TEvService::TRenameNodeMethod>(
                response->Record,
                msg->RequestInfo->CallContext,
                ctx);

            Metrics.RenameNode.Update(
                1,
                0,
                ctx.Now() - msg->RequestInfo->StartedTs);

            NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
        } else if (auto* x = std::get_if<
                NProtoPrivate::TRenameNodeInDestinationResponse>(&res))
        {
            auto response = std::make_unique<
                TEvIndexTablet::TEvRenameNodeInDestinationResponse>();
            response->Record = std::move(*x);

            CompleteResponse<TEvIndexTablet::TRenameNodeInDestinationMethod>(
                response->Record,
                msg->RequestInfo->CallContext,
                ctx);

            Metrics.RenameNode.Update(
                1,
                0,
                ctx.Now() - msg->RequestInfo->StartedTs);

            NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
        } else if (auto* x = std::get_if<NProto::TUnlinkNodeResponse>(&res)) {
            // If the node is external and its lock has not been released yet,
            // we should not respond to the client yet. We should remove the
            // node ref (if the request was successful) and unlock the node ref
            if (!msg->ShouldUnlockUponCompletion) {
                auto response =
                    std::make_unique<TEvService::TEvUnlinkNodeResponse>();
                response->Record = std::move(*x);

                CompleteResponse<TEvService::TUnlinkNodeMethod>(
                    response->Record,
                    msg->RequestInfo->CallContext,
                    ctx);

                Metrics.UnlinkNode.Update(
                    1,
                    0,
                    ctx.Now() - msg->RequestInfo->StartedTs);

                NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
            }
        } else {
            TABLET_VERIFY_C(
                0,
                TStringBuilder() << "bad variant index: " << res.index());
        }
    }

    // only if it is unlik operation and we should unlock node ref upon
    // completion
    WorkerActors.erase(ev->Sender);
    if (std::holds_alternative<NProto::TUnlinkNodeResponse>(res) &&
        msg->ShouldUnlockUponCompletion)
    {
        const auto& originalRequest = msg->OriginalRequest;
        auto response = std::get<NProto::TUnlinkNodeResponse>(res);

        AddTransaction<TEvService::TUnlinkNodeMethod>(*msg->RequestInfo);
        ExecuteTx<TCompleteUnlinkNode>(
            ctx,
            msg->RequestInfo,
            originalRequest,
            msg->OpLogEntryId,
            response);
    } else {
        ExecuteTx<TDeleteOpLogEntry>(ctx, msg->OpLogEntryId);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RegisterUnlinkNodeInShardActor(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    NProtoPrivate::TUnlinkNodeInShardRequest request,
    ui64 requestId,
    ui64 opLogEntryId,
    TUnlinkNodeInShardResult result,
    bool shouldUnlockUponCompletion)
{
    auto actor = std::make_unique<TUnlinkNodeInShardActor>(
        LogTag,
        std::move(requestInfo),
        ctx.SelfID,
        std::move(request),
        requestId,
        opLogEntryId,
        std::move(result),
        shouldUnlockUponCompletion);

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
