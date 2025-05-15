#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRenameNodeInDestinationActor final
    : public TActorBootstrapped<TRenameNodeInDestinationActor>
{
private:
    const TString LogTag;
    TRequestInfoPtr RequestInfo;
    const TActorId ParentId;
    const NProtoPrivate::TRenameNodeInDestinationRequest Request;
    const ui64 RequestId;
    const ui64 OpLogEntryId;

public:
    TRenameNodeInDestinationActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        ui64 requestId,
        ui64 opLogEntryId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequest(const TActorContext& ctx);

    void HandleRenameNodeInDestinationResponse(
        const TEvIndexTablet::TEvRenameNodeInDestinationResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        NProtoPrivate::TRenameNodeInDestinationResponse response);
};

////////////////////////////////////////////////////////////////////////////////

TRenameNodeInDestinationActor::TRenameNodeInDestinationActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        ui64 requestId,
        ui64 opLogEntryId)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , ParentId(parentId)
    , Request(std::move(request))
    , RequestId(requestId)
    , OpLogEntryId(opLogEntryId)
{}

void TRenameNodeInDestinationActor::Bootstrap(const TActorContext& ctx)
{
    SendRequest(ctx);
    Become(&TThis::StateWork);
}

void TRenameNodeInDestinationActor::SendRequest(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvRenameNodeInDestinationRequest>();
    request->Record = Request;

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending RenameNodeInDestination to shard %s, %lu, %s",
        LogTag.c_str(),
        Request.GetFileSystemId().c_str(),
        Request.GetNewParentId(),
        Request.GetNewName().c_str());

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release());
}

void TRenameNodeInDestinationActor::HandleRenameNodeInDestinationResponse(
    const TEvIndexTablet::TEvRenameNodeInDestinationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s RenameNodeInDestination failed for %s, %lu, %s with error"
                " %s, retrying",
                LogTag.c_str(),
                Request.GetFileSystemId().c_str(),
                Request.GetNewParentId(),
                Request.GetNewName().c_str(),
                FormatError(msg->GetError()).Quote().c_str());

            SendRequest(ctx);
            return;
        }

        const auto message = Sprintf(
            "RenameNodeInDestination failed for %s, %lu, %s with error %s"
            ", will not retry",
            Request.GetFileSystemId().c_str(),
            Request.GetNewParentId(),
            Request.GetNewName().c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s %s",
            LogTag.c_str(),
            message.c_str());

        ReportReceivedNodeOpErrorFromShard(message);

        ReplyAndDie(ctx, std::move(msg->Record));
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s RenameNodeInDestination succeeded for %s, %lu, %s",
        LogTag.c_str(),
        Request.GetFileSystemId().c_str(),
        Request.GetNewParentId(),
        Request.GetNewName().c_str());

    ReplyAndDie(ctx, std::move(msg->Record));
}

void TRenameNodeInDestinationActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    NProtoPrivate::TRenameNodeInDestinationResponse response;
    *response.MutableError() = MakeError(E_REJECTED, "tablet is shutting down");
    ReplyAndDie(ctx, std::move(response));
}

void TRenameNodeInDestinationActor::ReplyAndDie(
    const TActorContext& ctx,
    NProtoPrivate::TRenameNodeInDestinationResponse response)
{
    using TResponse = TEvIndexTabletPrivate::TEvNodeRenamedInDestination;
    ctx.Send(ParentId, std::make_unique<TResponse>(
        std::move(RequestInfo),
        Request.GetHeaders().GetSessionId(),
        RequestId,
        OpLogEntryId,
        Request.GetOriginalRequest(), // TODO: move
        std::move(response)));

    Die(ctx);
}

STFUNC(TRenameNodeInDestinationActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvRenameNodeInDestinationResponse,
            HandleRenameNodeInDestinationResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_PrepareRenameNodeInSource(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TPrepareRenameNodeInSource& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_DUPTX_SESSION(RenameNode, args);

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

    // validate old ref exists
    if (!ReadNodeRef(
            db,
            args.ParentNodeId,
            args.CommitId,
            args.Name,
            args.ChildRef))
    {
        return false;   // not ready
    }

    // read old node
    if (!args.ChildRef) {
        args.Error = ErrorInvalidTarget(args.ParentNodeId);
        return true;
    }

    if (!args.ChildRef->IsExternal()) {
        auto message = ReportRenameNodeRequestForLocalNode(TStringBuilder()
            << "PrepareRenameNodeInSource: "
            << args.Request.ShortDebugString());
        args.Error = MakeError(E_ARGUMENT, std::move(message));
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_PrepareRenameNodeInSource(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TPrepareRenameNodeInSource& args)
{
    FILESTORE_VALIDATE_TX_ERROR(RenameNode, args);
    if (args.Error.GetCode() == S_ALREADY) {
        return; // nothing to do
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "PrepareRenameNodeInSource");
    }

    // OpLogEntryId doesn't have to be a CommitId - it's just convenient
    // to use CommitId here in order not to generate some other unique
    // ui64
    args.OpLogEntry.SetEntryId(args.CommitId);
    auto* shardRequest =
        args.OpLogEntry.MutableRenameNodeInDestinationRequest();
    shardRequest->MutableHeaders()->CopyFrom(
        args.Request.GetHeaders());
    shardRequest->SetFileSystemId(args.NewParentShardId);
    shardRequest->SetNewParentId(args.Request.GetNewParentId());
    shardRequest->SetNewName(args.Request.GetNewName());
    shardRequest->SetFlags(args.Request.GetFlags());
    shardRequest->SetSourceNodeShardId(args.ChildRef->ShardId);
    shardRequest->SetSourceNodeShardNodeName(args.ChildRef->ShardNodeName);
    *shardRequest->MutableOriginalRequest() = args.Request;

    db.WriteOpLogEntry(args.OpLogEntry);

    // update old parent timestamps
    auto parent = CopyAttrs(args.ParentNode->Attrs, E_CM_CMTIME);
    UpdateNode(
        db,
        args.ParentNode->NodeId,
        args.ParentNode->MinCommitId,
        args.CommitId,
        parent,
        args.ParentNode->Attrs);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        auto message = ReportSessionNotFoundInTx(TStringBuilder()
            << "RenameNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return;
    }

    AddDupCacheEntry(
        db,
        session,
        args.RequestId,
        NProto::TRenameNodeResponse{},
        Config->GetDupCacheEntryCount());

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_PrepareRenameNodeInSource(
    const TActorContext& ctx,
    TTxIndexTablet::TPrepareRenameNodeInSource& args)
{
    InvalidateNodeCaches(args.ParentNodeId);
    if (args.ChildRef) {
        InvalidateNodeCaches(args.ChildRef->ChildNodeId);
    }
    if (args.ParentNode) {
        InvalidateNodeCaches(args.ParentNode->NodeId);
    }

    if (!HasError(args.Error) && !args.ChildRef) {
        auto message = ReportChildRefIsNull(TStringBuilder()
            << "RenameNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
    }

    if (!HasError(args.Error)) {
        auto& op = args.OpLogEntry;

        if (!op.HasRenameNodeInDestinationRequest()) {
            auto message = ReportNoRenameNodeInDestinationRequest(
                TStringBuilder() << "RenameNode: "
                    << args.Request.ShortDebugString());
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
        } else {
            auto& shardRequest = *op.MutableRenameNodeInDestinationRequest();
            LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
                "%s Renaming node in dst shard upon RenameNode: %s, %lu",
                LogTag.c_str(),
                shardRequest.GetFileSystemId().c_str(),
                shardRequest.GetNewParentId());

            RegisterRenameNodeInDestinationActor(
                ctx,
                args.RequestInfo,
                std::move(shardRequest),
                args.RequestId,
                args.OpLogEntry.GetEntryId());
            return;
        }
    }

    UnlockNodeRef({args.ParentNodeId, args.Name});

    Metrics.RenameNode.Update(
        1,
        0,
        ctx.Now() - args.RequestInfo->StartedTs);

    auto response =
        std::make_unique<TEvService::TEvRenameNodeResponse>(args.Error);
    CompleteResponse<TEvService::TRenameNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleNodeRenamedInDestination(
    const TEvIndexTabletPrivate::TEvNodeRenamedInDestination::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    ExecuteTx<TCommitRenameNodeInSource>(
        ctx,
        std::move(msg->RequestInfo),
        std::move(msg->Request),
        std::move(msg->Response),
        msg->OpLogEntryId);

    WorkerActors.erase(ev->Sender);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RegisterRenameNodeInDestinationActor(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    NProtoPrivate::TRenameNodeInDestinationRequest request,
    ui64 requestId,
    ui64 opLogEntryId)
{
    auto actor = std::make_unique<TRenameNodeInDestinationActor>(
        LogTag,
        std::move(requestInfo),
        ctx.SelfID,
        std::move(request),
        requestId,
        opLogEntryId);

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CommitRenameNodeInSource(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCommitRenameNodeInSource& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    // validate old ref exists
    if (!ReadNodeRef(
            db,
            args.Request.GetNodeId(),
            args.CommitId,
            args.Request.GetName(),
            args.ChildRef))
    {
        return false;   // not ready
    }

    if (!args.ChildRef || !args.ChildRef->IsExternal()) {
        auto message = ReportBadChildRefUponCommitRenameNodeInSource(
            TStringBuilder() << "CommitRenameNodeInSource: "
            << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_CommitRenameNodeInSource(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCommitRenameNodeInSource& args)
{
    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "CommitRenameNodeInSource");
    }

    UnlockNodeRef({args.Request.GetNodeId(), args.Request.GetName()});

    if (HasError(args.Response)) {
        NProto::TRenameNodeResponse response;
        *response.MutableError() = args.Response.GetError();
        PatchDupCacheEntry(
            db,
            args.SessionId,
            args.RequestId,
            std::move(response));
    } else {
        RemoveNodeRef(
            db,
            args.Request.GetNodeId(),
            args.ChildRef->MinCommitId,
            args.CommitId,
            args.Request.GetName(),
            args.ChildRef->ChildNodeId,
            args.ChildRef->ShardId,
            args.ChildRef->ShardNodeName);

        const bool isExchange = HasFlag(
            args.Request.GetFlags(),
            NProto::TRenameNodeRequest::F_EXCHANGE);

        if (isExchange) {
            // create source ref to target node
            CreateNodeRef(
                db,
                args.Request.GetNodeId(),
                args.CommitId,
                args.Request.GetName(),
                InvalidNodeId,
                args.Response.GetOldTargetNodeShardId(),
                args.Response.GetOldTargetNodeShardNodeName());
        }
    }

    db.DeleteOpLogEntry(args.OpLogEntryId);
}

void TIndexTabletActor::CompleteTx_CommitRenameNodeInSource(
    const TActorContext& ctx,
    TTxIndexTablet::TCommitRenameNodeInSource& args)
{
    InvalidateNodeCaches(args.Request.GetNodeId());
    CommitDupCacheEntry(args.SessionId, args.RequestId);

    if (args.RequestInfo) {
        RemoveTransaction(*args.RequestInfo);

        Metrics.RenameNode.Update(
            1,
            0,
            ctx.Now() - args.RequestInfo->StartedTs);

        auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(
            args.Response.GetError());
        CompleteResponse<TEvService::TRenameNodeMethod>(
            response->Record,
            args.RequestInfo->CallContext,
            ctx);

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }
}

}   // namespace NCloud::NFileStore::NStorage
