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
    NProto::TProfileLogRequestInfo ProfileLogRequest;
    const ui64 RequestId;
    const ui64 OpLogEntryId;

public:
    TRenameNodeInDestinationActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        NProto::TProfileLogRequestInfo profileLogRequest,
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
        NProto::TProfileLogRequestInfo profileLogRequest,
        ui64 requestId,
        ui64 opLogEntryId)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , ParentId(parentId)
    , Request(std::move(request))
    , ProfileLogRequest(std::move(profileLogRequest))
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

    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
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
        Request.GetFileSystemId(),
        Request.GetHeaders().GetRequestId(),
        Request.GetOriginalRequest(), // TODO: move
        std::move(ProfileLogRequest),
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

////////////////////////////////////////////////////////////////////////////////

class TDeleteResponseLogEntryActor final
    : public TActorBootstrapped<TDeleteResponseLogEntryActor>
{
private:
    const TString LogTag;
    const TActorId ParentId;
    const TString ShardFileSystemId;
    const ui64 ClientTabletId;
    const ui64 TabletRequestId;

public:
    TDeleteResponseLogEntryActor(
        TString logTag,
        const TActorId& parentId,
        TString shardFileSystemId,
        ui64 clientTabletId,
        ui64 tabletRequestId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequest(const TActorContext& ctx);

    void HandleDeleteResponseLogEntryResponse(
        const TEvIndexTablet::TEvDeleteResponseLogEntryResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& e);
};

////////////////////////////////////////////////////////////////////////////////

TDeleteResponseLogEntryActor::TDeleteResponseLogEntryActor(
        TString logTag,
        const TActorId& parentId,
        TString shardFileSystemId,
        ui64 clientTabletId,
        ui64 tabletRequestId)
    : LogTag(std::move(logTag))
    , ParentId(parentId)
    , ShardFileSystemId(std::move(shardFileSystemId))
    , ClientTabletId(clientTabletId)
    , TabletRequestId(tabletRequestId)
{}

void TDeleteResponseLogEntryActor::Bootstrap(const TActorContext& ctx)
{
    SendRequest(ctx);
    Become(&TThis::StateWork);
}

void TDeleteResponseLogEntryActor::SendRequest(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvDeleteResponseLogEntryRequest>();
    request->Record.SetFileSystemId(ShardFileSystemId);
    request->Record.SetClientTabletId(ClientTabletId);
    request->Record.SetRequestId(TabletRequestId);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending DeleteResponseLogEntryRequest to shard %s",
        LogTag.c_str(),
        request->Record.ShortUtf8DebugString().Quote().c_str());

    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TDeleteResponseLogEntryActor::HandleDeleteResponseLogEntryResponse(
    const TEvIndexTablet::TEvDeleteResponseLogEntryResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s DeleteResponseLogEntry failed for %s, %lu, %lu with error"
                " %s, retrying",
                LogTag.c_str(),
                ShardFileSystemId.c_str(),
                ClientTabletId,
                TabletRequestId,
                FormatError(msg->GetError()).Quote().c_str());

            SendRequest(ctx);
            return;
        }

        ReplyAndDie(ctx, msg->Record.GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s DeleteResponseLogEntry succeeded for %s, %lu, %lu",
        LogTag.c_str(),
        ShardFileSystemId.c_str(),
        ClientTabletId,
        TabletRequestId);

    ReplyAndDie(ctx, msg->Record.GetError());
}

void TDeleteResponseLogEntryActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TDeleteResponseLogEntryActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& e)
{
    if (HasError(e)) {
        const auto message = Sprintf(
            "DeleteResponseLogEntry failed for %s, %lu, %lu with error %s"
            ", will not retry",
            ShardFileSystemId.c_str(),
            ClientTabletId,
            TabletRequestId,
            FormatError(e).Quote().c_str());

        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s %s",
            LogTag.c_str(),
            message.c_str());
    }

    using TResponse = TEvIndexTabletPrivate::TEvResponseLogEntryDeleted;
    ctx.Send(ParentId, std::make_unique<TResponse>());

    Die(ctx);
}

STFUNC(TDeleteResponseLogEntryActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvDeleteResponseLogEntryResponse,
            HandleDeleteResponseLogEntryResponse);

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

void TIndexTabletActor::HandlePrepareRenameNodeInSource(
    const TEvIndexTabletPrivate::TEvPrepareRenameNodeInSourceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TPrepareRenameNodeInSource>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Request),
        NProto::TProfileLogRequestInfo{},
        std::move(msg->NewShardId),
        true /* isExplicitRequest */);
}

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
        args.OnCommitIdOverflow();
        return;
    }

    // OpLogEntryId doesn't have to be a CommitId - it's just convenient
    // to use CommitId here in order not to generate some other unique
    // ui64
    args.OpLogEntry.SetEntryId(args.CommitId);
    *args.OpLogEntry.MutableRenameNodeInDestinationRequest() =
        MakeRenameNodeInDestinationRequest(
            TabletID(),
            args.CommitId,
            args.Request,
            args.ChildRef->ShardId,
            args.ChildRef->ShardNodeName,
            args.NewParentShardId);

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

    if (args.IsExplicitRequest) {
        //
        // This branch is supposed to be used only in unit tests.
        //

        using TResponse =
            TEvIndexTabletPrivate::TEvPrepareRenameNodeInSourceResponse;
        auto response = std::make_unique<TResponse>(args.Error);
        response->OpLogEntryId = args.OpLogEntry.GetEntryId();
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
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
                std::move(args.ProfileLogRequest),
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

    FinalizeProfileLogRequestInfo(
        std::move(args.ProfileLogRequest),
        ctx.Now(),
        GetFileSystemId(),
        args.Error,
        ProfileLog);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleNodeRenamedInDestination(
    const TEvIndexTabletPrivate::TEvNodeRenamedInDestination::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    ExecuteTx<TCommitRenameNodeInSource>(
        ctx,
        msg->RequestInfo,
        std::move(msg->Request),
        std::move(msg->ProfileLogRequest),
        std::move(msg->Response),
        msg->OpLogEntryId,
        std::move(msg->ShardFileSystemId),
        msg->TabletRequestId,
        false /* isExplicitRequest */);

    WorkerActors.erase(ev->Sender);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCommitRenameNodeInSource(
    const TEvIndexTabletPrivate::TEvCommitRenameNodeInSourceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddInFlightRequest<TEvIndexTabletPrivate::TCommitRenameNodeInSourceMethod>(
        *requestInfo);

    ExecuteTx<TCommitRenameNodeInSource>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Request),
        NProto::TProfileLogRequestInfo{},
        std::move(msg->Response),
        msg->OpLogEntryId,
        std::move(msg->ShardFileSystemId),
        msg->TabletRequestId,
        true /* isExplicitRequest */);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RegisterRenameNodeInDestinationActor(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    NProtoPrivate::TRenameNodeInDestinationRequest request,
    NProto::TProfileLogRequestInfo profileLogRequest,
    ui64 requestId,
    ui64 opLogEntryId)
{
    auto actor = std::make_unique<TRenameNodeInDestinationActor>(
        LogTag,
        std::move(requestInfo),
        ctx.SelfID,
        std::move(request),
        std::move(profileLogRequest),
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
    Y_UNUSED(ctx);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
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
        args.Error = args.Response.GetError();
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

    Y_DEFER {
        FinalizeProfileLogRequestInfo(
            std::move(args.ProfileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            args.Error,
            ProfileLog);
    };

    if (!args.RequestInfo) {
        return;
    }

    RemoveInFlightRequest(*args.RequestInfo);

    //
    // Best-effort response log entry deletion attempt. The entries that don't
    // get deleted because of this request not reaching the shard will be
    // deleted in background when they become too old.
    //

    auto actor = std::make_unique<TDeleteResponseLogEntryActor>(
        LogTag,
        ctx.SelfID,
        std::move(args.ShardFileSystemId),
        TabletID(),
        args.TabletRequestId);

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);

    if (args.IsExplicitRequest) {
        using TResponse =
            TEvIndexTabletPrivate::TEvCommitRenameNodeInSourceResponse;
        auto response = std::make_unique<TResponse>(args.Error);
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    Metrics.RenameNode.Update(
        1,
        0,
        ctx.Now() - args.RequestInfo->StartedTs);

    auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(
        args.Error);
    CompleteResponse<TEvService::TRenameNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

void TIndexTabletActor::HandleResponseLogEntryDeleted(
    const TEvIndexTabletPrivate::TEvResponseLogEntryDeleted::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    WorkerActors.erase(ev->Sender);
}

////////////////////////////////////////////////////////////////////////////////

NProtoPrivate::TRenameNodeInDestinationRequest
MakeRenameNodeInDestinationRequest(
    ui64 tabletId,
    ui64 commitId,
    NProto::TRenameNodeRequest originalRequest,
    TString sourceNodeShardId,
    TString sourceNodeShardNodeName,
    TString newParentShardId)
{
    NProtoPrivate::TRenameNodeInDestinationRequest request;
    auto& headers = *request.MutableHeaders();
    headers.CopyFrom(originalRequest.GetHeaders());
    headers.SetRequestId(commitId);
    headers.MutableInternal()->SetClientTabletId(tabletId);
    request.SetFileSystemId(std::move(newParentShardId));
    request.SetNewParentId(originalRequest.GetNewParentId());
    request.SetNewName(originalRequest.GetNewName());
    request.SetFlags(originalRequest.GetFlags());
    request.SetSourceNodeShardId(std::move(sourceNodeShardId));
    request.SetSourceNodeShardNodeName(std::move(sourceNodeShardNodeName));
    *request.MutableOriginalRequest() = std::move(originalRequest);
    return request;
}

}   // namespace NCloud::NFileStore::NStorage
