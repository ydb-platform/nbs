#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(
    const NProtoPrivate::TRenameNodeInDestinationRequest& request)
{
    Y_UNUSED(request);

    return {};
}

////////////////////////////////////////////////////////////////////////////////
//
// This actor is needed to prepare the dst node (pointed to by NewName) for
// unlinking. If the dst node is a directory then special processing needs to
// be done. The actor performs the following steps:
// 1. gets NodeAttr for the src and dst nodes
// 2. if dst is not a directory, replies and dies
// 3. if dst is a directory, prepare-unlink is needed but first we need to write
//  an abort-unlink op to the tablet OpLog to make sure that if this whole op
//  fails at any point, we release the corresponding locks.
// 4. sends a prepare-unlink request to the shard in charge of the dst node
// 5. replies and dies
//

class TGetNodeInfoAndPrepareUnlinkActor final
    : public TActorBootstrapped<TGetNodeInfoAndPrepareUnlinkActor>
{
private:
    const TString LogTag;
    const TActorId ParentId;
    TString DstShardId;
    TString DstShardNodeName;
    TEvIndexTabletPrivate::TDoRenameNodeInDestination Result;

    static constexpr ui64 SourceCookie = 1;
    static constexpr ui64 DstCookie = 2;

    ui32 ResultCount = 0;

    using TEvPrepareUnlinkRequest =
        TEvIndexTablet::TEvPrepareUnlinkDirectoryNodeInShardRequest;
    using TEvPrepareUnlinkResponse =
        TEvIndexTablet::TEvPrepareUnlinkDirectoryNodeInShardResponse;

public:
    TGetNodeInfoAndPrepareUnlinkActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        NProto::TProfileLogRequestInfo profileLogRequest,
        TString dstShardId,
        TString dstShardNodeName);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequests(const TActorContext& ctx);

    void SendRequest(
        const TActorContext& ctx,
        const TString& shardId,
        const TString& shardNodeName,
        ui64 cookie);

    void LogAbort(const TActorContext& ctx);

    void PrepareUnlink(const TActorContext& ctx);

    void HandleGetNodeAttrResponse(
        const TEvService::TEvGetNodeAttrResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleLogAbortResponse(
        const TEvIndexTabletPrivate::TEvWriteOpLogEntryResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePrepareUnlinkResponse(
        const TEvPrepareUnlinkResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TGetNodeInfoAndPrepareUnlinkActor::TGetNodeInfoAndPrepareUnlinkActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        NProto::TProfileLogRequestInfo profileLogRequest,
        TString dstShardId,
        TString dstShardNodeName)
    : LogTag(std::move(logTag))
    , ParentId(parentId)
    , DstShardId(std::move(dstShardId))
    , DstShardNodeName(std::move(dstShardNodeName))
    , Result(
        std::move(requestInfo),
        std::move(request),
        std::move(profileLogRequest))
{}

void TGetNodeInfoAndPrepareUnlinkActor::Bootstrap(const TActorContext& ctx)
{
    SendRequests(ctx);
    Become(&TThis::StateWork);
}

void TGetNodeInfoAndPrepareUnlinkActor::SendRequest(
    const TActorContext& ctx,
    const TString& shardId,
    const TString& shardNodeName,
    ui64 cookie)
{
    auto request = std::make_unique<TEvService::TEvGetNodeAttrRequest>();
    request->Record.MutableHeaders()->CopyFrom(Result.Request.GetHeaders());
    request->Record.SetFileSystemId(shardId);
    request->Record.SetNodeId(RootNodeId);
    request->Record.SetName(shardNodeName);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending GetNodeAttrRequest to shard %s, %s",
        LogTag.c_str(),
        request->Record.GetFileSystemId().c_str(),
        request->Record.GetName().c_str());

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release(),
        0 /* flags */,
        cookie);
}

void TGetNodeInfoAndPrepareUnlinkActor::SendRequests(const TActorContext& ctx)
{
    SendRequest(
        ctx,
        Result.Request.GetSourceNodeShardId(),
        Result.Request.GetSourceNodeShardNodeName(),
        SourceCookie);

    SendRequest(
        ctx,
        DstShardId,
        DstShardNodeName,
        DstCookie);
}

void TGetNodeInfoAndPrepareUnlinkActor::LogAbort(
    const TActorContext& ctx)
{
    NProto::TOpLogEntry entry;
    auto* abortRequest = entry.MutableAbortUnlinkDirectoryNodeInShardRequest();
    abortRequest->MutableHeaders()->CopyFrom(Result.Request.GetHeaders());
    abortRequest->SetFileSystemId(DstShardId);
    abortRequest->SetNodeId(Result.DestinationNodeAttr.GetId());
    abortRequest->MutableOriginalRequest()->CopyFrom(Result.Request);

    using TRequest = TEvIndexTabletPrivate::TEvWriteOpLogEntryRequest;
    auto request = std::make_unique<TRequest>(std::move(entry));

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending WriteOpLogEntryRequest to parent %s",
        LogTag.c_str(),
        request->OpLogEntry.ShortUtf8DebugString().Quote().c_str());

    ctx.Send(ParentId, request.release());
}

void TGetNodeInfoAndPrepareUnlinkActor::PrepareUnlink(
    const TActorContext& ctx)
{
    auto request = std::make_unique<TEvPrepareUnlinkRequest>();
    request->Record.MutableHeaders()->CopyFrom(Result.Request.GetHeaders());
    request->Record.SetFileSystemId(DstShardId);
    request->Record.SetNodeId(Result.DestinationNodeAttr.GetId());
    request->Record.MutableOriginalRequest()->CopyFrom(Result.Request);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending PrepareUnlinkRequest to shard %s",
        LogTag.c_str(),
        request->Record.ShortUtf8DebugString().Quote().c_str());

    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TGetNodeInfoAndPrepareUnlinkActor::HandleGetNodeAttrResponse(
    const TEvService::TEvGetNodeAttrResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Got GetNodeAttrResponse %s, %lu",
        LogTag.c_str(),
        msg->Record.ShortUtf8DebugString().Quote().c_str(),
        ev->Cookie);

    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (ev->Cookie == SourceCookie) {
        Result.SourceNodeAttr = *msg->Record.MutableNode();
    } else {
        Y_DEBUG_ABORT_UNLESS(ev->Cookie == DstCookie);
        Result.DestinationNodeAttr = *msg->Record.MutableNode();
    }

    if (++ResultCount < 2) {
        return;
    }

    if (Result.DestinationNodeAttr.GetType() == NProto::E_DIRECTORY_NODE) {
        LogAbort(ctx);
        return;
    }

    ReplyAndDie(ctx, MakeError(S_OK));
}

void TGetNodeInfoAndPrepareUnlinkActor::HandleLogAbortResponse(
    const TEvIndexTabletPrivate::TEvWriteOpLogEntryResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s WriteOpLogEntry failed: %s",
            LogTag.c_str(),
            FormatError(msg->Error).Quote().c_str());
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Got WriteOpLogEntryResponse %lu",
        LogTag.c_str(),
        msg->OpLogEntryId);

    Result.OpLogEntryId = msg->OpLogEntryId;

    PrepareUnlink(ctx);
}

void TGetNodeInfoAndPrepareUnlinkActor::HandlePrepareUnlinkResponse(
    const TEvPrepareUnlinkResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Got PrepareUnlinkResponse %s",
        LogTag.c_str(),
        msg->Record.ShortUtf8DebugString().Quote().c_str());

    ReplyAndDie(ctx, msg->GetError());
}

void TGetNodeInfoAndPrepareUnlinkActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TGetNodeInfoAndPrepareUnlinkActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    using TResponse = TEvIndexTabletPrivate::TEvDoRenameNodeInDestination;
    Result.Error = std::move(error);
    ctx.Send(
        ParentId,
        std::make_unique<TResponse>(std::move(Result)));

    Die(ctx);
}

STFUNC(TGetNodeInfoAndPrepareUnlinkActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvService::TEvGetNodeAttrResponse, HandleGetNodeAttrResponse);

        HFunc(
            TEvIndexTabletPrivate::TEvWriteOpLogEntryResponse,
            HandleLogAbortResponse);

        HFunc(
            TEvIndexTablet::TEvPrepareUnlinkDirectoryNodeInShardResponse,
            HandlePrepareUnlinkResponse);

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

void TIndexTabletActor::HandleRenameNodeInDestination(
    const TEvIndexTablet::TEvRenameNodeInDestinationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TMethod = TEvIndexTablet::TRenameNodeInDestinationMethod;
    auto* session = AcceptRequest<TMethod>(ev, ctx, ValidateRequest);
    if (!session) {
        return;
    }

    auto* msg = ev->Get();

    NProto::TProfileLogRequestInfo profileLogRequest;
    InitTabletProfileLogRequestInfo(
        profileLogRequest,
        EFileStoreSystemRequest::RenameNodeInDestination,
        msg->Record,
        ctx.Now());

    // TODO(#2674): DupCache
    // DupCache is necessary not only to implement the non-idempotent checks
    // but to prevent the following race:
    // 1. source directory shard sends RenameNodeInDestinationRequest
    // 2. the request is successfully processed
    // 3. network connectivity breaks, source directory shard gets E_REJECTED
    // 4. meanwhile <NewParentNodeId, NewChildName> pair gets unlinked in the
    //  destination shard and then a completely new file is created under the
    //  same name in NewParentId
    // 5. source directory shard retries the RenameNodeInDestinationRequest
    //  leading to the deletion of the file created in p.4 and the
    //  <NewParentNodeId, NewChildName> now points to the file unlinked in p.4
    //  So both the new and the old file are now deleted plus we have a NodeRef
    //  pointing to a deleted file
    //
    //  A more reliable way to prevent such a race is to lock the affected
    //  NodeRef in destination shard and unlock it after
    //  CommitRenameNodeInSource
    /*
    const auto requestId = GetRequestId(msg->Record);
    if (const auto* e = session->LookupDupEntry(requestId)) {
        auto response = std::make_unique<TMethod::TResponse>();
        if (GetDupCacheEntry(e, response->Record)) {
            return NCloud::Reply(ctx, *ev, std::move(response));
        }

        session->DropDupEntry(requestId);
    }
    */

    const bool locked = TryLockNodeRef({
        msg->Record.GetNewParentId(),
        msg->Record.GetNewName()});
    if (!locked) {
        auto error = MakeError(E_REJECTED, TStringBuilder() << "node ref "
            << msg->Record.GetNewParentId()
            << " " << msg->Record.GetNewName()
            << " is locked for RenameNodeInDestination");
        auto response = std::make_unique<TMethod::TResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        FinalizeProfileLogRequestInfo(
            std::move(profileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            error,
            ProfileLog);
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TMethod>(*requestInfo);

    ExecuteTx<TRenameNodeInDestination>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record),
        std::move(profileLogRequest));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDoRenameNodeInDestination(
    const TEvIndexTabletPrivate::TEvDoRenameNodeInDestination::TPtr& ev,
    const TActorContext& ctx)
{
    WorkerActors.erase(ev->Sender);

    auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        UnlockNodeRef({
            msg->Request.GetNewParentId(),
            msg->Request.GetNewName()});

        using TMethod = TEvIndexTablet::TRenameNodeInDestinationMethod;
        auto response =
            std::make_unique<TMethod::TResponse>(std::move(msg->Error));
        NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
        return;
    }

    ExecuteTx<TRenameNodeInDestination>(
        ctx,
        std::move(msg->RequestInfo),
        std::move(msg->Request),
        std::move(msg->ProfileLogRequest),
        std::move(msg->SourceNodeAttr),
        std::move(msg->DestinationNodeAttr),
        msg->OpLogEntryId);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_RenameNodeInDestination(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TRenameNodeInDestination& args)
{
    Y_UNUSED(ctx);

    // TODO(#2674): DupCache
    // FILESTORE_VALIDATE_DUPTX_SESSION(RenameNode, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    // validate new parent node exists
    if (!ReadNode(
            db,
            args.NewParentNodeId,
            args.CommitId,
            args.NewParentNode))
    {
        return false;   // not ready
    }

    if (!args.NewParentNode) {
        args.Error = ErrorInvalidTarget(args.NewParentNodeId, args.NewName);
        return true;
    }

    if (args.NewParentNode->Attrs.GetIsPreparedForUnlink()) {
        args.Error = ErrorIsPreparedForUnlink(args.NewParentNode->NodeId);
        return true;
    }

    // TODO: AccessCheck

    // check if new ref exists
    if (!ReadNodeRef(
            db,
            args.NewParentNodeId,
            args.CommitId,
            args.NewName,
            args.NewChildRef))
    {
        return false;   // not ready
    }

    if (args.NewChildRef) {
        if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_NOREPLACE)) {
            if (args.NewChildRef->ShardId == args.Request.GetSourceNodeShardId()
                    && args.NewChildRef->ShardNodeName
                    == args.Request.GetSourceNodeShardNodeName())
            {
                // just an extra precaution for the case of DupCache-related
                // bugs
                args.Error = MakeError(S_ALREADY);
            } else {
                args.Error = ErrorAlreadyExists(args.NewName);
            }
            return true;
        }

        if (!args.NewChildRef->IsExternal()) {
            auto message = ReportRenameNodeRequestForLocalNode(TStringBuilder()
                << "RenameNodeInDestination: "
                << args.Request.ShortDebugString());
            args.Error = MakeError(E_ARGUMENT, std::move(message));
            return true;
        }

        // oldpath and newpath are existing hard links to the same file, then
        // rename() does nothing
        const bool isSameExternalNode =
            args.Request.GetSourceNodeShardId() == args.NewChildRef->ShardId
            && args.Request.GetSourceNodeShardNodeName()
                == args.NewChildRef->ShardNodeName;
        if (isSameExternalNode) {
            args.Error = MakeError(S_ALREADY, "is the same file");
            return true;
        }

        // EXCHANGE allows to rename any nodes
        if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_EXCHANGE)) {
            return true;
        }

        if (args.IsSecondPass) {
            // oldpath directory: newpath must either not exist, or it must
            // specify an empty directory.
            if (args.DestinationNodeAttr.GetType() == NProto::E_DIRECTORY_NODE) {
                if (args.SourceNodeAttr.GetType()
                        != NProto::E_DIRECTORY_NODE)
                {
                    args.Error =
                        ErrorIsDirectory(args.SourceNodeAttr.GetId());
                    return true;
                }

                // we don't need a separate emptiness check here because
                // emptiness is checked upon PrepareUnlinkDirectoryNodeInShard
                // which happens after the first pass
            }
        } else {
            args.SecondPassRequired = true;
        }
    } else if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_EXCHANGE)) {
        args.Error = ErrorInvalidTarget(args.NewParentNodeId, args.NewName);
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_RenameNodeInDestination(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TRenameNodeInDestination& args)
{
    FILESTORE_VALIDATE_TX_ERROR(RenameNodeInDestination, args);
    if (args.Error.GetCode() == S_ALREADY) {
        return;
    }

    if (args.SecondPassRequired) {
        return;
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    if (args.NewChildRef) {
        if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_EXCHANGE)) {
            // remove existing target ref
            RemoveNodeRef(
                db,
                args.NewParentNodeId,
                args.NewChildRef->MinCommitId,
                args.CommitId,
                args.NewName,
                args.NewChildRef->NodeId,
                args.NewChildRef->ShardId,
                args.NewChildRef->ShardNodeName);

            args.Response.SetOldTargetNodeShardId(args.NewChildRef->ShardId);
            args.Response.SetOldTargetNodeShardNodeName(
                args.NewChildRef->ShardNodeName);
        } else if (!args.NewChildRef->IsExternal()) {
            auto message = ReportUnexpectedLocalNode(TStringBuilder()
                << "RenameNodeInDestination: "
                << args.Request.ShortDebugString());
            // here the code is E_INVALID_STATE since we should've checked this
            // thing before - in HandleRenameNodeInDestination
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
            return;
        } else {
            if (args.AbortUnlinkOpLogEntryId) {
                db.DeleteOpLogEntry(args.AbortUnlinkOpLogEntryId);
            }

            // remove target ref
            UnlinkExternalNode(
                db,
                args.NewParentNode->NodeId,
                args.NewName,
                args.NewChildRef->ShardId,
                args.NewChildRef->ShardNodeName,
                args.NewChildRef->MinCommitId,
                args.CommitId);

            // OpLogEntryId doesn't have to be a CommitId - it's just convenient
            // to use CommitId here in order not to generate some other unique
            // ui64
            args.OpLogEntry.SetEntryId(args.CommitId);
            auto* shardRequest =
                args.OpLogEntry.MutableUnlinkNodeInShardRequest();
            shardRequest->MutableHeaders()->CopyFrom(
                args.Request.GetHeaders());
            shardRequest->SetFileSystemId(args.NewChildRef->ShardId);
            shardRequest->SetNodeId(RootNodeId);
            shardRequest->SetName(args.NewChildRef->ShardNodeName);
            shardRequest->SetUnlinkDirectory(
                args.DestinationNodeAttr.GetType() == NProto::E_DIRECTORY_NODE);
            const bool serialized = args.ProfileLogRequest.SerializeToString(
                args.OpLogEntry.MutableProfileLogRequest());
            if (!serialized) {
                ReportBrokenProfileLogRequest(TStringBuilder()
                    << "Written OpLogEntry: "
                    << args.OpLogEntry.ShortUtf8DebugString().Quote());
            }

            db.WriteOpLogEntry(args.OpLogEntry);
        }
    }

    // create target ref to source node
    CreateNodeRef(
        db,
        args.NewParentNodeId,
        args.CommitId,
        args.NewName,
        InvalidNodeId, // ChildNodeId
        args.Request.GetSourceNodeShardId(),
        args.Request.GetSourceNodeShardNodeName());

    auto newParent = CopyAttrs(args.NewParentNode->Attrs, E_CM_CMTIME);
    UpdateNode(
        db,
        args.NewParentNode->NodeId,
        args.NewParentNode->MinCommitId,
        args.CommitId,
        newParent,
        args.NewParentNode->Attrs);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        auto message = ReportSessionNotFoundInTx(TStringBuilder()
            << "RenameNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return;
    }

    // TODO(#2674): DupCache
    /*
    AddDupCacheEntry(
        db,
        session,
        args.RequestId,
        NProto::TRenameNodeResponse{},
        Config->GetDupCacheEntryCount());
        */

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_RenameNodeInDestination(
    const TActorContext& ctx,
    TTxIndexTablet::TRenameNodeInDestination& args)
{
    if (args.SecondPassRequired) {
        if (args.NewChildRef) {
            using TActor = TGetNodeInfoAndPrepareUnlinkActor;
            auto actor = std::make_unique<TActor>(
                LogTag,
                args.RequestInfo,
                ctx.SelfID,
                args.Request,
                std::move(args.ProfileLogRequest),
                args.NewChildRef->ShardId,
                args.NewChildRef->ShardNodeName);

            auto actorId = NCloud::Register(ctx, std::move(actor));
            WorkerActors.insert(actorId);
            return;
        }

        auto message = ReportChildRefIsNull(TStringBuilder()
            << "RenameNodeInDestination: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
    }

    if (HasError(args.Error)
            && args.IsSecondPass
            && args.DestinationNodeAttr.GetType() == NProto::E_DIRECTORY_NODE)
    {
        if (args.AbortUnlinkOpLogEntryId) {
            RegisterAbortUnlinkDirectoryInShardActor(
                ctx,
                args.RequestInfo,
                args.Request,
                std::move(args.ProfileLogRequest),
                args.Error,
                args.NewChildRef->ShardId,
                args.DestinationNodeAttr.GetId(),
                args.AbortUnlinkOpLogEntryId);
            return;
        }

        auto message = ReportMissingOpLogEntryId(TStringBuilder()
            << "RenameNodeInDestination: "
            << args.Request.ShortDebugString()
            << ", original error: " << FormatError(args.Error));
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
    }

    UnlockNodeRef({args.Request.GetNewParentId(), args.Request.GetNewName()});

    InvalidateNodeCaches(args.NewParentNodeId);
    if (args.NewChildRef) {
        InvalidateNodeCaches(args.NewChildRef->ChildNodeId);
    }
    if (args.NewParentNode) {
        InvalidateNodeCaches(args.NewParentNode->NodeId);
    }

    if (!HasError(args.Error)) {
        auto& op = args.OpLogEntry;
        if (op.HasUnlinkNodeInShardRequest()) {
            // rename + unlink is pretty rare so let's keep INFO level here
            LOG_INFO(
                ctx,
                TFileStoreComponents::TABLET,
                "%s Unlinking node in shard upon RenameNode: %s, %s",
                LogTag.c_str(),
                op.GetUnlinkNodeInShardRequest().GetFileSystemId().c_str(),
                op.GetUnlinkNodeInShardRequest().GetName().c_str());

            RegisterUnlinkNodeInShardActor(
                ctx,
                args.RequestInfo,
                std::move(*op.MutableUnlinkNodeInShardRequest()),
                std::move(args.ProfileLogRequest),
                args.RequestId,
                args.OpLogEntry.GetEntryId(),
                std::move(args.Response),
                false);

            return;
        }

        CommitDupCacheEntry(args.SessionId, args.RequestId);

        // TODO(#1350): support session events for external nodes
    }

    RemoveInFlightRequest(*args.RequestInfo);

    Metrics.RenameNodeInDestination.Update(
        1,
        0,
        ctx.Now() - args.RequestInfo->StartedTs);

    using TMethod = TEvIndexTablet::TRenameNodeInDestinationMethod;
    auto response = std::make_unique<TMethod::TResponse>(args.Error);
    CompleteResponse<TMethod>(
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

void TIndexTabletActor::HandleUnlinkDirectoryNodeAbortedInShard(
    const TEvIndexTabletPrivate::TEvUnlinkDirectoryNodeAbortedInShard::TPtr& ev,
    const TActorContext& ctx)
{
    WorkerActors.erase(ev->Sender);

    auto* msg = ev->Get();

    UnlockNodeRef({msg->Request.GetNewParentId(), msg->Request.GetNewName()});

    Y_DEFER {
        ExecuteTx<TDeleteOpLogEntry>(
            ctx,
            nullptr /* requestInfo */,
            msg->OpLogEntryId);

        FinalizeProfileLogRequestInfo(
            std::move(msg->ProfileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            msg->Error,
            ProfileLog);
    };

    if (!msg->RequestInfo) {
        return;
    }

    RemoveInFlightRequest(*msg->RequestInfo);

    Metrics.RenameNodeInDestination.Update(
        1,
        0,
        ctx.Now() - msg->RequestInfo->StartedTs);

    if (!HasError(msg->Error)) {
        msg->Error = std::move(msg->OriginalError);
    }

    using TMethod = TEvIndexTablet::TRenameNodeInDestinationMethod;
    auto response = std::make_unique<TMethod::TResponse>(msg->Error);
    CompleteResponse<TMethod>(
        response->Record,
        msg->RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
