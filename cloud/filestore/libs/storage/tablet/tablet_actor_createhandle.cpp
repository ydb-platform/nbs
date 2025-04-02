#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <util/generic/guid.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TCreateHandleRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    if (!request.GetName().empty()) {
        if (auto error = ValidateNodeName(request.GetName()); HasError(error)) {
            return error;
        }
    }

    if (HasFlag(request.GetFlags(), NProto::TCreateHandleRequest::E_TRUNCATE) &&
        !HasFlag(request.GetFlags(), NProto::TCreateHandleRequest::E_WRITE))
    {
        // POSIX: The result of using O_TRUNC without either O_RDWR o O_WRONLY
        // is undefined
        return ErrorInvalidArgument();
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCreateHandle(
    const TEvService::TEvCreateHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvService::TEvCreateHandleResponse;

    auto* session =
        AcceptRequest<TEvService::TCreateHandleMethod>(ev, ctx, ValidateRequest);
    if (!session) {
        return;
    }

    auto* msg = ev->Get();
    const auto requestId = GetRequestId(msg->Record);
    if (const auto* e = session->LookupDupEntry(requestId)) {
        const bool shouldStoreHandles = BehaveAsShard(msg->Record.GetHeaders())
            || GetFileSystem().GetShardFileSystemIds().empty();
        auto response = std::make_unique<TResponse>();

        // sometimes we may receive duplicate request ids - either due to
        // client-side bugs or due to our own bugs (e.g. not doing ResetSession
        // upon unmount) or ungraceful client reboots (without actual unmounts)
        // see #2033
        if (!GetDupCacheEntry(e, response->Record)) {
            // invalid entry type - it's certainly a request id collision
            session->DropDupEntry(requestId);
        } else if (msg->Record.GetName().empty() && msg->Record.GetNodeId()
                != response->Record.GetNodeAttr().GetId())
        {
            // this handle relates to a different node id => it's certainly a
            // request id collision as well
            ReportDuplicateRequestId(TStringBuilder() << "CreateHandle response"
                << " for different NodeId found for RequestId=" << requestId
                << ": " << response->Record.GetNodeAttr().GetId()
                << " != " << msg->Record.GetNodeId());
            session->DropDupEntry(requestId);
        } else if (shouldStoreHandles
                && !HasError(response->Record.GetError())
                && !FindHandle(response->Record.GetHandle()))
        {
            // there is no open handle associated with this CreateHandleResponse
            // even though it may be an actual retry attempt of some old request
            // it's still safer to disregard this DupCache entry and try to
            // rerun this request
            ReportDuplicateRequestId(TStringBuilder() << "CreateHandle response"
                << " with stale handle found for RequestId=" << requestId
                << ": ResponseNodeId=" << response->Record.GetNodeAttr().GetId()
                << " NodeId=" << msg->Record.GetNodeId()
                << " HandleId=" << response->Record.GetHandle());
            session->DropDupEntry(requestId);
        } else {
            return NCloud::Reply(ctx, *ev, std::move(response));
        }
    }

    if (msg->Record.GetShardFileSystemId()) {
        bool found = false;
        for (const auto& f: GetFileSystem().GetShardFileSystemIds()) {
            if (f == msg->Record.GetShardFileSystemId()) {
                found = true;
                break;
            }
        }

        if (!found) {
            auto error = MakeError(E_ARGUMENT, TStringBuilder() <<
                "invalid shard id: "
                << msg->Record.GetShardFileSystemId());
            LOG_ERROR(
                ctx,
                TFileStoreComponents::TABLET,
                "%s Can't create handle: %s",
                LogTag.c_str(),
                FormatError(error).Quote().c_str());

            auto response = std::make_unique<TResponse>(std::move(error));
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvService::TCreateHandleMethod>(*requestInfo);

    ExecuteTx<TCreateHandle>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CreateHandle(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateHandle& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_DUPTX_SESSION(CreateHandle, args);

    auto* session = FindSession(
        args.ClientId,
        args.SessionId,
        args.SessionSeqNo);
    if (!session) {
        auto message = ReportSessionNotFoundInTx(TStringBuilder()
            << "CreateHandle: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return true;
    }

    args.ReadCommitId = GetReadCommitId(session->GetCheckpointId());
    if (args.ReadCommitId == InvalidCommitId) {
        args.Error = ErrorInvalidCheckpoint(session->GetCheckpointId());
        return true;
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    // There could be two cases:
    // * access by parentId/name
    // * access by nodeId
    if (args.Name) {
        // check that parent exists and is the directory;
        // TODO: what if symlink?
        if (!ReadNode(db, args.NodeId, args.ReadCommitId, args.ParentNode)) {
            return false;
        }

        if (!args.ParentNode || args.ParentNode->Attrs.GetType() != NProto::E_DIRECTORY_NODE) {
            args.Error = ErrorIsNotDirectory(args.NodeId);
            return true;
        }

        // check whether child node exists
        TMaybe<IIndexTabletDatabase::TNodeRef> ref;
        if (!ReadNodeRef(db, args.NodeId, args.ReadCommitId, args.Name, ref)) {
            return false;   // not ready
        }

        if (!ref) {
            // if not check whether we should create one
            if (!HasFlag(args.Flags, NProto::TCreateHandleRequest::E_CREATE)) {
                args.Error = ErrorInvalidTarget(args.NodeId, args.Name);
                return true;
            }

            // validate there are enough free inodes
            if (GetUsedNodesCount() >= GetNodesCount()) {
                args.Error = ErrorNoSpaceLeft();
                return true;
            }

            auto shardId = args.RequestShardId;
            if (!BehaveAsShard(args.Request.GetHeaders())
                    && !GetFileSystem().GetShardFileSystemIds().empty()
                    && Config->GetShardIdSelectionInLeaderEnabled())
            {
                args.Error = SelectShard(0 /*fileSize*/, &shardId);
                if (HasError(args.Error)) {
                    return true;
                }
            }

            if (shardId) {
                args.ShardId = std::move(shardId);
                args.ShardNodeName = CreateGuidAsString();
                args.IsNewShardNode = true;
            }
        } else {
            // if yes check whether O_EXCL was specified, assume O_CREAT is also
            // specified
            if (HasFlag(args.Flags, NProto::TCreateHandleRequest::E_EXCLUSIVE)) {
                args.Error = ErrorAlreadyExists(args.Name);
                return true;
            }

            args.TargetNodeId = ref->ChildNodeId;
            args.ShardId = ref->ShardId;
            args.ShardNodeName = ref->ShardNodeName;
        }
    } else {
        args.TargetNodeId = args.NodeId;
        if (args.RequestShardId) {
            args.Error = MakeError(
                E_ARGUMENT,
                TStringBuilder() << "CreateHandle request without Name and with"
                " ShardId is not supported");
            return true;
        }
    }

    if (args.TargetNodeId != InvalidNodeId) {
        if (!ReadNode(db, args.TargetNodeId, args.ReadCommitId, args.TargetNode)) {
            return false;   // not ready
        }

        if (!args.TargetNode) {
            args.Error = ErrorInvalidTarget(args.TargetNodeId);
            return true;
        }

        // TODO: support for O_DIRECTORY
        if (args.TargetNode->Attrs.GetType() != NProto::E_REGULAR_NODE) {
            args.Error = ErrorIsDirectory(args.TargetNodeId);
            return true;
        }

        // TODO: AccessCheck
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_CreateHandle(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateHandle& args)
{
    FILESTORE_VALIDATE_TX_ERROR(CreateHandle, args);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        auto message = ReportSessionNotFoundInTx(TStringBuilder()
            << "CreateHandle: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return;
    }

    // TODO: check if session is read only
    args.WriteCommitId = GenerateCommitId();
    if (args.WriteCommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "CreateHandle");
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    if (args.TargetNodeId == InvalidNodeId
            && (args.ShardId.empty() || args.IsNewShardNode))
    {
        if (args.TargetNode) {
            auto message = ReportTargetNodeWithoutRef(TStringBuilder()
                << "CreateHandle: " << args.Request.ShortDebugString());
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
            return;
        }

        if (!args.ParentNode) {
            auto message = ReportParentNodeIsNull(TStringBuilder()
                << "CreateHandle: " << args.Request.ShortDebugString());
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
            return;
        }

        if (args.ShardId.empty()) {
            NProto::TNode attrs =
                CreateRegularAttrs(args.Mode, args.Uid, args.Gid);
            args.TargetNodeId = CreateNode(
                db,
                args.WriteCommitId,
                attrs);

            args.TargetNode = IIndexTabletDatabase::TNode {
                args.TargetNodeId,
                attrs,
                args.WriteCommitId,
                InvalidCommitId
            };
        }

        // TODO: support for O_TMPFILE
        CreateNodeRef(
            db,
            args.NodeId,
            args.WriteCommitId,
            args.Name,
            args.TargetNodeId,
            args.ShardId,
            args.ShardNodeName);

        // update parent cmtime as we created a new entry
        auto parent = CopyAttrs(args.ParentNode->Attrs, E_CM_CMTIME);
        UpdateNode(
            db,
            args.ParentNode->NodeId,
            args.ParentNode->MinCommitId,
            args.WriteCommitId,
            parent,
            args.ParentNode->Attrs);
        args.UpdatedNodes.push_back(args.ParentNode->NodeId);

    } else if (args.ShardId.empty()
        && HasFlag(args.Flags, NProto::TCreateHandleRequest::E_TRUNCATE))
    {
        auto e = Truncate(
            db,
            args.TargetNodeId,
            args.WriteCommitId,
            args.TargetNode->Attrs.GetSize(),
            0);
        if (HasError(e)) {
            args.Error = std::move(e);
            return;
        }

        auto attrs = CopyAttrs(args.TargetNode->Attrs, E_CM_CMTIME);
        attrs.SetSize(0);

        UpdateNode(
            db,
            args.TargetNodeId,
            args.TargetNode->MinCommitId,
            args.WriteCommitId,
            attrs,
            args.TargetNode->Attrs);
        args.UpdatedNodes.push_back(args.TargetNodeId);
    }

    if (args.ShardId.empty()) {
        auto* handle = CreateHandle(
            db,
            session,
            args.TargetNodeId,
            session->GetCheckpointId() ? args.ReadCommitId : InvalidCommitId,
            args.Flags);

        if (!handle) {
            auto message = ReportFailedToCreateHandle(TStringBuilder()
                << "CreateHandle: " << args.Request.ShortDebugString());
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
            return;
        }

        args.Response.SetHandle(handle->GetHandle());
        auto* node = args.Response.MutableNodeAttr();
        ConvertNodeFromAttrs(*node, args.TargetNodeId, args.TargetNode->Attrs);

        if (Config->GetGuestKeepCacheAllowed() &&
            !HasFlag(args.Flags, NProto::TCreateHandleRequest::E_WRITE))
        {
            // We set the GuestKeepCache to tell the client not to bother
            // invalidating the caches upon opening a read-only handle
            args.Response.SetGuestKeepCache(
                !session->HandlesStatsByNode.ShouldInvalidateCache(*node));
        }

        // We can remember the last time that the cache was invalidated by a
        // user of a given session in order not to invalidate the cache the next
        // time if the file was not modified
        if (!args.Response.GetGuestKeepCache()) {
            session->HandlesStatsByNode.OnGuestCacheInvalidated(*node);
        }
    } else {
        args.Response.SetShardFileSystemId(args.ShardId);
        args.Response.SetShardNodeName(args.ShardNodeName);
    }

    if (args.IsNewShardNode) {
        // OpLogEntryId doesn't have to be a CommitId - it's just convenient to
        // use CommitId here in order not to generate some other unique ui64
        args.OpLogEntry.SetEntryId(args.WriteCommitId);
        auto* shardRequest = args.OpLogEntry.MutableCreateNodeRequest();
        *shardRequest->MutableHeaders() = args.Request.GetHeaders();
        shardRequest->MutableFile()->SetMode(args.Mode);
        shardRequest->SetUid(args.Uid);
        shardRequest->SetGid(args.Gid);
        shardRequest->SetFileSystemId(args.ShardId);
        shardRequest->SetNodeId(RootNodeId);
        shardRequest->SetName(args.ShardNodeName);
        shardRequest->ClearShardFileSystemId();

        db.WriteOpLogEntry(args.OpLogEntry);
    }

    AddDupCacheEntry(
        db,
        session,
        args.RequestId,
        args.Response,
        Config->GetDupCacheEntryCount());

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_CreateHandle(
    const TActorContext& ctx,
    TTxIndexTablet::TCreateHandle& args)
{
    for (auto nodeId: args.UpdatedNodes) {
        InvalidateNodeCaches(nodeId);
    }

    if (args.Error.GetCode() == E_ARGUMENT) {
        // service actor sent something inappropriate, we'd better log it
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Can't create handle: %s",
            LogTag.c_str(),
            FormatError(args.Error).Quote().c_str());
    }

    if (args.OpLogEntry.HasCreateNodeRequest() && !HasError(args.Error)) {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s Creating node in shard upon CreateHandle: %s, %s",
            LogTag.c_str(),
            args.ShardId.c_str(),
            args.ShardNodeName.c_str());

        RegisterCreateNodeInShardActor(
            ctx,
            args.RequestInfo,
            std::move(*args.OpLogEntry.MutableCreateNodeRequest()),
            args.RequestId,
            args.OpLogEntry.GetEntryId(),
            std::move(args.Response));

        return;
    }

    RemoveTransaction(*args.RequestInfo);

    auto response =
        std::make_unique<TEvService::TEvCreateHandleResponse>(args.Error);

    if (!HasError(args.Error)) {
        CommitDupCacheEntry(args.SessionId, args.RequestId);
        response->Record = std::move(args.Response);
    }
    Metrics.CreateHandle.Update(1, 0, ctx.Now() - args.RequestInfo->StartedTs);

    CompleteResponse<TEvService::TCreateHandleMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
