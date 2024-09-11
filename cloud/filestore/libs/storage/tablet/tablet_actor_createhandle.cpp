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
    if (const auto* e = session->LookupDupEntry(GetRequestId(msg->Record))) {
        auto response = std::make_unique<TResponse>();
        GetDupCacheEntry(e, response->Record);
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    if (msg->Record.GetFollowerFileSystemId()) {
        bool found = false;
        for (const auto& f: GetFileSystem().GetFollowerFileSystemIds()) {
            if (f == msg->Record.GetFollowerFileSystemId()) {
                found = true;
                break;
            }
        }

        if (!found) {
            auto error = MakeError(E_ARGUMENT, TStringBuilder() <<
                "invalid follower id: "
                << msg->Record.GetFollowerFileSystemId());
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

            if (args.RequestFollowerId) {
                args.FollowerId = args.RequestFollowerId;
                args.FollowerName = CreateGuidAsString();
                args.IsNewFollowerNode = true;
            }
        } else {
            // if yes check whether O_EXCL was specified, assume O_CREAT is also specified
            if (HasFlag(args.Flags, NProto::TCreateHandleRequest::E_EXCLUSIVE)) {
                args.Error = ErrorAlreadyExists(args.Name);
                return true;
            }

            args.TargetNodeId = ref->ChildNodeId;
            args.FollowerId = ref->FollowerId;
            args.FollowerName = ref->FollowerName;
        }
    } else {
        args.TargetNodeId = args.NodeId;
        if (args.RequestFollowerId) {
            args.Error = MakeError(
                E_ARGUMENT,
                TStringBuilder() << "CreateHandle request without Name and with"
                " FollowerId is not supported");
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
            && (args.FollowerId.Empty() || args.IsNewFollowerNode))
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

        if (args.FollowerId.Empty()) {
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
            args.FollowerId,
            args.FollowerName);

        // update parent cmtime as we created a new entry
        auto parent = CopyAttrs(args.ParentNode->Attrs, E_CM_CMTIME);
        UpdateNode(
            db,
            args.ParentNode->NodeId,
            args.ParentNode->MinCommitId,
            args.WriteCommitId,
            parent,
            args.ParentNode->Attrs);

    } else if (args.FollowerId.Empty()
        && HasFlag(args.Flags, NProto::TCreateHandleRequest::E_TRUNCATE))
    {
        Truncate(
            db,
            args.TargetNodeId,
            args.WriteCommitId,
            args.TargetNode->Attrs.GetSize(),
            0);

        auto attrs = CopyAttrs(args.TargetNode->Attrs, E_CM_CMTIME);
        attrs.SetSize(0);

        UpdateNode(
            db,
            args.TargetNodeId,
            args.TargetNode->MinCommitId,
            args.WriteCommitId,
            attrs,
            args.TargetNode->Attrs);
    }

    if (args.FollowerId.Empty()) {
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
    } else {
        args.Response.SetFollowerFileSystemId(args.FollowerId);
        args.Response.SetFollowerNodeName(args.FollowerName);
    }

    if (args.IsNewFollowerNode) {
        // OpLogEntryId doesn't have to be a CommitId - it's just convenient to
        // use CommitId here in order not to generate some other unique ui64
        args.OpLogEntry.SetEntryId(args.WriteCommitId);
        auto* followerRequest = args.OpLogEntry.MutableCreateNodeRequest();
        *followerRequest->MutableHeaders() = args.Request.GetHeaders();
        followerRequest->MutableFile()->SetMode(args.Mode);
        followerRequest->SetUid(args.Uid);
        followerRequest->SetGid(args.Gid);
        followerRequest->SetFileSystemId(args.FollowerId);
        followerRequest->SetNodeId(RootNodeId);
        followerRequest->SetName(args.FollowerName);
        followerRequest->ClearFollowerFileSystemId();

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
    UpdateInMemoryIndexState(std::move(args.NodeUpdates));
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
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s Creating node in follower upon CreateHandle: %s, %s",
            LogTag.c_str(),
            args.FollowerId.c_str(),
            args.FollowerName.c_str());

        RegisterCreateNodeInFollowerActor(
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

    CompleteResponse<TEvService::TCreateHandleMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
