#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/storage/tablet/model/xattr_acl_helpers.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TSetNodeXAttrRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    if (auto error = ValidateXAttrName(request.GetName()); HasError(error)) {
        return error;
    }

    auto error = ValidateXAttrValue(request.GetName(), request.GetValue());
    if (HasError(error)) {
        return error;
    }

    return {};
}

// Classify an enabled POSIX ACL update and validate its value.
NProto::TError ValidatePosixAcl(const NProto::TSetNodeXAttrRequest& request,
    TTxIndexTablet::TXattrAcl& acl)
{
    if (!request.GetHeaders().GetGuestPosixAclEnabled()) {
        return {};
    }

    const bool isAccessAcl = (request.GetName() == PosixAclAccessXAttr);
    const bool isDefaultAcl = (request.GetName() == PosixAclDefaultXAttr);

    if (isAccessAcl || isDefaultAcl) {
        auto result = GetModeByXattrAcl(request.GetValue());
        if (HasError(result.GetError())) {
            return result.GetError();
        }

        const auto& xattrAcl = result.ExtractResult();
        acl.Type = xattrAcl.Type;
        acl.Mode = xattrAcl.Mode;

        if (isAccessAcl) {
            acl.Scope = TTxIndexTablet::TXattrAcl::EScope::Access;
        }
        if (isDefaultAcl) {
            acl.Scope = TTxIndexTablet::TXattrAcl::EScope::Default;
        }
    }

    return {};
}

}   // namespace
////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleSetNodeXAttr(
    const TEvService::TEvSetNodeXAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto pipeServerId = RegisterFileSystemEventClient(
        ev->Recipient,
        ev->Sender,
        msg->Record.GetHeaders());

    const TSession* session = AcceptRequest<TEvService::TSetNodeXAttrMethod>(
        ev,
        ctx,
        ValidateRequest);
    if (!session) {
        return;
    }

    TTxIndexTablet::TXattrAcl xattrAcl;
    NProto::TError aclError = ValidatePosixAcl(msg->Record, xattrAcl);
    if (HasError(aclError)) {
        auto response =
            std::make_unique<TEvService::TSetNodeXAttrMethod::TResponse>(
                std::move(aclError));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();
    requestInfo->PipeServerId = pipeServerId;
    requestInfo->FileSystemEventSourceId =
        msg->Record.GetHeaders().GetFileSystemEventSourceId();

    AddInFlightRequest<TEvService::TSetNodeXAttrMethod>(*requestInfo);

    ExecuteTx<TSetNodeXAttr>(
        ctx,
        std::move(requestInfo),
        msg->Record,
        std::move(xattrAcl));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_SetNodeXAttr(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetNodeXAttr& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_SESSION(SetNodeXAttr, args);

    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    // validate target node exists
    if (!ReadNode(*db, args.NodeId, args.CommitId, args.Node)) {
        return false;   // not ready
    }

    if (!args.Node) {
        args.Error = ErrorInvalidTarget(args.NodeId);
        return true;
    }

    if (args.Acl.Scope == TTxIndexTablet::TXattrAcl::EScope::Default &&
        !args.Acl.IsToDropAcl() &&
        args.Node->Attrs.GetType() != NProto::E_DIRECTORY_NODE)
    {
        args.Error = MakeError(
            E_FS_ACCESS,
            "Default POSIX ACLs can only be set on directories");
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.Node);
    // fetch previous version
    if (!ReadNodeAttr(*db, args.NodeId, args.CommitId, args.Name, args.Attr)) {
        return false;   // not ready
    }

    const auto flags = args.Request.GetFlags();
    if (args.Attr && HasProtoFlag(flags, NProto::TSetNodeXAttrRequest::F_CREATE)) {
        args.Error = ErrorAttributeAlreadyExists(args.Name);
        return true;
    }
    if (!args.Attr && HasProtoFlag(flags, NProto::TSetNodeXAttrRequest::F_REPLACE)) {
        args.Error = ErrorAttributeDoesNotExist(args.Name);
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_SetNodeXAttr(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetNodeXAttr& args)
{
    FILESTORE_VALIDATE_TX_ERROR(SetNodeXAttr, args);

    Y_UNUSED(ctx);

    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    // A header-only ACL clears the xattr. A minimal access ACL is represented
    // by the mode bits, so it also clears any existing access ACL xattr.
    const bool dropAcl = args.Acl.IsToDropAcl();
    if (dropAcl && args.Attr) {
        RemoveNodeAttr(
            *db,
            args.FileSystemEvents,
            args.NodeId,
            args.Attr->MinCommitId,
            args.CommitId,
            *args.Attr);
    }

    // Preserve the special mode bits and check whether the stored ACL changes
    // independently of the permissions it implies.
    const ui32 oldMode = args.Node->Attrs.GetMode();
    const ui32 newMode = args.Acl.IsToSetMode()
        ? (oldMode & ~(S_IRWXU | S_IRWXG | S_IRWXO)) | args.Acl.Mode
        : oldMode;
    bool aclChanged = false;
    if (args.Acl.Scope != TTxIndexTablet::TXattrAcl::EScope::Absent) {
        if (dropAcl) {
            aclChanged = args.Attr.Defined();
        } else {
            aclChanged =
                !args.Attr || args.Attr->Value != args.Request.GetValue();
        }
    }

    // Update ctime in the same transaction as the ACL, even when the ACL
    // changes without changing the mode.
    if (aclChanged || newMode != oldMode) {
        NProto::TNode attrs = CopyAttrs(args.Node->Attrs, E_CM_CTIME);
        attrs.SetMode(newMode);
        UpdateNode(
            *db,
            args.FileSystemEvents,
            args.NodeId,
            args.Node->MinCommitId,
            args.CommitId,
            attrs,
            args.Node->Attrs);
    }

    if (dropAcl) {
        return;
    }

    if (args.Attr) {
        args.Version = UpdateNodeAttr(
            *db,
            args.FileSystemEvents,
            args.NodeId,
            args.Attr->MinCommitId,
            args.CommitId,
            *args.Attr,
            args.Request.GetValue());
    } else {
        args.Version = CreateNodeAttr(
            *db,
            args.FileSystemEvents,
            args.NodeId,
            args.CommitId,
            args.Name,
            args.Request.GetValue());
    }
}

void TIndexTabletActor::CompleteTx_SetNodeXAttr(
    const TActorContext& ctx,
    TTxIndexTablet::TSetNodeXAttr& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvSetNodeXAttrResponse>(
        std::move(args.Error));
    response->Record.SetVersion(args.Version);
    if (!HasError(response->Record.GetError()) && args.Acl.IsToDropAcl()) {
        response->Record.SetXAttrAbsent(true);
    }

    //
    // The client may cache the value it has set or, for F_REPLACE, the
    // absence of the attribute. The events generated by this request have
    // already been sent (see SendTxFileSystemEvents), so the subscription
    // doesn't make the client invalidate its own change.
    //

    const auto& error = response->Record.GetError();
    if (!HasError(error) ||
        STATUS_FROM_CODE(error.GetCode()) == NProto::E_FS_NOXATTR)
    {
        const auto& pipeServerId = args.RequestInfo->PipeServerId;
        AddFileSystemEventNodeXAttrSubscriber(
            ctx,
            pipeServerId,
            args.NodeId,
            args.Name);
        const ui32 cacheTTL =
            GetFileSystemEventsCacheTTL(pipeServerId).MilliSeconds();
        response->Record.SetXAttrCacheTTL(cacheTTL);
        if (cacheTTL) {
            response->Record.SetXAttrCacheSourceId(
                args.RequestInfo->FileSystemEventSourceId);
        }
    }
    CompleteResponse<TEvService::TSetNodeXAttrMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
