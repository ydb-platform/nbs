#include "service_actor.h"

#include "service_actor_control_namespace.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

EControlNamespaceEntry ClassifyControlNamespaceEntry(ui64 nodeId)
{
    // A raw RPC caller can present any ino, not just ones a kernel client
    // would ever hand back to us - anything else carrying our reserved
    // shard number is Unknown, not None, so it doesn't fall through to
    // real shard-number handling.
    if (ExtractShardNo(nodeId) != ControlNamespaceShardNo) {
        return EControlNamespaceEntry::None;
    }
    if (nodeId == ControlDirIno) {
        return EControlNamespaceEntry::ControlDir;
    }
    if (nodeId == ControlFsIdFileIno) {
        return EControlNamespaceEntry::FsId;
    }
    return EControlNamespaceEntry::Unknown;
}

EControlNamespaceEntry ClassifyControlNamespaceEntry(
    ui64 parentId,
    TStringBuf name,
    TStringBuf controlNamespaceDirName)
{
    if (parentId == RootNodeId && name == controlNamespaceDirName) {
        return EControlNamespaceEntry::ControlDir;
    }
    if (parentId == ControlDirIno) {
        return name == ControlFsIdFileName ? EControlNamespaceEntry::FsId
                                           : EControlNamespaceEntry::Unknown;
    }
    if (parentId == ControlFsIdFileIno) {
        // fsid is a file - it has no children, but be defensive.
        return EControlNamespaceEntry::Unknown;
    }
    return EControlNamespaceEntry::None;
}

void FillControlDirAttr(NProto::TNodeAttr& attr)
{
    attr.SetId(ControlDirIno);
    attr.SetType(NProto::E_DIRECTORY_NODE);
    attr.SetMode(0555);
    attr.SetLinks(2);
    attr.SetSize(0);
}

void FillControlFsIdAttr(NProto::TNodeAttr& attr, const TString& fileSystemId)
{
    attr.SetId(ControlFsIdFileIno);
    attr.SetType(NProto::E_REGULAR_NODE);
    attr.SetMode(0444);
    attr.SetLinks(1);
    attr.SetSize(fileSystemId.size());
}

bool IsControlNamespaceEntry(EControlNamespaceEntry entry)
{
    return entry != EControlNamespaceEntry::None;
}

NProto::TError ControlNamespaceNotPermittedError()
{
    return MakeError(E_FS_PERM, "not permitted on the control namespace");
}

////////////////////////////////////////////////////////////////////////////////

bool TStorageServiceActor::TryHandleControlNamespaceGetNodeAttr(
    const TActorContext& ctx,
    const TEvService::TEvGetNodeAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    auto* msg = ev->Get();
    const auto& controlNamespaceDirName =
        StorageConfig->GetControlNamespaceDirName();

    if (controlNamespaceDirName.empty()) {
        return false;
    }

    const auto entry =
        msg->Record.GetName().empty()
            ? ClassifyControlNamespaceEntry(msg->Record.GetNodeId())
            : ClassifyControlNamespaceEntry(
                  msg->Record.GetNodeId(),
                  msg->Record.GetName(),
                  controlNamespaceDirName);

    if (Y_LIKELY(!IsControlNamespaceEntry(entry))) {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvGetNodeAttrResponse>();
    switch (entry) {
        case EControlNamespaceEntry::ControlDir:
            FillControlDirAttr(*response->Record.MutableNode());
            break;
        case EControlNamespaceEntry::FsId:
            FillControlFsIdAttr(
                *response->Record.MutableNode(),
                session->FileStore.GetFileSystemId());
            break;
        case EControlNamespaceEntry::Unknown:
            *response->Record.MutableError() =
                MakeError(E_FS_NOENT, "not found");
            break;
        case EControlNamespaceEntry::None:
            Y_UNREACHABLE();
    }
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceCreateHandle(
    const TActorContext& ctx,
    const TEvService::TEvCreateHandleRequest::TPtr& ev,
    const TSessionInfo* session)
{
    auto* msg = ev->Get();
    const auto& controlNamespaceDirName =
        StorageConfig->GetControlNamespaceDirName();

    if (controlNamespaceDirName.empty()) {
        return false;
    }

    const auto entry =
        msg->Record.GetName().empty()
            ? ClassifyControlNamespaceEntry(msg->Record.GetNodeId())
            : ClassifyControlNamespaceEntry(
                  msg->Record.GetNodeId(),
                  msg->Record.GetName(),
                  controlNamespaceDirName);

    if (Y_LIKELY(!IsControlNamespaceEntry(entry))) {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvCreateHandleResponse>();

    const bool wantsWrite = HasFlag(
        msg->Record.GetFlags(),
        ProtoFlag(NProto::TCreateHandleRequest::E_WRITE));

    switch (entry) {
        case EControlNamespaceEntry::Unknown:
            *response->Record.MutableError() =
                MakeError(E_FS_NOENT, "not found");
            break;
        case EControlNamespaceEntry::ControlDir:
            *response->Record.MutableError() = ErrorIsDirectory(ControlDirIno);
            break;
        case EControlNamespaceEntry::FsId:
            if (wantsWrite) {
                *response->Record.MutableError() =
                    ControlNamespaceNotPermittedError();
            } else {
                response->Record.SetHandle(ControlFsIdFileIno);
                FillControlFsIdAttr(
                    *response->Record.MutableNodeAttr(),
                    session->FileStore.GetFileSystemId());
            }
            break;
        case EControlNamespaceEntry::None:
            Y_UNREACHABLE();
    }

    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceCreateNode(
    const TActorContext& ctx,
    const TEvService::TEvCreateNodeRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(session);
    auto* msg = ev->Get();
    const auto& controlNamespaceDirName =
        StorageConfig->GetControlNamespaceDirName();

    if (controlNamespaceDirName.empty()) {
        return false;
    }

    const auto entry = ClassifyControlNamespaceEntry(
        msg->Record.GetNodeId(),
        msg->Record.GetName(),
        controlNamespaceDirName);

    // A hard link's target can also be a reserved ino - reject that too
    const auto linkTargetEntry =
        msg->Record.HasLink() ? ClassifyControlNamespaceEntry(
                                    msg->Record.GetLink().GetTargetNode())
                              : EControlNamespaceEntry::None;

    if (Y_LIKELY(
            !IsControlNamespaceEntry(entry) &&
            !IsControlNamespaceEntry(linkTargetEntry)))
    {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvCreateNodeResponse>(
        ControlNamespaceNotPermittedError());
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

// Handles for this namespace are just the target inodes
bool TStorageServiceActor::TryHandleControlNamespaceReadData(
    const TActorContext& ctx,
    const TEvService::TEvReadDataRequest::TPtr& ev,
    const TSessionInfo* session)
{
    auto* msg = ev->Get();

    if (StorageConfig->GetControlNamespaceDirName().empty()) {
        return false;
    }

    const auto entry = ClassifyControlNamespaceEntry(msg->Record.GetHandle());
    switch (entry) {
        case EControlNamespaceEntry::None:
            return false;
        case EControlNamespaceEntry::ControlDir: {
            auto response = std::make_unique<TEvService::TEvReadDataResponse>(
                ErrorIsDirectory(ControlDirIno));
            NCloud::Reply(ctx, *ev, std::move(response));
            return true;
        }
        case EControlNamespaceEntry::FsId:
            break;
        case EControlNamespaceEntry::Unknown: {
            auto response = std::make_unique<TEvService::TEvReadDataResponse>(
                MakeError(E_FS_NOENT, "not found"));
            NCloud::Reply(ctx, *ev, std::move(response));
            return true;
        }
    }

    auto response = std::make_unique<TEvService::TEvReadDataResponse>();
    const TString& content = session->FileStore.GetFileSystemId();
    const ui64 offset = msg->Record.GetOffset();
    const ui64 length = msg->Record.GetLength();
    if (offset < content.size()) {
        response->Record.SetBuffer(
            content.substr(offset, Min<ui64>(content.size() - offset, length)));
    }
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceWriteData(
    const TActorContext& ctx,
    const TEvService::TEvWriteDataRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(session);
    auto* msg = ev->Get();

    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(!IsControlNamespaceEntry(
            ClassifyControlNamespaceEntry(msg->Record.GetHandle()))))
    {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvWriteDataResponse>(
        ControlNamespaceNotPermittedError());
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceListNodes(
    const TActorContext& ctx,
    const TEvService::TEvListNodesRequest::TPtr& ev,
    const TSessionInfo* session)
{
    auto* msg = ev->Get();

    if (StorageConfig->GetControlNamespaceDirName().empty()) {
        return false;
    }

    const auto entry = ClassifyControlNamespaceEntry(msg->Record.GetNodeId());
    switch (entry) {
        case EControlNamespaceEntry::None:
            return false;
        case EControlNamespaceEntry::FsId: {
            auto response = std::make_unique<TEvService::TEvListNodesResponse>(
                ErrorIsNotDirectory(ControlFsIdFileIno));
            NCloud::Reply(ctx, *ev, std::move(response));
            return true;
        }
        case EControlNamespaceEntry::ControlDir:
            break;
        case EControlNamespaceEntry::Unknown: {
            auto response = std::make_unique<TEvService::TEvListNodesResponse>(
                MakeError(E_FS_NOENT, "not found"));
            NCloud::Reply(ctx, *ev, std::move(response));
            return true;
        }
    }

    auto response = std::make_unique<TEvService::TEvListNodesResponse>();
    if (msg->Record.GetCookie().empty()) {
        response->Record.AddNames(TString(ControlFsIdFileName));
        FillControlFsIdAttr(
            *response->Record.AddNodes(),
            session->FileStore.GetFileSystemId());
    }
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceRenameNode(
    const TActorContext& ctx,
    const TEvService::TEvRenameNodeRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(session);
    auto* msg = ev->Get();
    const auto& controlNamespaceDirName =
        StorageConfig->GetControlNamespaceDirName();

    if (controlNamespaceDirName.empty()) {
        return false;
    }

    const auto srcEntry = ClassifyControlNamespaceEntry(
        msg->Record.GetNodeId(),
        msg->Record.GetName(),
        controlNamespaceDirName);
    const auto dstEntry = ClassifyControlNamespaceEntry(
        msg->Record.GetNewParentId(),
        msg->Record.GetNewName(),
        controlNamespaceDirName);

    if (Y_LIKELY(
            !IsControlNamespaceEntry(srcEntry) &&
            !IsControlNamespaceEntry(dstEntry)))
    {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(
        ControlNamespaceNotPermittedError());
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceUnlinkNode(
    const TActorContext& ctx,
    const TEvService::TEvUnlinkNodeRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(session);
    auto* msg = ev->Get();
    const auto& controlNamespaceDirName =
        StorageConfig->GetControlNamespaceDirName();

    if (controlNamespaceDirName.empty()) {
        return false;
    }

    const auto entry = ClassifyControlNamespaceEntry(
        msg->Record.GetNodeId(),
        msg->Record.GetName(),
        controlNamespaceDirName);

    if (Y_LIKELY(!IsControlNamespaceEntry(entry))) {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvUnlinkNodeResponse>(
        ControlNamespaceNotPermittedError());
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceDestroyHandle(
    const TActorContext& ctx,
    const TEvService::TEvDestroyHandleRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(session);
    auto* msg = ev->Get();

    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(!IsControlNamespaceEntry(
            ClassifyControlNamespaceEntry(msg->Record.GetHandle()))))
    {
        return false;
    }

    // nothing real to destroy - the handle was synthesized locally
    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvService::TEvDestroyHandleResponse>());
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceGetNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvGetNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(session);
    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(!IsControlNamespaceEntry(
            ClassifyControlNamespaceEntry(ev->Get()->Record.GetNodeId()))))
    {
        return false;
    }

    auto response =
        std::make_unique<TEvService::TGetNodeXAttrMethod::TResponse>(
            ErrorAttributeDoesNotExist(ev->Get()->Record.GetName()));
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceListNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvListNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(session);
    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(!IsControlNamespaceEntry(
            ClassifyControlNamespaceEntry(ev->Get()->Record.GetNodeId()))))
    {
        return false;
    }

    auto response =
        std::make_unique<TEvService::TListNodeXAttrMethod::TResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceSetNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvSetNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(session);
    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(!IsControlNamespaceEntry(
            ClassifyControlNamespaceEntry(ev->Get()->Record.GetNodeId()))))
    {
        return false;
    }

    auto response =
        std::make_unique<TEvService::TSetNodeXAttrMethod::TResponse>(
            ControlNamespaceNotPermittedError());
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

}   // namespace NCloud::NFileStore::NStorage
