#include "service_actor.h"
#include "service_actor_control_namespace.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

bool IsControlNamespaceNode(ui64 ino)
{
    return ino == ControlDirIno || ino == ControlFsIdFileIno;
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

NProto::TError ControlNamespaceReadOnlyError()
{
    return MakeError(E_FS_ACCESS, "control namespace files are read-only");
}

////////////////////////////////////////////////////////////////////////////////

bool TStorageServiceActor::TryHandleControlNamespaceGetNodeAttr(
    const TActorContext& ctx,
    const TEvService::TEvGetNodeAttrRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    if (!StorageConfig->GetEnableControlNamespace() ||
        msg->Record.GetName().empty())
    {
        return false;
    }

    const ui64 nodeId = msg->Record.GetNodeId();
    const TStringBuf name = msg->Record.GetName();

    if (nodeId == RootNodeId && name == ControlDirName) {
        auto response = std::make_unique<TEvService::TEvGetNodeAttrResponse>();
        FillControlDirAttr(*response->Record.MutableNode());
        NCloud::Reply(ctx, *ev, std::move(response));
        return true;
    }

    if (nodeId == ControlDirIno) {
        auto response = std::make_unique<TEvService::TEvGetNodeAttrResponse>();
        if (name == ControlFsIdFileName) {
            FillControlFsIdAttr(
                *response->Record.MutableNode(),
                msg->Record.GetFileSystemId());
        } else {
            *response->Record.MutableError() =
                MakeError(E_FS_NOENT, "not found");
        }
        NCloud::Reply(ctx, *ev, std::move(response));
        return true;
    }

    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceCreateHandle(
    const TActorContext& ctx,
    const TEvService::TEvCreateHandleRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    if (!StorageConfig->GetEnableControlNamespace() ||
        msg->Record.GetName().empty())
    {
        return false;
    }

    const ui64 nodeId = msg->Record.GetNodeId();
    const TStringBuf name = msg->Record.GetName();

    const bool isDirLookup = nodeId == RootNodeId && name == ControlDirName;
    const bool isFsIdLookup =
        nodeId == ControlDirIno && name == ControlFsIdFileName;

    if (!isDirLookup && nodeId != ControlDirIno) {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvCreateHandleResponse>();

    const bool wantsWrite = HasFlag(
        msg->Record.GetFlags(),
        ProtoFlag(NProto::TCreateHandleRequest::E_WRITE));

    if (!isFsIdLookup && !isDirLookup) {
        *response->Record.MutableError() = MakeError(E_FS_NOENT, "not found");
    } else if (wantsWrite) {
        *response->Record.MutableError() = ControlNamespaceReadOnlyError();
    } else if (isDirLookup) {
        response->Record.SetHandle(ControlDirIno);
        FillControlDirAttr(*response->Record.MutableNodeAttr());
    } else {
        response->Record.SetHandle(ControlFsIdFileIno);
        FillControlFsIdAttr(
            *response->Record.MutableNodeAttr(),
            msg->Record.GetFileSystemId());
    }

    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceCreateNode(
    const TActorContext& ctx,
    const TEvService::TEvCreateNodeRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    if (!StorageConfig->GetEnableControlNamespace() ||
        !(msg->Record.GetNodeId() == ControlDirIno ||
          (msg->Record.GetNodeId() == RootNodeId &&
           msg->Record.GetName() == ControlDirName)))
    {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvCreateNodeResponse>(
        ControlNamespaceReadOnlyError());
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceRenameNode(
    const TActorContext& ctx,
    const TEvService::TEvRenameNodeRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    if (!StorageConfig->GetEnableControlNamespace()) {
        return false;
    }

    auto isControlDirTarget = [] (ui64 parentId, const TString& name) {
        return IsControlNamespaceNode(parentId) ||
               (parentId == RootNodeId && name == ControlDirName);
    };

    if (!isControlDirTarget(msg->Record.GetNodeId(), msg->Record.GetName()) &&
        !isControlDirTarget(
            msg->Record.GetNewParentId(),
            msg->Record.GetNewName()))
    {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(
        ControlNamespaceReadOnlyError());
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceReadData(
    const TActorContext& ctx,
    const TEvService::TEvReadDataRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    if (!StorageConfig->GetEnableControlNamespace() ||
        !(IsControlNamespaceNode(msg->Record.GetHandle()) ||
          IsControlNamespaceNode(msg->Record.GetNodeId())))
    {
        return false;
    }

    // fsid's content is the FileSystemId itself.
    auto response = std::make_unique<TEvService::TEvReadDataResponse>();
    const TString& content = msg->Record.GetFileSystemId();
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
    const TEvService::TEvWriteDataRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    if (!StorageConfig->GetEnableControlNamespace() ||
        !(IsControlNamespaceNode(msg->Record.GetHandle()) ||
          IsControlNamespaceNode(msg->Record.GetNodeId())))
    {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvWriteDataResponse>(
        ControlNamespaceReadOnlyError());
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceListNodes(
    const TActorContext& ctx,
    const TEvService::TEvListNodesRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    if (!StorageConfig->GetEnableControlNamespace() ||
        msg->Record.GetNodeId() != ControlDirIno)
    {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvListNodesResponse>();
    if (msg->Record.GetCookie().empty()) {
        // only "fsid" today, always fits on the first page
        response->Record.AddNames(TString(ControlFsIdFileName));
        FillControlFsIdAttr(
            *response->Record.AddNodes(),
            msg->Record.GetFileSystemId());
    }
    NCloud::Reply(ctx, *ev, std::move(response));
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceGetNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvGetNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    if (!StorageConfig->GetEnableControlNamespace() ||
        !IsControlNamespaceNode(ev->Get()->Record.GetNodeId()))
    {
        return false;
    }

    // no xattrs on control namespace nodes
    auto response = std::make_unique<TEvService::TGetNodeXAttrMethod::TResponse>(
        ErrorAttributeDoesNotExist(ev->Get()->Record.GetName()));
    ReplyToXAttrRequest<TEvService::TGetNodeXAttrMethod>(
        ctx,
        ev,
        std::move(response),
        session);
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceListNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvListNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    if (!StorageConfig->GetEnableControlNamespace() ||
        !IsControlNamespaceNode(ev->Get()->Record.GetNodeId()))
    {
        return false;
    }

    auto response =
        std::make_unique<TEvService::TListNodeXAttrMethod::TResponse>();
    ReplyToXAttrRequest<TEvService::TListNodeXAttrMethod>(
        ctx,
        ev,
        std::move(response),
        session);
    return true;
}

bool TStorageServiceActor::TryHandleControlNamespaceSetNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvSetNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    if (!StorageConfig->GetEnableControlNamespace() ||
        !IsControlNamespaceNode(ev->Get()->Record.GetNodeId()))
    {
        return false;
    }

    auto response = std::make_unique<TEvService::TSetNodeXAttrMethod::TResponse>(
        ControlNamespaceReadOnlyError());
    ReplyToXAttrRequest<TEvService::TSetNodeXAttrMethod>(
        ctx,
        ev,
        std::move(response),
        session);
    return true;
}

}   // namespace NCloud::NFileStore::NStorage
