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
    if (nodeId == ControlDirIno) {
        return EControlNamespaceEntry::ControlDir;
    }
    if (nodeId == ControlFsIdFileIno) {
        return EControlNamespaceEntry::FsId;
    }
    return EControlNamespaceEntry::None;
}

EControlNamespaceEntry ClassifyControlNamespaceEntry(
    ui64 parentId,
    TStringBuf name,
    TStringBuf dirName)
{
    if (parentId == RootNodeId && name == dirName) {
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

NProto::TError ControlNamespaceReadOnlyError()
{
    return MakeError(E_FS_ACCESS, "control namespace files are read-only");
}

////////////////////////////////////////////////////////////////////////////////
// BuildControlNamespaceResponse specializations, for the self-lookup-by-ino
// forms reached via ForwardRequestToShard rather than a dedicated
// TryHandleControlNamespaceXxx above.

template <>
std::unique_ptr<TEvService::TGetNodeAttrMethod::TResponse>
BuildControlNamespaceResponse<TEvService::TGetNodeAttrMethod>(
    const TEvService::TGetNodeAttrMethod::TRequest::TPtr& ev,
    const TString& fileSystemId)
{
    const auto& record = ev->Get()->Record;
    auto response =
        std::make_unique<TEvService::TGetNodeAttrMethod::TResponse>();
    switch (ClassifyControlNamespaceEntry(record.GetNodeId())) {
        case EControlNamespaceEntry::ControlDir:
            FillControlDirAttr(*response->Record.MutableNode());
            break;
        case EControlNamespaceEntry::FsId:
            FillControlFsIdAttr(*response->Record.MutableNode(), fileSystemId);
            break;
        default:
            *response->Record.MutableError() =
                MakeError(E_FS_NOENT, "not found");
            break;
    }
    return response;
}

template <>
std::unique_ptr<TEvService::TCreateHandleMethod::TResponse>
BuildControlNamespaceResponse<TEvService::TCreateHandleMethod>(
    const TEvService::TCreateHandleMethod::TRequest::TPtr& ev,
    const TString& fileSystemId)
{
    const auto& record = ev->Get()->Record;
    auto response =
        std::make_unique<TEvService::TCreateHandleMethod::TResponse>();

    const auto entry = ClassifyControlNamespaceEntry(record.GetNodeId());
    if (entry != EControlNamespaceEntry::ControlDir &&
        entry != EControlNamespaceEntry::FsId)
    {
        *response->Record.MutableError() = MakeError(E_FS_NOENT, "not found");
        return response;
    }

    const bool wantsWrite = HasFlag(
        record.GetFlags(),
        ProtoFlag(NProto::TCreateHandleRequest::E_WRITE));
    if (wantsWrite) {
        *response->Record.MutableError() = ControlNamespaceReadOnlyError();
        return response;
    }

    if (entry == EControlNamespaceEntry::ControlDir) {
        response->Record.SetHandle(ControlDirIno);
        FillControlDirAttr(*response->Record.MutableNodeAttr());
    } else {
        response->Record.SetHandle(ControlFsIdFileIno);
        FillControlFsIdAttr(*response->Record.MutableNodeAttr(), fileSystemId);
    }
    return response;
}

template <>
std::unique_ptr<TEvService::TConfirmCreateHandleMethod::TResponse>
BuildControlNamespaceResponse<TEvService::TConfirmCreateHandleMethod>(
    const TEvService::TConfirmCreateHandleMethod::TRequest::TPtr& ev,
    const TString& fileSystemId)
{
    Y_UNUSED(ev);
    Y_UNUSED(fileSystemId);
    // nothing real to confirm - the handle was synthesized locally
    return std::make_unique<
        TEvService::TConfirmCreateHandleMethod::TResponse>();
}

template <>
std::unique_ptr<TEvService::TDestroyHandleMethod::TResponse>
BuildControlNamespaceResponse<TEvService::TDestroyHandleMethod>(
    const TEvService::TDestroyHandleMethod::TRequest::TPtr& ev,
    const TString& fileSystemId)
{
    Y_UNUSED(ev);
    Y_UNUSED(fileSystemId);
    // nothing real to destroy - the handle was synthesized locally
    return std::make_unique<TEvService::TDestroyHandleMethod::TResponse>();
}

////////////////////////////////////////////////////////////////////////////////

bool TStorageServiceActor::TryHandleControlNamespaceGetNodeAttr(
    const TActorContext& ctx,
    const TEvService::TEvGetNodeAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    auto* msg = ev->Get();
    const auto& dirName = StorageConfig->GetControlNamespaceDirName();

    if (dirName.empty() || msg->Record.GetName().empty()) {
        return false;
    }

    const auto entry = ClassifyControlNamespaceEntry(
        msg->Record.GetNodeId(),
        msg->Record.GetName(),
        dirName);

    if (Y_LIKELY(entry == EControlNamespaceEntry::None)) {
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
        default:
            *response->Record.MutableError() =
                MakeError(E_FS_NOENT, "not found");
            break;
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
    const auto& dirName = StorageConfig->GetControlNamespaceDirName();

    if (dirName.empty() || msg->Record.GetName().empty()) {
        return false;
    }

    const auto entry = ClassifyControlNamespaceEntry(
        msg->Record.GetNodeId(),
        msg->Record.GetName(),
        dirName);

    if (Y_LIKELY(entry == EControlNamespaceEntry::None)) {
        return false;
    }

    auto response = std::make_unique<TEvService::TEvCreateHandleResponse>();

    const bool wantsWrite = HasFlag(
        msg->Record.GetFlags(),
        ProtoFlag(NProto::TCreateHandleRequest::E_WRITE));

    if (entry == EControlNamespaceEntry::Unknown) {
        *response->Record.MutableError() = MakeError(E_FS_NOENT, "not found");
    } else if (wantsWrite) {
        *response->Record.MutableError() = ControlNamespaceReadOnlyError();
    } else if (entry == EControlNamespaceEntry::ControlDir) {
        response->Record.SetHandle(ControlDirIno);
        FillControlDirAttr(*response->Record.MutableNodeAttr());
    } else {
        response->Record.SetHandle(ControlFsIdFileIno);
        FillControlFsIdAttr(
            *response->Record.MutableNodeAttr(),
            session->FileStore.GetFileSystemId());
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
    const auto& dirName = StorageConfig->GetControlNamespaceDirName();

    if (dirName.empty()) {
        return false;
    }

    const auto entry = ClassifyControlNamespaceEntry(
        msg->Record.GetNodeId(),
        msg->Record.GetName(),
        dirName);

    // A hard link's target can also be a reserved ino - reject that too,
    // rather than letting it reach ExtractShardNoSafe/SelectShard with a
    // shard number no real filesystem has (spurious invalid-shard alarm on
    // a sharded filesystem, or a bogus lookup on an unsharded one).
    const auto linkTargetEntry = msg->Record.HasLink()
        ? ClassifyControlNamespaceEntry(msg->Record.GetLink().GetTargetNode())
        : EControlNamespaceEntry::None;

    if (Y_LIKELY(
            entry == EControlNamespaceEntry::None &&
            linkTargetEntry == EControlNamespaceEntry::None))
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
    const auto& dirName = StorageConfig->GetControlNamespaceDirName();

    if (dirName.empty()) {
        return false;
    }

    const auto srcEntry = ClassifyControlNamespaceEntry(
        msg->Record.GetNodeId(),
        msg->Record.GetName(),
        dirName);
    const auto dstEntry = ClassifyControlNamespaceEntry(
        msg->Record.GetNewParentId(),
        msg->Record.GetNewName(),
        dirName);

    if (Y_LIKELY(
            srcEntry == EControlNamespaceEntry::None &&
            dstEntry == EControlNamespaceEntry::None))
    {
        return false;
    }

    if (!GetAndValidateSession<TEvService::TRenameNodeMethod>(ctx, ev)) {
        return true;
    }

    auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(
        ControlNamespaceReadOnlyError());
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
    const auto entry = ClassifyControlNamespaceEntry(msg->Record.GetHandle());

    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(entry == EControlNamespaceEntry::None))
    {
        return false;
    }

    if (entry == EControlNamespaceEntry::ControlDir) {
        auto response = std::make_unique<TEvService::TEvReadDataResponse>(
            ErrorIsDirectory(ControlDirIno));
        NCloud::Reply(ctx, *ev, std::move(response));
        return true;
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
        Y_LIKELY(
            ClassifyControlNamespaceEntry(msg->Record.GetHandle()) ==
            EControlNamespaceEntry::None))
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
    const TEvService::TEvListNodesRequest::TPtr& ev,
    const TSessionInfo* session)
{
    auto* msg = ev->Get();

    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(
            ClassifyControlNamespaceEntry(msg->Record.GetNodeId()) !=
            EControlNamespaceEntry::ControlDir))
    {
        return false;
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

bool TStorageServiceActor::TryHandleControlNamespaceGetNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvGetNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(
            ClassifyControlNamespaceEntry(ev->Get()->Record.GetNodeId()) ==
            EControlNamespaceEntry::None))
    {
        return false;
    }

    auto response =
        std::make_unique<TEvService::TGetNodeXAttrMethod::TResponse>(
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
    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(
            ClassifyControlNamespaceEntry(ev->Get()->Record.GetNodeId()) ==
            EControlNamespaceEntry::None))
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
    if (StorageConfig->GetControlNamespaceDirName().empty() ||
        Y_LIKELY(
            ClassifyControlNamespaceEntry(ev->Get()->Record.GetNodeId()) ==
            EControlNamespaceEntry::None))
    {
        return false;
    }

    auto response =
        std::make_unique<TEvService::TSetNodeXAttrMethod::TResponse>(
            ControlNamespaceReadOnlyError());
    ReplyToXAttrRequest<TEvService::TSetNodeXAttrMethod>(
        ctx,
        ev,
        std::move(response),
        session);
    return true;
}

}   // namespace NCloud::NFileStore::NStorage
