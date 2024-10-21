#include "fs.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <optional>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

NProto::TSetNodeAttrResponse TLocalFileSystem::SetNodeAttr(
    const NProto::TSetNodeAttrRequest& request)
{
    STORAGE_TRACE("SetNodeAttr " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
    }

    auto flags = request.GetFlags();
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MODE)) {
        node->Chmod(request.GetUpdate().GetMode());
    }

    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE)) {
        node->Truncate(request.GetUpdate().GetSize());
    }

    //
    // Uid & Gid
    //

    std::optional<unsigned int> uid = std::nullopt;
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_UID)) {
        uid = request.GetUpdate().GetUid();
    }

    std::optional<unsigned int> gid = std::nullopt;
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_GID)) {
        gid = request.GetUpdate().GetGid();
    }

    if (uid.has_value() && gid.has_value()) {
        node->Chown(*uid, *gid);
    }

    //
    // Atime & Mtime
    //

    TInstant atime;
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_ATIME)) {
        atime = TInstant::MicroSeconds(request.GetUpdate().GetATime());
    }

    TInstant mtime;
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MTIME)) {
        mtime = TInstant::MicroSeconds(request.GetUpdate().GetMTime());
    }

    if (mtime || atime) {
        node->Utimes(atime, mtime);
    }

    NProto::TSetNodeAttrResponse response;
    ConvertStats(node->Stat(), *response.MutableNode());

    return response;
}

NProto::TGetNodeAttrResponse TLocalFileSystem::GetNodeAttr(
    const NProto::TGetNodeAttrRequest& request)
{
    STORAGE_TRACE("GetNodeAttr " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
    }

    TFileStat stat;
    if (const auto& name = request.GetName()) {
        stat = node->Stat(name);
        if (!session->LookupNode(stat.INode)) {
            auto child = TIndexNode::Create(*node, name);
            // TODO: better? race between statting one child and creating another one
            // but maybe too costly...
            stat = child->Stat();
            if (!session->TryInsertNode(
                    std::move(child),
                    node->GetNodeId(),
                    name))
            {
                ReportLocalFsMaxSessionNodesInUse();
                return TErrorResponse(ErrorNoSpaceLeft());
            }
        }
    } else {
        stat = node->Stat();
    }

    NProto::TGetNodeAttrResponse response;
    ConvertStats(stat, *response.MutableNode());

    return response;
}

NProto::TSetNodeXAttrResponse TLocalFileSystem::SetNodeXAttr(
    const NProto::TSetNodeXAttrRequest& request)
{
    STORAGE_TRACE("SetNodeXAttr " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
    }

    node->SetXAttr(request.GetName(), request.GetValue());
    return {};
}

NProto::TGetNodeXAttrResponse TLocalFileSystem::GetNodeXAttr(
    const NProto::TGetNodeXAttrRequest& request)
{
    STORAGE_TRACE("GetNodeXAttr " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
    }

    NProto::TGetNodeXAttrResponse response;
    response.SetValue(node->GetXAttr(request.GetName()));
    return response;
}

NProto::TListNodeXAttrResponse TLocalFileSystem::ListNodeXAttr(
    const NProto::TListNodeXAttrRequest& request)
{
    STORAGE_TRACE("ListNodeXAttr " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
    }

    auto result = node->ListXAttrs();

    NProto::TListNodeXAttrResponse response;
    response.MutableNames()->Reserve(result.size());

    for (auto& key: result) {
        response.AddNames()->swap(key);
    }

    return response;
}

NProto::TRemoveNodeXAttrResponse TLocalFileSystem::RemoveNodeXAttr(
    const NProto::TRemoveNodeXAttrRequest& request)
{
    STORAGE_TRACE("RemoveNodeXAttr " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
    }

    node->RemoveXAttr(request.GetName());
    return {};
}

}   // namespace NCloud::NFileStore
