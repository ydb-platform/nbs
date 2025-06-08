#include "fs.h"

#include "lowlevel.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/generic/guid.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TIndexNodePtr TryCreateChildNode(const TIndexNode& parent, const TString& name)
{
    try {
        return TIndexNode::Create(parent, name);
    } catch (...) {
        // TODO: ignore only ENOENT
    }

    return nullptr;
}

}

////////////////////////////////////////////////////////////////////////////////

NProto::TResolvePathResponse TLocalFileSystem::ResolvePath(
    const NProto::TResolvePathRequest& request)
{
    STORAGE_TRACE("ResolvePath " << DumpMessage(request));

    // TODO

    return {};
}

NProto::TCreateNodeResponse TLocalFileSystem::CreateNode(
    const NProto::TCreateNodeRequest& request)
{
    STORAGE_TRACE("CreateNode " << DumpMessage(request));

    auto session = GetSession(request);
    auto parent = session->LookupNode(request.GetNodeId());
    if (!parent) {
        return TErrorResponse(ErrorInvalidParent(request.GetNodeId()));
    }

    NLowLevel::UnixCredentialsGuard credGuard(
        request.GetUid(),
        request.GetGid(),
        Config->GetGuestOnlyPermissionsCheckEnabled());
    TIndexNodePtr target;
    if (request.HasDirectory()) {
        int mode = request.GetDirectory().GetMode();
        if (!mode) {
            mode = Config->GetDefaultPermissions();
        }

        target = parent->CreateDirectory(request.GetName(), mode);
    } else if (request.HasFile()) {
        int mode = request.GetFile().GetMode();
        if (!mode) {
            mode = Config->GetDefaultPermissions();
        }

        target = parent->CreateFile(request.GetName(), mode);
    } else if (request.HasSymLink()) {
        target = parent->CreateSymlink(request.GetSymLink().GetTargetPath(), request.GetName());
    } else if (request.HasLink()) {
        auto node = session->LookupNode(request.GetLink().GetTargetNode());
        if (!node) {
            return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
        }

        target = node->CreateLink(*parent, request.GetName());
    } else if (request.HasSocket()) {
        int mode = request.GetSocket().GetMode();
        if (!mode) {
            mode = Config->GetDefaultPermissions();
        }

        target = parent->CreateSocket(request.GetName(), mode);
    } else {
        return TErrorResponse(ErrorInvalidArgument());
    }

    if (!request.HasLink()) {
        // For hard link no need to apply credentials since ownership is shared
        // between the links
        if (!credGuard.ApplyCredentials(target->GetNodeFd())) {
            parent->Unlink(request.GetName(), request.HasDirectory());
            return TErrorResponse(ErrorFailedToApplyCredentials(request.GetName()));
        }
    }

    auto stat = target->Stat();
    if (!session->TryInsertNode(
            std::move(target),
            parent->GetNodeId(),
            request.GetName()))
    {
        ReportLocalFsMaxSessionNodesInUse();
        return TErrorResponse(ErrorNoSpaceLeft());
    }

    NProto::TCreateNodeResponse response;
    ConvertStats(stat, *response.MutableNode());

    return response;
}

NProto::TUnlinkNodeResponse TLocalFileSystem::UnlinkNode(
    const NProto::TUnlinkNodeRequest& request)
{
    STORAGE_TRACE("UnlinkNode " << DumpMessage(request));

    auto session = GetSession(request);
    auto parent = session->LookupNode(request.GetNodeId());
    if (!parent) {
        return TErrorResponse(ErrorInvalidParent(request.GetNodeId()));
    }

    auto stat = parent->Stat(request.GetName());
    parent->Unlink(request.GetName(), request.GetUnlinkDirectory());

    if ((!stat.IsDir() && stat.NLinks == 1) || (stat.IsDir() && stat.NLinks <= 2)) {
        session->ForgetNode(stat.INode);
    }

    return {};
}

NProto::TRenameNodeResponse TLocalFileSystem::RenameNode(
    const NProto::TRenameNodeRequest& request)
{
    STORAGE_TRACE("RenameNode " << DumpMessage(request)
        << " flags: " << RenameFlagsToString(request.GetFlags()));

    auto session = GetSession(request);
    auto parent = session->LookupNode(request.GetNodeId());
    if (!parent) {
        return TErrorResponse(ErrorInvalidParent(request.GetNodeId()));
    }

    auto newparent = session->LookupNode(request.GetNewParentId());
    if (!newparent) {
        return TErrorResponse(ErrorInvalidParent(request.GetNodeId()));
    }

    std::optional<TFileStat> stat = std::nullopt;
    {
        auto newparentList = newparent->List(true);
        auto it = std::find_if(
            newparentList.begin(),
            newparentList.end(),
            [&](const auto& elm) {
                return elm.first == request.GetNewName();
            });
        if (it != newparentList.end()) {
            stat = std::move(it->second);
        }
    }

    const int flags = RenameFlagsToSystem(request.GetFlags());
    parent->Rename(request.GetName(), newparent, request.GetNewName(), flags);

    if (stat.has_value() &&
        !HasFlag(request.GetFlags(), NProto::TRenameNodeRequest::F_EXCHANGE) &&
        ((!stat.value().IsDir() && stat.value().NLinks == 1) ||
         (stat.value().IsDir() && stat.value().NLinks == 2)))
    {
        session->ForgetNode(stat.value().INode);
    }

    return {};
}

NProto::TAccessNodeResponse TLocalFileSystem::AccessNode(
    const NProto::TAccessNodeRequest& request)
{
    STORAGE_TRACE("AccessNode " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
    }

    node->Access(request.GetMask());

    return {};
}

NProto::TListNodesResponse TLocalFileSystem::ListNodes(
    const NProto::TListNodesRequest& request)
{
    STORAGE_TRACE("ListNodes " << DumpMessage(request));

    auto session = GetSession(request);
    auto parent = session->LookupNode(request.GetNodeId());
    if (!parent) {
        return TErrorResponse(ErrorInvalidParent(request.GetNodeId()));
    }

    auto entries = parent->List();

    NProto::TListNodesResponse response;
    response.MutableNames()->Reserve(entries.size());
    response.MutableNodes()->Reserve(entries.size());

    for (auto& entry: entries) {
        // If we can open node by handle there is no need to cache nodes when
        // listing. Instead we will resolve the node during the actual usage
        // getattr/read/write. This optimization will help us to avoid
        // populating cache when iterating large directories with > 1M files
        const auto ignoreCache =
            Config->GetDontPopulateNodeCacheWhenListingNodes() &&
            Config->GetOpenNodeByHandleEnabled();
        if (!ignoreCache && !session->LookupNode(entry.second.INode)) {
            auto node = TryCreateChildNode(*parent, entry.first);
            if (node && node->GetNodeId() == entry.second.INode) {
                if (!session->TryInsertNode(
                        std::move(node),
                        parent->GetNodeId(),
                        entry.first))
                {
                    ReportLocalFsMaxSessionNodesInUse();
                    return TErrorResponse(ErrorNoSpaceLeft());
                }
            }
        }

        response.MutableNames()->Add()->swap(entry.first);
        ConvertStats(entry.second, *response.MutableNodes()->Add());
    }

    return response;
}

NProto::TReadLinkResponse TLocalFileSystem::ReadLink(
    const NProto::TReadLinkRequest& request)
{
    STORAGE_TRACE("ReadLink " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
    }

    auto link = node->ReadLink();

    NProto::TReadLinkResponse response;
    response.SetSymLink(link);

    return response;
}

}   // namespace NCloud::NFileStore
