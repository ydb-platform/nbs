#include "memshard.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

auto CreateAttrs(ui64 id, ui32 mode, ui64 size, ui64 uid, ui64 gid)
{
    ui64 now = MicroSeconds();

    NProto::TNodeAttr attrs;
    attrs.SetId(id);
    attrs.SetType(NProto::E_REGULAR_NODE);
    attrs.SetMode(mode);
    attrs.SetATime(now);
    attrs.SetMTime(now);
    attrs.SetCTime(now);
    attrs.SetLinks(1);
    attrs.SetSize(size);
    attrs.SetUid(uid);
    attrs.SetGid(gid);

    return attrs;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;

struct TFileNode
{
    TBuffer Data;
};

struct THandle
{
    NProto::TNodeAttr* Attr = nullptr;
    TFileNode* FileNode = nullptr;
};

struct TDirectoryNode
{
    THashMap<TString, ui64> Name2Id;
};

/*
 *  This implementation is supposed to be used only for debugging and testing
 *  purposes and is deliberately simplified in multiple ways. For example, there
 *  is no isolation between different sessions - all sessions see all handles.
 */

class TMemFileSystemShard: public IFileSystemShard
{
private:
    const ui32 ShardNo;

    TDirectoryNode Root;
    THashMap<ui64, NProto::TNodeAttr> Attrs;
    THashMap<ui64, TFileNode> Files;
    THashMap<ui64, THandle> Handles;
    ui64 LastNodeId = RootNodeId;
    ui64 LastHandleId = 0;

public:
    explicit TMemFileSystemShard(ui32 shardNo)
        : ShardNo(shardNo)
    {}

public:
    TFuture<NProtoPrivate::TGetNodeAttrBatchResponse>
    GetNodeAttrBatch(NProtoPrivate::TGetNodeAttrBatchRequest request) override
    {
        NProtoPrivate::TGetNodeAttrBatchResponse response;
        if (request.GetNodeId() != RootNodeId) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return MakeFuture(std::move(response));
        }
        for (const auto& name: request.GetNames()) {
            const auto* p = Root.Name2Id.FindPtr(name);
            if (!p) {
                *response.MutableError() =
                    ErrorInvalidTarget(request.GetNodeId(), name);
                return MakeFuture(std::move(response));
            }

            const auto* a = Attrs.FindPtr(*p);
            if (!a) {
                *response.MutableError() = MakeError(
                    E_INVALID_STATE,
                    TStringBuilder() << "can't find attrs for " << *p);
                return MakeFuture(std::move(response));
            }

            auto* item = response.AddResponses();
            *item->MutableNode() = *a;
        }

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TGetNodeAttrResponse>
    GetNodeAttr(NProto::TGetNodeAttrRequest request) override
    {
        NProto::TGetNodeAttrResponse response;
        if (request.GetNodeId() != RootNodeId && !request.GetName().empty()) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return MakeFuture(std::move(response));
        }

        ui64 nodeId = request.GetNodeId();
        if (!request.GetName().empty()) {
            const auto* p = Root.Name2Id.FindPtr(request.GetName());
            if (!p) {
                *response.MutableError() =
                    ErrorInvalidTarget(request.GetNodeId(), request.GetName());
                return MakeFuture(std::move(response));
            }

            nodeId = *p;
        }

        const auto* a = Attrs.FindPtr(nodeId);
        if (!a) {
            NProto::TError e;
            if (!request.GetName().empty()) {
                e = MakeError(
                    E_INVALID_STATE,
                    TStringBuilder() << "can't find attrs for " << nodeId);
            } else {
                e = ErrorInvalidTarget(nodeId);
            }
            *response.MutableError() = std::move(e);
            return MakeFuture(std::move(response));
        }

        *response.MutableNode() = *a;
        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TSetNodeAttrResponse>
    SetNodeAttr(NProto::TSetNodeAttrRequest request) override
    {
        NProto::TSetNodeAttrResponse response;
        auto* a = Attrs.FindPtr(request.GetNodeId());
        if (!a) {
            *response.MutableError() = ErrorInvalidTarget(request.GetNodeId());
            return MakeFuture(std::move(response));
        }

        auto flags = request.GetFlags();
        const auto& update = request.GetUpdate();

        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MODE)) {
            a->SetMode(update.GetMode());
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_UID)) {
            a->SetUid(update.GetUid());
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_GID)) {
            a->SetGid(update.GetGid());
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_ATIME)) {
            a->SetATime(update.GetATime());
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MTIME)) {
            a->SetMTime(update.GetMTime());
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_CTIME)) {
            a->SetCTime(update.GetCTime());
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE)) {
            a->SetSize(update.GetSize());
        }

        *response.MutableNode() = *a;
        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TAccessNodeResponse>
    AccessNode(NProto::TAccessNodeRequest request) override
    {
        Y_UNUSED(request);
        return MakeFuture<NProto::TAccessNodeResponse>();
    }

    TFuture<NProto::TCreateNodeResponse>
    CreateNode(NProto::TCreateNodeRequest request) override
    {
        if (!request.HasFile()) {
            return NotImplemented<NProto::TCreateNodeResponse>(request);
        }

        NProto::TCreateNodeResponse response;
        if (request.GetNodeId() != RootNodeId) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return MakeFuture(std::move(response));
        }

        auto& nodeRef = Root.Name2Id[request.GetName()];
        if (nodeRef) {
            *response.MutableError() = ErrorAlreadyExists(request.GetName());
            return MakeFuture(std::move(response));
        }

        const ui64 nodeId = ShardedId(++LastNodeId, ShardNo);
        auto& a = Attrs[nodeId];
        a = CreateAttrs(
            nodeId,
            request.GetFile().GetMode(),
            0 /* size */,
            request.GetUid(),
            request.GetGid());
        Files[nodeId];
        nodeRef = nodeId;

        *response.MutableNode() = a;
        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TUnlinkNodeResponse>
    UnlinkNode(NProto::TUnlinkNodeRequest request) override
    {
        NProto::TUnlinkNodeResponse response;
        if (request.GetNodeId() != RootNodeId) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return MakeFuture(std::move(response));
        }

        auto it = Root.Name2Id.find(request.GetName());
        if (it == Root.Name2Id.end()) {
            *response.MutableError() = ErrorInvalidTarget(request.GetNodeId());
            return MakeFuture(std::move(response));
        }

        auto ait = Attrs.find(it->second);
        Y_ABORT_UNLESS(ait != Attrs.end());
        Y_ABORT_UNLESS(ait->second.GetLinks() > 0);
        if (ait->second.GetLinks() == 1) {
            Attrs.erase(ait);
            Files.erase(it->second);
        } else {
            ait->second.SetLinks(ait->second.GetLinks() - 1);
        }
        Root.Name2Id.erase(it);

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TCreateHandleResponse>
    CreateHandle(NProto::TCreateHandleRequest request) override
    {
        NProto::TCreateHandleResponse response;
        if (request.GetNodeId() != RootNodeId && !request.GetName().empty()) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return MakeFuture(std::move(response));
        }

        const ui32 flags = request.GetFlags();
        const auto createFlag = NProto::TCreateHandleRequest::E_CREATE;
        const auto exclFlag = NProto::TCreateHandleRequest::E_EXCLUSIVE;

        NProto::TNodeAttr* a = nullptr;
        if (request.GetName().empty()) {
            a = Attrs.FindPtr(request.GetNodeId());
            if (!a) {
                *response.MutableError() =
                    ErrorInvalidTarget(request.GetNodeId());
                return MakeFuture(std::move(response));
            }
        } else {
            auto it = Root.Name2Id.find(request.GetName());
            if (it == Root.Name2Id.end()) {
                if (HasFlag(flags, createFlag)) {
                    const ui64 nodeId = ShardedId(++LastNodeId, ShardNo);
                    it = Root.Name2Id.insert({request.GetName(), nodeId}).first;
                    a = &Attrs[nodeId];
                    *a = CreateAttrs(
                        nodeId,
                        request.GetMode(),
                        0 /* size */,
                        request.GetUid(),
                        request.GetGid());
                    Files[nodeId];
                } else {
                    *response.MutableError() = ErrorInvalidTarget(
                        request.GetNodeId(),
                        request.GetName());
                    return MakeFuture(std::move(response));
                }
            } else {
                if (HasFlag(flags, createFlag) && HasFlag(flags, exclFlag)) {
                    *response.MutableError() =
                        ErrorAlreadyExists(request.GetName());
                    return MakeFuture(std::move(response));
                }

                a = &Attrs[it->second];
            }
        }

        a->SetLinks(a->GetLinks() + 1);
        const ui64 handleId = ShardedId(++LastHandleId, ShardNo);
        auto& h = Handles[handleId];
        h.Attr = a;
        h.FileNode = &Files[a->GetId()];

        response.SetHandle(handleId);
        *response.MutableNodeAttr() = *a;
        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TDestroyHandleResponse>
    DestroyHandle(NProto::TDestroyHandleRequest request) override
    {
        NProto::TDestroyHandleResponse response;
        auto it = Handles.find(request.GetHandle());
        if (it == Handles.end()) {
            *response.MutableError() = ErrorInvalidHandle(request.GetHandle());
            return MakeFuture(std::move(response));
        }

        auto& h = it->second;
        Y_ABORT_UNLESS(h.Attr->GetLinks() > 0);
        if (h.Attr->GetLinks() == 1) {
            Files.erase(h.Attr->GetId());
            Attrs.erase(h.Attr->GetId());
        } else {
            h.Attr->SetLinks(h.Attr->GetLinks() - 1);
        }
        Handles.erase(it);

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TAllocateDataResponse>
    AllocateData(NProto::TAllocateDataRequest request) override
    {
        NProto::TAllocateDataResponse response;

        auto hit = Handles.find(request.GetHandle());
        if (hit == Handles.end()) {
            *response.MutableError() = ErrorInvalidHandle(request.GetHandle());
            return MakeFuture(std::move(response));
        }

        auto& a = *hit->second.Attr;
        auto& f = *hit->second.FileNode;

        const ui32 flags = request.GetFlags();
        const ui64 size = request.GetOffset() + request.GetLength();
        const ui64 minBorder = Min(size, a.GetSize());
        const bool needExtend = a.GetSize() < size &&
            !HasFlag(flags, NProto::TAllocateDataRequest::F_KEEP_SIZE);

        const bool shouldZero =
            (HasFlag(flags, NProto::TAllocateDataRequest::F_PUNCH_HOLE) ||
            HasFlag(flags, NProto::TAllocateDataRequest::F_ZERO_RANGE)) &&
            minBorder > request.GetOffset();

        if (f.Data.size() < size && needExtend) {
            f.Data.Resize(size);
        } else if (f.Data.size() < minBorder && shouldZero) {
            f.Data.Resize(minBorder);
        }

        if (shouldZero) {
            memset(
                f.Data.data() + request.GetOffset(),
                0,
                minBorder - request.GetOffset());
        }

        if (needExtend) {
            a.SetSize(size);
        }

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TAcquireLockResponse>
    AcquireLock(NProto::TAcquireLockRequest request) override
    {
        return NotImplemented<NProto::TAcquireLockResponse>(request);
    }

    TFuture<NProto::TReleaseLockResponse>
    ReleaseLock(NProto::TReleaseLockRequest request) override
    {
        return NotImplemented<NProto::TReleaseLockResponse>(request);
    }

    TFuture<NProto::TTestLockResponse>
    TestLock(NProto::TTestLockRequest request) override
    {
        return NotImplemented<NProto::TTestLockResponse>(request);
    }

    TFuture<NProto::TWriteDataResponse>
    WriteData(NProto::TWriteDataRequest request) override
    {
        NProto::TWriteDataResponse response;

        auto hit = Handles.find(request.GetHandle());
        if (hit == Handles.end()) {
            *response.MutableError() = ErrorInvalidHandle(request.GetHandle());
            return MakeFuture(std::move(response));
        }

        auto& a = *hit->second.Attr;
        auto& f = *hit->second.FileNode;

        const ui64 end = request.GetOffset()
            + request.GetBuffer().size() - request.GetBufferOffset();
        if (f.Data.size() < end) {
            f.Data.Resize(end);
        }

        if (a.GetSize() < end) {
            a.SetSize(end);
        }

        memcpy(
            f.Data.data() + request.GetOffset(),
            request.GetBuffer().data() + request.GetBufferOffset(),
            request.GetBuffer().size() - request.GetBufferOffset());

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TReadDataResponse>
    ReadData(NProto::TReadDataRequest request) override
    {
        if (request.IovecsSize()) {
            return NotImplemented<NProto::TReadDataResponse>(request);
        }

        NProto::TReadDataResponse response;

        auto hit = Handles.find(request.GetHandle());
        if (hit == Handles.end()) {
            *response.MutableError() = ErrorInvalidHandle(request.GetHandle());
            return MakeFuture(std::move(response));
        }

        auto& f = *hit->second.FileNode;

        auto& buf = *response.MutableBuffer();
        buf.resize(request.GetLength(), 0);

        if (request.GetOffset() < f.Data.size()) {
            memcpy(
                buf.begin(),
                f.Data.data() + request.GetOffset(),
                Min(request.GetLength(), f.Data.size() - request.GetOffset()));
        }

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TRemoveNodeXAttrResponse>
    RemoveNodeXAttr(NProto::TRemoveNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TRemoveNodeXAttrResponse>(request);
    }

    TFuture<NProto::TGetNodeXAttrResponse>
    GetNodeXAttr(NProto::TGetNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TGetNodeXAttrResponse>(request);
    }

    TFuture<NProto::TSetNodeXAttrResponse>
    SetNodeXAttr(NProto::TSetNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TSetNodeXAttrResponse>(request);
    }

    TFuture<NProto::TListNodeXAttrResponse>
    ListNodeXAttr(NProto::TListNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TListNodeXAttrResponse>(request);
    }

private:
    template <typename TResponse, typename TRequest>
    TFuture<TResponse> NotImplemented(const TRequest& request)
    {
        Y_UNUSED(request);

        TResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);
        return MakeFuture(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateMemFileSystemShard(ui32 shardNo)
{
    return std::make_shared<TMemFileSystemShard>(shardNo);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
