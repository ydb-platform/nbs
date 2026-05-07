#include "memshard.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;

struct TFileNode
{
    TBuffer Data;
};

struct TDirectoryNode
{
    THashMap<TString, ui64> Name2Id;
};

class TMemFileSystemShard: public IFileSystemShard
{
private:
    THashMap<ui64, NProto::TNodeAttr> Attrs;
    THashMap<ui64, TFileNode> Files;
    THashMap<ui64, TDirectoryNode> Dirs;
    THashMap<ui64, TFileNode*> Handles;

public:
    TMemFileSystemShard()
    {
        Dirs[RootNodeId] = {};
    }

public:
    TFuture<NProtoPrivate::TGetNodeAttrBatchResponse>
    GetNodeAttrBatch(NProtoPrivate::TGetNodeAttrBatchRequest request) override
    {
        NProtoPrivate::TGetNodeAttrBatchResponse response;
        const auto* dir = Dirs.FindPtr(request.GetNodeId());
        if (!dir) {
            *response.MutableError() = ErrorInvalidTarget(request.GetNodeId());
            return MakeFuture(std::move(response));
        }
        for (const auto& name: request.GetNames()) {
            const auto* p = dir->Name2Id.FindPtr(name);
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
        const auto* dir = Dirs.FindPtr(request.GetNodeId());
        if (!dir) {
            *response.MutableError() = ErrorInvalidTarget(request.GetNodeId());
            return MakeFuture(std::move(response));
        }

        const auto* p = dir->Name2Id.FindPtr(request.GetName());
        if (!p) {
            *response.MutableError() =
                ErrorInvalidTarget(request.GetNodeId(), request.GetName());
            return MakeFuture(std::move(response));
        }

        const auto* a = Attrs.FindPtr(*p);
        if (!a) {
            *response.MutableError() = MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "can't find attrs for " << *p);
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
        return NotImplemented<NProto::TAccessNodeResponse>(
            std::move(request));
    }

    TFuture<NProto::TCreateNodeResponse>
    CreateNode(NProto::TCreateNodeRequest request) override
    {
        return NotImplemented<NProto::TCreateNodeResponse>(
            std::move(request));
    }

    TFuture<NProto::TUnlinkNodeResponse>
    UnlinkNode(NProto::TUnlinkNodeRequest request) override
    {
        return NotImplemented<NProto::TUnlinkNodeResponse>(
            std::move(request));
    }

    TFuture<NProto::TCreateHandleResponse>
    CreateHandle(NProto::TCreateHandleRequest request) override
    {
        return NotImplemented<NProto::TCreateHandleResponse>(
            std::move(request));
    }

    TFuture<NProto::TDestroyHandleResponse>
    DestroyHandle(NProto::TDestroyHandleRequest request) override
    {
        return NotImplemented<NProto::TDestroyHandleResponse>(
            std::move(request));
    }

    TFuture<NProto::TAllocateDataResponse>
    AllocateData(NProto::TAllocateDataRequest request) override
    {
        return NotImplemented<NProto::TAllocateDataResponse>(
            std::move(request));
    }

    TFuture<NProto::TAcquireLockResponse>
    AcquireLock(NProto::TAcquireLockRequest request) override
    {
        return NotImplemented<NProto::TAcquireLockResponse>(
            std::move(request));
    }

    TFuture<NProto::TReleaseLockResponse>
    ReleaseLock(NProto::TReleaseLockRequest request) override
    {
        return NotImplemented<NProto::TReleaseLockResponse>(
            std::move(request));
    }

    TFuture<NProto::TTestLockResponse>
    TestLock(NProto::TTestLockRequest request) override
    {
        return NotImplemented<NProto::TTestLockResponse>(
            std::move(request));
    }

    TFuture<NProto::TWriteDataResponse>
    WriteData(NProto::TWriteDataRequest request) override
    {
        return NotImplemented<NProto::TWriteDataResponse>(
            std::move(request));
    }

    TFuture<NProto::TReadDataResponse>
    ReadData(NProto::TReadDataRequest request) override
    {
        return NotImplemented<NProto::TReadDataResponse>(
            std::move(request));
    }

    TFuture<NProto::TRemoveNodeXAttrResponse>
    RemoveNodeXAttr(NProto::TRemoveNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TRemoveNodeXAttrResponse>(
            std::move(request));
    }

    TFuture<NProto::TGetNodeXAttrResponse>
    GetNodeXAttr(NProto::TGetNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TGetNodeXAttrResponse>(
            std::move(request));
    }

    TFuture<NProto::TSetNodeXAttrResponse>
    SetNodeXAttr(NProto::TSetNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TSetNodeXAttrResponse>(
            std::move(request));
    }

    TFuture<NProto::TListNodeXAttrResponse>
    ListNodeXAttr(NProto::TListNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TListNodeXAttrResponse>(
            std::move(request));
    }

private:
    template <typename TResponse, typename TRequest>
    TFuture<TResponse> NotImplemented(TRequest request)
    {
        Y_UNUSED(request);

        TResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);
        return MakeFuture(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateMemFileSystemShard()
{
    return std::make_shared<TMemFileSystemShard>();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
