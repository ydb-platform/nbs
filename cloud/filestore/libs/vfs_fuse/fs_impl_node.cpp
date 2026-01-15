#include "fs_impl.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/vfs/fsync_queue.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;

////////////////////////////////////////////////////////////////////////////////
// nodes

void TFileSystem::Lookup(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t parent,
    TString name)
{
    STORAGE_DEBUG("Lookup #" << parent << " " << name.Quote());

    if (!ValidateNodeId(*callContext, req, parent)) {
        return;
    }

    auto request = StartRequest<NProto::TGetNodeAttrRequest>(parent);
    request->SetName(std::move(name));

    // We don't know ino yet so we have to adjust the node size after
    // receiving GetNodeAttr response.
    //
    // During GetNodeAttr call, a node may be flushed and the information
    // about the node may be removed from WriteBackCache, and GetNodeAttr
    // may return size without considering cached WriteData requests.
    //
    // Acquiring a node state reference prevents metadata from removal
    // for flushed nodes.
    const ui64 nodeStateRefId =
        WriteBackCache ? WriteBackCache.AcquireNodeStateRef() : 0;

    Session->GetNodeAttr(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (auto future) {
            if (auto self = ptr.lock()) {
                NProto::TGetNodeAttrResponse response = future.ExtractValue();
                const auto& error = response.GetError();
                if (!HasError(response)) {
                    self->AdjustNodeSize(*response.MutableNode());
                    self->ReplyEntry(
                        *callContext,
                        error,
                        req,
                        response.GetNode());
                } else if (error.GetCode() == E_FS_NAMETOOLONG) {
                    self->ReplyError(*callContext, error, req, ENAMETOOLONG);
                } else {
                    fuse_entry_param entry = {};
                    entry.entry_timeout =
                        Config->GetNegativeEntryTimeout().SecondsFloat();
                    self->ReplyEntry(
                        *callContext,
                        error,
                        req,
                        &entry);
                }
                if (nodeStateRefId) {
                    self->WriteBackCache.ReleaseNodeStateRef(nodeStateRefId);
                }
            }
        });
}

void TFileSystem::Forget(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    unsigned long nlookup)
{
    with_lock (NodeCacheLock) {
        NodeCache.ForgetNode(ino, nlookup);
    }

    ReplyNone(*callContext, {}, req);
}

void TFileSystem::ForgetMulti(
    TCallContextPtr callContext,
    fuse_req_t req,
    size_t count,
    fuse_forget_data* forgets)
{
    with_lock (NodeCacheLock) {
        for (size_t i = 0; i < count; ++i) {
            NodeCache.ForgetNode(forgets[i].ino, forgets[i].nlookup);
        }
    }

    ReplyNone(*callContext, {}, req);
}

void TFileSystem::MkDir(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t parent,
    TString name,
    mode_t mode)
{
    STORAGE_DEBUG("MkDir #" << parent << " " << name.Quote());

    if (!ValidateNodeId(*callContext, req, parent)) {
        return;
    }

    auto request = StartRequest<NProto::TCreateNodeRequest>(parent);
    request->SetName(std::move(name));
    SetUserNGroup(*request, fuse_req_ctx(req));

    auto* dir = request->MutableDirectory();
    dir->SetMode(mode & ~(S_IFMT));

    const auto reqId = callContext->RequestId;
    FSyncQueue->Enqueue(reqId, TNodeId {parent});

    Session->CreateNode(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue->Dequeue(reqId, error, TNodeId {parent});

            if (CheckResponse(self, *callContext, req, response)) {
                self->ReplyEntry(*callContext, error, req, response.GetNode());
            }
        });
}

void TFileSystem::RmDir(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t parent,
    TString name)
{
    STORAGE_DEBUG("RmDir #" << parent << " " << name.Quote());

    if (!ValidateNodeId(*callContext, req, parent)) {
        return;
    }

    auto request = StartRequest<NProto::TUnlinkNodeRequest>(parent);
    request->SetName(std::move(name));
    request->SetUnlinkDirectory(true);

    const auto reqId = callContext->RequestId;
    FSyncQueue->Enqueue(reqId, TNodeId {parent});

    Session->UnlinkNode(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue->Dequeue(reqId, error, TNodeId {parent});

            if (CheckResponse(self, *callContext, req, response)) {
                self->ReplyError(*callContext, error, req, 0);
            }
        });
}

void TFileSystem::MkNode(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t parent,
    TString name,
    mode_t mode,
    dev_t rdev)
{
    STORAGE_DEBUG("MkNode #" << parent << " " << name.Quote()
        << " mode: " << mode << " rdev: " << rdev);

    if (!ValidateNodeId(*callContext, req, parent)) {
        return;
    }

    auto request = StartRequest<NProto::TCreateNodeRequest>(parent);
    request->SetName(std::move(name));
    SetUserNGroup(*request, fuse_req_ctx(req));

    if (S_ISREG(mode)) {
        // just an empty file
        auto* file = request->MutableFile();
        file->SetMode(mode & ~(S_IFMT));
    } else if (S_ISSOCK(mode)) {
        // null file type for unix sockets
        auto* socket = request->MutableSocket();
        socket->SetMode(mode & ~(S_IFMT));
    } else if (S_ISFIFO(mode)) {
        auto* fifo = request->MutableFifo();
        fifo->SetMode(mode & ~(S_IFMT));
    } else if (S_ISCHR(mode)) {
        auto* charDevice = request->MutableCharDevice();
        charDevice->SetMode(mode & ~(S_IFMT));
        charDevice->SetDevice(rdev);
    } else if (S_ISBLK(mode)) {
        auto* blockDevice = request->MutableBlockDevice();
        blockDevice->SetMode(mode & ~(S_IFMT));
        blockDevice->SetDevice(rdev);
    } else {
        ReplyError(*callContext, ErrorNotSupported(""), req, ENOTSUP);
        return;
    }

    const auto reqId = callContext->RequestId;
    FSyncQueue->Enqueue(reqId, TNodeId {parent});

    Session->CreateNode(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue->Dequeue(reqId, error, TNodeId {parent});

            if (CheckResponse(self, *callContext, req, response)) {
                self->ReplyEntry(*callContext, error, req, response.GetNode());
            }
        });
}

void TFileSystem::Unlink(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t parent,
    TString name)
{
    STORAGE_DEBUG("Unlink #" << parent << " " << name.Quote());

    if (!ValidateNodeId(*callContext, req, parent)) {
        return;
    }

    auto request = StartRequest<NProto::TUnlinkNodeRequest>(parent);
    request->SetName(std::move(name));
    request->SetUnlinkDirectory(false);

    const auto reqId = callContext->RequestId;
    FSyncQueue->Enqueue(reqId, TNodeId {parent});

    Session->UnlinkNode(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue->Dequeue(reqId, error, TNodeId {parent});

            if (CheckResponse(self, *callContext, req, response)) {
                self->ReplyError(*callContext, error, req, 0);
            }
        });
}

void TFileSystem::Rename(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t parent,
    TString name,
    fuse_ino_t newparent,
    TString newname,
    int flags)
{
    ui32 protoFlags = SystemFlagsToRename(flags);

    STORAGE_DEBUG("Rename #" << parent << " " << name.Quote() << " -> #"
        << newparent << " " << newname.Quote() << " " << RenameFlagsToString(protoFlags));

    if (!ValidateNodeId(*callContext, req, parent)) {
        return;
    }

    auto request = StartRequest<NProto::TRenameNodeRequest>(parent);
    request->SetName(std::move(name));
    request->SetNewName(std::move(newname));
    request->SetNewParentId(newparent);
    request->SetFlags(protoFlags);

    const auto reqId = callContext->RequestId;
    FSyncQueue->Enqueue(reqId, TNodeId {parent});

    Session->RenameNode(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue->Dequeue(reqId, error, TNodeId {parent});

            if (CheckResponse(self, *callContext, req, response)) {
                // TODO: update tree
                self->ReplyError(*callContext, error, req, 0);
            }
        });
}

void TFileSystem::SymLink(
    TCallContextPtr callContext,
    fuse_req_t req,
    TString target,
    fuse_ino_t parent,
    TString name)
{
    STORAGE_DEBUG("SymLink #" << parent << " " <<  name.Quote() << " -> " << target.Quote());

    if (!ValidateNodeId(*callContext, req, parent)) {
        return;
    }

    auto request = StartRequest<NProto::TCreateNodeRequest>(parent);
    request->SetName(std::move(name));
    SetUserNGroup(*request, fuse_req_ctx(req));

    auto* link = request->MutableSymLink();
    link->SetTargetPath(std::move(target));

    const auto reqId = callContext->RequestId;
    FSyncQueue->Enqueue(reqId, TNodeId {parent});

    Session->CreateNode(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue->Dequeue(reqId, error, TNodeId {parent});

            if (CheckResponse(self, *callContext, req, response)) {
                self->ReplyEntry(*callContext, error, req, response.GetNode());
            }
        });
}

void TFileSystem::Link(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_ino_t newparent,
    TString newname)
{
    STORAGE_DEBUG("Link #" << ino << " -> #" << newparent << " " << newname.Quote());

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    auto request = StartRequest<NProto::TCreateNodeRequest>(newparent);
    request->SetName(std::move(newname));

    auto* link = request->MutableLink();
    link->SetTargetNode(ino);

    const auto reqId = callContext->RequestId;
    FSyncQueue->Enqueue(reqId, TNodeId {ino});

    Session->CreateNode(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue->Dequeue(reqId, error, TNodeId {ino});

            if (auto self = ptr.lock(); CheckResponse(self, *callContext, req, response)) {
                self->ReplyEntry(*callContext, error, req, response.GetNode());
            }
        });
}

void TFileSystem::ReadLink(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino)
{
    STORAGE_DEBUG("ReadLink #" << ino);

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    auto request = StartRequest<NProto::TReadLinkRequest>(ino);

    Session->ReadLink(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            const auto& response = future.GetValue();
            if (auto self = ptr.lock(); CheckResponse(self, *callContext, req, response)) {
                self->ReplyReadLink(
                    *callContext,
                    response.GetError(),
                    req,
                    response.GetSymLink().data());
            }
        });
}

void TFileSystem::Access(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    int mask)
{
    STORAGE_DEBUG("Access #" << ino << " mask " << mask);

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    auto request = StartRequest<NProto::TAccessNodeRequest>(ino);
    request->SetMask(mask);

    Session->AccessNode(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            const auto& response = future.GetValue();
            if (auto self = ptr.lock(); CheckResponse(self, *callContext, req, response)) {
                self->ReplyError(*callContext, response.GetError(), req, 0);
            }
        });
}

}   // namespace NCloud::NFileStore::NFuse
