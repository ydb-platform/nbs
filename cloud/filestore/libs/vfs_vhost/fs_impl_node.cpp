#include "fs_impl.h"
#include "fs_impl_reply.h"

#include <cloud/filestore/libs/vfs/convert.h>

#include <util/generic/size_literals.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NVFSVhost {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NFileStore::NVFS;

TFuture<NProto::TError> TFileSystem::Lookup(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{

    TString name;
    if (!in.Read(name) || !header.nodeid) {
        ReplyError(ctx, EINVAL);
        return MakeFuture(NProto::TError{});
    }

    STORAGE_DEBUG("Lookup #" << header.unique
        << " parent " << header.nodeid
        << " name " << name.Quote());

    auto request = CreateRequest<NProto::TGetNodeAttrRequest>(header.nodeid);
    request->SetName(std::move(name));

    return Session->GetNodeAttr(ctx->CallContext, std::move(request))
        .Apply([=, weak = weak_from_this()] (const auto& future) -> NProto::TError {
            auto self = weak.lock();
            if (!self) {
                return ErrorInvalidState();
            }

            auto& response = future.GetValue();
            if (HasError(response)) {
                return ReplyEntry(ctx, {});
            }

            return ReplyEntry(ctx, response.GetNode());
        });
}

TFuture<NProto::TError> TFileSystem::Access(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    fuse_access_in arg = {};
    if (!in.Read(arg) || !header.nodeid) {
        ReplyError(ctx, EINVAL);
        return MakeFuture(NProto::TError{});
    }

    STORAGE_DEBUG("Access #" << header.unique
        << " node " << header.nodeid
        << " mode " << arg.mask);

    auto request = CreateRequest<NProto::TAccessNodeRequest>(header.nodeid);
    request->SetMask(arg.mask);

    return Session->AccessNode(ctx->CallContext, std::move(request))
        .Apply([=, weak = weak_from_this()] (const auto& future) -> NProto::TError {
            const auto& response = future.GetValue();

            auto self = weak.lock();
            if (CheckFailed(self, ctx, response.GetError())) {
                return {};
            }

            ReplyOk(ctx);
            return {};
        });
}

TFuture<NProto::TError> TFileSystem::Forget(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    Y_UNUSED(ctx);

    fuse_forget_in arg = {};
    if (!in.Read(arg)) {
        return MakeFuture(ErrorIO());
    }

    STORAGE_DEBUG("Forget #" << header.unique
        << " node " << header.nodeid
        << " count " << arg.nlookup)

    // TODO: Clear cache if any

    // LAME: no reply in protocol
    return MakeFuture(NProto::TError{});
}

TFuture<NProto::TError> TFileSystem::BatchForget(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    Y_UNUSED(ctx);
    Y_UNUSED(header);

    fuse_batch_forget_in arg = {};
    if (!in.Read(arg)) {
        return MakeFuture(ErrorIO());
    }

    for (ui64 i = 0; i < arg.count; ++i) {
        fuse_forget_one forget = {};
        if (!in.Read(forget)) {
            return MakeFuture(ErrorIO());
        }

        // TODO: Clear cache if any
    }

    // LAME: no reply in protocol
    return MakeFuture(NProto::TError{});
}

TFuture<NProto::TError> TFileSystem::MkDir(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    TString name;
    fuse_mkdir_in arg = {};

    if (!in.ReadAll(arg, name) || !name || !header.nodeid) {
        ReplyError(ctx, EINVAL);
        return MakeFuture(NProto::TError{});
    }

    STORAGE_DEBUG("MkDir #" << header.unique
        << " parent " << header.nodeid << " name  " << name.Quote()
        << " mode " << arg.mode << " umask " << arg.umask);

    auto request = CreateRequest<NProto::TCreateNodeRequest>(header.nodeid);
    request->SetName(std::move(name));
    request->SetUid(header.uid);
    request->SetGid(header.gid);

    auto* dir = request->MutableDirectory();
    dir->SetMode(arg.mode & ~(S_IFMT));


    FSyncQueue.Enqueue(header.unique, TNodeId {header.nodeid});
    return Session->CreateNode(ctx->CallContext, std::move(request))
        .Apply([=, weak = weak_from_this()] (const auto& future) -> NProto::TError {
            const auto& response = future.GetValue();

            auto self = weak.lock();
            if (!self) {
                return ErrorInvalidState();
            }

            FSyncQueue.Dequeue(
                ctx->CallContext->RequestId,
                response.GetError(),
                header.nodeid);

            if (CheckFailed(ctx, response.GetError())) {
                return {};
            }

            return ReplyEntry(ctx, response.GetNode());
        });
}

TFuture<NProto::TError> TFileSystem::RmDir(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    TString name;
    if (!in.Read(name) || !name || !header.nodeid) {
        ReplyError(ctx, EINVAL);
        return MakeFuture(NProto::TError{});
    }

    STORAGE_DEBUG("RmDir #" << header.unique
        << " parent " << header.nodeid << " name  " << name.Quote());

    auto request = CreateRequest<NProto::TUnlinkNodeRequest>(header.nodeid);
    request->SetName(std::move(name));
    request->SetUnlinkDirectory(true);

    const auto reqId = ctx->CallContext->RequestId;
    FSyncQueue.Enqueue(reqId, TNodeId {header.nodeid});

    return Session->UnlinkNode(ctx->CallContext, std::move(request))
        .Apply([=, ptr = weak_from_this()] (const auto& future) -> NProto::TError {
            const auto& response = future.GetValue();

            auto self = ptr.lock();
            if (!self) {
                return ErrorInvalidState();
            }

            FSyncQueue.Dequeue(
                ctx->CallContext->RequestId,
                response.GetError(),
                header.nodeid);

            if (CheckFailed(ctx, response.GetError())) {
                return {};
            }

            ReplyOk(ctx);
            return {};
        });
}

TFuture<NProto::TError> TFileSystem::MkNode(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    TString name;
    struct fuse_mknod_in arg = {};
    if (!in.ReadAll(arg, name) || !name || !header.nodeid) {
        ReplyError(ctx, EINVAL);
        return MakeFuture(NProto::TError{});
    }

    STORAGE_DEBUG("MkNode #" << header.unique << " parent " << header.nodeid
        << " name " << name.Quote() << " mode: " << arg.mode);

    auto request = CreateRequest<NProto::TCreateNodeRequest>(header.nodeid);
    request->SetName(std::move(name));
    request->SetUid(header.uid);
    request->SetGid(header.gid);

    if (S_ISREG(arg.mode)) {
        // just an empty file
        auto* file = request->MutableFile();
        file->SetMode(arg.mode & ~(S_IFMT));
    } else if (S_ISSOCK(arg.mode)) {
        // null file type for unix sockets
        auto* socket = request->MutableSocket();
        socket->SetMode(arg.mode & ~(S_IFMT));
    } else {
        ReplyError(ctx, ENOTSUP);
        return MakeFuture(NProto::TError{});
    }

    const auto requestId = ctx->CallContext->RequestId;
    FSyncQueue.Enqueue(requestId, TNodeId {header.nodeid});

    return Session->CreateNode(ctx->CallContext, std::move(request))
        .Apply([=, ptr = weak_from_this()] (const auto& future) -> NProto::TError {
            const auto& response = future.GetValue();

            auto self = ptr.lock();
            if (!self) {
                return ErrorInvalidState();
            }

            FSyncQueue.Dequeue(
                ctx->CallContext->RequestId,
                response.GetError(),
                header.nodeid);

            if (CheckFailed(ctx, response.GetError())) {
                return {};
            }

            return ReplyEntry(ctx, response.GetNode());
        });
}

TFuture<NProto::TError> TFileSystem::Unlink(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    TString name;
    if (!in.Read(name) || !name || !header.nodeid) {
        ReplyError(ctx, EINVAL);
        return MakeFuture(NProto::TError{});
    }

    STORAGE_DEBUG("Unlink #" << header.unique << " parent " << header.nodeid
        << " " << name.Quote());

    auto request = CreateRequest<NProto::TUnlinkNodeRequest>(header.nodeid);
    request->SetName(std::move(name));
    request->SetUnlinkDirectory(false);

    const auto requestId = ctx->CallContext->RequestId;
    FSyncQueue.Enqueue(requestId, TNodeId {header.nodeid});

    return Session->UnlinkNode(ctx->CallContext, std::move(request))
        .Apply([=, ptr = weak_from_this()] (const auto& future) -> NProto::TError{
            const auto& response = future.GetValue();

            auto self = ptr.lock();
            if (!self) {
                return {};
            }

            FSyncQueue.Dequeue(
                requestId,
                response.GetError(),
                TNodeId {header.nodeid});

            if (CheckFailed(ctx, response.GetError())) {
                return {};
            }

            ReplyOk(ctx);
            return {};
        });
}

TFuture<NProto::TError> TFileSystem::Rename(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    TString old;
    TString newname;

    ui32 flags = 0;
    ui64 newparent = 0;

    if (header.opcode == FUSE_RENAME) {
        fuse_rename_in arg;
        if (!in.ReadAll(arg, old, newname) || !old || !newname || !header.nodeid) {
            ReplyError(ctx, EINVAL);
            return MakeFuture(NProto::TError{});
        }

        newparent = arg.newdir;
    } else if (header.opcode == FUSE_RENAME2) {
        fuse_rename2_in arg;
        if (!in.ReadAll(arg, old, newname) || !old || !newname || !header.nodeid) {
            ReplyError(ctx, EINVAL);
            return MakeFuture(NProto::TError{});
        }

        newparent = arg.newdir;
        flags = arg.flags;
    } else {
        Y_ABORT("unexpected op code %d", header.opcode);
    }

    ui32 protoFlags = SystemFlagsToRename(flags);

    STORAGE_DEBUG("Rename #" << header.unique << " parent " << header.nodeid
        << " " << old.Quote() << " ->  newparent " << newparent << " " << newname.Quote()
        << " flags " << flags);

    auto request = CreateRequest<NProto::TRenameNodeRequest>(header.nodeid);
    request->SetName(std::move(old));
    request->SetNewName(std::move(newname));
    request->SetNewParentId(newparent);
    request->SetFlags(protoFlags);

    const auto requestId = ctx->CallContext->RequestId;
    FSyncQueue.Enqueue(requestId, TNodeId {header.nodeid});

    return Session->RenameNode(ctx->CallContext, std::move(request))
        .Apply([=, ptr = weak_from_this()] (const auto& future) -> NProto::TError {
            const auto& response = future.GetValue();

            auto self = ptr.lock();
            if (!self) {
                return ErrorInvalidState();
            }

            FSyncQueue.Dequeue(
                requestId,
                response.GetError(),
                TNodeId {header.nodeid});

            if (CheckFailed(ctx, response.GetError())) {
                return {};
            }

            ReplyOk(ctx);
            return {};
        });
}

TFuture<NProto::TError> TFileSystem::Link(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    TString name;
    struct fuse_link_in arg = {};

    if (!in.ReadAll(arg, name) || !name || !header.nodeid) {
        ReplyError(ctx, EINVAL);
        return MakeFuture(NProto::TError{});
    }

    STORAGE_DEBUG("Link #" << header.unique << " node " << arg.oldnodeid
        << " -> new parent " <<  header.nodeid << " " << name.Quote());

    auto request = CreateRequest<NProto::TCreateNodeRequest>(header.nodeid);
    request->SetName(std::move(name));

    auto* link = request->MutableLink();
    link->SetTargetNode(arg.oldnodeid);

    const auto requestId = ctx->CallContext->RequestId;
    FSyncQueue.Enqueue(requestId, TNodeId {arg.oldnodeid});

    return Session->CreateNode(ctx->CallContext, std::move(request))
        .Apply([=, ptr = weak_from_this()] (const auto& future) -> NProto::TError {
            const auto& response = future.GetValue();

            auto self = ptr.lock();
            if (!self) {
                return ErrorInvalidState();
            }

            FSyncQueue.Dequeue(
                requestId,
                response.GetError(),
                TNodeId {arg.oldnodeid});

            if (CheckFailed(ctx, response.GetError())) {
                return {};
            }

            return ReplyEntry(ctx, response.GetNode());
        });
}

TFuture<NProto::TError> TFileSystem::SymLink(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    TString name, linkname;
    if (!in.ReadAll(name, linkname) || !name || !linkname || !header.nodeid) {
        ReplyError(ctx, EINVAL);
        return MakeFuture(NProto::TError{});
    }

    STORAGE_DEBUG("SymLink #" << header.unique << " parent " << header.nodeid
        << " " <<  name.Quote() << " -> " << linkname.Quote());

    auto request = CreateRequest<NProto::TCreateNodeRequest>(header.nodeid);
    request->SetName(std::move(name));
    request->SetUid(header.uid);
    request->SetGid(header.gid);

    auto* link = request->MutableSymLink();
    link->SetTargetPath(std::move(linkname));

    const auto requestId = ctx->CallContext->RequestId;
    FSyncQueue.Enqueue(requestId, TNodeId {header.nodeid});

    return Session->CreateNode(ctx->CallContext, std::move(request))
        .Apply([=, ptr = weak_from_this()] (const auto& future) -> NProto::TError {
            const auto& response = future.GetValue();

            auto self = ptr.lock();
            if (!self) {
                return ErrorInvalidState();
            }

            FSyncQueue.Dequeue(
                requestId,
                response.GetError(),
                TNodeId {header.nodeid});

            if (CheckFailed(ctx, response.GetError())) {
                return {};
            }

            return ReplyEntry(ctx, response.GetNode());
        });
}

TFuture<NProto::TError> TFileSystem::ReadLink(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    Y_UNUSED(in);

    STORAGE_DEBUG("ReadLink #" << header.nodeid);

    if (!header.nodeid) {
        ReplyError(ctx, EINVAL);
        return MakeFuture(NProto::TError{});
    }

    auto request = CreateRequest<NProto::TReadLinkRequest>(header.nodeid);

    return Session->ReadLink(ctx->CallContext, std::move(request))
        .Apply([=, ptr = weak_from_this()] (const auto& future) -> NProto::TError {
            const auto& response = future.GetValue();

            auto self = ptr.lock();
            if (CheckFailed(self, ctx, response.GetError())) {
                return {};
            }

            return Reply(ctx, response.GetSymLink());
        });
}

}   // namespace NCloud::NFileStore::NVFSVhost
