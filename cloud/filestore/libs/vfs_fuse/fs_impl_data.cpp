#include "fs_impl.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/vfs/fsync_queue.h>

#include <cloud/storage/core/libs/common/aligned_buffer.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsAligned(ui64 size, ui32 block)
{
    return AlignUp<ui64>(size, block) == size;
}

void InitNodeInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    bool dataOnly,
    TNodeId nodeId,
    THandle handle)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetMode(dataOnly);
    nodeInfo->SetNodeId(ToUnderlying(nodeId));
    nodeInfo->SetHandle(ToUnderlying(handle));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////
// read & write files

void TFileSystem::Create(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t parent,
    TString name,
    mode_t mode,
    fuse_file_info* fi)
{
    const auto [flags, unsupported] = SystemFlagsToHandle(fi->flags);
    STORAGE_DEBUG("Create #" << parent
        << " " << name.Quote()
        << " flags: " << HandleFlagsToString(flags)
        << " unsupported flags: " << unsupported
        << " mode: " << mode);

    if (!ValidateNodeId(*callContext, req, parent)) {
        return;
    }

    auto request = StartRequest<NProto::TCreateHandleRequest>(parent);
    request->SetName(std::move(name));
    request->SetMode(mode & ~(S_IFMT));
    request->SetFlags(flags);
    if (HasFlag(flags, NProto::TCreateHandleRequest::E_CREATE)) {
        SetUserNGroup(*request, fuse_req_ctx(req));
    }

    const auto reqId = callContext->RequestId;
    FSyncQueue.Enqueue(reqId, TNodeId {parent});

    Session->CreateHandle(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue.Dequeue(reqId, error, TNodeId {parent});

            if (CheckResponse(self, *callContext, req, response)) {
                self->ReplyCreate(
                    *callContext,
                    error,
                    req,
                    response.GetHandle(),
                    response.GetNodeAttr());
            }
        });
}

void TFileSystem::Open(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi)
{
    const auto [flags, unsupported] = SystemFlagsToHandle(fi->flags);
    STORAGE_DEBUG("Open #" << ino
        << " flags: " << HandleFlagsToString(flags)
        << " unsupported flags: " << unsupported);

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    auto request = StartRequest<NProto::TCreateHandleRequest>(ino);
    request->SetFlags(flags);

    Session->CreateHandle(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            const auto& response = future.GetValue();
            if (auto self = ptr.lock(); CheckResponse(self, *callContext, req, response)) {
                const auto& response = future.GetValue();

                fuse_file_info fi = {};
                fi.fh = response.GetHandle();

                self->ReplyOpen(*callContext, response.GetError(), req, &fi);
            }
        });
}

void TFileSystem::Release(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi)
{
    STORAGE_DEBUG("Release #" << ino << " @" << fi->fh);

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    if (Config->GetAsyncDestroyHandleEnabled()) {
        STORAGE_DEBUG("Add destroy handle request to queue #" << ino << " @" << fi->fh);
        with_lock(HandleOpsQueueLock) {
            const auto& res = HandleOpsQueue->AddDestroyRequest(ino, fi->fh);
            if (res == THandleOpsQueue::EResult::QueueOveflow) {
                // TODO(#1541): delay request
                STORAGE_ERROR("Failed to add destroy handle request to queue");
                ReplyError(
                    *callContext,
                    MakeError(E_FAIL, "HandleOpsQueue overflow"),
                    req,
                    0);
                return;
            }
            if (res == THandleOpsQueue::EResult::SerializationError) {
                TStringBuilder msg;
                msg << "Unable to add DestroyHandleRequest to HandleOpsQueue #"
                    << ino << " @" << fi->fh << ". Serialization failed";

                ReportHandleOpsQueueProcessError(msg);

                ReplyError(
                    *callContext,
                    MakeError(
                        E_FAIL,
                        msg),
                    req,
                    0);
                return;
            }
        }
        ReplyError(*callContext, {}, req, 0);
        return;
    }

    auto request = StartRequest<NProto::TDestroyHandleRequest>(ino);
    request->SetHandle(fi->fh);
    Session->DestroyHandle(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            const auto& response = future.GetValue();
            if (auto self = ptr.lock(); CheckResponse(self, *callContext, req, response)) {
                self->ReplyError(*callContext, response.GetError(), req, 0);
            }
        });
}

void TFileSystem::Read(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    size_t size,
    off_t offset,
    fuse_file_info* fi)
{
    STORAGE_DEBUG("Read #" << ino << " @" << fi->fh
        << " offset:" << offset
        << " size:" << size);

    if (size > Config->GetMaxBufferSize()) {
        ReplyError(
            *callContext,
            MakeError(
                E_FS_INVAL,
                TStringBuilder() << "Read size " << size
                    << " is greater than max buffer size " << Config->GetMaxBufferSize()),
            req,
            EINVAL);
        return;
    }

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    callContext->Unaligned = !IsAligned(offset, Config->GetBlockSize())
        || !IsAligned(size, Config->GetBlockSize());

    auto request = StartRequest<NProto::TReadDataRequest>(ino);
    request->SetHandle(fi->fh);
    request->SetOffset(offset);
    request->SetLength(size);

    Session->ReadData(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            const auto& response = future.GetValue();
            if (auto self = ptr.lock(); CheckResponse(self, *callContext, req, response)) {
                auto align = Config->GetDirectIoEnabled() ? Config->GetDirectIoAlign() : 0;
                auto [alignedData, size] = TAlignedBuffer::ExtractAlignedData(
                    response.GetBuffer(),
                    align);
                self->ReplyBuf(
                    *callContext,
                    response.GetError(),
                    req,
                    alignedData,
                    size);
            }
        });
}

void TFileSystem::Write(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    TStringBuf buffer,
    off_t offset,
    fuse_file_info* fi)
{
    STORAGE_DEBUG("Write #" << ino << " @" << fi->fh
        << " offset:" << offset
        << " size:" << buffer.size());

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    callContext->Unaligned = !IsAligned(offset, Config->GetBlockSize())
        || !IsAligned(buffer.size(), Config->GetBlockSize());

    auto align = Config->GetDirectIoEnabled() ? Config->GetDirectIoAlign() : 0;
    TAlignedBuffer alignedBuffer(buffer.size(), align);
    memcpy(
        (void*)(alignedBuffer.Begin()),
        (void*)buffer.data(),
        buffer.size());

    auto request = StartRequest<NProto::TWriteDataRequest>(ino);
    request->SetHandle(fi->fh);
    request->SetOffset(offset);
    request->SetBuffer(std::move(alignedBuffer.GetBuffer()));

    const auto handle = fi->fh;
    const auto reqId = callContext->RequestId;
    FSyncQueue.Enqueue(reqId, TNodeId {ino}, THandle {handle});

    Session->WriteData(callContext, std::move(request))
        .Subscribe([=, size = buffer.size(), ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue.Dequeue(reqId, error, TNodeId {ino}, THandle {handle});

            if (CheckResponse(self, *callContext, req, response)) {
                self->ReplyWrite(*callContext, error, req, size);
            }
        });
}

void TFileSystem::WriteBuf(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_bufvec* bufv,
    off_t offset,
    fuse_file_info* fi)
{
    size_t size = fuse_buf_size(bufv);
    STORAGE_DEBUG("WriteBuf #" << ino << " @" << fi->fh
        << " offset:" << offset
        << " size:" << size);

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    auto align = Config->GetDirectIoEnabled() ? Config->GetDirectIoAlign() : 0;
    TAlignedBuffer alignedBuffer(size, align);

    fuse_bufvec dst = FUSE_BUFVEC_INIT(size);
    dst.buf[0].mem = (void*)(alignedBuffer.Begin());

    ssize_t res = fuse_buf_copy(
        &dst, bufv
#if !defined(FUSE_VIRTIO)
        ,fuse_buf_copy_flags(0)
#endif
    );
    if (res < 0) {
        ReplyError(*callContext, MakeError(res), req, res);
        return;
    }
    Y_ABORT_UNLESS((size_t)res == size);

    callContext->Unaligned = !IsAligned(offset, Config->GetBlockSize())
        || !IsAligned(size, Config->GetBlockSize());

    auto request = StartRequest<NProto::TWriteDataRequest>(ino);
    request->SetHandle(fi->fh);
    request->SetOffset(offset);
    request->SetBuffer(std::move(alignedBuffer.GetBuffer()));

    const auto handle = fi->fh;
    const auto reqId = callContext->RequestId;
    FSyncQueue.Enqueue(reqId, TNodeId {ino}, THandle {handle});

    Session->WriteData(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue.Dequeue(reqId, error, TNodeId {ino}, THandle {handle});

            if (CheckResponse(self, *callContext, req, response)) {
                self->ReplyWrite(*callContext, error, req, size);
            }
        });
}

void TFileSystem::FAllocate(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    int mode,
    off_t offset,
    off_t length,
    fuse_file_info* fi)
{
    const ui32 protoFlags = SystemFlagsToFallocate(mode);

    STORAGE_DEBUG("FAllocate #" << ino << " @" << fi->fh
        << " offset:" << offset
        << " size:" << length
        << " mode:" << FallocateFlagsToString(protoFlags));

    if (offset < 0) {
        ReplyError(
            *callContext,
            MakeError(
                E_FS_INVAL,
                TStringBuilder() << "Incompatible offset " << offset),
            req,
            EINVAL);
        return;
    }

    if (length <= 0) {
        ReplyError(
            *callContext,
            MakeError(
                E_FS_INVAL,
                TStringBuilder() << "Incompatible length " << length),
            req,
            EINVAL);
        return;
    }

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    auto request = StartRequest<NProto::TAllocateDataRequest>(ino);
    request->SetHandle(fi->fh);
    request->SetOffset(offset);
    request->SetLength(length);
    request->SetFlags(protoFlags);

    const auto handle = fi->fh;
    const auto reqId = callContext->RequestId;
    FSyncQueue.Enqueue(reqId, TNodeId {ino}, THandle {handle});

    Session->AllocateData(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue.Dequeue(reqId, error, TNodeId {ino}, THandle {handle});

            if (CheckResponse(self, *callContext, req, response)) {
                self->ReplyError(*callContext, error, req, 0);
            }
        });
}

void TFileSystem::Flush(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi)
{
    STORAGE_DEBUG("Flush #" << ino << " @" << fi->fh);

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    const auto reqId = callContext->RequestId;

    NProto::TProfileLogRequestInfo requestInfo;
    InitProfileLogRequestInfo(requestInfo, EFileStoreFuseRequest::Flush, Now());
    InitNodeInfo(requestInfo, true, TNodeId{ino}, THandle{fi->fh});

    auto callback = [=, ptr = weak_from_this(), requestInfo = std::move(requestInfo)]
        (const auto& future) mutable {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();

            FinalizeProfileLogRequestInfo(
                std::move(requestInfo),
                Now(),
                self->Config->GetFileSystemId(),
                response,
                self->ProfileLog);

            if (self->CheckError(*callContext, req, response)) {
                self->ReplyError(*callContext, response, req, 0);
            }
        };

    FSyncQueue.WaitForDataRequests(reqId, TNodeId {ino}, THandle {fi->fh})
        .Subscribe(std::move(callback));
}

void TFileSystem::FSync(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    int datasync,
    fuse_file_info* fi)
{
    STORAGE_DEBUG("FSync #" << ino << " @" << (fi ? fi->fh : -1llu));

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    const auto reqId = callContext->RequestId;

    NProto::TProfileLogRequestInfo requestInfo;
    InitProfileLogRequestInfo(requestInfo, EFileStoreFuseRequest::Fsync, Now());
    InitNodeInfo(
        requestInfo,
        datasync,
        TNodeId{fi ? ino : InvalidNodeId},
        THandle{fi ? fi->fh : InvalidHandle});

    std::function<void(const TFuture<NProto::TError>&)>
    callback = [=, ptr = weak_from_this(), requestInfo = std::move(requestInfo)]
        (const auto& future) mutable {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();

            FinalizeProfileLogRequestInfo(
                std::move(requestInfo),
                Now(),
                self->Config->GetFileSystemId(),
                response,
                self->ProfileLog);

            if (self->CheckError(*callContext, req, response)) {
                self->ReplyError(*callContext, response, req, 0);
            }
        };

    if (fi) {
        callback = [ptr = weak_from_this(),
                    callContext,
                    ino,
                    datasync,
                    fh = fi->fh,
                    callback = std::move(callback)](const auto& future) mutable
        {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            if (HasError(future.GetValue())) {
                callback(future);
                return;
            }

            auto request = StartRequest<NProto::TFsyncRequest>(ino);
            request->SetHandle(fh);
            request->SetDataSync(datasync);
            self->Session->Fsync(callContext, std::move(request))
                .Apply([](const auto& future)
                       { return future.GetValue().GetError(); })
                .Subscribe(std::move(callback));
        };
    }

    if (fi) {
        if (datasync) {
            FSyncQueue.WaitForDataRequests(reqId, TNodeId {ino}, THandle {fi->fh})
                .Subscribe(std::move(callback));
        } else {
            FSyncQueue.WaitForRequests(reqId, TNodeId {ino})
                .Subscribe(std::move(callback));
        }
    } else {
        if (datasync) {
            FSyncQueue.WaitForDataRequests(reqId)
                .Subscribe(std::move(callback));
        } else {
            FSyncQueue.WaitForRequests(reqId)
                .Subscribe(std::move(callback));
        }
    }
}

void TFileSystem::FSyncDir(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    int datasync,
    fuse_file_info* fi)
{
    Y_ABORT_UNLESS(fi);

    STORAGE_DEBUG("FSyncDir #" << ino << " @" << fi->fh);

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    if (!ValidateDirectoryHandle(*callContext, req, ino, fi->fh)) {
        return;
    }

    const auto reqId = callContext->RequestId;

    NProto::TProfileLogRequestInfo requestInfo;
    InitProfileLogRequestInfo(
        requestInfo,
        EFileStoreFuseRequest::FsyncDir,
        Now());
    InitNodeInfo(
        requestInfo,
        datasync,
        TNodeId{ino},
        THandle{fi->fh});

    auto callback = [=, ptr = weak_from_this(), requestInfo = std::move(requestInfo)]
        (const auto& future) mutable {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();

            FinalizeProfileLogRequestInfo(
                std::move(requestInfo),
                Now(),
                self->Config->GetFileSystemId(),
                response,
                self->ProfileLog);

            if (self->CheckError(*callContext, req, response)) {
                self->ReplyError(*callContext, response, req, 0);
            }
        };

    auto waitCallback =
        [ptr = weak_from_this(),
         callContext,
         ino,
         datasync,
         callback = std::move(callback)](const auto& future) mutable
    {
        auto self = ptr.lock();
        if (!self) {
            return;
        }

        if (HasError(future.GetValue())) {
            callback(future);
            return;
        }

        auto request = StartRequest<NProto::TFsyncDirRequest>(ino);
        request->SetDataSync(datasync);
        self->Session->FsyncDir(callContext, std::move(request))
            .Apply([](const auto& future)
                   { return future.GetValue().GetError(); })
            .Subscribe(std::move(callback));
    };

    if (datasync) {
        FSyncQueue.WaitForDataRequests(reqId).Subscribe(
            std::move(waitCallback));
    } else {
        FSyncQueue.WaitForRequests(reqId).Subscribe(
            std::move(waitCallback));
    }
}

}   // namespace NCloud::NFileStore::NFuse
