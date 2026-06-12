#include "fs_impl.h"

#include "fs_impl_data.h"
#include "fuse.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/vfs/fsync_queue.h>

#include <cloud/storage/core/libs/common/aligned_buffer.h>

#include <library/cpp/threading/future/subscription/wait_all.h>

#include <fcntl.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsAligned(ui64 size, ui32 block)
{
    return AlignUp<ui64>(size, block) == size;
}

ui32 GetWriteSyncFlags(int flags)
{
    return flags & (O_SYNC | O_DSYNC);
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
// zero-copy iovec helpers

NProto::TError FillReadDataIovecs(
    const struct iovec* iov,
    int count,
    ui64 length,
    google::protobuf::RepeatedPtrField<NProto::TIovec>* iovecs)
{
    iovecs->Reserve(count);

    ui64 remainingSize = length;
    for (int index = 0; index < count; index++) {
        if (remainingSize == 0) {
            break;
        }
        const auto dataSize = std::min<ui64>(remainingSize, iov[index].iov_len);
        auto* iovec = iovecs->Add();
        iovec->SetBase(reinterpret_cast<ui64>(iov[index].iov_base));
        iovec->SetLength(dataSize);
        remainingSize -= dataSize;
    }

    if (remainingSize != 0) {
        return MakeError(
            E_FS_INVAL,
            "request length exceeds fuse buffer space");
    }

    return {};
}

NProto::TError FillReadDataIovecs(
    fuse_req_t req,
    ui64 length,
    google::protobuf::RepeatedPtrField<NProto::TIovec>* iovecs)
{
    struct iovec* iov = nullptr;
    int count = 0;
    int ret = fuse_out_buf(req, &iov, &count);
    if (ret == -1 || count <= 1) {
        return MakeError(
            E_FS_INVAL,
            TStringBuilder() << "Invalid fuse out buffers, ret=" << ret
                             << ", count=" << count);
    }

    // skip the first fuse out iovec where the headers are kept, the rest of the
    // iovecs contain pointers to data buffers
    return FillReadDataIovecs(iov + 1, count - 1, length, iovecs);
}

void FillWriteDataIovecs(
    const fuse_bufvec* bufv,
    google::protobuf::RepeatedPtrField<NProto::TIovec>* iovecs)
{
    iovecs->Reserve(bufv->count);
    for (size_t index = 0; index < bufv->count; ++index) {
        const auto* srcFuseBuf = &bufv->buf[index];
        if (srcFuseBuf->size == 0) {
            continue;
        }

        auto* iovec = iovecs->Add();
        iovec->SetBase(reinterpret_cast<ui64>(srcFuseBuf->mem));
        iovec->SetLength(srcFuseBuf->size);
    }
}

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
    // Kernel read requests can occur even on write-only files when write-back
    // caching is enabled (read-modify-write non page aligned range). Open
    // files with read/write access to support this.
    const auto overrideRead =
        Config->GetGuestWriteBackCacheEnabled()
            ? ProtoFlag(NProto::TCreateHandleRequest::E_READ)
            : 0;
    request->SetFlags(flags | overrideRead);
    if (HasFlag(flags, NProto::TCreateHandleRequest::E_CREATE)) {
        SetUserNGroup(*request, fuse_req_ctx(req));
    }

    const auto reqId = callContext->RequestId;
    FSyncQueue->Enqueue(reqId, TNodeId {parent});

    const ui64 version = GlobalCacheVersion.load(std::memory_order_acquire);

    Session->CreateHandle(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue->Dequeue(reqId, error, TNodeId {parent});

            if (CheckResponse(self, *callContext, req, response)) {
                if (HasFlag(flags, NProto::TCreateHandleRequest::E_CREATE)) {
                    self->InvalidateDirectoryEntriesInCache(parent);
                }
                self->ReplyCreateWithCache(
                    *callContext,
                    error,
                    req,
                    response.GetHandle(),
                    response.GetNodeAttr(),
                    version);
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
    // Kernel read requests can occur even on write-only files when write-back
    // caching is enabled (read-modify-write non page aligned range). Open
    // files with read/write access to support this.
    const auto overrideRead =
        Config->GetGuestWriteBackCacheEnabled()
            ? ProtoFlag(NProto::TCreateHandleRequest::E_READ)
            : 0;
    request->SetFlags(flags | overrideRead);

    Session->CreateHandle(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            const auto& response = future.GetValue();
            if (auto self = ptr.lock(); CheckResponse(self, *callContext, req, response)) {
                const auto& response = future.GetValue();

                fuse_file_info fi = {};
                fi.fh = response.GetHandle();
                if (self->Config->GetGuestPageCacheDisabled()) {
                    fi.direct_io = 1;
                }
                if (response.GetGuestKeepCache() &&
                    self->Config->GetGuestKeepCacheAllowed())
                {
                    fi.keep_cache = 1;
                }

                self->ReplyOpen(*callContext, response.GetError(), req, &fi);
            }
        });
}

bool TFileSystem::ProcessAsyncRelease(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    ui64 fh,
    const NCloud::NProto::TError& writeBackCacheError)
{
    with_lock (HandleOpsQueueLock) {
        const auto res = HandleOpsQueue->AddDestroyRequest(ino, fh);
        if (res == THandleOpsQueue::EResult::QueueOverflow) {
            STORAGE_DEBUG(
                "HandleOpsQueue overflow, can't add destroy handle request to "
                "queue #"
                << ino << " @" << fh);
            return false;
        }
        if (res == THandleOpsQueue::EResult::SerializationError) {
            TStringBuilder msg;
            msg << "Unable to add DestroyHandleRequest to HandleOpsQueue #"
                << ino << " @" << fh << ". Serialization failed";

            ReportHandleOpsQueueProcessError(msg);

            ReplyError(*callContext, MakeError(E_FAIL, msg), req, 0);
            return true;
        }
    }

    STORAGE_DEBUG(
        "Destroy handle request added to queue #" << ino << " @" << fh);

    if (CheckError(*callContext, req, writeBackCacheError)) {
        ReplyError(*callContext, {}, req, 0);
    }
    return true;
}

void TFileSystem::ReleaseImpl(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    ui64 handle,
    const NCloud::NProto::TError& writeBackCacheError)
{
    // If WriteBackCache was used, Release() previously asked it to flush the
    // data related to this handle and release the references to it. It could
    // return an error if data has been lost due to flush failure.
    //
    // We should propagate writeBackCacheError to the caller:
    // - DestroyHandle fails -> return DestroyHandle error;
    // - DestroyHandle succeeds -> return writeBackCacheError

    if (Config->GetAsyncDestroyHandleEnabled()) {
        if (!ProcessAsyncRelease(
                callContext,
                req,
                ino,
                handle,
                writeBackCacheError))
        {
            with_lock (DelayedReleaseQueueLock) {
                DelayedReleaseQueue.push(TReleaseRequest(
                    callContext,
                    req,
                    ino,
                    handle,
                    writeBackCacheError));
            }
        }
        return;
    }

    auto request = StartRequest<NProto::TDestroyHandleRequest>(ino);
    request->SetHandle(handle);
    Session->DestroyHandle(callContext, std::move(request))
        .Subscribe(
            [=,
             writeBackCacheError = writeBackCacheError,
             ptr = weak_from_this()](const auto& future)
            {
                auto self = ptr.lock();
                if (!self) {
                    return;
                }

                const auto& response = future.GetValue();
                if (CheckResponse(self, *callContext, req, response) &&
                    self->CheckError(*callContext, req, writeBackCacheError))
                {
                    self->ReplyError(*callContext, {}, req, 0);
                }
            });
}

void TFileSystem::Release(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi)
{
    auto handle = fi->fh;

    STORAGE_DEBUG("Release #" << ino << " @" << handle);

    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    if (WriteBackCache) {
        // ReleaseHandle ensures that the handle is no longer in use by
        // WriteBackCache (regardless of its result). It may return an error if
        // data has been lost due to flush failure - this error should be
        // propagated to the caller
        WriteBackCache.ReleaseHandle(ino, handle)
            .Subscribe(
                [=, ptr = weak_from_this()](const auto& future)
                {
                    if (auto self = ptr.lock()) {
                        const NProto::TError& error = future.GetValue();
                        self->ReleaseImpl(callContext, req, ino, handle, error);
                    }
                });
        return;
    }

    ReleaseImpl(std::move(callContext), req, ino, handle, {});
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

    // Build iovecs for both local and Kikimr services - the data is read
    // directly into the fuse output buffers (shared memory).
    if (Config->GetZeroCopyReadEnabled()) {
        auto error =
            FillReadDataIovecs(req, request->GetLength(), request->MutableIovecs());
        if (HasError(error)) {
            STORAGE_ERROR("Read zero-copy setup failed: " << error.GetMessage());
            ReplyError(*callContext, error, req, EINVAL);
            return;
        }
    }

    auto callback = [=, ptr = weak_from_this()](const auto& future)
    {
        auto self = ptr.lock();
        if (!self) {
            return;
        }

        const auto& response = future.GetValue();
        if (CheckResponse(self, *callContext, req, response)) {
            // Depending on the configuration of the filestore, data may still
            // be returned as a Buffer even when I/O vectors are provided in the
            // request.
            const auto& buffer = response.GetBuffer();
            if (buffer.empty()) {
                self->ReplyBuf(
                    *callContext,
                    response.GetError(),
                    req,
                    nullptr,
                    response.GetLength());
            } else {
                ui32 bufferOffset = response.GetBufferOffset();
                self->ReplyBuf(
                    *callContext,
                    response.GetError(),
                    req,
                    buffer.data() + bufferOffset,
                    buffer.size() - bufferOffset);
            }
        }
    };

    const auto strategy = GetWriteBackCacheRequestStrategy(fi);

    switch (strategy) {
        case EWriteBackCacheRequestStrategy::DoNotUse: {
            Session->ReadData(callContext, std::move(request))
                .Subscribe(std::move(callback));
            break;
        }
        case EWriteBackCacheRequestStrategy::UseDirect: {
            WriteBackCache
                .ReadDataDirect(std::move(callContext), std::move(request))
                .Subscribe(std::move(callback));
            break;
        }
        case EWriteBackCacheRequestStrategy::UseNonDirect: {
            WriteBackCache.ReadData(callContext, std::move(request))
                .Subscribe(std::move(callback));
            break;
        }
    }
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
    request->SetBufferOffset(alignedBuffer.AlignedDataOffset());
    request->SetBuffer(alignedBuffer.TakeBuffer());
    request->SetFlags(GetWriteSyncFlags(fi->flags));


    DoWrite(callContext, req, ino, std::move(request), buffer.size(), fi);
}

void TFileSystem::DoWrite(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    std::shared_ptr<NProto::TWriteDataRequest> request,
    ui64 size,
    fuse_file_info* fi)
{
    const auto strategy = GetWriteBackCacheRequestStrategy(fi);

    const auto handle = fi->fh;
    const auto reqId = callContext->RequestId;

    auto callback = [=, ptr = weak_from_this()](const auto& future)
    {
        auto self = ptr.lock();
        if (!self) {
            return;
        }

        const auto& response = future.GetValue();
        const auto& error = response.GetError();

        if (strategy != EWriteBackCacheRequestStrategy::UseNonDirect) {
            self->FSyncQueue
                ->Dequeue(reqId, error, TNodeId{ino}, THandle{handle});
        }

        if (self->CheckResponse(self, *callContext, req, response)) {
            //
            // Disallow result caching for all concurrently running operations
            // that read node attributes.
            //

            self->InvalidateNodeInCache(ino);
            self->ReplyWrite(*callContext, error, req, size);
        }
    };

    if (strategy != EWriteBackCacheRequestStrategy::UseNonDirect) {
        FSyncQueue->Enqueue(reqId, TNodeId{ino}, THandle{handle});
    }

    switch (strategy) {
        case EWriteBackCacheRequestStrategy::UseNonDirect: {
            WriteBackCache.WriteData(callContext, std::move(request))
                .Subscribe(std::move(callback));
            break;
        }
        case EWriteBackCacheRequestStrategy::DoNotUse: {
            Session->WriteData(callContext, std::move(request))
                .Subscribe(std::move(callback));
            break;
        }
        case EWriteBackCacheRequestStrategy::UseDirect: {
            WriteBackCache
                .WriteDataDirect(std::move(callContext), std::move(request))
                .Subscribe(std::move(callback));
            break;
        }
    }
}

void TFileSystem::WriteBuf(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_bufvec* bufv,
    off_t offset,
    fuse_file_info* fi)
{
    if (!ValidateNodeId(*callContext, req, ino)) {
        return;
    }

    size_t size = fuse_buf_size(bufv);
    STORAGE_DEBUG("WriteBuf #" << ino << " @" << fi->fh
        << " offset:" << offset
        << " size:" << size);

    auto align = Config->GetDirectIoEnabled() ? Config->GetDirectIoAlign() : 0;
    callContext->Unaligned = !IsAligned(offset, Config->GetBlockSize())
        || !IsAligned(size, Config->GetBlockSize());
    auto request = StartRequest<NProto::TWriteDataRequest>(ino);

    const bool isZeroCopyWrite = Config->GetZeroCopyWriteEnabled();
    if (!isZeroCopyWrite) {
        TAlignedBuffer alignedBuffer(size, align);

        fuse_bufvec dst = FUSE_BUFVEC_INIT(size);
        dst.buf[0].mem = (void*)(alignedBuffer.Begin());

        ssize_t res = fuse_buf_copy(
            &dst,
            bufv
#if !defined(FUSE_VIRTIO)
            ,
            fuse_buf_copy_flags(0)
#endif
        );
        if (res < 0) {
            ReplyError(*callContext, MakeError(res), req, res);
            return;
        }
        Y_ABORT_UNLESS((size_t)res == size);
        request->SetBufferOffset(alignedBuffer.AlignedDataOffset());
        request->SetBuffer(alignedBuffer.TakeBuffer());
    } else {
        // Zero-copy write for both local and Kikimr services - the data is
        // written directly from the fuse input buffers (shared memory).
        FillWriteDataIovecs(bufv, request->MutableIovecs());
    }
    request->SetHandle(fi->fh);
    request->SetOffset(offset);
    request->SetFlags(GetWriteSyncFlags(fi->flags));

    DoWrite(callContext, req, ino, std::move(request), size, fi);
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
    FSyncQueue->Enqueue(reqId, TNodeId {ino}, THandle {handle});

    Session->AllocateData(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& response = future.GetValue();
            const auto& error = response.GetError();
            self->FSyncQueue->Dequeue(reqId, error, TNodeId {ino}, THandle {handle});

            if (CheckResponse(self, *callContext, req, response)) {
                //
                // Disallow result caching for all concurrently running
                // operations that read node attributes.
                //

                InvalidateNodeInCache(ino);
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
    requestInfo.SetLoopThreadId(callContext->LoopThreadId);

    auto callback = [=, ptr = weak_from_this(), requestInfo = std::move(requestInfo)]
        (const auto& future) mutable {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& error = future.GetValue();

            FinalizeProfileLogRequestInfo(
                std::move(requestInfo),
                Now(),
                self->Config->GetFileSystemId(),
                error,
                self->ProfileLog);

            if (self->CheckError(*callContext, req, error)) {
                self->ReplyError(*callContext, error, req, 0);
            }
        };

    auto fsyncQueueFuture = FSyncQueue->WaitForDataRequests(
        reqId,
        TNodeId {ino},
        THandle {fi->fh});

    auto future = fsyncQueueFuture;

    if (WriteBackCache) {
        auto writeBackCacheFlushFuture = WriteBackCache.FlushNodeData(ino);
        auto waitAll =
            NWait::WaitAll(fsyncQueueFuture, writeBackCacheFlushFuture);
        future = waitAll.Apply(
            [=](const auto&)
            {
                const auto& flushError = writeBackCacheFlushFuture.GetValue();
                const auto& fsyncQueueError = fsyncQueueFuture.GetValue();
                return HasError(flushError) ? flushError : fsyncQueueError;
            });
    }

    future.Subscribe(std::move(callback));
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
    requestInfo.SetLoopThreadId(callContext->LoopThreadId);

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

    TFuture<NProto::TError> fsyncQueueFuture;

    if (fi) {
        if (datasync) {
            fsyncQueueFuture = FSyncQueue->WaitForDataRequests(
                reqId,
                TNodeId {ino},
                THandle {fi->fh});
        } else {
            fsyncQueueFuture = FSyncQueue->WaitForRequests(
                reqId,
                TNodeId {ino});
        }
    } else {
        if (datasync) {
            fsyncQueueFuture = FSyncQueue->WaitForDataRequests(reqId);
        } else {
            fsyncQueueFuture = FSyncQueue->WaitForRequests(reqId);
        }
    }

    auto future = fsyncQueueFuture;

    if (WriteBackCache) {
        auto convertOK = [] (const auto&) { return MakeError(S_OK); };

        TFuture<NProto::TError> writeBackCacheFlushFuture;
        if (fi) {
            writeBackCacheFlushFuture = WriteBackCache.FlushNodeData(ino)
                .Apply(convertOK);
        } else {
            writeBackCacheFlushFuture = WriteBackCache.FlushAllData()
                .Apply(convertOK);
        }

        future = NWait::WaitAll(fsyncQueueFuture, writeBackCacheFlushFuture)
            .Apply([=] (const auto&) { return fsyncQueueFuture.GetValue(); });
    }

    future.Subscribe(std::move(callback));
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
    requestInfo.SetLoopThreadId(callContext->LoopThreadId);

    auto callback = [=, ptr = weak_from_this(), requestInfo = std::move(requestInfo)]
        (const auto& future) mutable {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            const auto& error = future.GetValue();

            FinalizeProfileLogRequestInfo(
                std::move(requestInfo),
                Now(),
                self->Config->GetFileSystemId(),
                error,
                self->ProfileLog);

            if (self->CheckError(*callContext, req, error)) {
                self->ReplyError(*callContext, error, req, 0);
            }
        };

    auto waitCallback =
        [ptr = weak_from_this(),
         callContext,
         ino,
         datasync,
         callback = std::move(callback)] (const auto& future) mutable
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

    TFuture<NProto::TError> fsyncQueueFuture;

    if (datasync) {
        fsyncQueueFuture = FSyncQueue->WaitForDataRequests(reqId);
    } else {
        fsyncQueueFuture = FSyncQueue->WaitForRequests(reqId);
    }

    auto future = fsyncQueueFuture;

    if (WriteBackCache) {
        auto writeBackCacheFlushFuture = WriteBackCache.FlushAllData()
            .Apply([] (const auto&) { return MakeError(S_OK); });

        future = NWait::WaitAll(fsyncQueueFuture, writeBackCacheFlushFuture)
            .Apply([=] (const auto&) { return fsyncQueueFuture.GetValue(); });
    }

    future.Subscribe(std::move(waitCallback));
}

////////////////////////////////////////////////////////////////////////////////

EWriteBackCacheRequestStrategy TFileSystem::GetWriteBackCacheRequestStrategy(
    const fuse_file_info* fi) const
{
    if (!WriteBackCache) {
        return EWriteBackCacheRequestStrategy::DoNotUse;
    }

    switch (WriteBackCache.GetMode()) {
        case EWriteBackCacheMode::Normal:
            return fi->flags & (O_DIRECT | O_SYNC | O_DSYNC)
                       ? EWriteBackCacheRequestStrategy::UseDirect
                       : EWriteBackCacheRequestStrategy::UseNonDirect;

        case EWriteBackCacheMode::Draining:
            return EWriteBackCacheRequestStrategy::UseDirect;

        case EWriteBackCacheMode::Drained:
            return EWriteBackCacheRequestStrategy::DoNotUse;
    }
}

}   // namespace NCloud::NFileStore::NFuse
