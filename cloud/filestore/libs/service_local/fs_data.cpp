#include "fs.h"

#include "lowlevel.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>

#include <cloud/storage/core/libs/common/aligned_buffer.h>
#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/string/builder.h>
#include <util/system/sanitizers.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

NProto::TCreateHandleResponse TLocalFileSystem::CreateHandle(
    const NProto::TCreateHandleRequest& request)
{
    STORAGE_TRACE("CreateHandle " << DumpMessage(request)
        << " flags: " << HandleFlagsToString(request.GetFlags())
        << " mode: " << request.GetMode());

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidParent(request.GetNodeId()));
    }

    int flags = HandleFlagsToSystem(request.GetFlags());
    if (!Config->GetDirectIoEnabled()) {
        flags &= ~O_DIRECT;
    }
    const int mode = request.GetMode()
        ? request.GetMode() : Config->GetDefaultPermissions();

    NLowLevel::UnixCredentialsGuard credGuard(
        request.GetUid(),
        request.GetGid(),
        Config->GetGuestOnlyPermissionsCheckEnabled());
    NLowLevel::TOpenOrCreateResult openResult;
    TFileStat stat;
    ui64 nodeId;
    if (const auto& pathname = request.GetName()) {
        if (Config->GetGuestOnlyPermissionsCheckEnabled()) {
            openResult = node->OpenOrCreateHandle(pathname, flags, mode);
        } else {
            openResult.Handle = node->OpenHandle(pathname, flags, mode);
        }

        auto newnode = TIndexNode::Create(*node, pathname);

        if (openResult.WasCreated) {
            if (!credGuard.ApplyCredentials(newnode->GetNodeFd())) {
                node->Unlink(pathname, false);
                return TErrorResponse(
                    ErrorFailedToApplyCredentials(pathname));
            }
        }

        stat = newnode->Stat();
        nodeId = newnode->GetNodeId();

        if (!session->TryInsertNode(
                std::move(newnode),
                node->GetNodeId(),
                pathname))
        {
            ReportLocalFsMaxSessionNodesInUse();
            return TErrorResponse(ErrorNoSpaceLeft());
        }
    } else {
        openResult.Handle = node->OpenHandle(flags);
        stat = node->Stat();
        nodeId = node->GetNodeId();
    }

    // Don't persist flags that only make sense on initial open
    flags = flags & ~(O_CREAT | O_EXCL | O_TRUNC);

    auto [handleId, error] =
        session->InsertHandle(std::move(openResult.Handle), nodeId, flags);
    if (HasError(error)) {
        ReportLocalFsMaxSessionFileHandlesInUse();
        return TErrorResponse(error);
    }

    NProto::TCreateHandleResponse response;
    response.SetHandle(handleId);
    ConvertStats(stat, *response.MutableNodeAttr());

    return response;
}

NProto::TDestroyHandleResponse TLocalFileSystem::DestroyHandle(
    const NProto::TDestroyHandleRequest& request)
{
    STORAGE_TRACE("DestroyHandle " << DumpMessage(request));

    auto session = GetSession(request);
    session->DeleteHandle(request.GetHandle());

    return {};
}

TFuture<NProto::TReadDataLocalResponse> TLocalFileSystem::ReadDataLocalAsync(
    NProto::TReadDataLocalRequest& request,
    NProto::TProfileLogRequestInfo& logRequest)
{
    STORAGE_TRACE("ReadDataLocal " << DumpMessage(request));

    auto session = GetSession(request);
    auto* handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return MakeFuture<NProto::TReadDataLocalResponse>(
            TErrorResponse(ErrorInvalidHandle(request.GetHandle())));
    }

    auto promise = NewPromise<NProto::TReadDataLocalResponse>();
    FileIOService->AsyncReadV(*handle, request.GetOffset(), request.Buffers)
        .Subscribe(
            [&logRequest, promise](const TFuture<ui32>& f) mutable
            {
                NProto::TReadDataLocalResponse response;
                try {
                    auto bytesRead = f.GetValue();
                    response.BytesRead = bytesRead;
                } catch (const TServiceError& e) {
                    *response.MutableError() = MakeError(MAKE_FILESTORE_ERROR(
                        ErrnoToFileStoreError(STATUS_FROM_CODE(e.GetCode()))));
                } catch (...) {
                    *response.MutableError() =
                        MakeError(E_IO, CurrentExceptionMessage());
                }
                FinalizeProfileLogRequestInfo(logRequest, response);
                promise.SetValue(std::move(response));
            });
    return promise;
}

TFuture<NProto::TReadDataResponse> TLocalFileSystem::ReadDataAsync(
    NProto::TReadDataRequest& request,
    NProto::TProfileLogRequestInfo& logRequest)
{
    STORAGE_TRACE("ReadData " << DumpMessage(request));

    auto session = GetSession(request);
    auto* handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return MakeFuture<NProto::TReadDataResponse>(
            TErrorResponse(ErrorInvalidHandle(request.GetHandle())));
    }

    auto align = Config->GetDirectIoEnabled() ? Config->GetDirectIoAlign() : 0;
    auto b = std::make_shared<TAlignedBuffer>(request.GetLength(), align);
    NSan::Unpoison(b->Begin(), b->Size());

    TArrayRef<char> data(b->Begin(), b->End());
    auto promise = NewPromise<NProto::TReadDataResponse>();
    FileIOService->AsyncRead(*handle, request.GetOffset(), data).Subscribe(
        [&logRequest, b = std::move(b), promise] (const TFuture<ui32>& f) mutable {
            NProto::TReadDataResponse response;
            try {
                auto bytesRead = f.GetValue();
                b->TrimSize(bytesRead);
                response.SetBufferOffset(b->AlignedDataOffset());
                response.SetBuffer(b->TakeBuffer());
            } catch (const TServiceError& e) {
                *response.MutableError() = MakeError(MAKE_FILESTORE_ERROR(
                    ErrnoToFileStoreError(STATUS_FROM_CODE(e.GetCode()))));
            } catch (...) {
                *response.MutableError() =
                    MakeError(E_IO, CurrentExceptionMessage());
            }
            FinalizeProfileLogRequestInfo(logRequest, response);
            promise.SetValue(std::move(response));
        });
    return promise;
}

TFuture<NProto::TWriteDataLocalResponse> TLocalFileSystem::WriteDataLocalAsync(
    NProto::TWriteDataLocalRequest& request,
    NProto::TProfileLogRequestInfo& logRequest)
{
    STORAGE_TRACE("WriteDataLocal " << DumpMessage(request));

    auto session = GetSession(request);
    auto* handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return MakeFuture<NProto::TWriteDataLocalResponse>(
            TErrorResponse(ErrorInvalidHandle(request.GetHandle())));
    }

    auto promise = NewPromise<NProto::TWriteDataLocalResponse>();
    FileIOService->AsyncWriteV(*handle, request.GetOffset(), request.Buffers)
        .Subscribe(
            [this,
             &logRequest,
             promise,
             bytesExpected = request.BytesToWrite](
                const TFuture<ui32>& f) mutable
            {
                NProto::TWriteDataLocalResponse response;
                try {
                    auto bytesWritten = f.GetValue();
                    if (bytesWritten != bytesExpected) {
                        STORAGE_ERROR(
                            "WriteData bytesWritten=" << bytesWritten
                                                      << " != bytesExpected="
                                                      << bytesExpected);
                        *response.MutableError() = MakeError(E_REJECTED);
                    }
                } catch (const TServiceError& e) {
                    *response.MutableError() = MakeError(MAKE_FILESTORE_ERROR(
                        ErrnoToFileStoreError(STATUS_FROM_CODE(e.GetCode()))));
                } catch (...) {
                    *response.MutableError() =
                        MakeError(E_IO, CurrentExceptionMessage());
                }
                FinalizeProfileLogRequestInfo(logRequest, response);
                promise.SetValue(std::move(response));
            });
    return promise;
}

TFuture<NProto::TWriteDataResponse> TLocalFileSystem::WriteDataAsync(
    NProto::TWriteDataRequest& request,
    NProto::TProfileLogRequestInfo& logRequest)
{
    STORAGE_TRACE("WriteData " << DumpMessage(request));

    auto session = GetSession(request);
    auto* handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return MakeFuture<NProto::TWriteDataResponse>(
            TErrorResponse(ErrorInvalidHandle(request.GetHandle())));
    }

    auto b = std::move(*request.MutableBuffer());
    auto offset = request.GetBufferOffset();
    TArrayRef<char> data(b.begin() + offset, b.vend());
    auto promise = NewPromise<NProto::TWriteDataResponse>();
    FileIOService->AsyncWrite(*handle, request.GetOffset(), data)
        .Subscribe(
            [&logRequest, b = std::move(b), promise](
                const TFuture<ui32>& f) mutable
            {
                NProto::TWriteDataResponse response;
                try {
                    f.GetValue();
                } catch (const TServiceError& e) {
                    *response.MutableError() = MakeError(MAKE_FILESTORE_ERROR(
                        ErrnoToFileStoreError(STATUS_FROM_CODE(e.GetCode()))));
                } catch (...) {
                    *response.MutableError() =
                        MakeError(E_IO, CurrentExceptionMessage());
                }
                FinalizeProfileLogRequestInfo(logRequest, response);
                promise.SetValue(std::move(response));
            });

    return promise;
}

NProto::TAllocateDataResponse TLocalFileSystem::AllocateData(
    const NProto::TAllocateDataRequest& request)
{
    STORAGE_TRACE("AllocateData " << DumpMessage(request)
        << " flags: " << FallocateFlagsToString(request.GetFlags()));

    auto session = GetSession(request);
    auto* handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return TErrorResponse(ErrorInvalidHandle(request.GetHandle()));
    }

    const int flags = FallocateFlagsToSystem(request.GetFlags());

    NLowLevel::Allocate(
        *handle,
        flags,
        request.GetOffset(),
        request.GetLength());
    handle->Flush(); // TODO

    return {};
}

NProto::TFsyncResponse TLocalFileSystem::Fsync(
    const NProto::TFsyncRequest& request)
{
    STORAGE_TRACE("Fsync " << DumpMessage(request));

    auto session = GetSession(request);
    auto* handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return TErrorResponse(ErrorInvalidHandle(request.GetHandle()));
    }

    const bool flushed =
        request.GetDataSync() ? handle->FlushData() : handle->Flush();
    if (!flushed) {
        return TErrorResponse(E_IO, "flush failed");
    }

    return {};
}

NProto::TFsyncDirResponse TLocalFileSystem::FsyncDir(
    const NProto::TFsyncDirRequest& request)
{
    STORAGE_TRACE("FsyncDir " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(request.GetNodeId());
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(request.GetNodeId()));
    }

    auto handle = node->OpenHandle(O_RDONLY|O_DIRECTORY);
    const bool flushed =
        request.GetDataSync() ? handle.FlushData() : handle.Flush();
    if (!flushed) {
        return TErrorResponse(E_IO, "flush failed");
    }

    return {};
}

}   // namespace NCloud::NFileStore
