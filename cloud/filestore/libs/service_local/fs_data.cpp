#include "fs.h"

#include "lowlevel.h"

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
    const int mode = request.GetMode()
        ? request.GetMode() : Config->GetDefaultPermissions();

    TFileHandle handle;
    TFileStat stat;
    ui64 nodeId;
    if (const auto& pathname = request.GetName()) {
        handle = node->OpenHandle(pathname, flags, mode);

        auto newnode = TIndexNode::Create(*node, pathname);
        stat = newnode->Stat();
        nodeId = newnode->GetNodeId();

        if (!session->TryInsertNode(
                std::move(newnode),
                node->GetNodeId(),
                pathname))
        {
            return TErrorResponse(ErrorNoSpaceLeft());
        }
    } else {
        handle = node->OpenHandle(flags);
        stat = node->Stat();
        nodeId = node->GetNodeId();
    }

    // Don't persist flags that only make sense on initial open
    flags = flags & ~(O_CREAT | O_EXCL | O_TRUNC);

    auto handleId =
        session->InsertHandle(std::move(handle), nodeId, flags);
    if (!handleId) {
        return TErrorResponse(ErrorNoSpaceLeft());
    }

    NProto::TCreateHandleResponse response;
    response.SetHandle(*handleId);
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

TFuture<NProto::TReadDataResponse> TLocalFileSystem::ReadDataAsync(
    NProto::TReadDataRequest& request)
{
    STORAGE_TRACE("ReadData " << DumpMessage(request));

    auto session = GetSession(request);
    auto* handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return MakeFuture<NProto::TReadDataResponse>(
            TErrorResponse(ErrorInvalidHandle(request.GetHandle())));
    }

    auto b = TString::Uninitialized(request.GetLength());
    NSan::Unpoison(b.Data(), b.Size());

    TArrayRef<char> data(b.begin(), b.vend());
    auto promise = NewPromise<NProto::TReadDataResponse>();
    FileIOService->AsyncRead(*handle, request.GetOffset(), data).Subscribe(
        [b = std::move(b), promise] (const TFuture<ui32>& f) mutable {
            NProto::TReadDataResponse response;
            try {
                auto bytesRead = f.GetValue();
                b.resize(bytesRead);
                response.SetBuffer(std::move(b));
            } catch (...) {
                *response.MutableError() =
                    MakeError(E_IO, CurrentExceptionMessage());
            }
            promise.SetValue(std::move(response));
        });
    return promise;
}

TFuture<NProto::TWriteDataResponse> TLocalFileSystem::WriteDataAsync(
    NProto::TWriteDataRequest& request)
{
    STORAGE_TRACE("WriteData " << DumpMessage(request));

    auto session = GetSession(request);
    auto* handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return MakeFuture<NProto::TWriteDataResponse>(
            TErrorResponse(ErrorInvalidHandle(request.GetHandle())));
    }

    auto b = std::move(*request.MutableBuffer());
    TArrayRef<char> data(b.begin(), b.vend());
    auto promise = NewPromise<NProto::TWriteDataResponse>();
    FileIOService->AsyncWrite(*handle, request.GetOffset(), data).Subscribe(
        [b = std::move(b), promise] (const TFuture<ui32>& f) mutable {
            NProto::TWriteDataResponse response;
            try {
                f.GetValue();
            } catch (...) {
                *response.MutableError() =
                    MakeError(E_IO, CurrentExceptionMessage());
            }
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
