#include "fs.h"

#include "lowlevel.h"

#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/string/builder.h>

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

    const int flags = HandleFlagsToSystem(request.GetFlags());
    const int mode = request.GetMode()
        ? request.GetMode() : Config->GetDefaultPermissions();

    TFileHandle handle;
    TFileStat stat;
    if (const auto& pathname = request.GetName()) {
        handle = node->OpenHandle(pathname, flags, mode);

        auto newnode = TIndexNode::Create(*node, pathname);
        stat = newnode->Stat();

        session->TryInsertNode(std::move(newnode));
    } else {
        handle = node->OpenHandle(flags);
        stat = node->Stat();
    }

    const FHANDLE fd = handle;
    session->InsertHandle(std::move(handle));

    NProto::TCreateHandleResponse response;
    response.SetHandle(fd);
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

    const FHANDLE fd = *handle;
    auto b = std::move(*request.MutableBuffer());
    TArrayRef<char> data(b.begin(), b.vend());
    auto promise = NewPromise<NProto::TWriteDataResponse>();
    FileIOService->AsyncWrite(*handle, request.GetOffset(), data).Subscribe(
        [b = std::move(b), promise, fd] (const TFuture<ui32>& f) mutable {
            NProto::TWriteDataResponse response;
            try {
                f.GetValue();
                TFileHandle h(fd);
                const bool flushed = h.Flush();
                h.Release();
                if (!flushed) {
                    throw yexception() << "failed to flush " << fd;
                }
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

}   // namespace NCloud::NFileStore
