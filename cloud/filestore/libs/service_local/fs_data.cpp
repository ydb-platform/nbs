#include "fs.h"

#include "lowlevel.h"

#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////
// This wrapper will Release (and not Close) TFileHandle on scope exit

class TFileHandleRef
    : TNonCopyable
{
private:
    TFileHandle Handle;

public:
    explicit TFileHandleRef(const TFile& file)
        : Handle(file.GetHandle())
    {}

    ~TFileHandleRef() noexcept
    {
        Handle.Release();
    }

    operator TFileHandle& () noexcept
    {
        return Handle;
    }
};

////////////////////////////////////////////////////////////////////////////////

static void CompleteReadDataAsyncRequest(
    TFileIOCompletion* completion,
    const NProto::TError& error,
    ui32 bytes);

struct TReadDataAsyncRequest
    : TFileIOCompletion
{
    TFileHandleRef Handle;
    ui64 Offset;
    ui64 Length;
    TString Buffer;
    TPromise<NProto::TReadDataResponse> Response;

    TReadDataAsyncRequest(
        const TFile& file,
        ui64 offset,
        ui64 length,
        TPromise<NProto::TReadDataResponse> response)
        : TFileIOCompletion {.Func = &CompleteReadDataAsyncRequest}
        , Handle(file)
        , Offset(offset)
        , Length(length)
        , Buffer(TString::Uninitialized(length))
        , Response(response)
    {}
};

static void CompleteReadDataAsyncRequest(
    TFileIOCompletion* completion,
    const NProto::TError& error,
    ui32 bytes)
{
    std::unique_ptr<TReadDataAsyncRequest> request {static_cast<TReadDataAsyncRequest*>(completion)};

    NProto::TReadDataResponse response;
    *response.MutableError() = error;
    request->Buffer.resize(bytes);
    response.SetBuffer(std::move(request->Buffer));
    request->Response.SetValue(std::move(response));
}


}    // namespace

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
    const int mode = request.GetMode() ? request.GetMode() : Config->GetDefaultPermissions();

    TFile handle;
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

    session->InsertHandle(handle);

    NProto::TCreateHandleResponse response;
    response.SetHandle(handle.GetHandle());
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

NProto::TReadDataResponse TLocalFileSystem::ReadData(
    const NProto::TReadDataRequest& request)
{
    STORAGE_TRACE("ReadData " << DumpMessage(request));

    auto session = GetSession(request);
    auto handle = session->LookupHandle(request.GetHandle());
    if (!handle.IsOpen()) {
        return TErrorResponse(ErrorInvalidHandle(request.GetHandle()));
    }

    auto buffer = TString::Uninitialized(request.GetLength());
    size_t bytesRead = handle.Pread(
        const_cast<char*>(buffer.data()),
        buffer.size(),
        request.GetOffset());
    buffer.resize(bytesRead);

    NProto::TReadDataResponse response;
    response.SetBuffer(std::move(buffer));

    return response;
}

NProto::TWriteDataResponse TLocalFileSystem::WriteData(
    const NProto::TWriteDataRequest& request)
{
    STORAGE_TRACE("WriteData " << DumpMessage(request));

    auto session = GetSession(request);
    auto handle = session->LookupHandle(request.GetHandle());
    if (!handle.IsOpen()) {
        return TErrorResponse(ErrorInvalidHandle(request.GetHandle()));
    }

    const auto& buffer = request.GetBuffer();
    handle.Pwrite(buffer.data(), buffer.size(), request.GetOffset());
    handle.Flush(); // TODO

    return {};
}

NProto::TAllocateDataResponse TLocalFileSystem::AllocateData(
    const NProto::TAllocateDataRequest& request)
{
    STORAGE_TRACE("AllocateData " << DumpMessage(request)
        << " flags: " << FallocateFlagsToString(request.GetFlags()));

    auto session = GetSession(request);
    auto handle = session->LookupHandle(request.GetHandle());
    if (!handle.IsOpen()) {
        return TErrorResponse(ErrorInvalidHandle(request.GetHandle()));
    }

    const int flags = FallocateFlagsToSystem(request.GetFlags());

    NLowLevel::Allocate(handle, flags, request.GetOffset(), request.GetLength());
    handle.Flush(); // TODO

    return {};
}

TFuture<NProto::TReadDataResponse> TLocalFileSystem::ReadDataAsync(
    const NProto::TReadDataRequest& request)
{
    STORAGE_TRACE("ReadDataAsync " << DumpMessage(request));

    auto session = GetSession(request);
    auto file = session->LookupHandle(request.GetHandle());
    if (!file.IsOpen()) {
        return MakeFuture<NProto::TReadDataResponse>(
            TErrorResponse(ErrorInvalidHandle(request.GetHandle())));
    }

    auto response = NewPromise<NProto::TReadDataResponse>();
    auto readReq = std::make_unique<TReadDataAsyncRequest>(
        file,
        request.GetOffset(),
        request.GetLength(),
        response);

    FileIOService->AsyncRead(
        readReq->Handle,
        readReq->Offset,
        {const_cast<char*>(readReq->Buffer.data()), readReq->Length},
        readReq.get());

    Y_UNUSED(readReq.release());   // ownership transferred

    return response;
}

TFuture<NProto::TWriteDataResponse> TLocalFileSystem::WriteDataAsync(
    const NProto::TWriteDataRequest& request)
{
    Y_UNUSED(request);
    return {};
}

}   // namespace NCloud::NFileStore
