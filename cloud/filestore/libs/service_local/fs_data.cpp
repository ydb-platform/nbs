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
        request.GetGid());
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
            ReportLocalFsMaxSessionNodesInUse();
            return TErrorResponse(ErrorNoSpaceLeft());
        }
    } else {
        handle = node->OpenHandle(flags);
        stat = node->Stat();
        nodeId = node->GetNodeId();
    }

    // Don't persist flags that only make sense on initial open
    flags = flags & ~(O_CREAT | O_EXCL | O_TRUNC);

    auto [handleId, error] =
        session->InsertHandle(std::move(handle), nodeId, flags);
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

    Y_UNUSED(logRequest);
    return MakeFuture<NProto::TReadDataLocalResponse>();
}

TFuture<NProto::TReadDataResponse> TLocalFileSystem::ReadDataAsync(
    NProto::TReadDataRequest& request,
    NProto::TProfileLogRequestInfo& logRequest)
{
    STORAGE_TRACE("ReadData " << DumpMessage(request));
    Y_UNUSED(logRequest);
    return MakeFuture<NProto::TReadDataResponse>();
}

TFuture<NProto::TWriteDataLocalResponse> TLocalFileSystem::WriteDataLocalAsync(
    NProto::TWriteDataLocalRequest& request,
    NProto::TProfileLogRequestInfo& logRequest)
{
    STORAGE_TRACE("WriteDataLocal " << DumpMessage(request));
    Y_UNUSED(logRequest);
    return MakeFuture<NProto::TWriteDataLocalResponse>();
}

TFuture<NProto::TWriteDataResponse> TLocalFileSystem::WriteDataAsync(
    NProto::TWriteDataRequest& request,
    NProto::TProfileLogRequestInfo& logRequest)
{
    STORAGE_TRACE("WriteData " << DumpMessage(request));
    Y_UNUSED(logRequest);
    return MakeFuture<NProto::TWriteDataResponse>();
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
