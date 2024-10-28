#include "profile_log_events.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/tablet/model/blob.h>
#include <cloud/storage/core/protos/error.pb.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillRange(
    NProto::TProfileLogBlockRange& range,
    ui64 nodeId,
    ui64 handle,
    ui64 offset,
    ui64 bytes)
{
    range.SetNodeId(nodeId);
    if (handle != InvalidHandle) {
        range.SetHandle(handle);
    }
    range.SetOffset(offset);
    range.SetBytes(bytes);
}

template <typename T>
void AddBlobInfo(
    const ui32 blockSize,
    const T& blob,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    auto* info = profileLogRequest.AddBlobsInfo();
    info->SetCommitId(blob.BlobId.CommitId());
    info->SetUnique(blob.BlobId.UniqueId());

    ui64 blockCount = 0;
    ui64 lastBlockIndex = 0;
    ui64 lastNodeId = InvalidNodeId;
    for (const auto& block: blob.Blocks) {
        if (block.BlockIndex == lastBlockIndex + blockCount &&
            block.NodeId == lastNodeId)
        {
            ++blockCount;
        } else {
            if (lastNodeId != InvalidNodeId) {
                FillRange(
                    *info->AddRanges(),
                    lastNodeId,
                    InvalidHandle,
                    lastBlockIndex * blockSize,
                    blockCount * blockSize);
            }

            lastNodeId = block.NodeId;
            lastBlockIndex = block.BlockIndex;
            blockCount = 1;
        }
    }

    if (lastNodeId != InvalidNodeId) {
        FillRange(
            *info->AddRanges(),
            lastNodeId,
                    InvalidHandle,
            lastBlockIndex * blockSize,
            blockCount * blockSize);
    }
}

template<>
void AddBlobInfo(
    const ui32 blockSize,
    const TMergedBlobMeta& blob,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    auto* info = profileLogRequest.AddBlobsInfo();
    info->SetCommitId(blob.BlobId.CommitId());
    info->SetUnique(blob.BlobId.UniqueId());

    FillRange(
        *info->AddRanges(),
        blob.Block.NodeId,
        InvalidHandle,
        static_cast<ui64>(blob.Block.BlockIndex) * blockSize,
        static_cast<ui64>(blob.BlocksCount) * blockSize);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_MATERIALIZE_REQUEST(name, ...) #name,

static const TString SystemRequestNames[] = {
    FILESTORE_SYSTEM_REQUESTS(FILESTORE_MATERIALIZE_REQUEST)
};

#undef FILESTORE_MATERIALIZE_REQUEST

const TString& GetFileStoreSystemRequestName(
    EFileStoreSystemRequest requestType)
{
    size_t index = static_cast<size_t>(requestType);
    if (index >= FileStoreSystemRequestStart &&
        index < FileStoreSystemRequestStart + FileStoreSystemRequestCount)
    {
        return SystemRequestNames[index - FileStoreSystemRequestStart];
    }

    static const TString unknown = "Unknown";
    return unknown;
}

////////////////////////////////////////////////////////////////////////////////

void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    TInstant currentTs)
{
    profileLogRequest.SetTimestampMcs(currentTs.MicroSeconds());
}

void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    EFileStoreSystemRequest requestType,
    TInstant currentTs)
{
    profileLogRequest.SetRequestType(static_cast<ui32>(requestType));
    InitProfileLogRequestInfo(profileLogRequest, currentTs);
}

void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo&& profileLogRequest,
    TInstant currentTs,
    const TString& fileSystemId,
    const NCloud::NProto::TError& error,
    IProfileLogPtr profileLog)
{
    profileLogRequest.SetDurationMcs(
        currentTs.MicroSeconds() - profileLogRequest.GetTimestampMcs());
    profileLogRequest.SetErrorCode(error.GetCode());

    profileLog->Write({fileSystemId, std::move(profileLogRequest)});
}

////////////////////////////////////////////////////////////////////////////////

void AddRange(
    ui64 nodeId,
    ui64 offset,
    ui64 bytes,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    auto* range = profileLogRequest.AddRanges();
    FillRange(*range, nodeId, InvalidHandle, offset, bytes);
}

void AddRange(
    ui64 nodeId,
    ui64 handle,
    ui64 offset,
    ui64 bytes,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    auto* range = profileLogRequest.AddRanges();
    FillRange(*range, nodeId, handle, offset, bytes);
}

void AddCompactionRange(
    ui64 commitId,
    ui32 rangeId,
    ui32 blobsCount,
    ui32 deletionsCount,
    ui32 garbageBlocksCount,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    auto* range = profileLogRequest.AddCompactionRanges();
    range->SetCommitId(commitId);
    range->SetRangeId(rangeId);
    range->SetBlobsCount(blobsCount);
    range->SetDeletionsCount(deletionsCount);
    range->SetGarbageBlocksCount(garbageBlocksCount);
}

template <typename T>
void AddBlobsInfo(
    const ui32 blockSize,
    const TVector<T>& blobs,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    for (const auto& blob: blobs) {
        AddBlobInfo(blockSize, blob, profileLogRequest);
    }
}

template void AddBlobsInfo(
    const ui32 blockSize,
    const TVector<TMixedBlob>& blobs,
    NProto::TProfileLogRequestInfo& profileLogRequest);

template void AddBlobsInfo(
    const ui32 blockSize,
    const TVector<TMixedBlobMeta>& blobs,
    NProto::TProfileLogRequestInfo& profileLogRequest);

template void AddBlobsInfo(
    const ui32 blockSize,
    const TVector<TMergedBlobMeta>& blobs,
    NProto::TProfileLogRequestInfo& profileLogRequest);

}   // namespace NCloud::NFileStore::NStorage
