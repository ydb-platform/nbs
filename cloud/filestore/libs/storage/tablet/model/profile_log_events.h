#pragma once

#include <cloud/filestore/libs/diagnostics/public.h>

namespace NCloud::NFileStore::NProto {
    class TProfileLogRequestInfo;
}   // namespace NCloud::NFileStore::NProto

namespace NCloud::NProto {
    class TError;
}   // namespace NCloud::NProto

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SYSTEM_REQUESTS(xxx, ...)                                    \
    xxx(Flush,                              __VA_ARGS__)                       \
    xxx(FlushBytes,                         __VA_ARGS__)                       \
    xxx(Compaction,                         __VA_ARGS__)                       \
    xxx(Cleanup,                            __VA_ARGS__)                       \
    xxx(TrimBytes,                          __VA_ARGS__)                       \
    xxx(CollectGarbage,                     __VA_ARGS__)                       \
    xxx(DeleteGarbage,                      __VA_ARGS__)                       \
    xxx(ReadBlob,                           __VA_ARGS__)                       \
    xxx(WriteBlob,                          __VA_ARGS__)                       \
    xxx(AddBlob,                            __VA_ARGS__)                       \
    xxx(TruncateRange,                      __VA_ARGS__)                       \
    xxx(ZeroRange,                          __VA_ARGS__)                       \
// FILESTORE_SYSTEM_REQUESTS

#define FILESTORE_MATERIALIZE_REQUEST(name, ...) name,

enum class EFileStoreSystemRequest
{
    MIN = 10000,    // to combine with service requests
    FILESTORE_SYSTEM_REQUESTS(FILESTORE_MATERIALIZE_REQUEST)
    MAX
};

#undef FILESTORE_MATERIALIZE_REQUEST

constexpr size_t FileStoreSystemRequestStart =
    static_cast<size_t>(EFileStoreSystemRequest::MIN) + 1;

constexpr size_t FileStoreSystemRequestCount =
    static_cast<size_t>(EFileStoreSystemRequest::MAX) - FileStoreSystemRequestStart;

const TString& GetFileStoreSystemRequestName(
    EFileStoreSystemRequest requestType);

////////////////////////////////////////////////////////////////////////////////

void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    TInstant currentTs);

void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    EFileStoreSystemRequest requestType,
    TInstant currentTs);

void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo&& profileLogRequest,
    TInstant currentTs,
    const TString& fileSystemId,
    const NCloud::NProto::TError& error,
    IProfileLogPtr profileLog);

////////////////////////////////////////////////////////////////////////////////

void AddRange(
    ui64 nodeId,
    ui64 offset,
    ui64 bytes,
    NProto::TProfileLogRequestInfo& profileLogRequest);

void AddRange(
    ui64 nodeId,
    ui64 handle,
    ui64 offset,
    ui64 bytes,
    NProto::TProfileLogRequestInfo& profileLogRequest);

void AddCompactionRange(
    ui64 commitId,
    ui32 rangeId,
    ui32 blobsCount,
    ui32 deletionsCount,
    ui32 garbageBlocksCount,
    NProto::TProfileLogRequestInfo& profileLogRequest);

template <typename T>
void AddBlobsInfo(
    const ui32 blockSize,
    const TVector<T>& blobs,
    NProto::TProfileLogRequestInfo& profileLogRequest);

}   // namespace NCloud::NFileStore::NStorage
