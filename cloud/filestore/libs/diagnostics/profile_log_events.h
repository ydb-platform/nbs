#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace google::protobuf {

    template <typename T>
    class RepeatedPtrField;
}   // namespace google::protobuf

namespace NCloud::NProto {

////////////////////////////////////////////////////////////////////////////////

class TError;

}   // namespace NCloud

namespace NCloud::NFileStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TProfileLogRequestInfo;
class TWriteDataRequest;
class TReadDataResponse;
class TIovec;

}   // namespace NProto

namespace NFuse {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_FUSE_REQUESTS(xxx, ...)                                      \
    xxx(Flush,              __VA_ARGS__)                                       \
    xxx(Fsync,              __VA_ARGS__)                                       \
    xxx(FsyncDir,           __VA_ARGS__)                                       \
// FILESTORE_FUSE_REQUESTS

#define FILESTORE_MATERIALIZE_REQUEST(name, ...) name,

enum class EFileStoreFuseRequest
{
    MIN = 1'000,    // to combine with service requests
    FILESTORE_FUSE_REQUESTS(FILESTORE_MATERIALIZE_REQUEST)
    MAX
};

#undef FILESTORE_MATERIALIZE_REQUEST

constexpr size_t FileStoreFuseRequestStart =
    static_cast<size_t>(EFileStoreFuseRequest::MIN) + 1;

constexpr size_t FileStoreFuseRequestCount =
    static_cast<size_t>(EFileStoreFuseRequest::MAX) - FileStoreFuseRequestStart;

const TString& GetFileStoreFuseRequestName(EFileStoreFuseRequest requestType);

void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    EFileStoreFuseRequest requestType,
    TInstant currentTs);

void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo&& profileLogRequest,
    TInstant currentTs,
    const TString& fileSystemId,
    const NCloud::NProto::TError& error,
    IProfileLogPtr profileLog);

}  // namespace NFuse

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoRequest>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const TProtoRequest& request);

void UpdateRangeNodeIds(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    ui64 nodeId);

template <typename TProtoResponse>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const TProtoResponse& response);

void CalculateChecksums(
    const TStringBuf buffer,
    ui32 blockSize,
    NProto::TProfileLogRequestInfo& profileLogRequest);

/**
 * @brief Calculates request checksums. Copies data in the case when iovecs are
 * used.
 *
 * @param request - Request proto.
 * @param blockSize - Filesystem block size.
 * @param profileLogRequest - Profile log request reference.
 */
void CalculateWriteDataRequestChecksums(
    const NProto::TWriteDataRequest& request,
    ui32 blockSize,
    NProto::TProfileLogRequestInfo& profileLogRequest);

/**
 * @brief Calculates response checksums. Copies data in the case when iovecs are
 * used.
 *
 * @param iovecs - Request iovecs.
 * @param response - Response proto.
 * @param blockSize - Filesystem block size.
 * @param profileLogRequest - Profile log request reference.
 */
void CalculateReadDataResponseChecksums(
    const google::protobuf::RepeatedPtrField<NProto::TIovec>& iovecs,
    const NProto::TReadDataResponse& response,
    ui32 blockSize,
    NProto::TProfileLogRequestInfo& profileLogRequest);

}   // namespace NCloud::NFileStore
