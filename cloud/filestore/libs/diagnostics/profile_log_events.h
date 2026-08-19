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

enum class EFileStoreRequest;

namespace NFuse {

////////////////////////////////////////////////////////////////////////////////

void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    EFileStoreRequest requestType,
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

/**
 * @brief Calculates buffer checksums corresponding to the ranges in the
 * supplied profile-log-request.
 *
 * @param buffer - data buffer.
 * @param blockSize - Filesystem block size.
 * @param ignoreBufferOverflow - if true, buffer vs ranges inconsistencies will
 *  be ignored, otherwise a crit event will be raised and calculation will be
 *  aborted.
 * @param fsId - this will be added to crit event message.
 * @param profileLogRequest - Profile log request reference.
 *
 * @return false if the input is inconsistent, true - otherwise.
 */
bool CalculateChecksums(
    const TStringBuf buffer,
    ui32 blockSize,
    bool ignoreBufferOverflow,
    TStringBuf fsId,
    NProto::TProfileLogRequestInfo& profileLogRequest);

/**
 * @brief Calculates request checksums. Copies data in the case when iovecs are
 * used.
 *
 * @param request - Request proto.
 * @param blockSize - Filesystem block size.
 * @param profileLogRequest - Profile log request reference.
 *
 * @return false if the input is inconsistent, true - otherwise.
 */
bool CalculateWriteDataRequestChecksums(
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
