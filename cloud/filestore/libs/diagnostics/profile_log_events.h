#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NProto {

////////////////////////////////////////////////////////////////////////////////

class TError;

}   // namespace NCloud

namespace NCloud::NFileStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TProfileLogRequestInfo;

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

template <typename TProtoResponse>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const TProtoResponse& response);

}   // namespace NCloud::NFileStore
