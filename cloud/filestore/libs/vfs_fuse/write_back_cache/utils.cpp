#include "utils.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/printf.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// static
NCloud::NProto::TError TUtils::ValidateReadDataRequest(
    const NProto::TReadDataRequest& request,
    const TString& expectedFileSystemId)
{
    if (request.GetFileSystemId() != expectedFileSystemId) {
        return MakeError(
            E_ARGUMENT,
            Sprintf(
                "ReadData request has invalid FileSystemId, "
                "expected: '%s', actual: '%s'",
                expectedFileSystemId.c_str(),
                request.GetFileSystemId().c_str()));
    }

    if (request.GetLength() == 0) {
        return MakeError(E_ARGUMENT, "ReadData request has zero length");
    }

    return {};
}

// static
NCloud::NProto::TError TUtils::ValidateWriteDataRequest(
    const NProto::TWriteDataRequest& request,
    const TString& expectedFileSystemId)
{
    if (request.HasHeaders()) {
        return MakeError(
            E_ARGUMENT,
            "WriteData request has unexpected Headers field");
    }

    if (request.GetFileSystemId() != expectedFileSystemId) {
        return MakeError(
            E_ARGUMENT,
            Sprintf(
                "WriteData request has invalid FileSystemId, "
                "expected: '%s', actual: '%s'",
                expectedFileSystemId.c_str(),
                request.GetFileSystemId().c_str()));
    }

    if (request.GetIovecs().empty()) {
        if (request.GetBufferOffset() == request.GetBuffer().size()) {
            return MakeError(E_ARGUMENT, "WriteData request has zero length");
        }

        if (request.GetBufferOffset() > request.GetBuffer().size()) {
            return MakeError(
                E_ARGUMENT,
                Sprintf(
                    "WriteData request BufferOffset %u > buffer size %lu",
                    request.GetBufferOffset(),
                    request.GetBuffer().size()));
        }
    } else {
        if (request.GetBufferOffset()) {
            return MakeError(
                E_ARGUMENT,
                "WriteData request BufferOffset is not compatible with Iovecs");
        }

        if (request.GetBuffer()) {
            return MakeError(
                E_ARGUMENT,
                "WriteData request Buffer is not compatible with Iovecs");
        }

        for (const auto& iovec: request.GetIovecs()) {
            if (iovec.GetLength() == 0) {
                return MakeError(
                    E_ARGUMENT,
                    "WriteData request contains an Iovec with zero length");
            }
        }
    }

    return {};
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
