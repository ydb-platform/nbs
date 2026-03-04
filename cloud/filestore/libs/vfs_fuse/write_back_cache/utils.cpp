#include "utils.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/stream/mem.h>
#include <util/string/printf.h>

#include <fcntl.h>

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

    if (!request.GetIovecs().empty()) {
        ui64 totalLength = 0;
        for (const auto& iovec: request.GetIovecs()) {
            if (iovec.GetLength() == 0) {
                return MakeError(
                    E_ARGUMENT,
                    "ReadData request contains an Iovec with zero length");
            }
            totalLength += iovec.GetLength();
        }
        if (totalLength < request.GetLength()) {
            return MakeError(
                E_ARGUMENT,
                Sprintf(
                    "Total length of Iovecs %lu is less than request Length "
                    "%lu",
                    totalLength,
                    request.GetLength()));
        }
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

    if (request.GetFlags() & (O_SYNC | O_DSYNC)) {
        return MakeError(
            E_ARGUMENT,
            "WriteBackCache should not receive requests with O_SYNC or O_DSYNC "
            "flags");
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
