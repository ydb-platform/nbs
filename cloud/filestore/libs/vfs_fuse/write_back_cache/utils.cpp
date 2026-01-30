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

// static
bool TUtils::IsFullyCoveredByParts(
    const TVector<TCachedDataPart>& parts,
    ui64 byteCount)
{
    if (parts.empty() || parts.front().RelativeOffset != 0 ||
        parts.back().RelativeOffset + parts.back().Data.size() != byteCount)
    {
        return false;
    }

    ui64 offset = 0;

    for (const auto& part: parts) {
        if (part.RelativeOffset != offset) {
            return false;
        }
        offset += part.Data.size();
    }

    return true;
}

// static
NProto::TReadDataResponse TUtils::BuildReadDataResponse(
    const TVector<TCachedDataPart>& parts)
{
    if (parts.empty()) {
        return {};
    }

    const auto length = parts.back().RelativeOffset + parts.back().Data.size();

    auto buf = TString::Uninitialized(length);
    for (const auto& part: parts) {
        part.Data.copy(buf.begin() + part.RelativeOffset, part.Data.size());
    }

    NProto::TReadDataResponse response;
    response.SetBuffer(std::move(buf));
    return response;
}

// static
void TUtils::AugmentReadDataResponse(
    NProto::TReadDataResponse& response,
    const TCachedData& cachedData)
{
    const auto responseData =
        TStringBuf(response.GetBuffer()).Skip(response.GetBufferOffset());

    if (responseData.size() < cachedData.ReadDataByteCount) {
        auto tmp = TString(cachedData.ReadDataByteCount, 0);
        responseData.copy(tmp.begin(), responseData.size());
        response.SetBuffer(std::move(tmp));
        response.SetBufferOffset(0);
    }

    char* dst = response.MutableBuffer()->begin() + response.GetBufferOffset();

    for (const auto& part: cachedData.Parts) {
        part.Data.copy(dst + part.RelativeOffset, part.Data.size());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
