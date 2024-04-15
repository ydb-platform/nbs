#pragma once

#include "public.h"

#include "context.h"
#include "request.h"

#include <cloud/storage/core/libs/common/helpers.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TRequestInfo
{
    const EBlockStoreRequest Request;
    const ui64 RequestId;
    const TString DiskId;
    const TString ClientId;
    const TString InstanceId;

    TRequestInfo(
            EBlockStoreRequest request,
            ui64 requestId,
            TString diskId,
            TString clientId = {},
            TString instanceId = {})
        : Request(request)
        , RequestId(requestId)
        , DiskId(std::move(diskId))
        , ClientId(std::move(clientId))
        , InstanceId(std::move(instanceId))
    {}
};

IOutputStream& operator<<(IOutputStream& out, const TRequestInfo& info);

////////////////////////////////////////////////////////////////////////////////

ui64 CreateRequestId();

template <typename T>
ui64 GetRequestId(const T& request)
{
    return request.GetHeaders().GetRequestId();
}

////////////////////////////////////////////////////////////////////////////////

// NBS-2966. We can't get BlocksCount for TWriteBlocksRequest.
ui32 GetBlocksCount(const NProto::TWriteBlocksRequest& request) = delete;

template <typename T>
    requires requires(const T& t) { t.BlocksCount; }
ui32 GetBlocksCount(const T& request)
{
    return request.BlocksCount;
}

template <typename T>
    requires requires(const T& t) { t.GetBlocksCount(); }
ui32 GetBlocksCount(const T& request)
{
    return request.GetBlocksCount();
}

////////////////////////////////////////////////////////////////////////////////

ui64 CalculateBytesCount(
    const NProto::TWriteBlocksRequest& request,
    const ui32 blockSize);

template <typename T>
    requires requires(const T& t) { GetBlocksCount(t); }
ui64 CalculateBytesCount(const T& request, const ui32 blockSize)
{
    return GetBlocksCount(request) * static_cast<ui64>(blockSize);
}

////////////////////////////////////////////////////////////////////////////////

ui32 CalculateWriteRequestBlockCount(
    const NProto::TWriteBlocksRequest& request,
    const ui32 blockSize);
ui32 CalculateWriteRequestBlockCount(
    const NProto::TWriteBlocksLocalRequest& request,
    const ui32 blockSize);
ui32 CalculateWriteRequestBlockCount(
    const NProto::TZeroBlocksRequest& request,
    const ui32 blockSize);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires requires(const T& t) { t.GetStartIndex(); }
ui64 GetStartIndex(const T& request)
{
    return request.GetStartIndex();
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString GetRequestDetails(const T& request)
{
    constexpr bool IsTWriteBlocksRequest =
        std::is_base_of_v<T, NProto::TWriteBlocksRequest>;

    constexpr bool HasGetStartIndex =
        requires(const T& t) { t.GetStartIndex(); };

    if constexpr (IsTWriteBlocksRequest) {
        ui64 bytes = 0;
        for (const auto& buffer: request.GetBlocks().GetBuffers()) {
            bytes += buffer.Size();
        }

        return TStringBuilder()
               << " (offset: " << request.GetStartIndex()
               << ", size: " << bytes
               << ", buffers: " << request.GetBlocks().BuffersSize()
               << ")";
    } else if constexpr (HasGetStartIndex) {
        return TStringBuilder()
               << " (offset: " << request.GetStartIndex()
               << ", count: " << GetBlocksCount(request) << ")";
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString GetDiskId(const T& request)
{
    constexpr bool HasGetDiskId = requires { request.GetDiskId(); };
    if constexpr (HasGetDiskId) {
        return request.GetDiskId();
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString GetClientId(const T& request)
{
    return request.GetHeaders().GetClientId();
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr EBlockStoreRequest GetBlockStoreRequest();

constexpr EBlockStoreRequest TranslateLocalRequestType(
    EBlockStoreRequest requestType);

constexpr bool IsControlRequest(EBlockStoreRequest requestType);

constexpr bool IsDataChannel(NProto::ERequestSource source);

constexpr bool IsReadRequest(EBlockStoreRequest requestType);
constexpr bool IsWriteRequest(EBlockStoreRequest requestType);
constexpr bool IsReadWriteRequest(EBlockStoreRequest requestType);
constexpr bool IsNonLocalReadWriteRequest(EBlockStoreRequest requestType);

////////////////////////////////////////////////////////////////////////////////

bool IsReadWriteMode(const NProto::EVolumeAccessMode mode);

TString GetIpcTypeString(NProto::EClientIpcType ipcType);

template <typename T>
bool IsThrottlingDisabled(const T& request);

template <typename T>
consteval bool ShouldBeThrottled();

}   // namespace NCloud::NBlockStore

#define BLOCKSTORE_INCLUDE_REQUEST_HELPERS_INL
#include "request_helpers_inl.h"
#undef BLOCKSTORE_INCLUDE_REQUEST_HELPERS_INL
