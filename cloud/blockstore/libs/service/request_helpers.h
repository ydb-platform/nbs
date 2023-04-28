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

IOutputStream& operator <<(IOutputStream& out, const TRequestInfo& info);

////////////////////////////////////////////////////////////////////////////////

ui64 CreateRequestId();

template <typename T>
ui64 GetRequestId(const T& request);

template <typename T>
TString GetRequestDetails(const T& request);

template <typename T>
ui32 GetBlocksCount(const T& request);

template <typename T>
ui64 CalculateBytesCount(const T& request, const ui32 blockSize);

template <typename T>
ui32 CalculateWriteRequestBlockCount(const T& request, const ui32 blockSize);

template <typename T>
ui64 GetStartIndex(const T& request);

template <typename T>
TString GetDiskId(const T& request);

template <typename T>
TString GetClientId(const T& request);

template <typename T>
constexpr EBlockStoreRequest GetBlockStoreRequest();

constexpr bool IsReadWriteRequest(EBlockStoreRequest requestType);
constexpr bool IsNonLocalReadWriteRequest(EBlockStoreRequest requestType);

constexpr EBlockStoreRequest TranslateLocalRequestType(
    EBlockStoreRequest requestType);

constexpr bool IsControlRequest(EBlockStoreRequest requestType);

constexpr bool IsDataChannel(NProto::ERequestSource source);

constexpr bool IsReadRequest(EBlockStoreRequest requestType);
constexpr bool IsWriteRequest(EBlockStoreRequest requestType);

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
