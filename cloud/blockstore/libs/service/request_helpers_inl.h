#pragma once

#if !defined(BLOCKSTORE_INCLUDE_REQUEST_HELPERS_INL)
#   error "this file should not be included directly"
#endif

#include <cloud/storage/core/libs/common/random.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

inline IOutputStream& operator <<(IOutputStream& out, const TRequestInfo& info)
{
    if (info.DiskId) {
        out << "[d:" << info.DiskId << "] ";
    }

    if (info.ClientId) {
        out << "[c:" << info.ClientId << "] ";
    }

    if (info.InstanceId) {
        out << "[i:" << info.InstanceId << "] ";
    }

    out << GetBlockStoreRequestName(info.Request);

    if (info.RequestId) {
        out << " #" << info.RequestId;
    }

    return out;
}

////////////////////////////////////////////////////////////////////////////////

inline ui64 CreateRequestId()
{
    return RandInt<ui64, 1>();
}

template <typename T>
ui64 GetRequestId(const T& request)
{
    return request.GetHeaders().GetRequestId();
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString GetClientId(const T& request)
{
    return request.GetHeaders().GetClientId();
}

////////////////////////////////////////////////////////////////////////////////

namespace NImpl {
    Y_HAS_MEMBER(GetStartIndex);

    template <typename T, typename = void>
    struct TGetRequestDetailsSelector;

    template <typename T>
    struct TGetRequestDetailsSelector<T, std::enable_if_t<THasGetStartIndex<T>::value>>
    {
        static TString Get(const T& request)
        {
            return TStringBuilder()
                << " (offset: " << request.GetStartIndex()
                << ", count: " << GetBlocksCount(request)
                << ")";
        }
    };

    template <typename T>
    struct TGetRequestDetailsSelector<T, std::enable_if_t<!THasGetStartIndex<T>::value>>
    {
        static TString Get(const T& request)
        {
            Y_UNUSED(request);
            return {};
        }
    };

    template <>
    struct TGetRequestDetailsSelector<NProto::TWriteBlocksRequest, void>
    {
        static TString Get(const NProto::TWriteBlocksRequest& request)
        {
            ui64 bytes = 0;
            for (const auto& buffer: request.GetBlocks().GetBuffers()) {
                bytes += buffer.Size();
            }

            return TStringBuilder()
                << " (offset: " << request.GetStartIndex()
                << ", size: " << bytes
                << ", buffers: " << request.GetBlocks().BuffersSize()
                << ")";
        }
    };

    template <typename T>
    using TGetRequestDetails = TGetRequestDetailsSelector<T>;

    template <typename T, typename = void>
    struct TGetStartIndexSelector;

    template <typename T>
    struct TGetStartIndexSelector<T, std::enable_if_t<THasGetStartIndex<T>::value>>
    {
        static ui64 Get(const T& request)
        {
            return request.GetStartIndex();
        }
    };

    template <typename T>
    struct TGetStartIndexSelector<T, std::enable_if_t<!THasGetStartIndex<T>::value>>
    {
        static ui64 Get(const T& request)
        {
            Y_UNUSED(request);
            return 0;
        }
    };

    template <typename T>
    using TGetStartIndex = TGetStartIndexSelector<T>;
}   // namespace

template <typename T>
TString GetRequestDetails(const T& request)
{
    return NImpl::TGetRequestDetails<T>::Get(request);
}

template <typename T>
ui64 GetStartIndex(const T& request)
{
    return NImpl::TGetStartIndex<T>::Get(request);
}

////////////////////////////////////////////////////////////////////////////////

namespace NImpl {
    Y_HAS_MEMBER(GetBlocksCount);

    template <typename T, typename = void>
    struct TGetBlocksCountSelector;

    template <typename T>
    struct TGetBlocksCountSelector<T, std::enable_if_t<THasGetBlocksCount<T>::value>>
    {
        static ui32 Get(const T& args)
        {
            return args.GetBlocksCount();
        }
    };

    template <typename T>
    struct TGetBlocksCountSupported
    {
        static constexpr bool Value = true;
    };

    template <>
    struct TGetBlocksCountSupported<NProto::TWriteBlocksRequest>
    {
        static constexpr bool Value = false;
    };

    template <typename T>
    struct TGetBlocksCountSelector<T, std::enable_if_t<!THasGetBlocksCount<T>::value>>
    {
        static_assert(TGetBlocksCountSupported<T>::Value);

        static ui32 Get(const T& args)
        {
            Y_UNUSED(args);
            return 0;
        }
    };

    template <>
    struct TGetBlocksCountSelector<NProto::TWriteBlocksLocalRequest, void>
    {
        static ui32 Get(const NProto::TWriteBlocksLocalRequest& request)
        {
            return request.BlocksCount;
        }
    };

    template <typename T>
    using TGetBlocksCount = TGetBlocksCountSelector<T>;

}   // namespace NImpl

template <typename T>
ui32 GetBlocksCount(const T& request)
{
    return NImpl::TGetBlocksCount<T>::Get(request);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
ui64 CalculateBytesCount(const T& request, const ui32 blockSize)
{
    return GetBlocksCount(request) * static_cast<ui64>(blockSize);
}

template <>
inline ui64 CalculateBytesCount<NProto::TWriteBlocksRequest>(
    const NProto::TWriteBlocksRequest& request,
    const ui32 blockSize)
{
    Y_UNUSED(blockSize);

    ui64 bytes = 0;
    for (const auto& buffer: request.GetBlocks().GetBuffers()) {
        bytes += buffer.Size();
    }
    return bytes;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
ui32 CalculateWriteRequestBlockCount(const T&, const ui32)
{
    return 0;
}

template <>
inline ui32 CalculateWriteRequestBlockCount<NProto::TWriteBlocksRequest>(
    const NProto::TWriteBlocksRequest& request,
    const ui32 blockSize)
{
    ui64 bytes = 0;
    for (const auto& buffer: request.GetBlocks().GetBuffers()) {
        bytes += buffer.Size();
    }

    if (bytes % blockSize) {
        Y_FAIL("bytes %lu not divisible by blockSize %u", bytes, blockSize);
    }

    return bytes / blockSize;
}

template <>
inline ui32 CalculateWriteRequestBlockCount<NProto::TWriteBlocksLocalRequest>(
    const NProto::TWriteBlocksLocalRequest& request,
    const ui32 blockSize)
{
    if (blockSize != request.BlockSize) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
        ui64 sglistSize = 0;
        ui64 sglistBlockSize = 0;
#pragma clang diagnostic pop
        auto guard = request.Sglist.Acquire();
        if (guard && guard.Get().size()) {
            sglistSize = guard.Get().size();
            sglistBlockSize = guard.Get()[0].Size();
        }

        Y_VERIFY_DEBUG(false,
            "blockSize %u != request.BlockSize %u, request.BlocksCount=%u"
            ", sglist size=%lu, sglist buffer size=%lu",
            blockSize,
            request.BlockSize,
            request.BlocksCount,
            sglistSize,
            sglistBlockSize);
    }

    return request.BlocksCount;
}

template <>
inline ui32 CalculateWriteRequestBlockCount<NProto::TZeroBlocksRequest>(
    const NProto::TZeroBlocksRequest& request,
    const ui32 blockSize)
{
    Y_UNUSED(blockSize);

    return request.GetBlocksCount();
}

////////////////////////////////////////////////////////////////////////////////

namespace NImpl {
    Y_HAS_MEMBER(GetDiskId);

    template <typename T, typename = void>
    struct TGetDiskIdSelector;

    template <typename T>
    struct TGetDiskIdSelector<T, std::enable_if_t<THasGetDiskId<T>::value>>
    {
        static TString Get(const T& args)
        {
            return args.GetDiskId();
        }
    };

    template <typename T>
    struct TGetDiskIdSelector<T, std::enable_if_t<!THasGetDiskId<T>::value>>
    {
        static TString Get(const T& args)
        {
            Y_UNUSED(args);
            return {};
        }
    };

    template <typename T>
    using TGetDiskId = TGetDiskIdSelector<T>;

}   // namespace NImpl

template <typename T>
TString GetDiskId(const T& request)
{
    return NImpl::TGetDiskId<T>::Get(request);
}

////////////////////////////////////////////////////////////////////////////////

namespace NImpl {
    template <typename T>
    struct TRequest {};

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    template <>                                                                \
    struct TRequest<NProto::T##name##Request>                                  \
    {                                                                          \
        static constexpr EBlockStoreRequest Request = EBlockStoreRequest::name;\
    };                                                                         \
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

}   // namespace NImpl

template <typename T>
constexpr EBlockStoreRequest GetBlockStoreRequest()
{
    return NImpl::TRequest<T>::Request;
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsReadRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::ReadBlocksLocal:
            return true;

        default:
            return false;
    }
}

constexpr bool IsWriteRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ZeroBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::WriteBlocksLocal:
            return true;

        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsReadWriteRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::ZeroBlocks:
        case EBlockStoreRequest::ReadBlocksLocal:
        case EBlockStoreRequest::WriteBlocksLocal:
            return true;

        default:
            return false;
    }
}

constexpr bool IsNonLocalReadWriteRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::ZeroBlocks:
            return true;

        default:
            return false;
    }
}

constexpr EBlockStoreRequest TranslateLocalRequestType(
    EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocksLocal:
            return EBlockStoreRequest::ReadBlocks;
        case EBlockStoreRequest::WriteBlocksLocal:
            return EBlockStoreRequest::WriteBlocks;

        default:
            return requestType;
    }
}

constexpr bool IsControlRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::Ping:
        case EBlockStoreRequest::MountVolume:
        case EBlockStoreRequest::UnmountVolume:
        case EBlockStoreRequest::UploadClientMetrics:
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::ZeroBlocks:
        case EBlockStoreRequest::ReadBlocksLocal:
        case EBlockStoreRequest::WriteBlocksLocal:
        case EBlockStoreRequest::QueryAvailableStorage:
        case EBlockStoreRequest::ResumeDevice:
            return false;

        default:
            return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsDataChannel(NProto::ERequestSource source)
{
    switch (source) {
        case NProto::SOURCE_TCP_DATA_CHANNEL:
        case NProto::SOURCE_FD_DATA_CHANNEL:
            return true;

        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
bool IsThrottlingDisabled(const T& request)
{
    return HasProtoFlag(request.GetMountFlags(), NProto::MF_THROTTLING_DISABLED);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
consteval bool ShouldBeThrottled() {
    if constexpr (IsReadWriteRequest(GetBlockStoreRequest<T>())) {
        return true;
    } else {
        return false;
    }
}

}   // namespace NCloud::NBlockStore
