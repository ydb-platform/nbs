#pragma once

#if !defined(FILESTORE_REQUEST_H)
#error "should not be included directly - include request.h instead"
#endif

#include <cloud/filestore/public/api/protos/action.pb.h>
#include <cloud/filestore/public/api/protos/checkpoint.pb.h>
#include <cloud/filestore/public/api/protos/cluster.pb.h>
#include <cloud/filestore/public/api/protos/const.pb.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/endpoint.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>
#include <cloud/filestore/public/api/protos/headers.pb.h>
#include <cloud/filestore/public/api/protos/locks.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>
#include <cloud/filestore/public/api/protos/ping.pb.h>
#include <cloud/filestore/public/api/protos/session.pb.h>

#include <cloud/storage/core/protos/media.pb.h>
#include <cloud/storage/core/protos/request_source.pb.h>

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

struct TReadDataLocalRequest
    : public TReadDataRequest
{
    TVector<TArrayRef<char>> Buffers;
};

struct TReadDataLocalResponse
    : public TReadDataResponse
{
    TReadDataLocalResponse() = default;

    TReadDataLocalResponse(const TReadDataResponse& base)
        : TReadDataResponse(base)
    {}

    TReadDataLocalResponse(TReadDataResponse&& base)
        : TReadDataResponse(std::move(base))
    {}

    // number of bytes read
    ui64 BytesRead = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataLocalRequest
    : public TWriteDataRequest
{
    TVector<TArrayRef<const char>> Buffers;
    ui64 BytesToWrite = 0;
};

using TWriteDataLocalResponse = TWriteDataResponse;

}   // namespace NProto

namespace NImpl {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept THasFileSystemId = requires (T v)
{
    {v.GetFileSystemId()} -> std::convertible_to<TString>;
    {v.SetFileSystemId(std::declval<TString>())} -> std::same_as<void>;
};

template <typename T>
concept THasStorageMediaKind = requires (T v)
{
    {v.GetStorageMediaKind()} -> std::same_as<NCloud::NProto::EStorageMediaKind>;
    {v.SetStorageMediaKind(std::declval<NCloud::NProto::EStorageMediaKind>())} -> std::same_as<void>;
};

template <typename T>
concept THasResponseHeaders = requires (T v)
{
    {v.GetHeaders()} -> std::convertible_to<NProto::TResponseHeaders>;
    {v.MutableHeaders()} -> std::convertible_to<NProto::TResponseHeaders*>;
    {v.ClearHeaders()} -> std::same_as<void>;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TFileStoreRequest {};

#define FILESTORE_DECLARE_REQUEST(name, ...)                                   \
    template <>                                                                \
    struct TFileStoreRequest<NProto::T##name##Request>                         \
    {                                                                          \
        static constexpr EFileStoreRequest Request = EFileStoreRequest::name;  \
    };                                                                         \
// FILESTORE_DECLARE_REQUEST

FILESTORE_PROTO_REQUESTS(FILESTORE_DECLARE_REQUEST)

#undef FILESTORE_DECLARE_REQUEST

    template <>
    struct TFileStoreRequest<NProto::TReadDataLocalRequest>
    {
        static constexpr EFileStoreRequest Request = EFileStoreRequest::ReadData;
    };

    template <>
    struct TFileStoreRequest<NProto::TWriteDataLocalRequest>
    {
        static constexpr EFileStoreRequest Request = EFileStoreRequest::WriteData;
    };

}    // namespace NImpl

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr EFileStoreRequest GetFileStoreRequest()
{
    return NImpl::TFileStoreRequest<T>::Request;
}

template <typename T>
TString GetClientId(const T& request)
{
    return request.GetHeaders().GetClientId();
}

template <typename T>
ui64 GetSessionSeqNo(const T& request)
{
    return request.GetHeaders().GetSessionSeqNo();
}

template <typename T>
TString GetSessionId(const T& request)
{
    return request.GetHeaders().GetSessionId();
}

template <typename T>
ui64 GetRequestId(const T& request)
{
    return request.GetHeaders().GetRequestId();
}

template <typename T>
TString GetOriginFqdn(const T& request)
{
    return request.GetHeaders().GetOriginFqdn();
}

template <typename T>
TString GetFileSystemId(const T& request)
{
    if constexpr (NImpl::THasFileSystemId<T>) {
        return request.GetFileSystemId();
    }

    return {};
}

template <typename T>
void SetFileSystemId(TString fileSystemId, T& request)
{
    if constexpr (NImpl::THasFileSystemId<T>) {
        request.SetFileSystemId(std::move(fileSystemId));
    }
}

template <typename T>
TString GetStorageMediaKind(const T& request)
{
    if constexpr (NImpl::THasStorageMediaKind<T>) {
        switch (request.GetStorageMediaKind()) {
            case NCloud::NProto::STORAGE_MEDIA_SSD: {
                return "ssd";
            }
            case NCloud::NProto::STORAGE_MEDIA_DEFAULT:
            case NCloud::NProto::STORAGE_MEDIA_HYBRID:
            case NCloud::NProto::STORAGE_MEDIA_HDD: {
                return "hdd";
            }
            default: {
                Y_ABORT(
                    "unknown storage media kind: %u",
                    static_cast<ui32>(request.GetStorageMediaKind()));
            }
        }
    }

    return {};
}

template <typename T>
TString GetRequestName(const T& request)
{
    Y_UNUSED(request);
    return T::descriptor()->name();
}

template <typename T>
TRequestInfo GetRequestInfo(const T& request)
{
    return {
        GetRequestId(request),
        GetRequestName(request),
        GetFileSystemId(request),
        GetSessionId(request),
        GetClientId(request),
    };
}

template <typename T>
consteval bool HasResponseHeaders()
{
    return NImpl::THasResponseHeaders<T>;
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsDataChannel(NCloud::NProto::ERequestSource source)
{
    switch (source) {
        case NCloud::NProto::SOURCE_TCP_DATA_CHANNEL:
        case NCloud::NProto::SOURCE_FD_DATA_CHANNEL:
            return true;

        default:
            return false;
    }
}

}   // namespace NCloud::NFileStore
