#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>

#include <google/protobuf/message.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/strbuf.h>

#include <memory>

namespace NCloud::NFileStore::NProtoPrivate {
    class TTabletRdmaRequest;
}

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Outcome of one tablet request. On success Response is the record for
// MethodId, serialized by the RDMA target straight into the send buffer.
struct TTabletTransportResponse
{
    NCloud::NProto::TError Error;
    ui32 MethodId = 0;
    std::unique_ptr<google::protobuf::Message> Response;

    TTabletTransportResponse() = default;

    explicit TTabletTransportResponse(NCloud::NProto::TError error)
        : Error(std::move(error))
    {}

    TTabletTransportResponse(
            ui32 methodId,
            std::unique_ptr<google::protobuf::Message> response)
        : MethodId(methodId)
        , Response(std::move(response))
    {}
};

////////////////////////////////////////////////////////////////////////////////

// Bridges a decoded RDMA request into local request handling. envelope and
// requestBody are valid only during the call; requestBody points into the
// transport receive buffer.
struct IRdmaTabletRequestHandler
{
    virtual ~IRdmaTabletRequestHandler() = default;

    virtual NThreading::TFuture<TTabletTransportResponse> HandleRequest(
        const NProtoPrivate::TTabletRdmaRequest& envelope,
        TStringBuf requestBody,
        TCallContextBasePtr callContext) = 0;
};

}   // namespace NCloud::NFileStore::NStorage
