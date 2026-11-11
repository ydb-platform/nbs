#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <memory>

namespace google::protobuf {
    class Message;
}

namespace NCloud::NFileStore::NProtoPrivate {
    class TTabletRdmaRequest;
}

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Outcome of one tablet request. On success the body for MethodId is either
// Response (serialized) or ResponseProto (typed record, serialized by the RDMA
// target straight into the send buffer).
struct TTabletTransportResponse
{
    NCloud::NProto::TError Error;
    ui32 MethodId = 0;
    TString Response;
    std::shared_ptr<google::protobuf::Message> ResponseProto;

    TTabletTransportResponse() = default;

    explicit TTabletTransportResponse(NCloud::NProto::TError error)
        : Error(std::move(error))
    {}

    TTabletTransportResponse(ui32 methodId, TString response)
        : MethodId(methodId)
        , Response(std::move(response))
    {}

    TTabletTransportResponse(
            ui32 methodId,
            std::shared_ptr<google::protobuf::Message> responseProto)
        : MethodId(methodId)
        , ResponseProto(std::move(responseProto))
    {}
};

////////////////////////////////////////////////////////////////////////////////

// Bridges a decoded RDMA request into local request handling. envelope and
// requestBody point into the transport receive buffer and are valid only
// during the call.
struct IRdmaTabletRequestHandler
{
    virtual ~IRdmaTabletRequestHandler() = default;

    virtual NThreading::TFuture<TTabletTransportResponse> HandleRequest(
        const NProtoPrivate::TTabletRdmaRequest& envelope,
        TStringBuf requestBody,
        TCallContextBasePtr callContext) = 0;
};

}   // namespace NCloud::NFileStore::NStorage
