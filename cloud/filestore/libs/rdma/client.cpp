#include "client.h"

#include "protocol.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/rdma/iface/client.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <util/generic/ylimits.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

namespace {

using namespace NCloud::NStorage::NRdma;

////////////////////////////////////////////////////////////////////////////////

// Fits an error reply and stays within what a response sent inline can carry.
constexpr size_t MinResponseBufferSize = 200;

size_t ResponseBufferSize(size_t maxResponseBytes)
{
    static const size_t maxEnvelopeSize = [] {
        NProtoPrivate::TTabletRdmaResponse envelope;
        envelope.SetMethodId(Max<ui32>());
        return TProtoMessageSerializer::MessageByteSize(envelope, 0);
    }();

    return Max(maxEnvelopeSize + maxResponseBytes, MinResponseBufferSize);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TClientRequestPtr> AllocateRdmaTabletRequest(
    IClientEndpoint& endpoint,
    IClientRequestHandlerPtr handler,
    const NProtoPrivate::TTabletRdmaRequest& envelope,
    const google::protobuf::Message& request,
    size_t maxResponseBytes)
{
    const size_t requestBodyBytes = request.ByteSizeLong();

    auto [clientRequest, allocationError] = endpoint.AllocateRequest(
        std::move(handler),
        nullptr,   // context
        TProtoMessageSerializer::MessageByteSize(envelope, requestBodyBytes),
        ResponseBufferSize(maxResponseBytes));

    if (HasError(allocationError)) {
        return std::move(allocationError);
    }

    auto error = SafeExecute<NProto::TError>(
        [&]
        {
            const size_t offset =
                TProtoMessageSerializer::SerializeWithDataLength(
                    clientRequest->RequestBuffer,
                    TTabletRdmaProtocol::TabletRequest,
                    0,   // flags
                    envelope,
                    requestBodyBytes);

            auto* body = reinterpret_cast<ui8*>(
                             const_cast<char*>(
                                 clientRequest->RequestBuffer.data())) +
                         offset;
            auto* end = request.SerializeWithCachedSizesToArray(body);
            Y_DEBUG_ABORT_UNLESS(end == body + requestBodyBytes);

            return NProto::TError();
        });

    if (HasError(error)) {
        return error;
    }

    return std::move(clientRequest);
}

TResultOrError<ui32> ParseRdmaTabletResponse(
    const TClientRequest& request,
    ui32 status,
    size_t responseBytes,
    google::protobuf::Message& response)
{
    const TStringBuf buffer = request.ResponseBuffer.Head(responseBytes);

    if (status == RDMA_PROTO_THROTTLED) {
        return MakeError(E_FS_THROTTLED, ParseError(buffer).GetMessage());
    }

    if (status != RDMA_PROTO_OK) {
        return ParseError(buffer);
    }

    auto resultOrError = TTabletRdmaProtocol::Serializer()->Parse(buffer);
    if (HasError(resultOrError)) {
        return resultOrError.GetError();
    }

    auto parsed = resultOrError.ExtractResult();
    if (parsed.MsgId != TTabletRdmaProtocol::TabletResponse) {
        return MakeError(
            E_NOT_IMPLEMENTED,
            "unexpected RDMA message id in response");
    }

    const auto* envelope =
        static_cast<const NProtoPrivate::TTabletRdmaResponse*>(
            parsed.Proto.get());

    if (!response.ParseFromArray(parsed.Data.data(), parsed.Data.size())) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "malformed RDMA response body for method "
                             << envelope->GetMethodId());
    }

    return envelope->GetMethodId();
}

}   // namespace NCloud::NFileStore::NStorage
