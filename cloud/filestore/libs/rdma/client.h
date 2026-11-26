#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>

#include <google/protobuf/message.h>

namespace NCloud::NFileStore::NProtoPrivate {
    class TTabletRdmaRequest;
}

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Allocates a request of the RDMA client endpoint and encodes a tablet request
// into it: the envelope and the record. maxResponseBytes is the largest
// response record that the request can get; a larger one fails the request.
// The caller sends the request through the endpoint, handler gets the
// response.
TResultOrError<NCloud::NStorage::NRdma::TClientRequestPtr>
AllocateRdmaTabletRequest(
    NCloud::NStorage::NRdma::IClientEndpoint& endpoint,
    NCloud::NStorage::NRdma::IClientRequestHandlerPtr handler,
    const NProtoPrivate::TTabletRdmaRequest& envelope,
    const google::protobuf::Message& request,
    size_t maxResponseBytes);

// Decodes the response to such a request, as its handler gets it, into
// response and returns the event type of the response.
//
// Returns an error if the tablet has not answered the request: E_FS_THROTTLED
// if the RDMA server throttled it, any other error is a failure of the
// transport. The tablet's own errors come inside the response record.
TResultOrError<ui32> ParseRdmaTabletResponse(
    const NCloud::NStorage::NRdma::TClientRequest& request,
    ui32 status,
    size_t responseBytes,
    google::protobuf::Message& response);

}   // namespace NCloud::NFileStore::NStorage
