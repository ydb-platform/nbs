#include "rdma_device_request_handler.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void ConvertRdmaErrorIfNeeded(ui32 rdmaStatus, NProto::TError& err)
{
    // If we know that an agent is unavailable to respond to a request that was
    // submitted, we will cancel that request. We can  receive a message
    // indicating that the agent is unavailable only on the mirror disk, so we
    // convert the error to E_REJECTED and try again immediately.
    if (rdmaStatus == NRdma::RDMA_PROTO_FAIL && err.GetCode() == E_CANCELLED) {
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_INSTANT_RETRIABLE);
        err = MakeError(
            E_REJECTED,
            std::move(*err.MutableMessage()) + ", converted to E_REJECTED",
            flags);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
