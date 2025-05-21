#include "rdma_device_request_handler.h"

namespace NCloud::NBlockStore::NStorage {

namespace {

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRdmaDeviceRequestHandler::IRdmaDeviceRequestHandler(
        NActors::TActorSystem* actorSystem,
        TNonreplicatedPartitionConfigPtr partConfig,
        TRequestInfoPtr requestInfo,
        ui64 requestId,
        NActors::TActorId parentActorId,
        ui32 requestBlockCount,
        ui32 responseCount)
    : ActorSystem(actorSystem)
    , PartConfig(std::move(partConfig))
    , RequestInfo(std::move(requestInfo))
    , RequestId(requestId)
    , ParentActorId(parentActorId)
    , RequestBlockCount(requestBlockCount)
    , ResponseCount(responseCount)
{}

bool IRdmaDeviceRequestHandler::HandleSubResponse(
    TDeviceRequestRdmaContext& reqCtx,
    ui32 status,
    TStringBuf buffer)
{
    RequestsResult.emplace_back(reqCtx.DeviceIdx);

    if (status == NRdma::RDMA_PROTO_OK) {
        auto err = ProcessSubResponse(reqCtx, buffer);
        if (HasError(err)) {
            RequestsResult.back().Error = err;
            Error = std::move(err);
        }
    } else {
        Error = NRdma::ParseError(buffer);
        ConvertRdmaErrorIfNeeded(status, Error);
        RequestsResult.back().Error = Error;
    }

    --ResponseCount;
    return ResponseCount == 0;
}

void IRdmaDeviceRequestHandler::HandleResponse(
    NRdma::TClientRequestPtr req,
    ui32 status,
    size_t responseBytes)
{
    TRequestScope timer(*RequestInfo);
    auto guard = Guard(Lock);

    auto buffer = req->ResponseBuffer.Head(responseBytes);
    auto* reqCtx = static_cast<TDeviceRequestRdmaContext*>(req->Context.get());

    if (!HandleSubResponse(*reqCtx, status, buffer)) {
        return;
    }

    ProcessError(*ActorSystem, *PartConfig, Error);

    auto response = CreateResponse(std::move(Error));
    auto completion = CreateCompletionEvent();

    timer.Finish();

    ActorSystem
        ->Send(RequestInfo->Sender, response.release(), 0, RequestInfo->Cookie);

    ActorSystem->Send(ParentActorId, completion.release(), 0, RequestId);
}

}   // namespace NCloud::NBlockStore::NStorage
