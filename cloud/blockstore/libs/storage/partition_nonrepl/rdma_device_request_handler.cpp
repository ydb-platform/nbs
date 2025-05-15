#include "rdma_device_request_handler.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void ConvertRdmaErrorIfNeeded(ui32 rdmaStatus, NProto::TError& err)
{
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

    SendEvent(RequestInfo->Sender, std::move(response), RequestInfo->Cookie);
    SendEvent(ParentActorId, std::move(completion), RequestId);
}

void IRdmaDeviceRequestHandler::SendEvent(
    NActors::TActorId recipient,
    std::unique_ptr<NActors::IEventBase> ev,
    ui64 cookie) const
{
    auto completionEvent = std::make_unique<NActors::IEventHandle>(
        recipient,
        NActors::TActorId(),
        ev.release(),
        0,
        cookie);
    ActorSystem->Send(completionEvent.release());
}

}   // namespace NCloud::NBlockStore::NStorage
