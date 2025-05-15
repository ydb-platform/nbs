#include "direct_copy_actor.h"

#include <cloud/blockstore/libs/common/iovector.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
auto PrepareRequest(
    const NProto::TDirectCopyBlocksRequest& copyBlocks,
    T* request)
{
    auto& rec = request->Record;
    auto* headers = rec.MutableHeaders();
    headers->SetIsBackgroundRequest(
        copyBlocks.GetHeaders().GetIsBackgroundRequest());
    headers->SetClientId(TString(copyBlocks.GetTargetClientId()));
    headers->SetVolumeRequestId(copyBlocks.GetHeaders().GetVolumeRequestId());

    rec.SetDeviceUUID(copyBlocks.GetTargetDeviceUUID());
    rec.SetStartIndex(copyBlocks.GetTargetStartIndex());
    rec.SetBlockSize(copyBlocks.GetBlockSize());
}

}   // namespace

TDirectCopyActor::TDirectCopyActor(
        const TActorId& source,
        TRequestInfoPtr requestInfo,
        NProto::TDirectCopyBlocksRequest request,
        ui64 recommendedBandwidth)
    : Source(source)
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , RecommendedBandwidth(recommendedBandwidth)
{}

void TDirectCopyActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    ReadStartAt = ctx.Now();

    auto readRequest =
        std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();

    auto& rec = readRequest->Record;
    *rec.MutableHeaders() = Request.GetHeaders();
    // We will move the read buffer directly to the write request, and it does
    // not support empty blocks, so just in case remove the optimization flag.
    rec.MutableHeaders()->SetOptimizeNetworkTransfer(
        NProto::EOptimizeNetworkTransfer::NO_OPTIMIZATION);

    rec.SetDeviceUUID(Request.GetSourceDeviceUUID());
    rec.SetStartIndex(Request.GetSourceStartIndex());
    rec.SetBlockSize(Request.GetBlockSize());
    rec.SetBlocksCount(Request.GetBlockCount());

    ctx.Send(Source, std::move(readRequest), 0, RequestInfo->Cookie);
}

bool TDirectCopyActor::HandleError(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    if (SUCCEEDED(error.GetCode())) {
        return false;
    }

    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksResponse>(error));

    Die(ctx);
    return true;
}

void TDirectCopyActor::Done(const NActors::TActorContext& ctx)
{
    auto readDuration = WriteStartAt - ReadStartAt;
    auto writeDuration = ctx.Now() - WriteStartAt;

    auto response = std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksResponse>(
        MakeError(S_OK));
    response->Record.SetAllZeroes(AllZeroes);
    response->Record.SetReadDuration(readDuration.MicroSeconds());
    response->Record.SetWriteDuration(writeDuration.MicroSeconds());
    response->Record.SetRecommendedBandwidth(RecommendedBandwidth);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TDirectCopyActor::HandleReadBlocksResponse(
    const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    WriteStartAt = ctx.Now();

    AllZeroes = IsAllZeroes(msg->Record.GetBlocks());

    IEventBasePtr request;
    if (AllZeroes) {
        auto zeroRequest =
            std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
        PrepareRequest(Request, zeroRequest.get());
        zeroRequest->Record.SetBlocksCount(Request.GetBlockCount());
        request = std::move(zeroRequest);
    } else {
        auto writeRequest =
            std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
        PrepareRequest(Request, writeRequest.get());
        writeRequest->Record.MutableBlocks()->Swap(msg->Record.MutableBlocks());
        request = std::move(writeRequest);
    }

    auto event = std::make_unique<IEventHandle>(
        MakeDiskAgentServiceId(Request.GetTargetNodeId()),
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        ev->Cookie,
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());
}

void TDirectCopyActor::HandleWriteBlocksResponse(
    const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    Done(ctx);
}

void TDirectCopyActor::HandleWriteBlocksUndelivery(
    const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    HandleError(ctx, MakeError(E_REJECTED, "WriteBlocks request undelivered"));
}

void TDirectCopyActor::HandleZeroBlocksResponse(
    const TEvDiskAgent::TEvZeroDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    Done(ctx);
}

void TDirectCopyActor::HandleZeroBlocksUndelivery(
    const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    HandleError(ctx, MakeError(E_REJECTED, "ZeroBlocks request undelivered"));
}

STFUNC(TDirectCopyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgent::TEvReadDeviceBlocksResponse,
            HandleReadBlocksResponse);

        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksResponse,
            HandleWriteBlocksResponse);
        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksRequest,
            HandleWriteBlocksUndelivery);
        HFunc(
            TEvDiskAgent::TEvZeroDeviceBlocksResponse,
            HandleZeroBlocksResponse);
        HFunc(
            TEvDiskAgent::TEvZeroDeviceBlocksRequest,
            HandleZeroBlocksUndelivery);
        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_AGENT_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
