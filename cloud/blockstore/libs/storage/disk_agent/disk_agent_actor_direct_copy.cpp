#include "disk_agent_actor.h"

#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDirectCopyActor final: public TActorBootstrapped<TDirectCopyActor>
{
private:
    const TActorId Owner;
    TRequestInfoPtr RequestInfo;
    NProto::TDirectCopyBlocksRequest Request;

public:
    TDirectCopyActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        NProto::TDirectCopyBlocksRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    bool HandleError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void Done(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleReadBlocksResponse(
        const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteBlocksUndelivery(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteBlocksResponse(
        const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDirectCopyActor::TDirectCopyActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        NProto::TDirectCopyBlocksRequest request)
    : Owner(owner)
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
{}

void TDirectCopyActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto readRequest =
        std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();

    auto& rec = readRequest->Record;
    *rec.MutableHeaders() = Request.GetHeaders();

    rec.SetDeviceUUID(Request.GetSourceDeviceUUID());
    rec.SetStartIndex(Request.GetSourceStartIndex());
    rec.SetBlockSize(Request.GetBlockSize());
    rec.SetBlocksCount(Request.GetBlockCount());

    ctx.Send(Owner, std::move(readRequest), 0, RequestInfo->Cookie);
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
    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksResponse>(
            MakeError(S_OK)));

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

    auto writeRequest =
        std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();

    auto& rec = writeRequest->Record;
    auto* headers = rec.MutableHeaders();
    headers->SetIsBackgroundRequest(
        Request.GetHeaders().GetIsBackgroundRequest());
    headers->SetClientId(TString(Request.GetTargetClientId()));

    rec.MutableBlocks()->Swap(msg->Record.MutableBlocks());
    rec.SetDeviceUUID(Request.GetTargetDeviceUUID());
    rec.SetStartIndex(Request.GetTargetStartIndex());
    rec.SetBlockSize(Request.GetBlockSize());

    auto event = std::make_unique<IEventHandle>(
        MakeDiskAgentServiceId(Request.GetTargetNodeId()),
        ctx.SelfID,
        writeRequest.release(),
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

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_AGENT_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleDirectCopyBlocks(
    const TEvDiskAgent::TEvDirectCopyBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "DirectCopyBlocks received, SourceUUID=%s %s, TargetUUID=%s %s",
        record.GetSourceDeviceUUID().Quote().c_str(),
        DescribeRange(TBlockRange64::WithLength(
                          record.GetSourceStartIndex(),
                          record.GetBlockCount()))
            .c_str(),
        record.GetTargetDeviceUUID().Quote().c_str(),
        DescribeRange(TBlockRange64::WithLength(
                          record.GetTargetStartIndex(),
                          record.GetBlockCount()))
            .c_str());

    NCloud::Register<TDirectCopyActor>(
        ctx,
        SelfId(),
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        std::move(record));
}

}   // namespace NCloud::NBlockStore::NStorage
