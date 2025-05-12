#include "multi_agent_write_device_blocks_actor.h"

#include <cloud/blockstore/libs/common/iovector.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvDiskAgent::TEvWriteDeviceBlocksRequest> PrepareRequest(
    NProto::TWriteDeviceBlocksRequest& source,
    const NProto::TReplicationTarget& additionalTarget,
    bool takeBlocks)
{
    auto result = std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();

    auto& rec = result->Record;
    *rec.MutableHeaders() = source.GetHeaders();
    rec.SetDeviceUUID(additionalTarget.GetDeviceUUID());
    rec.SetStartIndex(additionalTarget.GetStartIndex());
    rec.SetBlockSize(source.GetBlockSize());
    if (takeBlocks) {
        rec.MutableBlocks()->Swap(source.MutableBlocks());
    } else {
        *rec.MutableBlocks() = source.GetBlocks();
    }
    rec.SetVolumeRequestId(source.GetVolumeRequestId());
    rec.SetMultideviceRequest(source.GetMultideviceRequest());

    return result;
}

}   // namespace

TMultiAgentWriteDeviceBlocksActor::TMultiAgentWriteDeviceBlocksActor(
        const TActorId& parent,
        TRequestInfoPtr requestInfo,
        NProto::TWriteDeviceBlocksRequest request,
        TDuration maxRequestTimeout)
    : Parent(parent)
    , RequestInfo(std::move(requestInfo))
    , MaxRequestTimeout(maxRequestTimeout)
    , Request(std::move(request))
{}

void TMultiAgentWriteDeviceBlocksActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    Responses.resize(Request.GetReplicationTargets().size());

    if (Request.GetDeviceUUID()) {
        auto error = MakeError(
            E_ARGUMENT,
            TStringBuilder() << "A multi-agent write request should have an "
                                "empty deviceUUID, but it is set to "
                             << Request.GetDeviceUUID().Quote());
        ReplyAndDie(ctx, std::move(error));
        return;
    }

    // Send write requests to agents
    ui64 requestId = 0;
    for (const auto& additionalTarget: Request.GetReplicationTargets()) {
        const bool lastRequest = requestId == Responses.size() - 1;
        auto remoteWrite =
            PrepareRequest(Request, additionalTarget, lastRequest);

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(additionalTarget.GetNodeId()),
            ctx.SelfID,
            remoteWrite.release(),
            IEventHandle::FlagForwardOnNondelivery,
            requestId,
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(event.release());
        ++requestId;
    }

    ctx.Schedule(MaxRequestTimeout, new NActors::TEvents::TEvWakeup());
}

void TMultiAgentWriteDeviceBlocksActor::ReplyAndDie(
    const NActors::TActorContext& ctx,
    NProto::TError error)
{
    auto response =
        std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksResponse>(
            std::move(error));

    // Save responses from all requests.
    for (auto& subresponse: Responses) {
        auto* replicationResponse = response->Record.AddReplicationResponses();
        *replicationResponse =
            subresponse
                ? std::move(*subresponse)
                : MakeError(E_CANCELLED);   // For responses that have not been
                                            // received, save E_CANCELLED.
    }

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TMultiAgentWriteDeviceBlocksActor::HandleWriteBlocksResponse(
    const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Responses[ev->Cookie] = msg->GetError();

    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    bool allDone = AllOf(
        Responses,
        [](const std::optional<NProto::TError>& r) { return r.has_value(); });

    if (allDone) {
        ReplyAndDie(ctx, MakeError(S_OK));
    }
}

void TMultiAgentWriteDeviceBlocksActor::HandleWriteBlocksUndelivery(
    const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto error = MakeError(
        E_REJECTED,
        TStringBuilder() << "WriteDeviceBlocks request undelivered for NodeId="
                         << ev->Recipient.NodeId() << " DeviceUUID="
                         << msg->Record.GetDeviceUUID().Quote());
    Responses[ev->Cookie] = error;
    ReplyAndDie(ctx, std::move(error));
}

void TMultiAgentWriteDeviceBlocksActor::HandleTimeout(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(
        E_TIMEOUT,
        TStringBuilder() << "MultiAgentWriteDeviceBlocks timeout");

    for (auto& subResponse: Responses) {
        if (!subResponse) {
            subResponse = error;
        }
    }

    ReplyAndDie(ctx, std::move(error));
}

STFUNC(TMultiAgentWriteDeviceBlocksActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksResponse,
            HandleWriteBlocksResponse);
        HFunc(NActors::TEvents::TEvWakeup, HandleTimeout);
        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksRequest,
            HandleWriteBlocksUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_AGENT_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
