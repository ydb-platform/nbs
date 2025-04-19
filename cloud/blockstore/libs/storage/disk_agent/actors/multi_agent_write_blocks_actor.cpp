#include "multi_agent_write_blocks_actor.h"

#include <cloud/blockstore/libs/common/iovector.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 LocalWriteCookie = 0;
constexpr ui64 RemoteWriteCookie = 1;

std::unique_ptr<TEvDiskAgent::TEvWriteDeviceBlocksRequest> PrepareRequest(
    const NProto::TWriteDeviceBlocksRequest& source,
    const NProto::TAdditionalTarget& additionalTarget)
{
    auto result = std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();

    auto& rec = result->Record;
    *rec.MutableHeaders() = source.GetHeaders();
    rec.SetDeviceUUID(additionalTarget.GetDeviceUUID());
    rec.SetStartIndex(additionalTarget.GetStartIndex());
    rec.SetBlockSize(source.GetBlockSize());
    *rec.MutableBlocks() = source.GetBlocks();
    rec.SetVolumeRequestId(source.GetVolumeRequestId());
    rec.SetMultideviceRequest(source.GetMultideviceRequest());

    return result;
}

}   // namespace

TMultiAgentWriteBlocksActor::TMultiAgentWriteBlocksActor(
        const TActorId& parent,
        TRequestInfoPtr requestInfo,
        NProto::TWriteDeviceBlocksRequest request)
    : Parent(parent)
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
{}

void TMultiAgentWriteBlocksActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    Responses.resize(Request.GetAdditionalTargets().size() + 1);

    // Send write requests to remote agents
    ui64 requestId = RemoteWriteCookie;
    for (const auto& additionalTarget: Request.GetAdditionalTargets()) {
        auto remoteWrite = PrepareRequest(Request, additionalTarget);

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

    // Send write request to self
    {
        auto localWrite =
            std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();

        Request.MutableAdditionalTargets()->Clear();
        localWrite->Record = std::move(Request);

        auto event = std::make_unique<IEventHandle>(
            Parent,
            ctx.SelfID,
            localWrite.release(),
            0,   // flags
            LocalWriteCookie,
            nullptr   // forwardOnNondelivery
        );
        ctx.Send(event.release());
    }
}

void TMultiAgentWriteBlocksActor::ReplyAndDie(
    const NActors::TActorContext& ctx,
    NProto::TError error)
{
    auto response =
        std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksResponse>(
            std::move(error));

    // Save responses from all requests.
    for (auto& subresponse: Responses) {
        auto* sub = response->Record.AddSubResponse();
        *sub =
            subresponse
                ? std::move(*subresponse)
                : MakeError(E_CANCELLED);   // For responses that have not been
                                            // received, save E_CANCELLED.
    }

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TMultiAgentWriteBlocksActor::HandleWriteBlocksResponse(
    const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Responses[ev->Cookie] = msg->GetError();

    if (!SUCCEEDED(msg->GetError().GetCode())) {
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

void TMultiAgentWriteBlocksActor::HandleWriteBlocksUndelivery(
    const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(
        E_REJECTED,
        TStringBuilder() << "WriteBlocks request undelivered for agent "
                         << ev->Cookie);
    Responses[ev->Cookie] = error;
    ReplyAndDie(ctx, std::move(error));
}

STFUNC(TMultiAgentWriteBlocksActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
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

}   // namespace NCloud::NBlockStore::NStorage
