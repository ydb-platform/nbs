#include "part_nonrepl_actor.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentReadActor final
    : public TActorBootstrapped<TDiskAgentReadActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const NProto::TReadBlocksRequest Request;
    const TVector<TDeviceRequest> DeviceRequests;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TActorId Part;

    TInstant StartTime;
    ui32 RequestsCompleted = 0;

    NProto::TReadBlocksResponse Response;

public:
    TDiskAgentReadActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadBlocksRequest request,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReadBlocks(const TActorContext& ctx);

    bool HandleError(const TActorContext& ctx, NProto::TError error);

    void Done(const TActorContext& ctx, IEventBasePtr response, bool failed);

private:
    STFUNC(StateWork);

    void HandleReadDeviceBlocksResponse(
        const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskAgentReadActor::TDiskAgentReadActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadBlocksRequest request,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , DeviceRequests(std::move(deviceRequests))
    , PartConfig(std::move(partConfig))
    , Part(part)
{}

void TDiskAgentReadActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "DiskAgentRead",
        RequestInfo->CallContext->RequestId);

    StartTime = ctx.Now();

    ReadBlocks(ctx);
}

void TDiskAgentReadActor::ReadBlocks(const TActorContext& ctx)
{
    const auto blockRange = TBlockRange64::WithLength(
        Request.GetStartIndex(),
        Request.GetBlocksCount()
    );

    const auto blockSize = PartConfig->GetBlockSize();

    for (ui32 i = 0; i < blockRange.Size(); ++i) {
        Response.MutableBlocks()->MutableBuffers()->Add();
    }

    ui32 cookie = 0;
    for (const auto& deviceRequest: DeviceRequests) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
        request->Record.MutableHeaders()->CopyFrom(Request.GetHeaders());
        request->Record.SetDeviceUUID(deviceRequest.Device.GetDeviceUUID());
        request->Record.SetStartIndex(deviceRequest.DeviceBlockRange.Start);
        request->Record.SetBlockSize(blockSize);
        request->Record.SetBlocksCount(deviceRequest.DeviceBlockRange.Size());

        TAutoPtr<IEventHandle> event(
            new IEventHandle(
                MakeDiskAgentServiceId(deviceRequest.Device.GetNodeId()),
                ctx.SelfID,
                request.get(),
                IEventHandle::FlagForwardOnNondelivery,
                cookie++,
                &ctx.SelfID // forwardOnNondelivery
            ));
        request.release();

        ctx.Send(event);
    }
}

bool TDiskAgentReadActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (FAILED(error.GetCode())) {
        ProcessError(ctx, *PartConfig, error);

        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>(
            std::move(error)
        );

        Done(ctx, std::move(response), true);
        return true;
    }

    return false;
}

void TDiskAgentReadActor::Done(
    const TActorContext& ctx,
    IEventBasePtr response,
    bool failed)
{
    LWTRACK(
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvReadBlocksCompleted>();
    auto& counters = *completion->Stats.MutableUserReadCounters();
    completion->TotalCycles = RequestInfo->GetTotalCycles();
    completion->ActorSystemTime = ctx.Now() - StartTime;

    ui32 blocks = 0;
    for (const auto& dr: DeviceRequests) {
        blocks += dr.BlockRange.Size();
        completion->DeviceIndices.push_back(dr.DeviceIdx);
    }
    counters.SetBlocksCount(blocks);
    completion->Failed = failed;
    completion->ExecCycles = RequestInfo->GetExecCycles();

    NCloud::Send(
        ctx,
        Part,
        std::move(completion));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentReadActor::HandleReadDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;

    LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "ReadBlocks undelivered for " << LogDevice(device));

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentReadActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;

    LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "ReadBlocks request timed out. Disk id: "
        << PartConfig->GetName() << " Device: " << LogDevice(device));

    HandleError(
        ctx,
        PartConfig->MakeError(E_TIMEOUT, "ReadBlocks request timed out"));
}

void TDiskAgentReadActor::HandleReadDeviceBlocksResponse(
    const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    auto& respBuffers = *Response.MutableBlocks()->MutableBuffers();

    const auto& blockRange = DeviceRequests[ev->Cookie].BlockRange;
    Y_ABORT_UNLESS(msg->Record.GetBlocks().BuffersSize() == blockRange.Size());
    for (ui32 i = 0; i < blockRange.Size(); ++i) {
        respBuffers[blockRange.Start + i - Request.GetStartIndex()] =
            std::move(*msg->Record.MutableBlocks()->MutableBuffers(i));
    }

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();
    response->Record = std::move(Response);
    Done(ctx, std::move(response), false);
}

STFUNC(TDiskAgentReadActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(TEvDiskAgent::TEvReadDeviceBlocksRequest, HandleReadDeviceBlocksUndelivery);
        HFunc(TEvDiskAgent::TEvReadDeviceBlocksResponse, HandleReadDeviceBlocksResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TReadBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        requestInfo->CallContext->RequestId);

    auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    TRequest request;
    bool ok = InitRequests<TEvService::TReadBlocksMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests,
        &request
    );

    if (!ok) {
        return;
    }

    auto actorId = NCloud::Register<TDiskAgentReadActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(deviceRequests),
        PartConfig,
        SelfId());

    RequestsInProgress.AddReadRequest(actorId, std::move(request));
}

void TNonreplicatedPartitionActor::HandleReadBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvReadBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Complete read blocks", SelfId().ToString().c_str());

    UpdateStats(msg->Stats);

    const auto requestBytes = msg->Stats.GetUserReadCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ReadBlocks.AddRequest(time, requestBytes);
    NetworkBytes += requestBytes;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    RequestsInProgress.RemoveRequest(ev->Sender);
    if (!msg->Failed) {
        for (const auto i: msg->DeviceIndices) {
            OnResponse(i, msg->ActorSystemTime);
        }
    }

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
