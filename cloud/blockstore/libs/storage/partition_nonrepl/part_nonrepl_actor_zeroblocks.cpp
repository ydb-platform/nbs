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

class TDiskAgentZeroActor final
    : public TActorBootstrapped<TDiskAgentZeroActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const NProto::TZeroBlocksRequest Request;
    const TVector<TDeviceRequest> DeviceRequests;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TActorId Part;
    const ui32 BlockSize;

    ui32 RequestsCompleted = 0;

    NProto::TZeroBlocksResponse Response;

public:
    TDiskAgentZeroActor(
        TRequestInfoPtr requestInfo,
        NProto::TZeroBlocksRequest request,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part,
        ui32 blockSize);

    void Bootstrap(const TActorContext& ctx);

private:
    void ZeroBlocks(const TActorContext& ctx);

    bool HandleError(const TActorContext& ctx, NProto::TError error);

    void Done(const TActorContext& ctx, IEventBasePtr response, bool failed);

private:
    STFUNC(StateWork);

    void HandleZeroDeviceBlocksResponse(
        const TEvDiskAgent::TEvZeroDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleZeroDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskAgentZeroActor::TDiskAgentZeroActor(
        TRequestInfoPtr requestInfo,
        NProto::TZeroBlocksRequest request,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part,
        ui32 blockSize)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , DeviceRequests(std::move(deviceRequests))
    , PartConfig(std::move(partConfig))
    , Part(part)
    , BlockSize(blockSize)
{
    ActivityType = TBlockStoreActivities::PARTITION_WORKER;
}

void TDiskAgentZeroActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "DiskAgentZero",
        RequestInfo->CallContext->RequestId);

    ZeroBlocks(ctx);
}

void TDiskAgentZeroActor::ZeroBlocks(const TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& deviceRequest: DeviceRequests) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
        request->Record.MutableHeaders()->CopyFrom(Request.GetHeaders());
        request->Record.SetDeviceUUID(deviceRequest.Device.GetDeviceUUID());
        request->Record.SetStartIndex(deviceRequest.DeviceBlockRange.Start);
        request->Record.SetBlockSize(BlockSize);
        request->Record.SetBlocksCount(deviceRequest.DeviceBlockRange.Size());
        request->Record.SetSessionId(Request.GetSessionId());

        auto traceId = RequestInfo->TraceId.Clone();
        BLOCKSTORE_TRACE_SENT(ctx, &traceId, this, request);

        TAutoPtr<IEventHandle> event(
            new IEventHandle(
                MakeDiskAgentServiceId(deviceRequest.Device.GetNodeId()),
                ctx.SelfID,
                request.get(),
                IEventHandle::FlagForwardOnNondelivery,
                cookie++,
                &ctx.SelfID,    // forwardOnNondelivery
                std::move(traceId)
            ));
        request.release();

        ctx.Send(event);
    }
}

bool TDiskAgentZeroActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (FAILED(error.GetCode())) {
        ProcessError(ctx, *PartConfig, error);

        auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>(
            std::move(error)
        );

        Done(ctx, std::move(response), true);
        return true;
    }

    return false;
}

void TDiskAgentZeroActor::Done(
    const TActorContext& ctx,
    IEventBasePtr response,
    bool failed)
{
    BLOCKSTORE_TRACE_SENT(ctx, &RequestInfo->TraceId, this, response);

    LWTRACK(
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "ZeroBlocks",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted>();
    auto& counters = *completion->Stats.MutableUserWriteCounters();
    completion->TotalCycles = RequestInfo->GetTotalCycles();
    ui32 blocks = 0;
    for (const auto& dr: DeviceRequests) {
        blocks += dr.BlockRange.Size();
        completion->DeviceIndices.push_back(dr.DeviceIdx);
    }
    counters.SetBlocksCount(blocks);
    completion->Failed = failed;

    NCloud::Send(
        ctx,
        Part,
        std::move(completion),
        0,  // cookie
        std::move(RequestInfo->TraceId));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentZeroActor::HandleZeroDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& uuid = DeviceRequests[ev->Cookie].Device.GetDeviceUUID();

    LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "ZeroBlocks undelivered for " << uuid);

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentZeroActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& uuid = DeviceRequests[ev->Cookie].Device.GetDeviceUUID();

    LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "ZeroBlocks request timed out. Disk id: " << PartConfig->GetName() <<
        " Device id: " << uuid);

    HandleError(
        ctx,
        PartConfig->MakeError(E_TIMEOUT, "ZeroBlocks request timed out"));
}

void TDiskAgentZeroActor::HandleZeroDeviceBlocksResponse(
    const TEvDiskAgent::TEvZeroDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    BLOCKSTORE_TRACE_RECEIVED(ctx, &RequestInfo->TraceId, this, msg, &ev->TraceId);

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>();
    response->Record = std::move(Response);
    Done(ctx, std::move(response), false);
}

STFUNC(TDiskAgentZeroActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(TEvDiskAgent::TEvZeroDeviceBlocksRequest, HandleZeroDeviceBlocksUndelivery);
        HFunc(TEvDiskAgent::TEvZeroDeviceBlocksResponse, HandleZeroDeviceBlocksResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TZeroBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext,
        std::move(ev->TraceId));

    TRequestScope timer(*requestInfo);

    BLOCKSTORE_TRACE_RECEIVED(ctx, &requestInfo->TraceId, this, msg);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ZeroBlocks",
        requestInfo->CallContext->RequestId);

    auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    TRequest request;
    bool ok = InitRequests<TEvService::TZeroBlocksMethod>(
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

    auto actorId = NCloud::Register<TDiskAgentZeroActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(deviceRequests),
        PartConfig,
        SelfId(),
        PartConfig->GetBlockSize());

    RequestsInProgress.AddWriteRequest(actorId, std::move(request));
}

void TNonreplicatedPartitionActor::HandleZeroBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Complete zero blocks", SelfId().ToString().c_str());

    UpdateStats(msg->Stats);

    const auto requestBytes = msg->Stats.GetUserWriteCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ZeroBlocks.AddRequest(time, requestBytes);

    RequestsInProgress.RemoveRequest(ev->Sender);
    if (!msg->Failed) {
        for (const auto i: msg->DeviceIndices) {
            DeviceStats[i] = {};
        }
    }

    DrainActorCompanion.ProcessDrainRequests(ctx);

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
