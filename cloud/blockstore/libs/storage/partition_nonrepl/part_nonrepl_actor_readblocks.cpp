#include "part_nonrepl_actor.h"

#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

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
    using EStatus = TEvNonreplPartitionPrivate::TOperationCompleted::EStatus;

    const TRequestInfoPtr RequestInfo;
    const NProto::TReadBlocksRequest Request;
    const TVector<TDeviceRequest> DeviceRequests;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TActorId Part;
    const TRequestTimeoutPolicy TimeoutPolicy;

    TInstant StartTime;
    ui32 RequestsCompleted = 0;

    NProto::TReadBlocksResponse Response;

    const bool SkipVoidBlocksToOptimizeNetworkTransfer;

    ui32 VoidBlockCount = 0;
    ui32 NonVoidBlockCount = 0;

public:
    TDiskAgentReadActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadBlocksRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReadBlocks(const TActorContext& ctx);

    bool HandleError(
        const TActorContext& ctx,
        NProto::TError error,
        bool timedOut);

    void Done(const TActorContext& ctx, IEventBasePtr response, EStatus status);

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
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , DeviceRequests(std::move(deviceRequests))
    , PartConfig(std::move(partConfig))
    , Part(part)
    , TimeoutPolicy(std::move(timeoutPolicy))
    , SkipVoidBlocksToOptimizeNetworkTransfer(
          Request.GetHeaders().GetOptimizeNetworkTransfer() ==
          NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS)
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
    ctx.Schedule(TimeoutPolicy.Timeout, new TEvents::TEvWakeup());

    ReadBlocks(ctx);
}

void TDiskAgentReadActor::ReadBlocks(const TActorContext& ctx)
{
    const auto blockRange = TBlockRange64::WithLength(
        Request.GetStartIndex(),
        Request.GetBlocksCount()
    );

    const auto blockSize = PartConfig->GetBlockSize();

    auto* responseBuffers = Response.MutableBlocks()->MutableBuffers();
    responseBuffers->Reserve(blockRange.Size());
    for (ui32 i = 0; i < blockRange.Size(); ++i) {
        responseBuffers->Add();
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

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(deviceRequest.Device.GetNodeId()),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            cookie++,
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(std::move(event));
    }
}

bool TDiskAgentReadActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error,
    bool timedOut)
{
    if (FAILED(error.GetCode())) {
        ProcessError(ctx, *PartConfig, error);

        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>(
            std::move(error));

        Done(
            ctx,
            std::move(response),
            timedOut ? EStatus::Timeout : EStatus::Fail);
        return true;
    }

    return false;
}

void TDiskAgentReadActor::Done(
    const TActorContext& ctx,
    IEventBasePtr response,
    EStatus status)
{
    LWTRACK(
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvReadBlocksCompleted>(
            status,
            RequestInfo->GetTotalCycles(),
            RequestInfo->GetExecCycles(),
            status == EStatus::Timeout ? TimeoutPolicy.Timeout
                                       : ctx.Now() - StartTime);

    ui32 blocks = 0;
    for (const auto& dr: DeviceRequests) {
        blocks += dr.BlockRange.Size();
        completion->DeviceIndices.push_back(dr.DeviceIdx);
    }
    completion->Stats.MutableUserReadCounters()->SetBlocksCount(blocks);

    completion->NonVoidBlockCount = NonVoidBlockCount;
    completion->VoidBlockCount = VoidBlockCount;

    NCloud::Send(ctx, Part, std::move(completion));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentReadActor::HandleReadDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "ReadBlocks request #"
            << GetRequestId(Request) << " undelivered. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentReadActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "ReadBlocks request #"
            << GetRequestId(Request) << " timed out. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    HandleError(
        ctx,
        PartConfig->MakeError(
            TimeoutPolicy.ErrorCode,
            TimeoutPolicy.OverrideMessage ? TimeoutPolicy.OverrideMessage
                                          : "ReadBlocks request timed out"),
        true);
}

void TDiskAgentReadActor::HandleReadDeviceBlocksResponse(
    const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError(), false)) {
        return;
    }

    auto& srcBuffers = *msg->Record.MutableBlocks()->MutableBuffers();
    auto& destBuffers = *Response.MutableBlocks()->MutableBuffers();
    const auto blockSize = PartConfig->GetBlockSize();

    const auto& blockRange = DeviceRequests[ev->Cookie].BlockRange;
    Y_ABORT_UNLESS(msg->Record.GetBlocks().BuffersSize() == blockRange.Size());
    for (ui32 i = 0; i < blockRange.Size(); ++i) {
        auto& srcBuffer = srcBuffers[i];
        auto& destBuffer =
            destBuffers[blockRange.Start + i - Request.GetStartIndex()];
        destBuffer.swap(srcBuffer);
        if (destBuffer.empty()) {
            STORAGE_CHECK_PRECONDITION(SkipVoidBlocksToOptimizeNetworkTransfer);
            destBuffer.resize(blockSize, 0);
            ++VoidBlockCount;
        } else if (SkipVoidBlocksToOptimizeNetworkTransfer) {
            ++NonVoidBlockCount;
        }
    }

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();
    response->Record = std::move(Response);
    response->Record.SetAllZeroes(VoidBlockCount == Request.GetBlocksCount());
    Done(ctx, std::move(response), EStatus::Success);
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
    TRequestTimeoutPolicy timeoutPolicy;
    bool ok = InitRequests<TEvService::TReadBlocksMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests,
        &timeoutPolicy);

    if (!ok) {
        return;
    }

    if (Config->GetOptimizeVoidBuffersTransferForReadsEnabled()) {
        msg->Record.MutableHeaders()->SetOptimizeNetworkTransfer(
            NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS);
    }

    auto actorId = NCloud::Register<TDiskAgentReadActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(timeoutPolicy),
        std::move(deviceRequests),
        PartConfig,
        SelfId());

    RequestsInProgress.AddReadRequest(actorId);
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

    PartCounters->Interconnect.ReadBytes.Increment(requestBytes);
    PartCounters->Interconnect.ReadCount.Increment(1);

    PartCounters->RequestCounters.ReadBlocks.RequestNonVoidBytes +=
        static_cast<ui64>(msg->NonVoidBlockCount) * PartConfig->GetBlockSize();
    PartCounters->RequestCounters.ReadBlocks.RequestVoidBytes +=
        static_cast<ui64>(msg->VoidBlockCount) * PartConfig->GetBlockSize();

    NetworkBytes += requestBytes;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    RequestsInProgress.RemoveRequest(ev->Sender);
    OnRequestCompleted(*msg, ctx.Now());

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
