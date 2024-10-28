#include "part_nonrepl_actor.h"

#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

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
    const NProto::TReadBlocksLocalRequest Request;
    const TVector<TDeviceRequest> DeviceRequests;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TActorId Part;
    const bool SkipVoidBlocksToOptimizeNetworkTransfer;
    const TRequestTimeoutPolicy TimeoutPolicy;

    TInstant StartTime;
    ui32 RequestsCompleted = 0;
    ui32 VoidBlockCount = 0;
    ui32 NonVoidBlockCount = 0;

public:
    TDiskAgentReadActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadBlocksLocalRequest request,
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
        bool timedout);

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
        NProto::TReadBlocksLocalRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , DeviceRequests(std::move(deviceRequests))
    , PartConfig(std::move(partConfig))
    , Part(part)
    , SkipVoidBlocksToOptimizeNetworkTransfer(
          Request.GetHeaders().GetOptimizeNetworkTransfer() ==
          NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS)
    , TimeoutPolicy(std::move(timeoutPolicy))
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
    const auto blockSize = PartConfig->GetBlockSize();

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

        ctx.Send(event.release());
    }
}

bool TDiskAgentReadActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error,
    bool timedout)
{
    if (FAILED(error.GetCode())) {
        ProcessError(ctx, *PartConfig, error);

        auto response =
            std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
                std::move(error));
        Done(
            ctx,
            std::move(response),
            timedout ? EStatus::Timedout : EStatus::Failed);
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
        std::make_unique<TEvNonreplPartitionPrivate::TEvReadBlocksCompleted>();
    auto& counters = *completion->Stats.MutableUserReadCounters();
    completion->TotalCycles = RequestInfo->GetTotalCycles();
    completion->ExecCycles = RequestInfo->GetExecCycles();
    completion->ExecutionTime = ctx.Now() - StartTime;

    ui32 blocks = 0;
    for (const auto& dr: DeviceRequests) {
        blocks += dr.BlockRange.Size();
        completion->DeviceIndices.push_back(dr.DeviceIdx);
    }
    counters.SetBlocksCount(blocks);
    completion->Status = status;

    completion->NonVoidBlockCount = NonVoidBlockCount;
    completion->VoidBlockCount = VoidBlockCount;

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
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "ReadBlocksLocal request #"
            << GetRequestId(Request) << " undelivered. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    HandleError(
        ctx,
        PartConfig->MakeError(
            TimeoutPolicy.ErrorCode,
            "ReadBlocksLocal request undelivered"),
        true);
}

void TDiskAgentReadActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "ReadBlocksLocal request #"
            << GetRequestId(Request) << " timed out. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    HandleError(
        ctx,
        PartConfig->MakeError(
            TimeoutPolicy.ErrorCode,
            TimeoutPolicy.OverrideMessage
                ? TimeoutPolicy.OverrideMessage
                : "ReadBlocksLocal request timed out"),
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

    auto guard = Request.Sglist.Acquire();

    if (!guard) {
        HandleError(
            ctx,
            PartConfig->MakeError(
                E_CANCELLED,
                "failed to acquire sglist in DiskAgentReadActor"),
            false);
        return;
    }

    const auto blockRange = DeviceRequests[ev->Cookie].BlockRange;

    if (blockRange.Size() != 0) {
        auto voidBlockStat = CopyToSgList(
            msg->Record.GetBlocks(),
            guard.Get(),
            blockRange.Start - Request.GetStartIndex(),
            PartConfig->GetBlockSize());

        if (SkipVoidBlocksToOptimizeNetworkTransfer) {
            NonVoidBlockCount +=
                voidBlockStat.TotalBlockCount - voidBlockStat.VoidBlockCount;
            VoidBlockCount += voidBlockStat.VoidBlockCount;
        } else {
            STORAGE_CHECK_PRECONDITION(voidBlockStat.VoidBlockCount == 0);
        }
    }

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>();
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

void TNonreplicatedPartitionActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TReadBlocksLocalMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        requestInfo->CallContext->RequestId);

    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    bool ok = InitRequests<TEvService::TReadBlocksLocalMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests,
        &timeoutPolicy
    );

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

}   // namespace NCloud::NBlockStore::NStorage
