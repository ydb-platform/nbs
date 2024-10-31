#include "part_nonrepl_actor.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

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
    using EStatus = TEvNonreplPartitionPrivate::TOperationCompleted::EStatus;

    const TRequestInfoPtr RequestInfo;
    const NProto::TZeroBlocksRequest Request;
    const TVector<TDeviceRequest> DeviceRequests;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TActorId Part;
    const ui32 BlockSize;
    const bool AssignVolumeRequestId;
    const TRequestTimeoutPolicy TimeoutPolicy;

    TInstant StartTime;
    ui32 RequestsCompleted = 0;

    NProto::TZeroBlocksResponse Response;

public:
    TDiskAgentZeroActor(
        TRequestInfoPtr requestInfo,
        NProto::TZeroBlocksRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part,
        ui32 blockSize,
        bool assignVolumeRequestId);

    void Bootstrap(const TActorContext& ctx);

private:
    void ZeroBlocks(const TActorContext& ctx);

    bool HandleError(
        const TActorContext& ctx,
        NProto::TError error,
        bool timedout);

    void Done(const TActorContext& ctx, IEventBasePtr response, EStatus status);

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
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part,
        ui32 blockSize,
        bool assignVolumeRequestId)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , DeviceRequests(std::move(deviceRequests))
    , PartConfig(std::move(partConfig))
    , Part(part)
    , BlockSize(blockSize)
    , AssignVolumeRequestId(assignVolumeRequestId)
    , TimeoutPolicy(std::move(timeoutPolicy))
{}

void TDiskAgentZeroActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "DiskAgentZero",
        RequestInfo->CallContext->RequestId);

    StartTime = ctx.Now();
    ctx.Schedule(TimeoutPolicy.Timeout, new TEvents::TEvWakeup());

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
        if (AssignVolumeRequestId) {
            request->Record.SetVolumeRequestId(RequestInfo->Cookie);
            request->Record.SetMultideviceRequest(DeviceRequests.size() > 1);
        }

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(deviceRequest.Device.GetNodeId()),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            cookie++,
            &ctx.SelfID // forwardOnNondelivery
        );

        ctx.Send(std::move(event));
    }
}

bool TDiskAgentZeroActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error,
    bool timedout)
{
    if (FAILED(error.GetCode())) {
        ProcessError(ctx, *PartConfig, error);

        auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>(
            std::move(error));

        Done(
            ctx,
            std::move(response),
            timedout ? EStatus::Timeout : EStatus::Fail);

        return true;
    }

    return false;
}

void TDiskAgentZeroActor::Done(
    const TActorContext& ctx,
    IEventBasePtr response,
    EStatus status)
{
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
    completion->ExecCycles = RequestInfo->GetExecCycles();
    completion->ExecutionTime = status == EStatus::Timeout
                                    ? TimeoutPolicy.Timeout
                                    : ctx.Now() - StartTime;

    ui32 blocks = 0;
    for (const auto& dr: DeviceRequests) {
        blocks += dr.BlockRange.Size();
        completion->DeviceIndices.push_back(dr.DeviceIdx);
    }
    counters.SetBlocksCount(blocks);
    completion->Status = status;

    NCloud::Send(
        ctx,
        Part,
        std::move(completion));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentZeroActor::HandleZeroDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "ZeroBlocks request #"
            << GetRequestId(Request) << " undelivered. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    HandleError(
        ctx,
        PartConfig->MakeError(
            TimeoutPolicy.ErrorCode,
            "ZeroBlocks request undelivered"),
        true);
}

void TDiskAgentZeroActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "ZeroBlocks request #"
            << GetRequestId(Request) << " timed out. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    HandleError(
        ctx,
        PartConfig->MakeError(
            TimeoutPolicy.ErrorCode,
            TimeoutPolicy.OverrideMessage ? TimeoutPolicy.OverrideMessage
                                          : "ZeroBlocks request timed out"),
        true);
}

void TDiskAgentZeroActor::HandleZeroDeviceBlocksResponse(
    const TEvDiskAgent::TEvZeroDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError(), false)) {
        return;
    }

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>();
    response->Record = std::move(Response);
    Done(ctx, std::move(response), EStatus::Success);
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
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ZeroBlocks",
        requestInfo->CallContext->RequestId);

    auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    bool ok = InitRequests<TEvService::TZeroBlocksMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests,
        &timeoutPolicy);

    if (!ok) {
        return;
    }

    const bool assignVolumeRequestId =
        Config->GetAssignIdToWriteAndZeroRequestsEnabled() &&
        !msg->Record.GetHeaders().GetIsBackgroundRequest();

    auto actorId = NCloud::Register<TDiskAgentZeroActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(timeoutPolicy),
        std::move(deviceRequests),
        PartConfig,
        SelfId(),
        PartConfig->GetBlockSize(),
        assignVolumeRequestId);

    RequestsInProgress.AddWriteRequest(actorId);
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
    NetworkBytes += requestBytes;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    RequestsInProgress.RemoveRequest(ev->Sender);
    OnRequestCompleted(*msg, ctx.Now());
    DrainActorCompanion.ProcessDrainRequests(ctx);

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
