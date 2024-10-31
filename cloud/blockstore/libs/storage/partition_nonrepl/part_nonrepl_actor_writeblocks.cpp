#include "part_nonrepl_actor.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/iovector.h>
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

class TDiskAgentWriteActor final
    : public TActorBootstrapped<TDiskAgentWriteActor>
{
private:
    using EStatus = TEvNonreplPartitionPrivate::TOperationCompleted::EStatus;

    const TRequestInfoPtr RequestInfo;
    NProto::TWriteBlocksRequest Request;
    const TVector<TDeviceRequest> DeviceRequests;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TActorId Part;
    const bool AssignVolumeRequestId;
    const TRequestTimeoutPolicy TimeoutPolicy;

    TInstant StartTime;
    ui32 RequestsCompleted = 0;

    bool ReplyLocal;

public:
    TDiskAgentWriteActor(
        TRequestInfoPtr requestInfo,
        NProto::TWriteBlocksRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part,
        bool assignVolumeRequestId,
        bool replyLocal);

    void Bootstrap(const TActorContext& ctx);

private:
    void WriteBlocks(const TActorContext& ctx);

    bool HandleError(
        const TActorContext& ctx,
        NProto::TError error,
        bool timedout);

    void Done(const TActorContext& ctx, IEventBasePtr response, EStatus status);

    IEventBasePtr CreateResponse(NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleWriteDeviceBlocksResponse(
        const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskAgentWriteActor::TDiskAgentWriteActor(
        TRequestInfoPtr requestInfo,
        NProto::TWriteBlocksRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part,
        bool assignVolumeRequestId,
        bool replyLocal)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , DeviceRequests(std::move(deviceRequests))
    , PartConfig(std::move(partConfig))
    , Part(part)
    , AssignVolumeRequestId(assignVolumeRequestId)
    , TimeoutPolicy(std::move(timeoutPolicy))
    , ReplyLocal(replyLocal)
{}

void TDiskAgentWriteActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "DiskAgentWrite",
        RequestInfo->CallContext->RequestId);

    StartTime = ctx.Now();
    ctx.Schedule(TimeoutPolicy.Timeout, new TEvents::TEvWakeup());

    WriteBlocks(ctx);
}

void TDiskAgentWriteActor::WriteBlocks(const TActorContext& ctx)
{
    TDeviceRequestBuilder builder(
        DeviceRequests,
        PartConfig->GetBlockSize(),
        Request);

    ui32 cookie = 0;
    for (const auto& deviceRequest: DeviceRequests) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
        request->Record.MutableHeaders()->CopyFrom(Request.GetHeaders());
        request->Record.SetDeviceUUID(deviceRequest.Device.GetDeviceUUID());
        request->Record.SetStartIndex(deviceRequest.DeviceBlockRange.Start);
        request->Record.SetBlockSize(PartConfig->GetBlockSize());
        if (AssignVolumeRequestId) {
            request->Record.SetVolumeRequestId(RequestInfo->Cookie);
            request->Record.SetMultideviceRequest(DeviceRequests.size() > 1);
        }

        builder.BuildNextRequest(request->Record);

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

bool TDiskAgentWriteActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error,
    bool timedout)
{
    if (FAILED(error.GetCode())) {
        ProcessError(ctx, *PartConfig, error);

        Done(
            ctx,
            CreateResponse(std::move(error)),
            timedout ? EStatus::Timeout : EStatus::Fail);

        return true;
    }

    return false;
}

IEventBasePtr TDiskAgentWriteActor::CreateResponse(NProto::TError error)
{
    if (ReplyLocal) {
        return std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            std::move(error));
    }

    return std::make_unique<TEvService::TEvWriteBlocksResponse>(
        std::move(error));
}

void TDiskAgentWriteActor::Done(
    const TActorContext& ctx,
    IEventBasePtr response,
    EStatus status)
{
    LWTRACK(
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "WriteBlocks",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted>();
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

void TDiskAgentWriteActor::HandleWriteDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "WriteBlocks request #"
            << GetRequestId(Request) << " undelivered. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    HandleError(
        ctx,
        PartConfig->MakeError(
            TimeoutPolicy.ErrorCode,
            "WriteBlocks request undelivered"),
        true);
}

void TDiskAgentWriteActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "WriteBlocks request #"
            << GetRequestId(Request) << " timed out. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    HandleError(
        ctx,
        PartConfig->MakeError(
            TimeoutPolicy.ErrorCode,
            TimeoutPolicy.OverrideMessage ? TimeoutPolicy.OverrideMessage
                                          : "WriteBlocks request timed out"),
        true);
}

void TDiskAgentWriteActor::HandleWriteDeviceBlocksResponse(
    const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError(), false)) {
        return;
    }

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    Done(ctx, CreateResponse(NProto::TError()), EStatus::Success);
}

STFUNC(TDiskAgentWriteActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksRequest,
            HandleWriteDeviceBlocksUndelivery);
        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksResponse,
            HandleWriteDeviceBlocksResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TWriteBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "WriteBlocks",
        requestInfo->CallContext->RequestId);

    auto replyError = [this] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>(
            PartConfig->MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "WriteBlocks",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    for (const auto& buffer: msg->Record.GetBlocks().GetBuffers()) {
        if (buffer.size() % PartConfig->GetBlockSize() != 0) {
            replyError(
                ctx,
                *requestInfo,
                E_ARGUMENT,
                TStringBuilder() << "buffer not divisible by blockSize: "
                    << buffer.size() << " % " << PartConfig->GetBlockSize()
                    << " != 0");
            return;
        }
    }

    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        CalculateWriteRequestBlockCount(msg->Record, PartConfig->GetBlockSize())
    );

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    bool ok = InitRequests<TEvService::TWriteBlocksMethod>(
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

    auto actorId = NCloud::Register<TDiskAgentWriteActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(timeoutPolicy),
        std::move(deviceRequests),
        PartConfig,
        SelfId(),
        assignVolumeRequestId,
        false); // replyLocal

    RequestsInProgress.AddWriteRequest(actorId);
}

void TNonreplicatedPartitionActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TWriteBlocksLocalMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "WriteBlocks",
        requestInfo->CallContext->RequestId);

    auto replyError = [this] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            PartConfig->MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "WriteBlocks",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    auto guard = msg->Record.Sglist.Acquire();

    if (!guard) {
        replyError(
            ctx,
            *requestInfo,
            E_CANCELLED,
            "failed to acquire sglist in NonreplicatedPartitionActor");
        return;
    }

    auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.BlocksCount);

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    bool ok = InitRequests<TEvService::TWriteBlocksLocalMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests,
        &timeoutPolicy);

    if (!ok) {
        return;
    }

    // convert local request to remote

    SgListCopy(
        guard.Get(),
        ResizeIOVector(
            *msg->Record.MutableBlocks(),
            msg->Record.BlocksCount,
            PartConfig->GetBlockSize()));

    const bool assignVolumeRequestId =
        Config->GetAssignIdToWriteAndZeroRequestsEnabled() &&
        !msg->Record.GetHeaders().GetIsBackgroundRequest();

    auto actorId = NCloud::Register<TDiskAgentWriteActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(timeoutPolicy),
        std::move(deviceRequests),
        PartConfig,
        SelfId(),
        assignVolumeRequestId,
        true); // replyLocal

    RequestsInProgress.AddWriteRequest(actorId);
}

void TNonreplicatedPartitionActor::HandleWriteBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Complete write blocks", SelfId().ToString().c_str());

    UpdateStats(msg->Stats);

    const auto requestBytes = msg->Stats.GetUserWriteCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.WriteBlocks.AddRequest(time, requestBytes);
    PartCounters->Interconnect.WriteBytes.Increment(requestBytes);
    PartCounters->Interconnect.WriteCount.Increment(1);

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
