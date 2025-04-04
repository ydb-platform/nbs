#include "part_nonrepl_actor.h"

#include "part_nonrepl_actor_base_request.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentWriteActor final: public TDiskAgentBaseRequestActor
{
private:
    const bool AssignVolumeRequestId;
    const bool ReplyLocal;

    NProto::TWriteBlocksRequest Request;
    ui32 RequestsCompleted = 0;

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

protected:
    void SendRequest(const NActors::TActorContext& ctx) override;
    NActors::IEventBasePtr MakeResponse(NProto::TError error) override;
    TCompletionEventAndBody MakeCompletionResponse(ui32 blocks) override;
    bool OnMessage(TAutoPtr<NActors::IEventHandle>& ev) override;

private:
    void HandleWriteDeviceBlocksResponse(
        const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
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
    : TDiskAgentBaseRequestActor(
          std::move(requestInfo),
          GetRequestId(request),
          "WriteBlocks",
          std::move(timeoutPolicy),
          std::move(deviceRequests),
          std::move(partConfig),
          part)
    , AssignVolumeRequestId(assignVolumeRequestId)
    , ReplyLocal(replyLocal)
    , Request(std::move(request))
{}

void TDiskAgentWriteActor::SendRequest(const TActorContext& ctx)
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
            request->Record.SetVolumeRequestId(
                Request.GetHeaders().GetVolumeRequestId());
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

NActors::IEventBasePtr TDiskAgentWriteActor::MakeResponse(
    NProto::TError error)
{
    if (ReplyLocal) {
        return std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            std::move(error));
    }

    return std::make_unique<TEvService::TEvWriteBlocksResponse>(
        std::move(error));
}

TDiskAgentBaseRequestActor::TCompletionEventAndBody
TDiskAgentWriteActor::MakeCompletionResponse(ui32 blocks)
{
    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted>();

    completion->Stats.MutableUserWriteCounters()->SetBlocksCount(blocks);

    return TCompletionEventAndBody(std::move(completion));
}

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

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentWriteActor::HandleWriteDeviceBlocksResponse(
    const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, msg->GetError(), EStatus::Fail);
        return;
    }

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    Done(ctx, MakeResponse(NProto::TError()), EStatus::Success);
}

bool TDiskAgentWriteActor::OnMessage(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksRequest,
            HandleWriteDeviceBlocksUndelivery);
        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksResponse,
            HandleWriteDeviceBlocksResponse);
        default:
            return false;
    }
    return true;
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
    TRequestData request;
    bool ok = InitRequests<TEvService::TWriteBlocksMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests,
        &timeoutPolicy,
        &request);

    if (!ok) {
        return;
    }

    const bool assignVolumeRequestId =
        Config->GetAssignIdToWriteAndZeroRequestsEnabled();

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

    RequestsInProgress.AddWriteRequest(actorId, std::move(request));
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
    TRequestData request;
    bool ok = InitRequests<TEvService::TWriteBlocksLocalMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests,
        &timeoutPolicy,
        &request);

    if (!ok) {
        return;
    }

    if (guard.Get().empty()) {
        // can happen only if there is a bug in the code of the layers above
        // this one
        ReportEmptyRequestSgList();
        replyError(
            ctx,
            *requestInfo,
            E_ARGUMENT,
            "empty SgList in request");
        return;
    }

    // convert local request to remote

    // copying request data into a new TIOVector and moving it to msg->Record
    // afterwards since msg->Record.Blocks can be holding current request data
    // or parts of it
    NProto::TIOVector blocks;
    SgListCopy(
        guard.Get(),
        ResizeIOVector(
            blocks,
            msg->Record.BlocksCount,
            PartConfig->GetBlockSize()));
    *msg->Record.MutableBlocks() = std::move(blocks);

    // explicitly clearing request data (SgList) just in case anyone adds some
    // code to TDiskAgentWriteActor that tries to use it
    msg->Record.Sglist.SetSgList({});

    const bool assignVolumeRequestId =
        Config->GetAssignIdToWriteAndZeroRequestsEnabled();

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

    RequestsInProgress.AddWriteRequest(actorId, std::move(request));
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
