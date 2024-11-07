#include "part_nonrepl_actor.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TPartialChecksum
{
    ui64 Value;
    ui64 Size;
};

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentChecksumActor final
    : public TActorBootstrapped<TDiskAgentChecksumActor>
{
private:
    using EStatus = TEvNonreplPartitionPrivate::TOperationCompleted::EStatus;

    const TRequestInfoPtr RequestInfo;
    const NProto::TChecksumBlocksRequest Request;
    const TVector<TDeviceRequest> DeviceRequests;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TActorId Part;
    const TRequestTimeoutPolicy TimeoutPolicy;

    TInstant StartTime;
    TMap<ui64, TPartialChecksum> Checksums;
    ui32 RequestsCompleted = 0;

public:
    TDiskAgentChecksumActor(
        TRequestInfoPtr requestInfo,
        NProto::TChecksumBlocksRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part);

    void Bootstrap(const TActorContext& ctx);

private:
    void ChecksumBlocks(const TActorContext& ctx);

    bool HandleError(
        const TActorContext& ctx,
        NProto::TError error,
        bool timedOut);

    void Done(const TActorContext& ctx, IEventBasePtr response, EStatus status);

private:
    STFUNC(StateWork);

    void HandleChecksumDeviceBlocksResponse(
        const TEvDiskAgent::TEvChecksumDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleChecksumDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskAgentChecksumActor::TDiskAgentChecksumActor(
        TRequestInfoPtr requestInfo,
        NProto::TChecksumBlocksRequest request,
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
{}

void TDiskAgentChecksumActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "DiskAgentChecksum",
        RequestInfo->CallContext->RequestId);

    StartTime = ctx.Now();
    ctx.Schedule(TimeoutPolicy.Timeout, new TEvents::TEvWakeup());

    ChecksumBlocks(ctx);
}

void TDiskAgentChecksumActor::ChecksumBlocks(const TActorContext& ctx)
{
    const auto blockSize = PartConfig->GetBlockSize();

    ui32 cookie = 0;
    for (const auto& deviceRequest: DeviceRequests) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvChecksumDeviceBlocksRequest>();
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

bool TDiskAgentChecksumActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error,
    bool timedOut)
{
    if (FAILED(error.GetCode())) {
        ProcessError(ctx, *PartConfig, error);

        auto response = std::make_unique<TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse>(
            std::move(error));

        Done(
            ctx,
            std::move(response),
            timedOut ? EStatus::Timeout : EStatus::Fail);
        return true;
    }

    return false;
}

void TDiskAgentChecksumActor::Done(
    const TActorContext& ctx,
    IEventBasePtr response,
    EStatus status)
{
    LWTRACK(
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "ChecksumBlocks",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    auto completion = std::make_unique<
        TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted>(
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
    completion->Stats.MutableSysChecksumCounters()->SetBlocksCount(blocks);

    NCloud::Send(ctx, Part, std::move(completion));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentChecksumActor::HandleChecksumDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "ChecksumBlocks request #"
            << GetRequestId(Request) << " undelivered. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentChecksumActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "ChecksumBlocks request #"
            << GetRequestId(Request) << " timed out. Disk id: "
            << PartConfig->GetName() << " Device: " << LogDevice(device));

    HandleError(
        ctx,
        MakeError(
            TimeoutPolicy.ErrorCode,
            TimeoutPolicy.OverrideMessage ? TimeoutPolicy.OverrideMessage
                                          : "ChecksumBlocks request timed out"),
        true);
}

void TDiskAgentChecksumActor::HandleChecksumDeviceBlocksResponse(
    const TEvDiskAgent::TEvChecksumDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError(), false)) {
        return;
    }

    const auto& deviceRequest = DeviceRequests[ev->Cookie];
    const auto rangeStart = deviceRequest.BlockRange.Start;
    const auto rangeSize = deviceRequest.DeviceBlockRange.Size() * PartConfig->GetBlockSize();
    Checksums[rangeStart] = {msg->Record.GetChecksum(), rangeSize};

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    TBlockChecksum checksum;
    for (const auto& [_, partialChecksum]: Checksums) {
        checksum.Combine(partialChecksum.Value, partialChecksum.Size);
    }

    auto response = std::make_unique<TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse>();
    response->Record.SetChecksum(checksum.GetValue());

    Done(ctx, std::move(response), EStatus::Success);
}

STFUNC(TDiskAgentChecksumActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(TEvDiskAgent::TEvChecksumDeviceBlocksRequest, HandleChecksumDeviceBlocksUndelivery);
        HFunc(TEvDiskAgent::TEvChecksumDeviceBlocksResponse, HandleChecksumDeviceBlocksResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleChecksumBlocks(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ChecksumBlocks",
        requestInfo->CallContext->RequestId);

    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    bool ok = InitRequests<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
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

    auto actorId = NCloud::Register<TDiskAgentChecksumActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(timeoutPolicy),
        std::move(deviceRequests),
        PartConfig,
        SelfId());

    RequestsInProgress.AddReadRequest(actorId);
}

void TNonreplicatedPartitionActor::HandleChecksumBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Complete checksum blocks", SelfId().ToString().c_str());

    const auto requestBytes = msg->Stats.GetSysChecksumCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ChecksumBlocks.AddRequest(time, requestBytes);
    NetworkBytes += requestBytes;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    RequestsInProgress.RemoveRequest(ev->Sender);
    OnRequestCompleted(*msg, ctx.Now());
    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
