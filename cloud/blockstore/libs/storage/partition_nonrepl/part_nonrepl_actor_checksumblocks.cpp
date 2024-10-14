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
#include <util/string/vector.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using EReason = TEvNonreplPartitionPrivate::TCancelRequest::EReason;

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
    const TRequestInfoPtr RequestInfo;
    const NProto::TChecksumBlocksRequest Request;
    const TVector<TDeviceRequest> DeviceRequests;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TActorId Part;

    TInstant StartTime;
    TMap<ui64, TPartialChecksum> Checksums;
    ui32 RequestsCompleted = 0;

public:
    TDiskAgentChecksumActor(
        TRequestInfoPtr requestInfo,
        NProto::TChecksumBlocksRequest request,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part);

    void Bootstrap(const TActorContext& ctx);

private:
    void ChecksumBlocks(const TActorContext& ctx);

    void HandleError(const TActorContext& ctx, NProto::TError error);

    void Done(const TActorContext& ctx, IEventBasePtr response, bool failed);

private:
    STFUNC(StateWork);

    void HandleChecksumDeviceBlocksResponse(
        const TEvDiskAgent::TEvChecksumDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleChecksumDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleCancelRequest(
        const TEvNonreplPartitionPrivate::TEvCancelRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskAgentChecksumActor::TDiskAgentChecksumActor(
        TRequestInfoPtr requestInfo,
        NProto::TChecksumBlocksRequest request,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , DeviceRequests(std::move(deviceRequests))
    , PartConfig(std::move(partConfig))
    , Part(part)
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

void TDiskAgentChecksumActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error)
{
    Y_DEBUG_ABORT_UNLESS(FAILED(error.GetCode()));

    ProcessError(ctx, *PartConfig, error);

    auto response =
        std::make_unique<TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse>(
            std::move(error));
    Done(ctx, std::move(response), true);
}

void TDiskAgentChecksumActor::Done(
    const TActorContext& ctx,
    IEventBasePtr response,
    bool failed)
{
    LWTRACK(
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        "ChecksumBlocks",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted>();
    auto& counters = *completion->Stats.MutableSysChecksumCounters();
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

void TDiskAgentChecksumActor::HandleCancelRequest(
    const TEvNonreplPartitionPrivate::TEvCancelRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TVector<TString> devices;
    for (const auto& reuqest: DeviceRequests) {
        devices.push_back(reuqest.Device.GetDeviceUUID());
    }

    switch (msg->Reason) {
        case EReason::Timeouted:
            LOG_WARN_S(
                ctx,
                TBlockStoreComponents::PARTITION_WORKER,
                "ChecksumBlocks request #"
                    << GetRequestId(Request) << " timed out. Disk id: "
                    << PartConfig->GetName() << " Devices: ["
                    << JoinVectorIntoString(devices, ", ") << "]");

            HandleError(
                ctx,
                PartConfig->MakeError(
                    E_TIMEOUT,
                    "ChecksumBlocks request timed out"));
            return;
        case EReason::Canceled:
            LOG_WARN_S(
                ctx,
                TBlockStoreComponents::PARTITION_WORKER,
                "ChecksumBlocks request #"
                    << GetRequestId(Request)
                    << " is canceled from outside. Disk id: "
                    << PartConfig->GetName() << " Devices: ["
                    << JoinVectorIntoString(devices, ", ") << "]");

            HandleError(
                ctx,
                PartConfig->MakeError(
                    E_CANCELLED,
                    "ChecksumBlocks request is canceled"));
            return;
    }

    Y_DEBUG_ABORT_UNLESS(false);
    HandleError(
        ctx,
        PartConfig->MakeError(
            E_CANCELLED,
            TStringBuilder()
                << "ChecksumBlocks request got an unknown cancel reason: "
                << static_cast<int>(msg->Reason)));
}

// void TDiskAgentChecksumActor::HandleTimeout(
//     const TEvents::TEvWakeup::TPtr& ev,
//     const TActorContext& ctx)
// {
//     const auto& device = DeviceRequests[ev->Cookie].Device;
//     LOG_WARN_S(
//         ctx,
//         TBlockStoreComponents::PARTITION_WORKER,
//         "ChecksumBlocks request #"
//             << GetRequestId(Request) << " timed out. Disk id: "
//             << PartConfig->GetName() << " Device: " << LogDevice(device));

//     HandleError(ctx, MakeError(E_TIMEOUT, "ChecksumBlocks request timed out"));
// }

void TDiskAgentChecksumActor::HandleChecksumDeviceBlocksResponse(
    const TEvDiskAgent::TEvChecksumDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetError().GetCode())) {
        HandleError(ctx, msg->GetError());
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

    Done(ctx, std::move(response), false);
}

STFUNC(TDiskAgentChecksumActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgent::TEvChecksumDeviceBlocksRequest,
            HandleChecksumDeviceBlocksUndelivery);
        HFunc(
            TEvDiskAgent::TEvChecksumDeviceBlocksResponse,
            HandleChecksumDeviceBlocksResponse);
        HFunc(
            TEvNonreplPartitionPrivate::TEvCancelRequest,
            HandleCancelRequest);

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
    TRequest request;
    bool ok = InitRequests<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
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

    auto actorId = NCloud::Register<TDiskAgentChecksumActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(deviceRequests),
        PartConfig,
        SelfId());

    RequestsInProgress.AddReadRequest(actorId, std::move(request));
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
