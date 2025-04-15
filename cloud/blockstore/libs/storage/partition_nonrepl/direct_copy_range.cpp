#include "direct_copy_range.h"

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/copy_range.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_common.h>
#include <cloud/storage/core/libs/common/sglist.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 SourcePartitionTag = 1;
constexpr ui64 TargetPartitionTag = 2;

}   // namespace

TDirectCopyRangeActor::TDirectCopyRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TActorId source,
        TActorId target,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NActors::TActorId volumeActorId,
        bool assignVolumeRequestId)
    : BlockSize(blockSize)
    , Range(range)
    , SourceActor(source)
    , TargetActor(target)
    , WriterClientId(std::move(writerClientId))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , VolumeActorId(volumeActorId)
    , AssignVolumeRequestId(assignVolumeRequestId)
    , RequestInfo(std::move(requestInfo))
{}

void TDirectCopyRangeActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "DirectCopyRange",
        RequestInfo->CallContext->RequestId);

    if (AssignVolumeRequestId) {
        GetVolumeRequestId(ctx);
        return;
    }
    GetDevicesInfo(ctx);
}

void TDirectCopyRangeActor::GetVolumeRequestId(
    const NActors::TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        VolumeActorId,
        std::make_unique<TEvVolumePrivate::TEvTakeVolumeRequestIdRequest>());
}

void TDirectCopyRangeActor::GetDevicesInfo(const TActorContext& ctx)
{
    using EPurpose =
        TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::EPurpose;

    ctx.Send(
        SourceActor,
        std::make_unique<
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest>(
            EPurpose::ForReading,
            Range),
        0,
        SourcePartitionTag);
    ctx.Send(
        TargetActor,
        std::make_unique<
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest>(
            EPurpose::ForWriting,
            Range),
        0,
        TargetPartitionTag);
}

void TDirectCopyRangeActor::DirectCopy(const NActors::TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksRequest>();
    auto& rec = request->Record;

    rec.MutableHeaders()->SetIsBackgroundRequest(true);
    rec.MutableHeaders()->SetClientId(TString(BackgroundOpsClientId));
    rec.MutableHeaders()->SetVolumeRequestId(VolumeRequestId);
    rec.SetSourceDeviceUUID(SourceInfo->Device.GetDeviceUUID());
    rec.SetSourceStartIndex(SourceInfo->DeviceBlockRange.Start);
    rec.SetBlockSize(BlockSize);
    rec.SetBlockCount(SourceInfo->DeviceBlockRange.Size());
    rec.SetTargetNodeId(TargetInfo->Device.GetNodeId());
    rec.SetTargetClientId(
        WriterClientId ? WriterClientId : TString(BackgroundOpsClientId));
    rec.SetTargetDeviceUUID(TargetInfo->Device.GetDeviceUUID());
    rec.SetTargetStartIndex(TargetInfo->DeviceBlockRange.Start);

    auto event = std::make_unique<IEventHandle>(
        MakeDiskAgentServiceId(SourceInfo->Device.GetNodeId()),
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        0,
        &ctx.SelfID   // forwardOnNondelivery
    );

    StartTs = ctx.Now();
    ctx.Send(std::move(event));
    ctx.Schedule(
        SourceInfo->RequestTimeout + TargetInfo->RequestTimeout,
        new TEvents::TEvWakeup());
}

void TDirectCopyRangeActor::Fallback(const TActorContext& ctx)
{
    NCloud::Register<TCopyRangeActor>(
        ctx,
        std::move(RequestInfo),
        BlockSize,
        Range,
        SourceActor,
        TargetActor,
        WriterClientId,
        BlockDigestGenerator);

    Die(ctx);
}

void TDirectCopyRangeActor::Done(const TActorContext& ctx, NProto::TError error)
{
    using EExecutionSide =
        TEvNonreplPartitionPrivate::TEvRangeMigrated::EExecutionSide;

    ProcessError(
        *NActors::TActorContext::ActorSystem(),
        *TargetInfo->PartConfig,
        error);

    const auto writeTs = StartTs + ReadDuration;
    auto response =
        std::make_unique<TEvNonreplPartitionPrivate::TEvRangeMigrated>(
            std::move(error),
            EExecutionSide::Remote,
            Range,
            StartTs,
            ReadDuration,
            writeTs,
            WriteDuration,
            TVector<IProfileLog::TBlockInfo>(),
            RecommendedBandwidth,
            AllZeroes,
            RequestInfo->GetExecCycles());

    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "DirectCopyRange",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TDirectCopyRangeActor::HandleVolumeRequestId(
    const TEvVolumePrivate::TEvTakeVolumeRequestIdResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        Done(ctx, msg->GetError());
        return;
    }

    VolumeRequestId = msg->VolumeRequestId;
    GetDevicesInfo(ctx);
}

void TDirectCopyRangeActor::HandleGetDeviceForRange(
    const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (ev->Cookie == SourcePartitionTag) {
        SourceInfo.reset(ev->Release().Release());
    } else if (ev->Cookie == TargetPartitionTag) {
        TargetInfo.reset(ev->Release().Release());
    }

    if (!SourceInfo || !TargetInfo) {
        return;
    }

    if (FAILED(SourceInfo->Error.GetCode()) ||
        FAILED(TargetInfo->Error.GetCode()))
    {
        Fallback(ctx);
        return;
    }

    DirectCopy(ctx);
}

void TDirectCopyRangeActor::HandleDirectCopyUndelivered(
    const TEvDiskAgent::TEvDirectCopyBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    Done(
        ctx,
        MakeError(E_REJECTED, "DirectCopyBlocksRequest request undelivered"));
}

void TDirectCopyRangeActor::HandleDirectCopyBlocksResponse(
    const TEvDiskAgent::TEvDirectCopyBlocksResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (SUCCEEDED(msg->GetError().GetCode())) {
        ReadDuration = TDuration::MicroSeconds(msg->Record.GetReadDuration());
        WriteDuration = TDuration::MicroSeconds(msg->Record.GetReadDuration());
        AllZeroes = msg->Record.GetAllZeroes();
        RecommendedBandwidth = msg->Record.GetRecommendedBandwidth();
    }

    Done(ctx, msg->GetError());
}

void TDirectCopyRangeActor::HandleRangeMigrationTimeout(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(
        E_TIMEOUT,
        TStringBuilder() << "Range " << DescribeRange(Range)
                         << " migration timeout");
    Done(ctx, std::move(error));
}

void TDirectCopyRangeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Done(ctx, MakeError(E_REJECTED, "Dead"));
}

STFUNC(TDirectCopyRangeActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvVolumePrivate::TEvTakeVolumeRequestIdResponse,
            HandleVolumeRequestId);

        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse,
            HandleGetDeviceForRange);
        HFunc(
            TEvDiskAgent::TEvDirectCopyBlocksResponse,
            HandleDirectCopyBlocksResponse);
        HFunc(
            TEvDiskAgent::TEvDirectCopyBlocksRequest,
            HandleDirectCopyUndelivered);

        HFunc(TEvents::TEvWakeup, HandleRangeMigrationTimeout);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
