#include "copy_range.h"

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/storage/core/libs/common/sglist.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NPartition;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TCopyRangeActor::TCopyRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TActorId source,
        TActorId target,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NActors::TActorId volumeActorId,
        bool assignVolumeRequestId,
        TActorId actorToLockAndDrainRange)
    : RequestInfo(std::move(requestInfo))
    , BlockSize(blockSize)
    , Range(range)
    , Source(source)
    , Target(target)
    , WriterClientId(std::move(writerClientId))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , VolumeActorId(volumeActorId)
    , AssignVolumeRequestId(assignVolumeRequestId)
    , ActorToLockAndDrainRange(actorToLockAndDrainRange)
{
}

void TCopyRangeActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "CopyRange",
        RequestInfo->CallContext->RequestId);

    if (ActorToLockAndDrainRange) {
        LockAndDrainRange(ctx);
        return;
    }
    if (AssignVolumeRequestId) {
        GetVolumeRequestId(ctx);
        return;
    }
    ReadBlocks(ctx);
}

void TCopyRangeActor::GetVolumeRequestId(const NActors::TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        VolumeActorId,
        std::make_unique<TEvVolumePrivate::TEvTakeVolumeRequestIdRequest>());
}

void TCopyRangeActor::LockAndDrainRange(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        ActorToLockAndDrainRange,
        std::make_unique<TEvPartition::TEvLockAndDrainRangeRequest>(Range));
}

void TCopyRangeActor::ReadBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(BackgroundOpsClientId));

    auto event = std::make_unique<IEventHandle>(
        Source,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        0,            // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());

    ReadStartTs = ctx.Now();
}

void TCopyRangeActor::WriteBlocks(
    const TActorContext& ctx,
    NProto::TReadBlocksResponse& readResponse)
{
    NProto::TIOVector blocks = std::move(*readResponse.MutableBlocks());
    // BlobStorage-based volumes returns empty blocks for zero-blocks.
    for (auto& block: *blocks.MutableBuffers()) {
        if (block.empty()) {
            block = TString(BlockSize, 0);
        }
    }

    auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    if (readResponse.HasChecksum()) {
        request->Record.MutableChecksums()->Add(
            std::move(*readResponse.MutableChecksum()));
    }
    request->Record.MutableBlocks()->Swap(&blocks);
    auto clientId =
        WriterClientId ? WriterClientId : TString(BackgroundOpsClientId);

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(std::move(clientId));
    headers->SetVolumeRequestId(VolumeRequestId);

    const auto& buffers = request->Record.GetBlocks().GetBuffers();
    for (int i = 0; i < buffers.size();++i) {
        const TString& block = buffers.Get(i);
        auto blockIndex = i + Range.Start;

        const auto digest = BlockDigestGenerator->ComputeDigest(
            blockIndex,
            TBlockDataRef(block.data(), block.size()));

        if (digest.Defined()) {
            AffectedBlockInfos.push_back({blockIndex, *digest});
        }
    }

    auto event = std::make_unique<IEventHandle>(
        Target,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        0,            // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());

    WriteStartTs = ctx.Now();
}

void TCopyRangeActor::ZeroBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    auto clientId =
        WriterClientId ? WriterClientId : TString(BackgroundOpsClientId);
    request->Record.SetBlocksCount(Range.Size());

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(std::move(clientId));

    for (const auto blockIndex: xrange(Range)) {
        const auto digest = BlockDigestGenerator->ComputeDigest(
            blockIndex,
            TBlockDataRef::CreateZeroBlock(BlockSize));

        if (digest.Defined()) {
            AffectedBlockInfos.push_back({blockIndex, *digest});
        }
    }

    auto event = std::make_unique<IEventHandle>(
        Target,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        0,            // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());

    WriteStartTs = ctx.Now();
}

void TCopyRangeActor::Done(const TActorContext& ctx, NProto::TError error)
{
    if (NeedToReleaseRange) {
        NCloud::Send(
            ctx,
            ActorToLockAndDrainRange,
            std::make_unique<TEvPartition::TEvReleaseRange>(Range));
    }

    using EExecutionSide =
        TEvNonreplPartitionPrivate::TEvRangeMigrated::EExecutionSide;

    auto response =
        std::make_unique<TEvNonreplPartitionPrivate::TEvRangeMigrated>(
            std::move(error),
            EExecutionSide::Local,
            Range,
            ReadStartTs,
            ReadDuration,
            WriteStartTs,
            WriteDuration,
            std::move(AffectedBlockInfos),
            0,   // RecommendedBandwidth,
            AllZeroes,
            RequestInfo->GetExecCycles());

    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "CopyRange",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCopyRangeActor::HandleVolumeRequestId(
    const TEvVolumePrivate::TEvTakeVolumeRequestIdResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        Done(ctx, msg->GetError());
        return;
    }

    VolumeRequestId = msg->VolumeRequestId;
    ReadBlocks(ctx);
}

void TCopyRangeActor::HandleReadUndelivery(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    Y_UNUSED(ev);

    Done(ctx, MakeError(E_REJECTED, "ReadBlocks request undelivered"));
}

void TCopyRangeActor::HandleLockAndDrainRangeResponse(
    const TEvPartition::TEvLockAndDrainRangeResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        Done(ctx, msg->GetError());
        return;
    }
    NeedToReleaseRange = true;
    if (AssignVolumeRequestId) {
        GetVolumeRequestId(ctx);
        return;
    }
    ReadBlocks(ctx);
}

void TCopyRangeActor::HandleReadResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    auto* msg = ev->Get();

    if (HasError(msg->Record.GetError())) {
        Done(ctx, msg->Record.GetError());
        return;
    }

    if (msg->Record.GetAllZeroes()) {
        AllZeroes = true;
        ZeroBlocks(ctx);
    } else {
        WriteBlocks(ctx, msg->Record);
    }
}

void TCopyRangeActor::HandleWriteUndelivery(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    Y_UNUSED(ev);

    Done(ctx, MakeError(E_REJECTED, "WriteBlocks request undelivered"));
}

void TCopyRangeActor::HandleWriteResponse(
    const TEvService::TEvWriteBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    auto* msg = ev->Get();

    Done(ctx, msg->Record.GetError());
}

void TCopyRangeActor::HandleZeroUndelivery(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    Y_UNUSED(ev);

    Done(ctx, MakeError(E_REJECTED, "ZeroBlocks request undelivered"));
}

void TCopyRangeActor::HandleZeroResponse(
    const TEvService::TEvZeroBlocksResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    auto* msg = ev->Get();

    Done(ctx, msg->Record.GetError());
}

void TCopyRangeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Done(ctx, MakeError(E_REJECTED, "Dead"));
}

STFUNC(TCopyRangeActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvVolumePrivate::TEvTakeVolumeRequestIdResponse,
            HandleVolumeRequestId);
        HFunc(
            TEvPartition::TEvLockAndDrainRangeResponse,
            HandleLockAndDrainRangeResponse);

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadUndelivery);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteUndelivery);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroUndelivery);
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadResponse);
        HFunc(TEvService::TEvWriteBlocksResponse, HandleWriteResponse);
        HFunc(TEvService::TEvZeroBlocksResponse, HandleZeroResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
