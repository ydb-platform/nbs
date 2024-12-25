#include "copy_range.h"

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>
#include <cloud/storage/core/libs/common/sglist.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TCopyRangeActor::TCopyRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TActorId source,
        TActorId target,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator)
    : RequestInfo(std::move(requestInfo))
    , BlockSize(blockSize)
    , Range(range)
    , Source(source)
    , Target(target)
    , WriterClientId(std::move(writerClientId))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
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

    ReadBlocks(ctx);
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

void TCopyRangeActor::WriteBlocks(const TActorContext& ctx, NProto::TIOVector blocks)
{
    auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.MutableBlocks()->Swap(&blocks);
    auto clientId =
        WriterClientId ? WriterClientId : TString(BackgroundOpsClientId);

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(std::move(clientId));

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

void TCopyRangeActor::HandleReadUndelivery(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    Y_UNUSED(ev);

    Done(ctx, MakeError(E_REJECTED, "ReadBlocks request undelivered"));
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
        WriteBlocks(ctx, std::move(*msg->Record.MutableBlocks()));
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

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadUndelivery);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteUndelivery);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroUndelivery);
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadResponse);
        HFunc(TEvService::TEvWriteBlocksResponse, HandleWriteResponse);
        HFunc(TEvService::TEvZeroBlocksResponse, HandleZeroResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
