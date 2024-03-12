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
    , Range(range)
    , Source(source)
    , Target(target)
    , WriterClientId(std::move(writerClientId))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , Buffer(TString(Range.Size() * blockSize, 0))
    , SgList(Buffer.GetGuardedSgList())
{
    auto sgListOrError = SgListNormalize(SgList.Acquire().Get(), blockSize);

    Y_ABORT_UNLESS(!HasError(sgListOrError));
    SgList.SetSgList(sgListOrError.ExtractResult());
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
    const auto blockSize = Buffer.Get().Size() / Range.Size();

    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());
    request->Record.BlockSize = blockSize;
    request->Record.Sglist = SgList;

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

void TCopyRangeActor::WriteBlocks(const TActorContext& ctx)
{
    const auto blockSize = Buffer.Get().Size() / Range.Size();

    auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
    request->Record.SetStartIndex(Range.Start);
    auto clientId =
        WriterClientId ? WriterClientId : TString(BackgroundOpsClientId);
    request->Record.BlocksCount = Range.Size();
    request->Record.BlockSize = blockSize;
    request->Record.Sglist = SgList;

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(std::move(clientId));

    for (const auto blockIndex: xrange(Range)) {
        auto* data = Buffer.Get().Data() + (blockIndex - Range.Start) * blockSize;

        const auto digest = BlockDigestGenerator->ComputeDigest(
            blockIndex,
            TBlockDataRef(data, blockSize)
        );

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

void TCopyRangeActor::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvNonreplPartitionPrivate::TEvRangeMigrated>(
        std::move(Error),
        Range,
        ReadStartTs,
        ReadDuration,
        WriteStartTs,
        WriteDuration,
        std::move(AffectedBlockInfos),
        RequestInfo->GetExecCycles()
    );

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
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "ReadBlocks request undelivered");

    Done(ctx);
}

void TCopyRangeActor::HandleReadResponse(
    const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    auto* msg = ev->Get();

    Error = msg->Record.GetError();

    if (HasError(Error)) {
        Done(ctx);
        return;
    }

    WriteBlocks(ctx);
}

void TCopyRangeActor::HandleWriteUndelivery(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "WriteBlocks request undelivered");

    Done(ctx);
}

void TCopyRangeActor::HandleWriteResponse(
    const TEvService::TEvWriteBlocksLocalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    auto* msg = ev->Get();

    Error = msg->Record.GetError();
    Done(ctx);
}

void TCopyRangeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "Dead");
    Done(ctx);
}

STFUNC(TCopyRangeActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadUndelivery);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteUndelivery);
        HFunc(TEvService::TEvReadBlocksLocalResponse, HandleReadResponse);
        HFunc(TEvService::TEvWriteBlocksLocalResponse, HandleWriteResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
