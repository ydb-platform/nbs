#include "actor_writefreshblocks.h"
#include "cloud/blockstore/libs/storage/core/block_handler.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>

#include <cloud/storage/core/libs/common/verify.h>

#include <library/cpp/lwtrace/all.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NPartition;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TWriteFreshBlocksActor::TWriteFreshBlocksActor(
        const NActors::TActorId& owner,
        const NActors::TActorId& actorToAddFreshBlocks,
        ui64 commitId,
        ui32 channel,
        ui32 blockCount,
        TVector<TRequest> requests,
        TVector<TBlockRange32> blockRanges,
        TVector<IWriteBlocksHandlerPtr> writeHandlers,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui64 tabletId,
        bool waitForAddFreshBlocksResponseBeforeResponse)
    : Owner(owner)
    , ActorToAddFreshBlocks(actorToAddFreshBlocks)
    , CommitId(commitId)
    , Channel(channel)
    , BlockCount(blockCount)
    , Requests(std::move(requests))
    , BlockRanges(std::move(blockRanges))
    , WriteHandlers(std::move(writeHandlers))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , IsZeroRequest(
          Requests.size() == 1 &&
          Requests.front().RequestType == EFreshRequestType::ZeroBlocks)
    , WaitForAddFreshBlocksResponseBeforeResponse(
          waitForAddFreshBlocksResponseBeforeResponse)
    , TabletId(tabletId)
{
    if (!IsZeroRequest) {
        const bool hasAnyZeroRequest = AnyOf(
            Requests,
            [](auto r) { return r.RequestType == EFreshRequestType::ZeroBlocks; });

        STORAGE_VERIFY(
            !hasAnyZeroRequest && BlockRanges.size() == WriteHandlers.size(),
            TWellKnownEntityTypes::TABLET,
            tabletId);
    } else {
        STORAGE_VERIFY(
            WriteHandlers.empty() && BlockRanges.size() == 1,
            TWellKnownEntityTypes::TABLET,
            tabletId);
    }
}

void TWriteFreshBlocksActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TDeque<TRequestScope> timers;

    ui64 requestId = 0;

    for (const auto& r: Requests) {
        LWTRACK(
            RequestReceived_PartitionWorker,
            r.RequestInfo->CallContext->LWOrbit,
            IsZeroRequest ? "ZeroFreshBlocks" : "WriteFreshBlocks",
            r.RequestInfo->CallContext->RequestId);

        timers.emplace_back(*r.RequestInfo);

        if (!r.RequestInfo->CallContext->LWOrbit.Fork(CombinedContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                r.RequestInfo->CallContext->LWOrbit,
                "TEvPartitionCommonPrivate::TEvWriteBlobRequest",
                r.RequestInfo->CallContext->RequestId);
        }

        if (r.RequestInfo->CallContext->LWOrbit.HasShuttles()) {
            requestId = r.RequestInfo->CallContext->RequestId;
        }
    }
    CombinedContext->RequestId = requestId;

    Become(&TThis::StateWork);

    WriteBlob(ctx);
}

NProto::TError TWriteFreshBlocksActor::BuildBlobContentAndComputeDigest()
{
    if (IsZeroRequest) {
        BlobContent = BuildZeroFreshBlocksBlobContent(BlockRanges.front());
        BlobSize = BlobContent.size();

        return {};
    }

    TVector<TGuardHolder> holders(Reserve(BlockRanges.size()));

    auto blockRange = BlockRanges.begin();
    auto writeHandler = WriteHandlers.begin();

    while (blockRange != BlockRanges.end()) {
        const auto& holder = holders.emplace_back(
            (**writeHandler).GetBlocks(ConvertRangeSafe(*blockRange)));

        if (!holder.Acquired()) {
            return MakeError(
                E_CANCELLED,
                "failed to acquire sglist in WriteFreshBlocksActor");
        }

        const auto& sgList = holder.GetSgList();

        for (size_t index = 0; index < sgList.size(); ++index) {
            const ui32 blockIndex = blockRange->Start + index;

            const auto digest = BlockDigestGenerator->ComputeDigest(
                blockIndex,
                sgList[index]);

            if (digest.Defined()) {
                AffectedBlockInfos.push_back({blockIndex, *digest});
            }
        }

        ++blockRange;
        ++writeHandler;
    }

    BlobContent = BuildWriteFreshBlocksBlobContent(BlockRanges, holders);
    BlobSize = BlobContent.size();

    return {};
}

void TWriteFreshBlocksActor::WriteBlob(const NActors::TActorContext& ctx)
{
    auto error = BuildBlobContentAndComputeDigest();
    if (HandleError(ctx, error)) {
        return;
    }

    Y_ABORT_UNLESS(!BlobContent.empty());

    const auto [generation, step] = ParseCommitId(CommitId);

    TPartialBlobId blobId(
        generation,
        step,
        Channel,
        static_cast<ui32>(BlobContent.size()),
        0,  // cookie
        0   // partId
    );

    auto request = std::make_unique<TEvPartitionCommonPrivate::TEvWriteBlobRequest>(
        CombinedContext,
        blobId,
        std::move(BlobContent),
        0,      // blockSizeForChecksums
        false); // async

    NCloud::Send(
        ctx,
        Owner,
        std::move(request));
}

void TWriteFreshBlocksActor::AddBlocks(const NActors::TActorContext& ctx)
{
    STORAGE_VERIFY(BlobSize > 0, TWellKnownEntityTypes::TABLET, TabletId);

    IEventBasePtr request =
        std::make_unique<TEvPartitionCommonPrivate::TEvAddFreshBlocksRequest>(
            CombinedContext,
            CommitId,
            BlobSize,
            std::move(BlockRanges),
            std::move(WriteHandlers));

    NCloud::Send(ctx, ActorToAddFreshBlocks, std::move(request));
}

template <typename TEvent>
void TWriteFreshBlocksActor::NotifyCompleted(
    const NActors::TActorContext& ctx,
    std::unique_ptr<TEvent> ev)
{
    ev->ExecCycles = Requests.front().RequestInfo->GetExecCycles();
    ev->TotalCycles = Requests.front().RequestInfo->GetTotalCycles();
    ev->CommitId = CommitId;
    ev->AffectedBlockInfos = std::move(AffectedBlockInfos);

    auto execTime = CyclesToDurationSafe(ev->ExecCycles);
    auto waitTime =
        CyclesToDurationSafe(Requests.front().RequestInfo->GetWaitCycles());

    auto& counters = *ev->Stats.MutableUserWriteCounters();
    counters.SetRequestsCount(Requests.size());
    counters.SetBatchCount(1);
    counters.SetBlocksCount(BlockCount);
    counters.SetExecTime(execTime.MicroSeconds());
    counters.SetWaitTime(waitTime.MicroSeconds());

    NCloud::Send(ctx, Owner, std::move(ev));
}

bool TWriteFreshBlocksActor::HandleError(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        ReplyAllAndDie(ctx, error);
        return true;
    }
    return false;
}

void TWriteFreshBlocksActor::ReplyWrite(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    for (const auto& r: Requests) {
        IEventBasePtr response = CreateWriteBlocksResponse(
            r.RequestType == EFreshRequestType::WriteBlocksLocal,
            error);

        LWTRACK(
            ResponseSent_Partition,
            r.RequestInfo->CallContext->LWOrbit,
            "WriteFreshBlocks",
            r.RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *r.RequestInfo, std::move(response));
    }
}

void TWriteFreshBlocksActor::ReplyZero(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    for (const auto& r: Requests) {
        IEventBasePtr response =
            std::make_unique<TEvService::TEvZeroBlocksResponse>(error);

        LWTRACK(
            ResponseSent_Partition,
            r.RequestInfo->CallContext->LWOrbit,
            "ZeroFreshBlocks",
            r.RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *r.RequestInfo, std::move(response));
    }
}

void TWriteFreshBlocksActor::Reply(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    if (ReplySent) {
        return;
    }

    if (IsZeroRequest) {
        ReplyZero(ctx, error);
    } else {
        ReplyWrite(ctx, error);
    }

    ReplySent = true;
}

void TWriteFreshBlocksActor::NotifyWrite(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    auto completeEvent =
        std::make_unique<TEvPartitionCommonPrivate::TEvWriteFreshBlocksCompleted>(
            error);
    NotifyCompleted(ctx, std::move(completeEvent));
}

void TWriteFreshBlocksActor::NotifyZero(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    auto completeEvent =
        std::make_unique<TEvPartitionCommonPrivate::TEvZeroFreshBlocksCompleted>(
            error);
    NotifyCompleted(ctx, std::move(completeEvent));
}

void TWriteFreshBlocksActor::Notify(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    if (IsZeroRequest) {
        NotifyZero(ctx, error);
    } else {
        NotifyWrite(ctx, error);
    }
}

void TWriteFreshBlocksActor::ReplyAllAndDie(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    Reply(ctx, error);
    Notify(ctx, error);

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteFreshBlocksActor::HandleWriteBlobResponse(
    const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    for (const auto& r: Requests) {
        r.RequestInfo->AddExecCycles(msg->ExecCycles);
    }

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    for (const auto& r: Requests) {
        r.RequestInfo->CallContext->LWOrbit.Join(CombinedContext->LWOrbit);
    }

    // WARNING: Don't swap AddBlocks and Reply. This order ensures that the
    // update of the fresh cache in the partition occurs before the response to
    // the user is sent. Therefore, (by FIFO mailbox guarantee) the reads will
    // be consistent.
    AddBlocks(ctx);
    if (!WaitForAddFreshBlocksResponseBeforeResponse) {
        Reply(ctx, {});
    }
}

void TWriteFreshBlocksActor::HandleAddFreshBlocksResponse(
    const TEvPartitionCommonPrivate::TEvAddFreshBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (HasError(ev->Get()->GetError())) {
        ReportAddFreshBlocksResultedInError(
            "unexpected error in AddFreshBlocksResponse",
            {{"error", FormatError(ev->Get()->GetError())},
             {"tabletId", ToString(TabletId)}});
    }

    ReplyAllAndDie(ctx, {});
}

void TWriteFreshBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    ReplyAllAndDie(ctx, error);
}

STFUNC(TWriteFreshBlocksActor::StateWork)
{
    TVector<TRequestScope> timers;

    for (const auto& r: Requests) {
        timers.emplace_back(*r.RequestInfo);
    }

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvPartitionCommonPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(
            TEvPartitionCommonPrivate::TEvAddFreshBlocksResponse,
            HandleAddFreshBlocksResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
