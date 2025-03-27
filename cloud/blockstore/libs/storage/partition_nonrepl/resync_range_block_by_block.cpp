#include "resync_range_block_by_block.h"

#include "cloud/blockstore/libs/storage/disk_agent/model/public.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct THealStat
{
    // How many times a replica has been used as a source for fixing a minor
    // error.
    size_t HealCount = 0;

    // How many blocks in a replica with minor errors have been fixed.
    size_t MinorErrorCount = 0;

    // How many blocks in a replica with major errors have been fixed.
    size_t MajorErrorCount = 0;

    size_t MajorFoundErrorCount = 0;

    [[nodiscard]] TString Print() const
    {
        return TStringBuilder()
               << "[healer=" << HealCount << ", minor=" << MinorErrorCount
               << ", major=" << MajorErrorCount << "]";
    }
};

struct TBlockVariant
{
    THealStat& HealStat;
    TString& Block;
};
using TBlockVariants = TVector<TBlockVariant>;

void HealMinors(const TBlockVariants& blockVariants)
{
    using TLess =
        decltype([](const auto* lhs, const auto* rhs) { return *lhs < *rhs; });
    TMap<TString*, size_t, TLess> variations;

    for (const auto& blockVariant: blockVariants) {
        variations[&blockVariant.Block]++;
    }

    if (variations.size() == 1) {
        // All replicas match.
        return;
    }

    for (auto [data, count]: variations) {
        if (count > 1) {
            // We found minor mismatch!
            for (const auto& blockVariant: blockVariants) {
                if (blockVariant.Block == *data) {
                    ++blockVariant.HealStat.HealCount;
                } else {
                    // Heal block.
                    blockVariant.Block = *data;
                    ++blockVariant.HealStat.MinorErrorCount;
                }
            }
        }
    }
}

}   // namespace

TResyncRangeBlockByBlockActor::TResyncRangeBlockByBlockActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TVector<TReplicaDescriptor> replicas,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NProto::EResyncPolicy resyncPolicy)
    : RequestInfo(std::move(requestInfo))
    , BlockSize(blockSize)
    , Range(range)
    , Replicas(std::move(replicas))
    , WriterClientId(std::move(writerClientId))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , ResyncPolicy(resyncPolicy)
{}

void TResyncRangeBlockByBlockActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ResyncRangeBlockByBlock",
        RequestInfo->CallContext->RequestId);

    ReadBuffers.resize(Replicas.size());
    for (size_t replica = 0; replica < Replicas.size(); replica++) {
        ActorsToResync.push_back(replica);
        ReadReplicaBlocks(ctx, replica);
    }
}

void TResyncRangeBlockByBlockActor::PrepareWriteBuffers(
    const NActors::TActorContext& ctx)
{
    TVector<THealStat> healStat(Replicas.size());

    // Let's try to fix range block by block. At the same time, we will
    // calculate which replicas were used to fix the rest of the replicas.
    for (size_t blockIndex = 0; blockIndex < Range.Size(); ++blockIndex) {
        TBlockVariants blockVariants;
        blockVariants.reserve(Replicas.size());
        for (size_t replica = 0; replica < Replicas.size(); ++replica) {
            blockVariants.push_back(TBlockVariant{
                .HealStat = healStat[replica],
                .Block = *ReadBuffers[replica].MutableBuffers(blockIndex)});
        }
        HealMinors(blockVariants);
    }

    // We find the replica that fixed the largest number of blocks and use it as
    // a sample to fix the rest of the replicas.
    size_t bestHealer = 0;
    for (size_t i = 1; i < Replicas.size(); ++i) {
        if (healStat[bestHealer].HealCount < healStat[i].HealCount) {
            bestHealer = i;
        }
    }

    for (size_t blockIndex = 0; blockIndex < Range.Size(); ++blockIndex) {
        const auto& donorBlock = ReadBuffers[bestHealer].GetBuffers(blockIndex);

        for (size_t replica = 0; replica < Replicas.size(); ++replica) {
            if (replica == bestHealer) {
                continue;
            }

            auto& replicaBlock =
                *ReadBuffers[replica].MutableBuffers(blockIndex);

            if (donorBlock == replicaBlock) {
                continue;
            }

            if (ResyncPolicy ==
                NProto::EResyncPolicy::MINOR_AND_MAJOR_BLOCK_BY_BLOCK)
            {
                // Replace block.
                replicaBlock = donorBlock;
                ++healStat[replica].MajorErrorCount;
            } else {
                ++healStat[replica].MajorFoundErrorCount;
            }
        }
    }

    size_t minorFixCount = Accumulate(
        healStat,
        size_t{},
        [](size_t count, const THealStat& h)
        { return count + h.MinorErrorCount; });
    size_t majorFixCount = Accumulate(
        healStat,
        size_t{},
        [](size_t count, const THealStat& h)
        { return count + h.MajorErrorCount; });
    TString replicaStat = Accumulate(
        healStat,
        TString(),
        [](const TString& acc, const THealStat& h)
        { return acc.empty() ? h.Print() : acc + " " + h.Print(); });

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Replica %lu elected as best healer. In range %s %lu blocks were "
        "healed and %lu blocks were overwritten during resync. %s%s",
        Replicas[bestHealer].ReplicaId.c_str(),
        bestHealer,
        Range.Print().c_str(),
        minorFixCount,
        majorFixCount,
        (majorFixCount == 0 ? "All blocks healed as minor! " : ""),
        replicaStat.c_str());

    // Do writes only to replicas with fixed errors.
    ActorsToResync.clear();
    for (size_t i = 0; i < Replicas.size(); ++i) {
        if (healStat[i].MinorErrorCount || healStat[i].MajorErrorCount) {
            ActorsToResync.push_back(i);
        }
    }
}

void TResyncRangeBlockByBlockActor::ReadReplicaBlocks(
    const TActorContext& ctx,
    size_t replicaIndex)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(BackgroundOpsClientId));

    auto event = std::make_unique<NActors::IEventHandle>(
        Replicas[replicaIndex].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        replicaIndex,   // cookie
        &ctx.SelfID     // forwardOnNondelivery
    );

    ctx.Send(event.release());

    ReadStartTs = ctx.Now();
}

void TResyncRangeBlockByBlockActor::WriteBlocks(const TActorContext& ctx)
{
    for (size_t i = 0; i < ActorsToResync.size(); ++i) {
        WriteReplicaBlocks(ctx, ActorsToResync[i], std::move(ReadBuffers[i]));
    }

    WriteStartTs = ctx.Now();
}

void TResyncRangeBlockByBlockActor::WriteReplicaBlocks(
    const TActorContext& ctx,
    size_t replicaIndex,
    NProto::TIOVector data)
{
    auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    auto clientId =
        WriterClientId ? WriterClientId : TString(BackgroundOpsClientId);
    data.Swap(request->Record.MutableBlocks());

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(std::move(clientId));

    for (size_t i = 0; i < request->Record.GetBlocks().BuffersSize(); ++i) {
        size_t blockIndex = Range.Start + i;
        const auto digest = BlockDigestGenerator->ComputeDigest(
            blockIndex,
            TBlockDataRef(
                request->Record.GetBlocks().GetBuffers(i).data(),
                BlockSize));

        if (digest.Defined()) {
            AffectedBlockInfos.push_back({blockIndex, *digest});
        }
    }

    auto event = std::make_unique<NActors::IEventHandle>(
        Replicas[replicaIndex].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        replicaIndex,          // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Replica %lu Overwrite block range %s during resync",
        Replicas[replicaIndex].ReplicaId.c_str(),
        Replicas[replicaIndex].ReplicaIndex,
        DescribeRange(Range).c_str());

    ctx.Send(event.release());
}

void TResyncRangeBlockByBlockActor::Done(const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvNonreplPartitionPrivate::TEvRangeResynced>(
            std::move(Error),
            Range,
            TInstant(),    // checksumStartTs
            TDuration(),   // checksumDuration
            ReadStartTs,
            ReadDuration,
            WriteStartTs,
            WriteDuration,
            std::move(AffectedBlockInfos));

    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ResyncRangeBlockByBlock",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TResyncRangeBlockByBlockActor::HandleReadUndelivery(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "ReadBlocks request undelivered");

    Done(ctx);
}

void TResyncRangeBlockByBlockActor::HandleReadResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    auto* msg = ev->Get();
    auto replicaIdx = ev->Cookie;

    Error = msg->Record.GetError();

    if (HasError(Error)) {
        Done(ctx);
        return;
    }

    msg->Record.MutableBlocks()->Swap(&ReadBuffers[replicaIdx]);

    bool allReadsDone = AllOf(
        ReadBuffers,
        [](const NProto::TIOVector& data) { return data.BuffersSize() != 0; });
    if (!allReadsDone) {
        return;
    }

    PrepareWriteBuffers(ctx);
    WriteBlocks(ctx);
}

void TResyncRangeBlockByBlockActor::HandleWriteUndelivery(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "WriteBlocks request undelivered");

    Done(ctx);
}

void TResyncRangeBlockByBlockActor::HandleWriteResponse(
    const TEvService::TEvWriteBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    auto* msg = ev->Get();

    Error = msg->Record.GetError();

    if (HasError(Error)) {
        Done(ctx);
        return;
    }

    if (++ResyncedCount == ActorsToResync.size()) {
        Done(ctx);
    }
}

void TResyncRangeBlockByBlockActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "Dead");
    Done(ctx);
}

STFUNC(TResyncRangeBlockByBlockActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);


        HFunc(TEvService::TEvReadBlocksRequest, HandleReadUndelivery);
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadResponse);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteUndelivery);
        HFunc(TEvService::TEvWriteBlocksResponse, HandleWriteResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
