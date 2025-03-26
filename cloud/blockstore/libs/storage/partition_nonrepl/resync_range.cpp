#include "resync_range.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>

#include <cloud/storage/core/libs/common/sglist.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TLessComparatorForTStringPtr
{
    bool operator()(const TString* lhs, const TString* rhs) const
    {
        return *lhs < *rhs;
    }
};

struct THealStat
{
    // How many times a replica has been used as a source for fixing a minor
    // error.
    size_t HealCount = 0;

    // How many blocks in a replica with minor errors have been fixed.
    size_t MinorErrorCount = 0;

    // How many blocks in a replica with major errors have been fixed.
    size_t MajorErrorCount = 0;

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

}   // namespace

TResyncRangeActor::TResyncRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TVector<TReplicaDescriptor> replicas,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator)
    : RequestInfo(std::move(requestInfo))
    , BlockSize(blockSize)
    , Range(range)
    , Replicas(std::move(replicas))
    , WriterClientId(std::move(writerClientId))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
{}

void TResyncRangeActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ResyncRange",
        RequestInfo->CallContext->RequestId);

    ChecksumRangeActorCompanion.CalculateChecksums(ctx, Range);
}

void TResyncRangeActor::CompareChecksums(const TActorContext& ctx)
{
    const auto& checksums = ChecksumRangeActorCompanion.GetChecksums();
    THashMap<ui64, ui32> checksumCount;
    ui32 majorCount = 0;
    ui64 majorChecksum = 0;

    for (auto checksum : checksums) {
         if (++checksumCount[checksum] > majorCount) {
            majorCount = checksumCount[checksum];
            majorChecksum = checksum;
        }
    }

    if (majorCount == Replicas.size()) {
        // all checksums match
        Status = EStatus::Ok;
        Done(ctx);
        return;
    }

    Status = majorCount == 1 ? EStatus::MajorMismatch : EStatus::MinorMismatch;

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Resync range %s. %s mismatch, checksums: [%s]",
        Replicas[0].ReplicaId.c_str(),
        DescribeRange(Range).c_str(),
        (Status == EStatus::MajorMismatch ? "Major" : "Minor"),
        JoinSeq(", ", checksums).c_str());

    if (Status == EStatus::MinorMismatch) {
        ResyncMinor(ctx, checksums, majorChecksum);
    } else {
        ResyncMajor(ctx);
    }
}

void TResyncRangeActor::ResyncMinor(
    const NActors::TActorContext& ctx,
    const TVector<ui64>& checksums,
    ui64 majorChecksum)
{
    ReadBuffers.resize(1);
    ui64 majorIndx = 0;
    for (size_t i = 0; i < checksums.size(); i++) {
        if (checksums[i] != majorChecksum) {
            ActorsToResync.push_back(i);
        } else {
            majorIndx = i;
        }
    }
    ReadBlocks(ctx, majorIndx);
}

void TResyncRangeActor::ResyncMajor(const NActors::TActorContext& ctx)
{
    ReadBuffers.resize(Replicas.size());
    for (size_t i = 0; i < Replicas.size(); i++) {
        ActorsToResync.push_back(i);
        ReadBlocks(ctx, i);
    }
}

void TResyncRangeActor::PrepareWriteBuffer(const NActors::TActorContext& ctx)
{
    if (Status == EStatus::MinorMismatch) {
        return;
    }

    TVector<THealStat> heals(Replicas.size());

    auto healMinors = [&](const TBlockVariants& blockVariants)
    {
        TMap<TString*, size_t, TLessComparatorForTStringPtr> variations;

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
    };

    // Let's try to fix range block by block. At the same time, we will
    // calculate which replicas were used to fix the rest of the replicas.
    for (size_t i = 0; i < Range.Size(); ++i) {
        TBlockVariants blockVariants;
        blockVariants.reserve(Replicas.size());
        for (size_t replica = 0; replica < Replicas.size(); ++replica) {
            blockVariants.push_back(TBlockVariant{
                .HealStat = heals[replica],
                .Block = *ReadBuffers[replica].MutableBuffers()->Mutable(i)});
        }
        healMinors(blockVariants);
    }

    // We find the replica that fixed the largest number of blocks and use it as
    // a sample to fix the rest of the replicas.
    size_t bestHealer = 0;
    for (size_t i = 1; i < Replicas.size(); ++i) {
        if (heals[bestHealer].HealCount < heals[i].HealCount) {
            bestHealer = i;
        }
    }

    for (size_t i = 0; i < Range.Size(); ++i) {
        const auto& healerBlock = ReadBuffers[bestHealer].GetBuffers(i);

        for (size_t replica = 0; replica < Replicas.size(); ++replica) {
            if (replica == bestHealer) {
                continue;
            }

            auto& replicaBlock =
                *ReadBuffers[replica].MutableBuffers()->Mutable(i);

            if (healerBlock != replicaBlock) {
                // Replace block.
                replicaBlock = healerBlock;
                ++heals[replica].MajorErrorCount;
            }
        }
    }

    size_t minorFixCount = Accumulate(
        heals,
        size_t{},
        [](size_t count, const THealStat& h)
        { return count + h.MinorErrorCount; });
    size_t majorFixCount = Accumulate(
        heals,
        size_t{},
        [](size_t count, const THealStat& h)
        { return count + h.MajorErrorCount; });
    TString replicaStat = Accumulate(
        heals,
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
        if (heals[i].MinorErrorCount || heals[i].MajorErrorCount) {
            ActorsToResync.push_back(i);
        }
    }
}

void TResyncRangeActor::ReadBlocks(const TActorContext& ctx, size_t idx)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(BackgroundOpsClientId));

    auto event = std::make_unique<NActors::IEventHandle>(
        Replicas[idx].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        idx,          // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());

    ReadStartTs = ctx.Now();
}

void TResyncRangeActor::WriteBlocks(const TActorContext& ctx)
{
    for (size_t i = 0; i < ActorsToResync.size(); ++i) {
        const bool lastReplica = i == ActorsToResync.size() - 1;
        if (lastReplica) {
            WriteReplicaBlocks(
                ctx,
                ActorsToResync[i],
                std::move(ReadBuffers[0]));
        } else {
            WriteReplicaBlocks(ctx, ActorsToResync[i], ReadBuffers[0]);
        }
    }

    WriteStartTs = ctx.Now();
}

void TResyncRangeActor::WriteReplicaBlocks(
    const TActorContext& ctx,
    size_t idx,
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
        Replicas[idx].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        idx,          // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Replica %lu Overwrite block range %s during resync",
        Replicas[idx].ReplicaId.c_str(),
        Replicas[idx].ReplicaIndex,
        DescribeRange(Range).c_str());

    ctx.Send(event.release());
}

void TResyncRangeActor::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvNonreplPartitionPrivate::TEvRangeResynced>(
        std::move(Error),
        Range,
        ChecksumRangeActorCompanion.GetChecksumStartTs(),
        ChecksumRangeActorCompanion.GetChecksumDuration(),
        ReadStartTs,
        ReadDuration,
        WriteStartTs,
        WriteDuration,
        std::move(AffectedBlockInfos)
    );

    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ResyncRange",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TResyncRangeActor::HandleChecksumUndelivery(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ChecksumRangeActorCompanion.HandleChecksumUndelivery(ctx);
    Error = ChecksumRangeActorCompanion.GetError();

    Done(ctx);
}

void TResyncRangeActor::HandleChecksumResponse(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ChecksumRangeActorCompanion.HandleChecksumResponse(ev, ctx);

    Error = ChecksumRangeActorCompanion.GetError();
    if (HasError(Error)) {
        Done(ctx);
        return;
    }

    if (ChecksumRangeActorCompanion.IsFinished()) {
        CompareChecksums(ctx);
    }
}

void TResyncRangeActor::HandleReadUndelivery(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "ReadBlocks request undelivered");

    Done(ctx);
}

void TResyncRangeActor::HandleReadResponse(
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

    msg->Record.MutableBlocks()->Swap(
        &ReadBuffers[Status == EStatus::MinorMismatch ? 0 : replicaIdx]);

    bool allReadsDone = AllOf(
        ReadBuffers,
        [](const NProto::TIOVector& data) { return data.BuffersSize() != 0; });
    if (!allReadsDone) {
        return;
    }
    PrepareWriteBuffer(ctx);
    WriteBlocks(ctx);
}

void TResyncRangeActor::HandleWriteUndelivery(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "WriteBlocks request undelivered");

    Done(ctx);
}

void TResyncRangeActor::HandleWriteResponse(
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

void TResyncRangeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "Dead");
    Done(ctx);
}

STFUNC(TResyncRangeActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest,
            HandleChecksumUndelivery);
        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse,
            HandleChecksumResponse);
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadUndelivery);
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadResponse);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteUndelivery);
        HFunc(TEvService::TEvWriteBlocksResponse, HandleWriteResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
