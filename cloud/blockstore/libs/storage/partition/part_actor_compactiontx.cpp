#include "part_actor.h"
#include "part_compaction.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NCloud::NStorage;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompactionBlockVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
{
private:
    TTxPartition::TRangeCompaction& Args;
    const ui64 MaxCommitId;

public:
    TCompactionBlockVisitor(
            TTxPartition::TRangeCompaction& args,
            ui64 maxCommitId)
        : Args(args)
        , MaxCommitId(maxCommitId)
    {}

    bool Visit(const TFreshBlock& block) override
    {
        Args.MarkBlock(
            block.Meta.BlockIndex,
            block.Meta.CommitId,
            block.Content);
        return true;
    }

    bool KeepTrackOfAffectedBlocks = false;

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        if (commitId > MaxCommitId) {
            return true;
        }

        Args.MarkBlock(
            blockIndex,
            commitId,
            blobId,
            blobOffset,
            KeepTrackOfAffectedBlocks);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

void PrepareRangeCompaction(
    const TStorageConfig& config,
    const bool incrementalCompactionEnabled,
    const ui64 commitId,
    const bool fullCompaction,
    const TActorContext& ctx,
    const ui64 tabletId,
    bool& ready,
    TPartitionDatabase& db,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    const TString& logTitle)
{
    TCompactionBlockVisitor visitor(args, commitId);
    state.FindFreshBlocks(visitor, args.BlockRange, commitId);
    visitor.KeepTrackOfAffectedBlocks = true;
    ready &= state.FindMixedBlocksForCompaction(db, visitor, args.RangeIdx);
    visitor.KeepTrackOfAffectedBlocks = false;
    ready &= db.FindMergedBlocks(
        visitor,
        args.BlockRange,
        true,   // precharge
        state.GetMaxBlocksInBlob(),
        commitId);

    if (ready && incrementalCompactionEnabled && !fullCompaction) {
        THashMap<TPartialBlobId, ui32, TPartialBlobIdHash> liveBlocks;
        for (const auto& m: args.BlockMarks) {
            if (m.CommitId && m.BlobId) {
                ++liveBlocks[m.BlobId];
            }
        }

        TVector<TPartialBlobId> blobIds;
        blobIds.reserve(liveBlocks.size());
        for (const auto& x: liveBlocks) {
            blobIds.push_back(x.first);
        }

        Sort(
            blobIds,
            [&](const TPartialBlobId& l, const TPartialBlobId& r)
            { return liveBlocks[l] < liveBlocks[r]; });

        auto it = blobIds.begin();
        args.BlobsSkipped = blobIds.size();
        ui32 blocks = 0;

        while (it != blobIds.end()) {
            const auto bytes = blocks * state.GetBlockSize();
            const auto blobCountOk =
                args.BlobsSkipped <=
                config.GetMaxSkippedBlobsDuringCompaction();
            const auto byteCountOk =
                bytes >= config.GetTargetCompactionBytesPerOp();

            if (blobCountOk && byteCountOk) {
                break;
            }

            blocks += liveBlocks[*it];
            --args.BlobsSkipped;
            ++it;
        }

        // liveBlocks will contain only skipped blobs after this
        for (auto it2 = blobIds.begin(); it2 != it; ++it2) {
            liveBlocks.erase(*it2);
        }

        while (it != blobIds.end()) {
            args.BlocksSkipped += liveBlocks[*it];
            ++it;
        }

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Dropping last %u blobs, %u blocks, remaining blobs: %u, "
            "blocks: %u",
            logTitle.c_str(),
            args.BlobsSkipped,
            args.BlocksSkipped,
            liveBlocks.size(),
            blocks);

        THashSet<ui32> skippedBlockIndices;

        for (const auto& x: liveBlocks) {
            auto ab = args.AffectedBlobs.find(x.first);
            Y_ABORT_UNLESS(ab != args.AffectedBlobs.end());
            for (const auto blockIndex: ab->second.AffectedBlockIndices) {
                // we can actually add extra indices to skippedBlockIndices,
                // but it does not cause data corruption - the important thing
                // is to ensure that all skipped indices are added, not that
                // all non-skipped are preserved
                skippedBlockIndices.insert(blockIndex);
            }
            args.AffectedBlobs.erase(ab);
        }

        if (liveBlocks.size()) {
            TAffectedBlocks affectedBlocks;
            for (const auto& b: args.AffectedBlocks) {
                if (!skippedBlockIndices.contains(b.BlockIndex)) {
                    affectedBlocks.push_back(b);
                }
            }
            args.AffectedBlocks = std::move(affectedBlocks);

            for (auto& m: args.BlockMarks) {
                if (liveBlocks.contains(m.BlobId)) {
                    m = {};
                }
            }
        }
    }

    const ui32 checksumBoundary =
        config.GetDiskPrefixLengthWithBlockChecksumsInBlobs()
        / state.GetBlockSize();
    args.ChecksumsEnabled = args.BlockRange.Start < checksumBoundary;

    for (auto& kv: args.AffectedBlobs) {
        if (db.ReadBlockMask(kv.first, kv.second.BlockMask)) {
            Y_ABORT_UNLESS(kv.second.BlockMask.Defined(),
                "Could not read block mask for blob: %s",
                ToString(MakeBlobId(tabletId, kv.first)).data());
        } else {
            ready = false;
        }

        if (args.ChecksumsEnabled) {
            if (db.ReadBlobMeta(kv.first, kv.second.BlobMeta)) {
                Y_ABORT_UNLESS(kv.second.BlobMeta.Defined(),
                    "Could not read blob meta for blob: %s",
                    ToString(MakeBlobId(tabletId, kv.first)).data());
            } else {
                ready = false;
            }
        }
    }
}

void CompleteRangeCompaction(
    const bool blobPatchingEnabled,
    const ui32 mergedBlobThreshold,
    const ui64 commitId,
    TTabletStorageInfo& tabletStorageInfo,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    TVector<TCompactionBlobRequest>& requests,
    TVector<TRangeCompactionInfo>& rangeCompactionInfos,
    ui32 maxDiffPercentageForBlobPatching,
    ui32 rangeCompactionIndex)
{
    const EChannelPermissions compactionPermissions =
        EChannelPermission::SystemWritesAllowed;
    const auto initialRequestsSize = requests.size();

    // at first we count number of data blocks
    size_t dataBlocksCount = 0, zeroBlocksCount = 0;

    for (const auto& mark: args.BlockMarks) {
        if (mark.CommitId) {
            const bool isFresh = !mark.BlockContent.empty();
            const bool isMixedOrMerged = !IsDeletionMarker(mark.BlobId);
            // there could be fresh block OR merged/mixed block
            Y_ABORT_UNLESS(!(isFresh && isMixedOrMerged));
            if (isFresh || isMixedOrMerged) {
                ++dataBlocksCount;
            } else {
                ++zeroBlocksCount;
            }
        }
    }

    // determine the results kind
    TPartialBlobId dataBlobId, zeroBlobId;
    TBlockMask dataBlobSkipMask, zeroBlobSkipMask;

    auto channelDataKind = EChannelDataKind::Merged;
    if (dataBlocksCount) {
        ui32 skipped = 0;
        for (const auto& mark: args.BlockMarks) {
            const bool isFresh = !mark.BlockContent.empty();
            const bool isMixedOrMerged = !IsDeletionMarker(mark.BlobId);
            if (!isFresh && !isMixedOrMerged) {
                ++skipped;
            }
        }

        const auto blobSize = (args.BlockRange.Size() - skipped) * state.GetBlockSize();
        if (blobSize < mergedBlobThreshold) {
            channelDataKind = EChannelDataKind::Mixed;
        }
        dataBlobId = state.GenerateBlobId(
            channelDataKind,
            compactionPermissions,
            commitId,
            blobSize,
            rangeCompactionIndex);
    }

    if (zeroBlocksCount) {
        // for zeroed region we will write blob without any data
        // XXX same commitId used for 2 blobs: data blob and zero blob
        // we differentiate between them by storing the last block index in
        // MergedBlocksIndex::RangeEnd not for the last block of the processed
        // compaction range but for the last actual block that's referenced by
        // the corresponding blob
        zeroBlobId = state.GenerateBlobId(
            channelDataKind,
            compactionPermissions,
            commitId,
            0,
            rangeCompactionIndex);
    }

    // now build the blob content for all blocks to be written
    TBlockBuffer blobContent(TProfilingAllocator::Instance());
    TVector<ui32> blockChecksums;
    TVector<ui32> zeroBlocks;

    ui32 blockIndex = args.BlockRange.Start;
    TPartialBlobId patchingCandidate;
    ui32 patchingCandidateChangedBlockCount = 0;
    for (auto& mark: args.BlockMarks) {
        if (mark.CommitId) {
            if (mark.BlockContent) {
                Y_ABORT_UNLESS(IsDeletionMarker(mark.BlobId));
                requests.emplace_back(
                    mark.BlobId,
                    TActorId(),
                    mark.BlobOffset,
                    blockIndex,
                    blobContent.GetBlocksCount(),
                    0,
                    rangeCompactionIndex);

                // fresh block will be written
                blobContent.AddBlock({
                    mark.BlockContent.data(),
                    mark.BlockContent.size()
                });

                if (args.ChecksumsEnabled) {
                    blockChecksums.push_back(
                        ComputeDefaultDigest(blobContent.GetBlocks().back()));
                }

                if (zeroBlobId) {
                    zeroBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
                }
            } else if (!IsDeletionMarker(mark.BlobId)) {
                const auto proxy = tabletStorageInfo.BSProxyIDForChannel(
                    mark.BlobId.Channel(),
                    mark.BlobId.Generation());

                requests.emplace_back(
                    mark.BlobId,
                    proxy,
                    mark.BlobOffset,
                    blockIndex,
                    blobContent.GetBlocksCount(),
                    tabletStorageInfo.GroupFor(
                        mark.BlobId.Channel(),
                        mark.BlobId.Generation()),
                    rangeCompactionIndex);

                // we will read this block later
                blobContent.AddBlock(state.GetBlockSize(), char(0));

                // block checksum is simply moved from the affected blob's meta
                if (args.ChecksumsEnabled) {
                    ui32 blockChecksum = 0;

                    auto* affectedBlob = args.AffectedBlobs.FindPtr(mark.BlobId);
                    Y_DEBUG_ABORT_UNLESS(affectedBlob);
                    if (affectedBlob) {
                        if (auto* meta = affectedBlob->BlobMeta.Get()) {
                            if (mark.BlobOffset < meta->BlockChecksumsSize()) {
                                blockChecksum =
                                    meta->GetBlockChecksums(mark.BlobOffset);
                            }
                        }
                    }

                    blockChecksums.push_back(blockChecksum);
                }

                if (zeroBlobId) {
                    zeroBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
                }

                if (blobPatchingEnabled) {
                    if (!patchingCandidate &&
                        mark.BlobId.BlobSize() == dataBlobId.BlobSize())
                    {
                        patchingCandidate = mark.BlobId;
                        ++patchingCandidateChangedBlockCount;
                    } else if (patchingCandidate == mark.BlobId) {
                        ++patchingCandidateChangedBlockCount;
                    }
                }
            } else {
                dataBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
                zeroBlocks.push_back(blockIndex);
            }
        } else {
            if (dataBlobId) {
                dataBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
            }
            if (zeroBlobId) {
                zeroBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
            }
        }

        ++blockIndex;
    }

    if (patchingCandidate) {
        TPartialBlobId targetBlobId(
            dataBlobId.Generation(),
            dataBlobId.Step(),
            patchingCandidate.Channel(),
            dataBlobId.BlobSize(),
            dataBlobId.Cookie(),
            0);

        TLogoBlobID realTargetBlobId = MakeBlobId(
            tabletStorageInfo.TabletID,
            targetBlobId);

        ui32 originalChannel = patchingCandidate.Channel();
        ui32 originalGroup = tabletStorageInfo.GroupFor(
            originalChannel,
            patchingCandidate.Generation());
        Y_ABORT_UNLESS(originalGroup != Max<ui32>());

        ui32 patchedChannel = realTargetBlobId.Channel();
        ui32 patchedGroup = tabletStorageInfo.GroupFor(
            patchedChannel,
            realTargetBlobId.Generation());
        Y_ABORT_UNLESS(patchedGroup != Max<ui32>());

        bool found = TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(
            MakeBlobId(tabletStorageInfo.TabletID, patchingCandidate),
            &realTargetBlobId,
            0xfe0000,
            originalGroup,
            patchedGroup);

        ui32 blockCount = patchingCandidate.BlobSize() / state.GetBlockSize();
        ui32 patchingBlockCount =
            dataBlocksCount - patchingCandidateChangedBlockCount;
        ui32 changedPercentage = 100 * patchingBlockCount / blockCount;

        if (found &&
            (!maxDiffPercentageForBlobPatching ||
            changedPercentage <= maxDiffPercentageForBlobPatching))
        {
            dataBlobId = TPartialBlobId(
                dataBlobId.Generation(),
                dataBlobId.Step(),
                patchingCandidate.Channel(),
                dataBlobId.BlobSize(),
                realTargetBlobId.Cookie(),
                0);
        } else {
            patchingCandidate = {};
        }
    }

    rangeCompactionInfos.emplace_back(
        args.BlockRange,
        patchingCandidate,
        dataBlobId,
        dataBlobSkipMask,
        zeroBlobId,
        zeroBlobSkipMask,
        args.BlobsSkipped,
        args.BlocksSkipped,
        std::move(blockChecksums),
        channelDataKind,
        std::move(blobContent),
        std::move(zeroBlocks),
        std::move(args.AffectedBlobs),
        std::move(args.AffectedBlocks));

    if (!dataBlobId && !zeroBlobId) {
        const auto rangeDescr = DescribeRange(args.BlockRange);
        Y_ABORT("No blocks in compacted range: %s", rangeDescr.c_str());
    }
    Y_ABORT_UNLESS(requests.size() - initialRequestsSize == dataBlocksCount);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleCompactionTx(
    const TEvPartitionPrivate::TEvCompactionTxRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "CompactionTx",
        requestInfo->CallContext->RequestId);

    AddTransaction<TEvPartitionPrivate::TCompactionMethod>(*requestInfo);

    auto tx = CreateTx<TCompaction>(
        requestInfo,
        msg->CommitId,
        msg->RangeCompactionIndex,
        msg->CompactionOptions,
        std::move(msg->Ranges));

    ui64 minCommitId = State->GetCommitQueue().GetMinCommitId();
    Y_ABORT_UNLESS(minCommitId <= msg->CommitId);

    if (minCommitId == msg->CommitId) {
        // start execution
        ExecuteTx(ctx, std::move(tx));
    } else {
        // delay execution until all previous commits completed
        State->GetCommitQueue().Enqueue(std::move(tx), msg->CommitId);
    }
}

bool TPartitionActor::PrepareCompaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCompaction& args)
{
    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    const bool incrementalCompactionEnabled =
        Config->GetIncrementalCompactionEnabled() ||
        Config->IsIncrementalCompactionFeatureEnabled(
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId());
    const bool fullCompaction =
        args.CompactionOptions.test(ToBit(ECompactionOption::Full));

    bool ready = true;

    for (auto& rangeCompaction: args.RangeCompactions) {
        PrepareRangeCompaction(
            *Config,
            incrementalCompactionEnabled,
            args.CommitId,
            fullCompaction,
            ctx,
            TabletID(),
            ready,
            db,
            *State,
            rangeCompaction,
            LogTitle.GetWithTime());
    }

    return ready;
}

void TPartitionActor::ExecuteCompaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCompaction& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteCompaction(
    const TActorContext& ctx,
    TTxPartition::TCompaction& args)
{
    RemoveTransaction(*args.RequestInfo);

    for (auto& rangeCompaction: args.RangeCompactions) {
        State->RaiseRangeTemperature(rangeCompaction.RangeIdx);
    }

    // TODO:_ rename ...ForCloud -> ...ForDisk ???
    const bool blobPatchingEnabledForCloud =
        Config->IsBlobPatchingFeatureEnabled(
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId());
    const bool blobPatchingEnabled =
        Config->GetBlobPatchingEnabled() || blobPatchingEnabledForCloud;

    const auto mergedBlobThreshold =
        PartitionConfig.GetStorageMediaKind() ==
                NCloud::NProto::STORAGE_MEDIA_SSD
            ? 0
            : Config->GetCompactionMergedBlobThresholdHDD();

    TVector<TRangeCompactionInfo> rangeCompactionInfos;
    TVector<TCompactionBlobRequest> requests;

    for (auto& rangeCompaction: args.RangeCompactions) {
        CompleteRangeCompaction(
            blobPatchingEnabled,
            mergedBlobThreshold,
            args.CommitId,
            *Info(),
            *State,
            rangeCompaction,
            requests,
            rangeCompactionInfos,
            Config->GetMaxDiffPercentageForBlobPatching(),
            args.RangeCompactionIndex + static_cast<ui32>(rangeCompactionInfos.size()));

        if (rangeCompactionInfos.back().OriginalBlobId) {
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::PARTITION,
                "%s Selected patching candidate: %s, data blob: %s",
                LogTitle.GetWithTime().c_str(),
                ToString(rangeCompactionInfos.back().OriginalBlobId).c_str(),
                ToString(rangeCompactionInfos.back().DataBlobId).c_str());
        }
    }

    // TODO:_ or to it in the beginning of the Complete... function?
    TRequestScope timer(*args.RequestInfo);

    // auto response =
    //     std::make_unique<TEvPartitionPrivate::TEvCompactionTxResponse>();
    // auto response =
    //     std::make_unique<TEvPartitionPrivate::TEvCompactionTxResponse>(
    //         MakeError(S_OK));
    // response->Record.SetRangeCompactionInfos(std::move(rangeCompactionInfos));
    // response->Record.SetCompactionRequests(std::move(requests));
    auto response =
        std::make_unique<TEvPartitionPrivate::TEvCompactionTxResponse>(
            std::move(rangeCompactionInfos),
            std::move(requests));

    // TODO:_ ExecCycles?
    // response->ExecCycles = args.RequestInfo->GetExecCycles();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "CompactionTx",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    // TODO:_ where we resigister TCompactionActor?
    // auto actor = NCloud::Register<TCompactionActor>(
    //     ctx,
    //     args.RequestInfo,
    //     TabletID(),
    //     PartitionConfig.GetDiskId(),
    //     SelfId(),
    //     State->GetBlockSize(),
    //     State->GetMaxBlocksInBlob(),
    //     Config->GetMaxAffectedBlocksPerCompaction(),
    //     Config->GetComputeDigestForEveryBlockOnCompaction(),
    //     BlockDigestGenerator,
    //     GetBlobStorageAsyncRequestTimeout(),
    //     compactionType,
    //     args.CommitId,
    //     std::move(PendingRangeCompactionInfos),
    //     std::move(PendingCompactionRequests),
    //     LogTitle.GetChild(GetCycleCount()));
    // LOG_DEBUG(
    //     ctx,
    //     TBlockStoreComponents::PARTITION,
    //     "%s Partition registered TCompactionActor with id [%lu]; commit id %lu",
    //     LogTitle.GetWithTime().c_str(),
    //     actor.ToString().c_str(),
    //     args.CommitId);

    // Actors.Insert(actor);

    PendingRangeCompactionInfos.clear();
    PendingCompactionRequests.clear();
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
