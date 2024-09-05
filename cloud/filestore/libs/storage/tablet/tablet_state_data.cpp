#include "tablet_state_impl.h"

#include "profile_log_events.h"

#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IntersectsWithFresh(
    const TFreshBytes& freshBytes,
    const TFreshBlocks& freshBlocks,
    const ui32 blockSize,
    ui64 nodeId,
    ui32 blockIndex)
{
    const bool isFreshBlock =
        freshBlocks.FindBlock(nodeId, blockIndex);
    const bool intersectsWithFreshBytes =
        freshBytes.Intersects(
            nodeId,
            TByteRange::BlockRange(blockIndex, blockSize)
        );
    return isFreshBlock || intersectsWithFreshBytes;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////
// Writes

bool TIndexTabletState::EnqueueWriteBatch(std::unique_ptr<TWriteRequest> request)
{
    bool shouldTriggerWrite = Impl->WriteBatch.Empty();
    Impl->WriteBatch.PushBack(request.release());
    return shouldTriggerWrite;
}

TWriteRequestList TIndexTabletState::DequeueWriteBatch()
{
    // TODO: deduplicate writes (NBS-2161)
    return std::move(Impl->WriteBatch);
}

bool TIndexTabletState::GenerateBlobId(
    ui64 commitId,
    ui32 blobSize,
    ui32 blobIndex,
    TPartialBlobId* blobId) const
{
    auto [gen, step] = ParseCommitId(commitId);

    const auto channel = Impl->Channels.SelectChannel(
        EChannelDataKind::Mixed,
        ChannelMinFreeSpace,
        ChannelFreeSpaceThreshold);
    if (!channel) {
        return false;
    }

    *blobId = TPartialBlobId(
        gen,
        step,
        *channel,
        blobSize,
        blobIndex,
        0);

    return true;
}

void TIndexTabletState::Truncate(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    ui64 currentSize,
    ui64 targetSize)
{
    if (currentSize <= targetSize) {
        return;
    }

    TByteRange range(targetSize, currentSize - targetSize, GetBlockSize());

    if (TruncateBlocksThreshold && range.BlockCount() > TruncateBlocksThreshold) {
        EnqueueTruncateOp(nodeId, range);
        return;
    }

    TruncateRange(db, nodeId, commitId, range);
}

void TIndexTabletState::TruncateRange(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    TByteRange range)
{
    const TByteRange tailAlignedRange(
        range.Offset,
        AlignUp<ui64>(range.End(), range.BlockSize) - range.Offset,
        range.BlockSize);

    DeleteRange(db, nodeId, commitId, tailAlignedRange);

    const TByteRange headBound(
         range.Offset,
         range.FirstAlignedBlock() * range.BlockSize - range.Offset,
         range.BlockSize);
    if (headBound.Length) {
        WriteFreshBytes(
            db,
            nodeId,
            commitId,
            headBound.Offset,
            // FIXME: do not allocate each time
            TString(headBound.Length, 0));
    }

    InvalidateReadAheadCache(nodeId);
}

void TIndexTabletState::ZeroRange(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    TByteRange range)
{
    DeleteRange(db, nodeId, commitId, range);

    const TByteRange headBound(
         range.Offset,
         range.UnalignedHeadLength(),
         range.BlockSize);
    if (headBound.Length) {
        WriteFreshBytes(
            db,
            nodeId,
            commitId,
            headBound.Offset,
            // FIXME: do not allocate each time
            TString(headBound.Length, 0));
    }

    const TByteRange tailBound(
         range.UnalignedTailOffset(),
         range.UnalignedTailLength(),
         range.BlockSize);
    if (tailBound.Length) {
        WriteFreshBytes(
            db,
            nodeId,
            commitId,
            tailBound.Offset,
            // FIXME: do not allocate each time
            TString(tailBound.Length, 0));
    }
}

void TIndexTabletState::DeleteRange(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    const TByteRange& range)
{
    const ui64 deletedBlockCount = range.AlignedBlockCount();
    if (deletedBlockCount) {
        MarkFreshBlocksDeleted(
            db,
            nodeId,
            commitId,
            range.FirstAlignedBlock(),
            deletedBlockCount);

        const bool useLargeDeletionMarkers = LargeDeletionMarkersEnabled
            && deletedBlockCount >= LargeDeletionMarkersThreshold;
        if (useLargeDeletionMarkers) {
            SplitRange(
                range.FirstAlignedBlock(),
                deletedBlockCount,
                LargeDeletionMarkerBlocks,
                [&] (ui32 blockOffset, ui32 blocksCount) {
                    Impl->LargeBlocks.AddDeletionMarker({
                        nodeId,
                        commitId,
                        static_cast<ui32>(
                            range.FirstAlignedBlock() + blockOffset),
                        blocksCount});
                    db.WriteLargeDeletionMarkers(
                        nodeId,
                        commitId,
                        range.FirstAlignedBlock() + blockOffset,
                        blocksCount);
                });

            IncrementLargeDeletionMarkersCount(db, deletedBlockCount);
        } else {
            SplitRange(
                range.FirstAlignedBlock(),
                deletedBlockCount,
                BlockGroupSize,
                [&] (ui32 blockOffset, ui32 blocksCount) {
                    MarkMixedBlocksDeleted(
                        db,
                        nodeId,
                        commitId,
                        range.FirstAlignedBlock() + blockOffset,
                        blocksCount);
                });
        }
    }

    WriteFreshBytesDeletionMarker(
        db,
        nodeId,
        commitId,
        range.Offset,
        range.Length);
}

void TIndexTabletState::EnqueueTruncateOp(ui64 nodeId, TByteRange range)
{
    Impl->TruncateQueue.EnqueueOperation(nodeId, range);
}

TTruncateQueue::TEntry TIndexTabletState::DequeueTruncateOp()
{
    TABLET_VERIFY(Impl->TruncateQueue.HasPendingOperations());
    return Impl->TruncateQueue.DequeueOperation();
}

bool TIndexTabletState::HasPendingTruncateOps() const
{
    return Impl->TruncateQueue.HasPendingOperations();
}

void TIndexTabletState::CompleteTruncateOp(ui64 nodeId)
{
    Impl->TruncateQueue.CompleteOperation(nodeId);
}

void TIndexTabletState::AddTruncate(TIndexTabletDatabase& db, ui64 nodeId, TByteRange range)
{
    EnqueueTruncateOp(nodeId, range);
    db.WriteTruncateQueueEntry(nodeId, range);
}

void TIndexTabletState::DeleteTruncate(TIndexTabletDatabase& db, ui64 nodeId)
{
    db.DeleteTruncateQueueEntry(nodeId);
}

bool TIndexTabletState::HasActiveTruncateOp(ui64 nodeId) const
{
    return Impl->TruncateQueue.HasActiveOperation(nodeId);
}

bool TIndexTabletState::IsWriteAllowed(
    const TIndexTabletState::TBackpressureThresholds& thresholds,
    TString* message) const
{
    const auto freshBlocksDataSize = GetFreshBlocksCount() * GetBlockSize();

    if (freshBlocksDataSize >= thresholds.Flush) {
        *message = TStringBuilder() << "freshBlocksDataSize: "
            << freshBlocksDataSize;
        return false;
    }

    const auto freshBytesCount = GetFreshBytesCount();
    if (freshBytesCount >= thresholds.FlushBytes) {
        *message = TStringBuilder() << "freshBytesCount: " << freshBytesCount;
        return false;
    }

    const auto compactionScore = GetRangeToCompact().Score;
    if (compactionScore >= thresholds.CompactionScore) {
        *message = TStringBuilder() << "compactionScore: " << compactionScore;
        return false;
    }

    const auto cleanupScore = GetRangeToCleanup().Score;
    if (cleanupScore >= thresholds.CleanupScore) {
        *message = TStringBuilder() << "cleanupScore: " << cleanupScore;
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// FreshBytes

void TIndexTabletState::LoadFreshBytes(
    const TVector<TIndexTabletDatabase::TFreshBytesEntry>& bytes)
{
    auto checkpoints = Impl->Checkpoints.GetCheckpoints();
    auto cit = checkpoints.begin();
    for (const auto& b: bytes) {
        if (cit != checkpoints.end() && (*cit)->GetCommitId() < b.MinCommitId) {
            Impl->FreshBytes.OnCheckpoint((*cit)->GetCommitId());
        }

        if (b.Data) {
            Impl->FreshBytes.AddBytes(
                b.NodeId,
                b.Offset,
                b.Data,
                b.MinCommitId);
        } else {
            Impl->FreshBytes.AddDeletionMarker(
                b.NodeId,
                b.Offset,
                b.Len,
                b.MinCommitId);
        }
    }
}

void TIndexTabletState::FindFreshBytes(
    IFreshBytesVisitor& visitor,
    ui64 nodeId,
    ui64 commitId,
    TByteRange byteRange) const
{
    Impl->FreshBytes.FindBytes(
        visitor,
        nodeId,
        byteRange,
        commitId);
}

NProto::TError TIndexTabletState::CheckFreshBytes(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset,
    TStringBuf data) const
{
    return Impl->FreshBytes.CheckBytes(
        nodeId,
        offset,
        data,
        commitId);
}

void TIndexTabletState::WriteFreshBytes(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    ui64 offset,
    TStringBuf data)
{
    Impl->FreshBytes.AddBytes(
        nodeId,
        offset,
        data,
        commitId);

    db.WriteFreshBytes(
        nodeId,
        commitId,
        offset,
        data);

    IncrementFreshBytesCount(db, data.Size());

    InvalidateReadAheadCache(nodeId);
}

void TIndexTabletState::WriteFreshBytesDeletionMarker(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    ui64 offset,
    ui64 len)
{
    Impl->FreshBytes.AddDeletionMarker(
        nodeId,
        offset,
        len,
        commitId);

    db.WriteFreshBytesDeletionMarker(
        nodeId,
        commitId,
        offset,
        len);

    IncrementDeletedFreshBytesCount(db, len);

    InvalidateReadAheadCache(nodeId);
}

TFlushBytesCleanupInfo TIndexTabletState::StartFlushBytes(
    TVector<TBytes>* bytes,
    TVector<TBytes>* deletionMarkers)
{
    return Impl->FreshBytes.StartCleanup(
        GetCurrentCommitId(),
        bytes,
        deletionMarkers);
}

TFlushBytesStats TIndexTabletState::FinishFlushBytes(
    TIndexTabletDatabase& db,
    ui64 itemLimit,
    ui64 chunkId,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    ui64 sz = 0;
    ui64 deletedSz = 0;
    ui64 cnt = 0;
    ui64 deletedCnt = 0;
    Impl->FreshBytes.VisitTop(
        itemLimit,
        [&] (const TBytes& bytes, bool isDeletionMarker) {
            db.DeleteFreshBytes(bytes.NodeId, bytes.MinCommitId, bytes.Offset);
            if (isDeletionMarker) {
                deletedSz += bytes.Length;
                ++deletedCnt;
            } else {
                sz += bytes.Length;
                ++cnt;
            }

            auto* range = profileLogRequest.AddRanges();
            range->SetNodeId(bytes.NodeId);
            range->SetOffset(bytes.Offset);
            range->SetBytes(bytes.Length);
    });

    auto completed = Impl->FreshBytes.FinishCleanup(
        chunkId,
        cnt,
        deletedCnt);

    auto [freshBytes, deletedFreshBytes] = Impl->FreshBytes.GetTotalBytes();
    SetFreshBytesCount(db, freshBytes);
    SetDeletedFreshBytesCount(db, deletedFreshBytes);

    return {sz + deletedSz, completed};
}

////////////////////////////////////////////////////////////////////////////////
// FreshBlocks

void TIndexTabletState::LoadFreshBlocks(
    const TVector<TIndexTabletDatabase::TFreshBlock>& blocks)
{
    for (const auto& block: blocks) {
        bool added = Impl->FreshBlocks.AddBlock(
            block.NodeId,
            block.BlockIndex,
            block.BlockData,
            GetBlockSize(),
            block.MinCommitId,
            block.MaxCommitId);
        TABLET_VERIFY(added);
    }
}

void TIndexTabletState::FindFreshBlocks(IFreshBlockVisitor& visitor) const
{
    Impl->FreshBlocks.FindBlocks(visitor);
}

void TIndexTabletState::FindFreshBlocks(
    IFreshBlockVisitor& visitor,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount) const
{
    Impl->FreshBlocks.FindBlocks(visitor, nodeId, blockIndex, blocksCount, commitId);
}

TMaybe<TFreshBlock> TIndexTabletState::FindFreshBlock(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex) const
{
    return Impl->FreshBlocks.FindBlock(nodeId, blockIndex, commitId);
}

void TIndexTabletState::WriteFreshBlock(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    TStringBuf blockData)
{
    bool added = Impl->FreshBlocks.AddBlock(
        nodeId,
        blockIndex,
        blockData,
        GetBlockSize(),
        commitId);
    TABLET_VERIFY(added);

    db.WriteFreshBlock(nodeId, commitId, blockIndex, blockData);

    IncrementFreshBlocksCount(db);

    InvalidateReadAheadCache(nodeId);
}

void TIndexTabletState::MarkFreshBlocksDeleted(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount)
{
    auto blocks = Impl->FreshBlocks.MarkBlocksDeleted(
        nodeId,
        blockIndex,
        blocksCount,
        commitId);

    for (const auto& [blockIndex, minCommitId]: blocks) {
        db.MarkFreshBlockDeleted(
            nodeId,
            minCommitId,
            commitId,
            blockIndex);
    }

    InvalidateReadAheadCache(nodeId);
}

void TIndexTabletState::DeleteFreshBlocks(
    TIndexTabletDatabase& db,
    const TVector<TBlock>& blocks)
{
    for (const auto& block: blocks) {
        Impl->FreshBlocks.RemoveBlock(
            block.NodeId,
            block.BlockIndex,
            block.MinCommitId);

        db.DeleteFreshBlock(
            block.NodeId,
            block.MinCommitId,
            block.BlockIndex);
    }

    DecrementFreshBlocksCount(db, blocks.size());
}

////////////////////////////////////////////////////////////////////////////////
// MixedBlocks

bool TIndexTabletState::LoadMixedBlocks(
    TIndexTabletDatabase& db,
    ui32 rangeId)
{
    if (Impl->MixedBlocks.IsLoaded(rangeId)) {
        Impl->MixedBlocks.RefRange(rangeId);
        return true;
    }

    TVector<TIndexTabletDatabase::TMixedBlob> blobs;
    TVector<TDeletionMarker> deletionMarkers;

    if (!db.ReadMixedBlocks(rangeId, blobs, AllocatorRegistry.GetAllocator(EAllocatorTag::BlockList)) ||
        !db.ReadDeletionMarkers(rangeId, deletionMarkers))
    {
        // not ready
        return false;
    }

    Impl->MixedBlocks.RefRange(rangeId);
    for (auto& blob: blobs) {
        bool added = Impl->MixedBlocks.AddBlocks(
            rangeId,
            blob.BlobId,
            std::move(blob.BlockList),
            TMixedBlobStats {
                blob.GarbageBlocks,
                blob.CheckpointBlocks
            });
        TABLET_VERIFY(added);
    }

    for (const auto& deletion: deletionMarkers) {
        Impl->MixedBlocks.AddDeletionMarker(rangeId, deletion);
    }

    return true;
}

void TIndexTabletState::ReleaseMixedBlocks(ui32 rangeId)
{
    Impl->MixedBlocks.UnRefRange(rangeId);
}

void TIndexTabletState::ReleaseMixedBlocks(
    const TSet<ui32>& ranges)
{
    for (ui32 rangeId: ranges) {
        ReleaseMixedBlocks(rangeId);
    }
}

void TIndexTabletState::FindMixedBlocks(
    IMixedBlockVisitor& visitor,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount) const
{
    ui32 rangeId = GetMixedRangeIndex(nodeId, blockIndex, blocksCount);

    Impl->MixedBlocks.FindBlocks(
        visitor,
        rangeId,
        nodeId,
        commitId,
        blockIndex,
        blocksCount);
}

void TIndexTabletState::WriteMixedBlocks(
    TIndexTabletDatabase& db,
    const TPartialBlobId& blobId,
    const TBlock& block,
    ui32 blocksCount)
{
    ui32 rangeId = GetMixedRangeIndex(block.NodeId, block.BlockIndex, blocksCount);

    auto blockList = TBlockList::EncodeBlocks(
        block,
        blocksCount,
        GetAllocator(EAllocatorTag::BlockList));
    db.WriteMixedBlocks(rangeId, blobId, blockList, 0, 0);

    IncrementMixedBlobsCount(db);
    IncrementMixedBlocksCount(db, blocksCount);

    if (Impl->MixedBlocks.IsLoaded(rangeId)) {
        bool added = Impl->MixedBlocks.AddBlocks(
            rangeId,
            blobId,
            std::move(blockList));
        TABLET_VERIFY(added);
    }

    AddNewBlob(db, blobId);

    InvalidateReadAheadCache(block.NodeId);
}

bool TIndexTabletState::WriteMixedBlocks(
    TIndexTabletDatabase& db,
    const TPartialBlobId& blobId,
    /*const*/ TVector<TBlock>& blocks)
{
    ui32 rangeId = GetMixedRangeIndex(blocks);

    if (WriteMixedBlocks(db, rangeId, blobId, blocks)) {
        AddNewBlob(db, blobId);

        return true;
    }

    return false;
}

bool TIndexTabletState::WriteMixedBlocks(
    TIndexTabletDatabase& db,
    ui32 rangeId,
    const TPartialBlobId& blobId,
    /*const*/ TVector<TBlock>& blocks)
{
    const bool isMixedRangeLoaded = Impl->MixedBlocks.IsLoaded(rangeId);
    if (isMixedRangeLoaded) {
        Impl->MixedBlocks.ApplyDeletionMarkers(GetRangeIdHasher(), blocks);
    }
    Impl->LargeBlocks.ApplyDeletionMarkers(blocks);

    auto rebaseResult = RebaseMixedBlocks(blocks);

    if (!rebaseResult.LiveBlocks) {
        AddGarbageBlob(db, blobId);

        return false;
    }

    if (rebaseResult.GarbageBlocks) {
        IncrementGarbageBlocksCount(db, rebaseResult.GarbageBlocks);
    }
    if (rebaseResult.CheckpointBlocks) {
        IncrementCheckpointBlocksCount(db, rebaseResult.CheckpointBlocks);
    }

    for (ui64 checkpointId: rebaseResult.UsedCheckpoints) {
        AddCheckpointBlob(db, checkpointId, rangeId, blobId);
    }

    auto blockList = TBlockList::EncodeBlocks(
        blocks,
        GetAllocator(EAllocatorTag::BlockList));

    db.WriteMixedBlocks(
        rangeId,
        blobId,
        blockList,
        rebaseResult.GarbageBlocks,
        rebaseResult.CheckpointBlocks);

    IncrementMixedBlobsCount(db);
    IncrementMixedBlocksCount(db, blocks.size());

    if (isMixedRangeLoaded) {
        bool added = Impl->MixedBlocks.AddBlocks(
            rangeId,
            blobId,
            std::move(blockList),
            TMixedBlobStats {
                rebaseResult.GarbageBlocks,
                rebaseResult.CheckpointBlocks
            });
        TABLET_VERIFY(added);
    }

    InvalidateReadAheadCache(blocks[0].NodeId);

    return true;
}

void TIndexTabletState::DeleteMixedBlocks(
    TIndexTabletDatabase& db,
    const TPartialBlobId& blobId,
    const TVector<TBlock>& blocks)
{
    ui32 rangeId = GetMixedRangeIndex(blocks);

    DeleteMixedBlocks(db, rangeId, blobId, blocks);

    AddGarbageBlob(db, blobId);
}

void TIndexTabletState::DeleteMixedBlocks(
    TIndexTabletDatabase& db,
    ui32 rangeId,
    const TPartialBlobId& blobId,
    const TVector<TBlock>& blocks)
{
    TMixedBlobStats stats;

    bool removed = Impl->MixedBlocks.RemoveBlocks(rangeId, blobId, &stats);
    TABLET_VERIFY(removed);

    if (stats.GarbageBlocks) {
        DecrementGarbageBlocksCount(db, stats.GarbageBlocks);
    }
    if (stats.CheckpointBlocks) {
        DecrementCheckpointBlocksCount(db, stats.CheckpointBlocks);
    }

    db.DeleteMixedBlocks(rangeId, blobId);

    DecrementMixedBlobsCount(db);
    DecrementMixedBlocksCount(db, blocks.size());
}

TRebaseResult TIndexTabletState::RebaseMixedBlocks(TVector<TBlock>& blocks) const
{
    return RebaseBlocks(
        blocks,
        GetCurrentCommitId(),
        [this](ui64 nodeId, ui64 commitId)
        { return Impl->Checkpoints.FindCheckpoint(nodeId, commitId); },
        [this](ui64 nodeId, ui32 blockIndex)
        {
            return IntersectsWithFresh(
                Impl->FreshBytes,
                Impl->FreshBlocks,
                GetBlockSize(),
                nodeId,
                blockIndex
            );
        });
}

TVector<TMixedBlobMeta> TIndexTabletState::GetBlobsForCompaction(ui32 rangeId) const
{
    auto blobs = Impl->MixedBlocks.GetBlobsForCompaction(rangeId);
    for (auto& blob: blobs) {
        RebaseMixedBlocks(blob.Blocks);
        Impl->LargeBlocks.ApplyDeletionMarkers(blob.Blocks);
    }
    return blobs;
}

TMixedBlobMeta TIndexTabletState::FindBlob(ui32 rangeId, TPartialBlobId blobId) const
{
    return Impl->MixedBlocks.FindBlob(rangeId, blobId);
}

void TIndexTabletState::MarkMixedBlocksDeleted(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount)
{
    ui32 rangeId = GetMixedRangeIndex(nodeId, blockIndex, blocksCount);

    db.WriteDeletionMarkers(
        rangeId,
        nodeId,
        commitId,
        blockIndex,
        blocksCount);

    // XXX consider incrementing deletion marker count by 1, not by blocksCount
    IncrementDeletionMarkersCount(db, blocksCount);

    if (Impl->MixedBlocks.IsLoaded(rangeId)) {
        Impl->MixedBlocks.AddDeletionMarker(
            rangeId, {nodeId, commitId, blockIndex, blocksCount}
        );
    }

    const auto stats = GetCompactionStats(rangeId);
    db.WriteCompactionMap(
        rangeId,
        stats.BlobsCount,
        stats.DeletionsCount + blocksCount
    );
    UpdateCompactionMap(
        rangeId,
        stats.BlobsCount,
        stats.DeletionsCount + blocksCount
    );

    InvalidateReadAheadCache(nodeId);
}

void TIndexTabletState::UpdateBlockLists(
    TIndexTabletDatabase& db,
    TMixedBlobMeta& blob)
{
    const auto rangeId = GetMixedRangeIndex(blob.Blocks);
    DeleteMixedBlocks(db, rangeId, blob.BlobId, blob.Blocks);
    WriteMixedBlocks(db, rangeId, blob.BlobId, blob.Blocks);
}

ui32 TIndexTabletState::CleanupBlockDeletions(
    TIndexTabletDatabase& db,
    ui32 rangeId,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    auto affectedBlobs = Impl->MixedBlocks.ApplyDeletionMarkers(rangeId);

    ui64 removedBlobs = 0;
    for (auto& blob: affectedBlobs) {
        Impl->LargeBlocks.ApplyDeletionMarkers(blob.Blocks);
        DeleteMixedBlocks(db, rangeId, blob.BlobId, blob.Blocks);

        bool written = WriteMixedBlocks(db, rangeId, blob.BlobId, blob.Blocks);
        if (!written) {
            ++removedBlobs;
        }
    }

    if (PriorityRangesForCleanup) {
        const auto& pr = PriorityRangesForCleanup.front();
        if (pr.RangeId == rangeId) {
            // TODO(#1923): think about checkpoints once more
            Impl->LargeBlocks.MarkProcessed(
                pr.NodeId,
                GetCurrentCommitId(),
                pr.BlockIndex,
                pr.BlockCount);

            PriorityRangesForCleanup.pop_front();
        }
    }

    AddBlobsInfo(GetBlockSize(), affectedBlobs, profileLogRequest);

    auto deletionMarkers = Impl->MixedBlocks.ExtractDeletionMarkers(rangeId);

    ui32 deletionMarkerCount = 0;
    for (const auto& deletionMarker: deletionMarkers) {
        db.DeleteDeletionMarker(
            rangeId,
            deletionMarker.NodeId,
            deletionMarker.CommitId,
            deletionMarker.BlockIndex);

        deletionMarkerCount += deletionMarker.BlockCount;
    }

    DecrementDeletionMarkersCount(db, deletionMarkerCount);

    auto largeDeletionMarkers =
        Impl->LargeBlocks.ExtractProcessedDeletionMarkers();
    ui32 largeDeletionMarkerCount = 0;
    for (const auto& deletionMarker: largeDeletionMarkers) {
        db.DeleteLargeDeletionMarker(
            deletionMarker.NodeId,
            deletionMarker.CommitId,
            deletionMarker.BlockIndex);

        largeDeletionMarkerCount += deletionMarker.BlockCount;
    }

    DecrementLargeDeletionMarkersCount(db, largeDeletionMarkerCount);

    auto stats = GetCompactionStats(rangeId);
    // FIXME: return SafeDecrement after NBS-4475
    stats.BlobsCount = (stats.BlobsCount > removedBlobs) ? (stats.BlobsCount - removedBlobs) : 0;
    stats.DeletionsCount = 0;

    db.WriteCompactionMap(
        rangeId,
        stats.BlobsCount,
        stats.DeletionsCount
    );
    UpdateCompactionMap(
        rangeId,
        stats.BlobsCount,
        stats.DeletionsCount
    );

    AddCompactionRange(
        GetCurrentCommitId(),
        rangeId,
        stats.BlobsCount,
        stats.DeletionsCount,
        profileLogRequest);

    return deletionMarkerCount;
}

void TIndexTabletState::RewriteMixedBlocks(
    TIndexTabletDatabase& db,
    ui32 rangeId,
    /*const*/ TMixedBlobMeta& blob,
    const TMixedBlobStats& stats)
{
    if (stats.GarbageBlocks) {
        DecrementGarbageBlocksCount(db, stats.GarbageBlocks);
    }
    if (stats.CheckpointBlocks) {
        DecrementCheckpointBlocksCount(db, stats.CheckpointBlocks);
    }

    db.DeleteMixedBlocks(rangeId, blob.BlobId);

    Impl->MixedBlocks.ApplyDeletionMarkers(GetRangeIdHasher(), blob.Blocks);
    Impl->LargeBlocks.ApplyDeletionMarkers(blob.Blocks);

    auto rebaseResult = RebaseMixedBlocks(blob.Blocks);

    if (!rebaseResult.LiveBlocks) {
        DeleteMixedBlocks(db, blob.BlobId, blob.Blocks);

        return;
    }

    if (rebaseResult.GarbageBlocks) {
        IncrementGarbageBlocksCount(db, rebaseResult.GarbageBlocks);
    }
    if (rebaseResult.CheckpointBlocks) {
        IncrementCheckpointBlocksCount(db, rebaseResult.CheckpointBlocks);
    }

    for (ui64 checkpointId: rebaseResult.UsedCheckpoints) {
        AddCheckpointBlob(db, checkpointId, rangeId, blob.BlobId);
    }

    auto blockList = TBlockList::EncodeBlocks(blob.Blocks, GetAllocator(EAllocatorTag::BlockList));

    db.WriteMixedBlocks(
        rangeId,
        blob.BlobId,
        blockList,
        rebaseResult.GarbageBlocks,
        rebaseResult.CheckpointBlocks);

    if (Impl->MixedBlocks.IsLoaded(rangeId)) {
        bool removed = Impl->MixedBlocks.RemoveBlocks(rangeId, blob.BlobId);
        TABLET_VERIFY(removed);

        bool added = Impl->MixedBlocks.AddBlocks(
            rangeId,
            blob.BlobId,
            std::move(blockList),
            TMixedBlobStats {
                rebaseResult.GarbageBlocks,
                rebaseResult.CheckpointBlocks
            });
        TABLET_VERIFY(added);
    }
}

ui32 TIndexTabletState::GetMixedRangeIndex(ui64 nodeId, ui32 blockIndex) const
{
    TABLET_VERIFY(Impl->RangeIdHasher);

    return NStorage::GetMixedRangeIndex(
        *Impl->RangeIdHasher,
        nodeId,
        blockIndex);
}

ui32 TIndexTabletState::GetMixedRangeIndex(
    ui64 nodeId,
    ui32 blockIndex,
    ui32 blocksCount) const
{
    TABLET_VERIFY(Impl->RangeIdHasher);

    return NStorage::GetMixedRangeIndex(
        *Impl->RangeIdHasher,
        nodeId,
        blockIndex,
        blocksCount);
}

ui32 TIndexTabletState::GetMixedRangeIndex(const TVector<TBlock>& blocks) const
{
    TABLET_VERIFY(Impl->RangeIdHasher);

    return NStorage::GetMixedRangeIndex(*Impl->RangeIdHasher, blocks);
}

const IBlockLocation2RangeIndex& TIndexTabletState::GetRangeIdHasher() const
{
    TABLET_VERIFY(Impl->RangeIdHasher);

    return *Impl->RangeIdHasher;
}

////////////////////////////////////////////////////////////////////////////////
// LargeBlocks

void TIndexTabletState::FindLargeBlocks(
    ILargeBlockVisitor& visitor,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount) const
{
    Impl->LargeBlocks.FindBlocks(
        visitor,
        nodeId,
        commitId,
        blockIndex,
        blocksCount);
}

////////////////////////////////////////////////////////////////////////////////
// Garbage

void TIndexTabletState::LoadGarbage(
    const TVector<TPartialBlobId>& newBlobs,
    const TVector<TPartialBlobId>& garbageBlobs)
{
    for (const auto& blobId: newBlobs) {
        bool added = Impl->GarbageQueue.AddNewBlob(blobId);
        TABLET_VERIFY(added);
    }

    for (const auto& blobId: garbageBlobs) {
        bool added = Impl->GarbageQueue.AddGarbageBlob(blobId);
        TABLET_VERIFY(added);
    }
}

void TIndexTabletState::AcquireCollectBarrier(ui64 commitId)
{
    Impl->GarbageQueue.AcquireCollectBarrier(commitId);
}

// returns true if the barrier was present
bool TIndexTabletState::TryReleaseCollectBarrier(ui64 commitId)
{
    return Impl->GarbageQueue.TryReleaseCollectBarrier(commitId);
}

bool TIndexTabletState::IsCollectBarrierAcquired(ui64 commitId) const
{
    return Impl->GarbageQueue.IsCollectBarrierAcquired(commitId);
}

ui64 TIndexTabletState::GetCollectCommitId() const
{
    // should not collect after any barrier
    return Min(
        GetCurrentCommitId() - 1,
        Impl->GarbageQueue.GetCollectCommitId());
}

void TIndexTabletState::AddNewBlob(
    TIndexTabletDatabase& db,
    const TPartialBlobId& blobId)
{
    bool added = Impl->GarbageQueue.AddNewBlob(blobId);
    TABLET_VERIFY(added);

    db.WriteNewBlob(blobId);
    IncrementGarbageQueueSize(db, blobId.BlobSize());
}

void TIndexTabletState::AddGarbageBlob(
    TIndexTabletDatabase& db,
    const TPartialBlobId& blobId)
{
    bool added = Impl->GarbageQueue.AddGarbageBlob(blobId);
    TABLET_VERIFY(added);

    db.WriteGarbageBlob(blobId);
    IncrementGarbageQueueSize(db, blobId.BlobSize());
}

TVector<TPartialBlobId> TIndexTabletState::GetNewBlobs(ui64 collectCommitId) const
{
    return Impl->GarbageQueue.GetNewBlobs(collectCommitId);
}

TVector<TPartialBlobId> TIndexTabletState::GetGarbageBlobs(ui64 collectCommitId) const
{
    return Impl->GarbageQueue.GetGarbageBlobs(collectCommitId);
}

void TIndexTabletState::DeleteGarbage(
    TIndexTabletDatabase& db,
    ui64 collectCommitId,
    const TVector<TPartialBlobId>& newBlobs,
    const TVector<TPartialBlobId>& garbageBlobs)
{
    SetLastCollectCommitId(db, collectCommitId);

    ui64 blobSizeSum = 0;

    for (const auto& blobId: newBlobs) {
        bool removed = Impl->GarbageQueue.RemoveNewBlob(blobId);
        TABLET_VERIFY(removed);

        db.DeleteNewBlob(blobId);

        blobSizeSum += blobId.BlobSize();
    }

    for (const auto& blobId: garbageBlobs) {
        bool removed = Impl->GarbageQueue.RemoveGarbageBlob(blobId);
        TABLET_VERIFY(removed);

        db.DeleteGarbageBlob(blobId);

        blobSizeSum += blobId.BlobSize();
    }

    DecrementGarbageQueueSize(db, blobSizeSum);
}

////////////////////////////////////////////////////////////////////////////////
// Compaction

void TIndexTabletState::UpdateCompactionMap(
    ui32 rangeId,
    ui32 blobsCount,
    ui32 deletionsCount)
{
    Impl->CompactionMap.Update(rangeId, blobsCount, deletionsCount);
}

TCompactionStats TIndexTabletState::GetCompactionStats(ui32 rangeId) const
{
    return Impl->CompactionMap.Get(rangeId);
}

TCompactionCounter TIndexTabletState::GetRangeToCompact() const
{
    return Impl->CompactionMap.GetTopCompactionScore();
}

TCompactionCounter TIndexTabletState::GetRangeToCleanup() const
{
    return Impl->CompactionMap.GetTopCleanupScore();
}

TMaybe<TIndexTabletState::TPriorityRange>
TIndexTabletState::NextPriorityRangeForCleanup() const
{
    const auto t = LargeDeletionMarkersCleanupThreshold;
    if (PriorityRangesForCleanup.empty()
            && GetLargeDeletionMarkersCount() >= t)
    {
        auto one = Impl->LargeBlocks.GetOne();
        SplitRange(
            one.BlockIndex,
            one.BlockCount,
            BlockGroupSize,
            [&] (ui32 blockOffset, ui32 blocksCount) {
                PriorityRangesForCleanup.push_back({
                    one.NodeId,
                    one.BlockIndex + blockOffset,
                    blocksCount,
                    GetMixedRangeIndex(
                        one.NodeId,
                        one.BlockIndex + blockOffset)});
            });
    }

    if (PriorityRangesForCleanup) {
        return PriorityRangesForCleanup.front();
    }

    return {};
}

ui32 TIndexTabletState::GetPriorityRangeCount() const
{
    return PriorityRangesForCleanup.size();
}

TCompactionMapStats TIndexTabletState::GetCompactionMapStats(ui32 topSize) const
{
    return Impl->CompactionMap.GetStats(topSize);
}

TVector<ui32> TIndexTabletState::GetNonEmptyCompactionRanges() const
{
    return Impl->CompactionMap.GetNonEmptyCompactionRanges();
}

TVector<ui32> TIndexTabletState::GetAllCompactionRanges() const
{
    return Impl->CompactionMap.GetAllCompactionRanges();
}

TVector<TCompactionRangeInfo> TIndexTabletState::GetTopRangesByCompactionScore(ui32 topSize) const
{
    return Impl->CompactionMap.GetTopRangesByCompactionScore(topSize);
}

TVector<TCompactionRangeInfo> TIndexTabletState::GetTopRangesByCleanupScore(ui32 topSize) const
{
    return Impl->CompactionMap.GetTopRangesByCleanupScore(topSize);
}

void TIndexTabletState::LoadCompactionMap(
    const TVector<TCompactionRangeInfo>& ranges)
{
    Impl->CompactionMap.Update(ranges);
}

void TIndexTabletState::EnqueueForcedRangeOperation(
    TEvIndexTabletPrivate::EForcedRangeOperationMode mode,
    TVector<ui32> ranges)
{
    PendingForcedRangeOperations.emplace_back(mode, std::move(ranges));
}

TIndexTabletState::TPendingForcedRangeOperation TIndexTabletState::
    DequeueForcedRangeOperation()
{
    if (PendingForcedRangeOperations.empty()) {
        return {};
    }

    auto op = std::move(PendingForcedRangeOperations.back());
    PendingForcedRangeOperations.pop_back();

    return op;
}

void TIndexTabletState::StartForcedRangeOperation(
    TEvIndexTabletPrivate::EForcedRangeOperationMode mode,
    TVector<ui32> ranges)
{
    TABLET_VERIFY(!ForcedRangeOperationState.Defined());
    ForcedRangeOperationState.ConstructInPlace(mode, std::move(ranges));
}

void TIndexTabletState::CompleteForcedRangeOperation()
{
    ForcedRangeOperationState.Clear();
}

////////////////////////////////////////////////////////////////////////////////
// ReadAhead

bool TIndexTabletState::TryFillDescribeResult(
    ui64 nodeId,
    ui64 handle,
    const TByteRange& range,
    NProtoPrivate::TDescribeDataResponse* response)
{
    return Impl->ReadAheadCache.TryFillResult(nodeId, handle, range, response);
}

TMaybe<TByteRange> TIndexTabletState::RegisterDescribe(
    ui64 nodeId,
    ui64 handle,
    const TByteRange inputRange)
{
    return Impl->ReadAheadCache.RegisterDescribe(nodeId, handle, inputRange);
}

void TIndexTabletState::InvalidateReadAheadCache(ui64 nodeId)
{
    Impl->ReadAheadCache.InvalidateCache(nodeId);
}

void TIndexTabletState::RegisterReadAheadResult(
    ui64 nodeId,
    ui64 handle,
    const TByteRange& range,
    const NProtoPrivate::TDescribeDataResponse& result)
{
    Impl->ReadAheadCache.RegisterResult(nodeId, handle, range, result);
}

TReadAheadCacheStats TIndexTabletState::CalculateReadAheadCacheStats() const
{
    return Impl->ReadAheadCache.GetStats();
}

}   // namespace NCloud::NFileStore::NStorage
