#include "tablet_state_impl.h"

#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

#include <util/generic/guid.h>

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

NProto::TError TIndexTabletState::Truncate(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    ui64 currentSize,
    ui64 targetSize)
{
    if (currentSize <= targetSize) {
        return {};
    }

    TByteRange range(targetSize, currentSize - targetSize, GetBlockSize());

    if (TruncateBlocksThreshold && range.BlockCount() > TruncateBlocksThreshold) {
        EnqueueTruncateOp(nodeId, range);
        return {};
    }

    return TruncateRange(db, nodeId, commitId, range);
}

NProto::TError TIndexTabletState::TruncateRange(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    TByteRange range)
{
    const TByteRange tailAlignedRange(
        range.Offset,
        AlignUp<ui64>(range.End(), range.BlockSize) - range.Offset,
        range.BlockSize);

    auto e = DeleteRange(db, nodeId, commitId, tailAlignedRange);
    if (HasError(e)) {
        return e;
    }

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

    return {};
}

NProto::TError TIndexTabletState::ZeroRange(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    TByteRange range)
{
    auto e = DeleteRange(db, nodeId, commitId, range);
    if (HasError(e)) {
        return e;
    }

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

    return {};
}

NProto::TError TIndexTabletState::DeleteRange(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    const TByteRange& range)
{
    const ui64 deletedBlockCount = range.AlignedBlockCount();
    if (deletedBlockCount) {
        const bool useLargeDeletionMarkers = LargeDeletionMarkersEnabled
            && deletedBlockCount >= LargeDeletionMarkersThreshold;
        if (useLargeDeletionMarkers) {
            const auto t = LargeDeletionMarkersThresholdForBackpressure;
            if (GetLargeDeletionMarkersCount() >= t) {
                return MakeError(E_REJECTED, TStringBuilder()
                    << "too many large deletion markers: "
                    << GetLargeDeletionMarkersCount() << " >= " << t);
            }

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

        MarkFreshBlocksDeleted(
            db,
            nodeId,
            commitId,
            range.FirstAlignedBlock(),
            deletedBlockCount);
    }

    WriteFreshBytesDeletionMarker(
        db,
        nodeId,
        commitId,
        range.Offset,
        range.Length);

    return {};
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
    const TIndexTabletState::TBackpressureValues& values,
    TString* message)
{
    if (values.Flush >= thresholds.Flush) {
        *message = TStringBuilder() << "freshBlocksDataSize: " << values.Flush;
        return false;
    }

    if (values.FlushBytes >= thresholds.FlushBytes) {
        *message = TStringBuilder() << "freshBytesCount: " << values.FlushBytes;
        return false;
    }

    if (values.CompactionScore >= thresholds.CompactionScore) {
        *message = TStringBuilder()
                   << "compactionScore: " << values.CompactionScore;
        return false;
    }

    if (values.CleanupScore >= thresholds.CleanupScore) {
        *message = TStringBuilder() << "cleanupScore: " << values.CleanupScore;
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

    IncrementFreshBytesCount(db, data.size());
    UpdateFreshBytesItemCount();

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

    auto freshBytes = Impl->FreshBytes.GetTotalBytes();
    auto deletedFreshBytes = Impl->FreshBytes.GetTotalDeletedBytes();
    SetFreshBytesCount(db, freshBytes);
    SetDeletedFreshBytesCount(db, deletedFreshBytes);
    UpdateFreshBytesItemCount();

    return {sz + deletedSz, completed};
}

ui32 TIndexTabletState::GetFreshBytesItemCount() const
{
    return FileSystemStats.GetFreshBytesItemCount();
}

void TIndexTabletState::UpdateFreshBytesItemCount()
{
    auto freshBytesItemCount = Impl->FreshBytes.GetTotalDataItemCount();
    FileSystemStats.SetFreshBytesItemCount(freshBytesItemCount);
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

bool TIndexTabletState::LoadMixedBlocks(IIndexTabletDatabase& db, ui32 rangeId)
{
    if (Impl->MixedBlocks.IsLoaded(rangeId)) {
        Impl->MixedBlocks.RefRange(rangeId);
        return true;
    }

    TVector<TIndexTabletDatabase::IIndexTabletDatabase::TMixedBlob> blobs;
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

TWriteMixedBlocksResult TIndexTabletState::WriteMixedBlocks(
    TIndexTabletDatabase& db,
    const TPartialBlobId& blobId,
    /*const*/ TVector<TBlock>& blocks)
{
    ui32 rangeId = GetMixedRangeIndex(blocks);

    auto result = WriteMixedBlocks(db, rangeId, blobId, blocks);
    if (result.NewBlob) {
        AddNewBlob(db, blobId);
    }
    return result;
}

TWriteMixedBlocksResult TIndexTabletState::WriteMixedBlocks(
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

    if (!rebaseResult.LiveBlocksCount) {
        AddGarbageBlob(db, blobId);

        return {.GarbageBlocksCount = 0, .NewBlob = false};
    }

    if (rebaseResult.GarbageBlocksCount) {
        IncrementGarbageBlocksCount(db, rebaseResult.GarbageBlocksCount);
    }
    if (rebaseResult.CheckpointBlocksCount) {
        IncrementCheckpointBlocksCount(db, rebaseResult.CheckpointBlocksCount);
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
        rebaseResult.GarbageBlocksCount,
        rebaseResult.CheckpointBlocksCount);

    IncrementMixedBlobsCount(db);
    IncrementMixedBlocksCount(db, blocks.size());

    if (isMixedRangeLoaded) {
        bool added = Impl->MixedBlocks.AddBlocks(
            rangeId,
            blobId,
            std::move(blockList),
            TMixedBlobStats {
                rebaseResult.GarbageBlocksCount,
                rebaseResult.CheckpointBlocksCount
            });
        TABLET_VERIFY(added);
    }

    InvalidateReadAheadCache(blocks[0].NodeId);

    return {
        .GarbageBlocksCount = rebaseResult.GarbageBlocksCount,
        .NewBlob = true
    };
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

TDeleteMixedBlocksResult TIndexTabletState::DeleteMixedBlocks(
    TIndexTabletDatabase& db,
    ui32 rangeId,
    const TPartialBlobId& blobId,
    const TVector<TBlock>& blocks)
{
    TMixedBlobStats stats;

    bool removed = Impl->MixedBlocks.RemoveBlocks(rangeId, blobId, &stats);
    TABLET_VERIFY(removed);

    if (stats.GarbageBlocksCount) {
        DecrementGarbageBlocksCount(db, stats.GarbageBlocksCount);
    }
    if (stats.CheckpointBlocksCount) {
        DecrementCheckpointBlocksCount(db, stats.CheckpointBlocksCount);
    }

    db.DeleteMixedBlocks(rangeId, blobId);

    DecrementMixedBlobsCount(db);
    DecrementMixedBlocksCount(db, blocks.size());

    return {.GarbageBlocksCount = stats.GarbageBlocksCount};
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
        stats.DeletionsCount + blocksCount,
        stats.GarbageBlocksCount);
    UpdateCompactionMap(
        rangeId,
        stats.BlobsCount,
        stats.DeletionsCount + blocksCount,
        stats.GarbageBlocksCount,
        false /* compacted */);

    InvalidateReadAheadCache(nodeId);
}

bool TIndexTabletState::UpdateBlockLists(
    TIndexTabletDatabase& db,
    TMixedBlobMeta& blob)
{
    const auto rangeId = GetMixedRangeIndex(blob.Blocks);
    DeleteMixedBlocks(db, rangeId, blob.BlobId, blob.Blocks);
    return WriteMixedBlocks(db, rangeId, blob.BlobId, blob.Blocks).NewBlob;
}

ui32 TIndexTabletState::CleanupBlockDeletions(
    TIndexTabletDatabase& db,
    ui32 rangeId,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    auto affectedBlobs =
        Impl->MixedBlocks.ApplyDeletionMarkersAndGetMetas(rangeId);

    ui64 removedBlobs = 0;
    ui32 deletedGarbageBlocksCount = 0;
    ui32 newGarbageBlocksCount = 0;
    TVector<TMixedBlobMeta> updatedBlobs;
    for (auto& blob: affectedBlobs) {
        const bool affected =
            Impl->LargeBlocks.ApplyDeletionMarkers(blob.BlobMeta.Blocks);
        if (!blob.Affected && !affected) {
            // small optimization - not rewriting blob metas for the blobs that
            // were not affected by deletion markers
            continue;
        }

        auto deleteBlocksResult = DeleteMixedBlocks(
            db,
            rangeId,
            blob.BlobMeta.BlobId,
            blob.BlobMeta.Blocks);

        auto writeBlocksResult = WriteMixedBlocks(
            db,
            rangeId,
            blob.BlobMeta.BlobId,
            blob.BlobMeta.Blocks);

        deletedGarbageBlocksCount += deleteBlocksResult.GarbageBlocksCount;
        newGarbageBlocksCount += writeBlocksResult.GarbageBlocksCount;

        if (!writeBlocksResult.NewBlob) {
            ++removedBlobs;
        }

        updatedBlobs.emplace_back(std::move(blob.BlobMeta));
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

    AddBlobsInfo(GetBlockSize(), updatedBlobs, profileLogRequest);

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
    UpdateMinDeletionMarkersCountSinceTabletStart();

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
    if (stats.BlobsCount > removedBlobs) {
        stats.BlobsCount = stats.BlobsCount - removedBlobs;
        if (stats.GarbageBlocksCount > deletedGarbageBlocksCount) {
            stats.GarbageBlocksCount -= deletedGarbageBlocksCount;
            stats.GarbageBlocksCount += newGarbageBlocksCount;
        } else {
            stats.GarbageBlocksCount = newGarbageBlocksCount;
        }
    } else {
        stats.BlobsCount = 0;
        stats.GarbageBlocksCount = 0;
    }
    stats.DeletionsCount = 0;

    db.WriteCompactionMap(
        rangeId,
        stats.BlobsCount,
        stats.DeletionsCount,
        stats.GarbageBlocksCount);
    UpdateCompactionMap(
        rangeId,
        stats.BlobsCount,
        stats.DeletionsCount,
        stats.GarbageBlocksCount,
        false /* compacted */);

    AddCompactionRange(
        GetCurrentCommitId(),
        rangeId,
        stats.BlobsCount,
        stats.DeletionsCount,
        stats.GarbageBlocksCount,
        profileLogRequest);

    return deletionMarkerCount;
}

void TIndexTabletState::RewriteMixedBlocks(
    TIndexTabletDatabase& db,
    ui32 rangeId,
    /*const*/ TMixedBlobMeta& blob,
    const TMixedBlobStats& stats)
{
    if (stats.GarbageBlocksCount) {
        DecrementGarbageBlocksCount(db, stats.GarbageBlocksCount);
    }
    if (stats.CheckpointBlocksCount) {
        DecrementCheckpointBlocksCount(db, stats.CheckpointBlocksCount);
    }

    db.DeleteMixedBlocks(rangeId, blob.BlobId);

    Impl->MixedBlocks.ApplyDeletionMarkers(GetRangeIdHasher(), blob.Blocks);
    Impl->LargeBlocks.ApplyDeletionMarkers(blob.Blocks);

    auto rebaseResult = RebaseMixedBlocks(blob.Blocks);

    if (!rebaseResult.LiveBlocksCount) {
        DeleteMixedBlocks(db, blob.BlobId, blob.Blocks);

        return;
    }

    if (rebaseResult.GarbageBlocksCount) {
        IncrementGarbageBlocksCount(db, rebaseResult.GarbageBlocksCount);
    }
    if (rebaseResult.CheckpointBlocksCount) {
        IncrementCheckpointBlocksCount(db, rebaseResult.CheckpointBlocksCount);
    }

    for (ui64 checkpointId: rebaseResult.UsedCheckpoints) {
        AddCheckpointBlob(db, checkpointId, rangeId, blob.BlobId);
    }

    auto blockList = TBlockList::EncodeBlocks(blob.Blocks, GetAllocator(EAllocatorTag::BlockList));

    db.WriteMixedBlocks(
        rangeId,
        blob.BlobId,
        blockList,
        rebaseResult.GarbageBlocksCount,
        rebaseResult.CheckpointBlocksCount);

    if (Impl->MixedBlocks.IsLoaded(rangeId)) {
        bool removed = Impl->MixedBlocks.RemoveBlocks(rangeId, blob.BlobId);
        TABLET_VERIFY(removed);

        bool added = Impl->MixedBlocks.AddBlocks(
            rangeId,
            blob.BlobId,
            std::move(blockList),
            TMixedBlobStats {
                rebaseResult.GarbageBlocksCount,
                rebaseResult.CheckpointBlocksCount
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

ui32 TIndexTabletState::CalculateMixedIndexRangeGarbageBlockCount(
    ui32 rangeId) const
{
    return Impl->MixedBlocks.CalculateGarbageBlockCount(rangeId);
}


TBlobMetaMapStats TIndexTabletState::GetBlobMetaMapStats() const
{
    return Impl->MixedBlocks.GetBlobMetaMapStats();
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
    ui32 deletionsCount,
    ui32 garbageBlocksCount,
    bool compacted)
{
    Impl->CompactionMap.Update(
        rangeId,
        blobsCount,
        deletionsCount,
        garbageBlocksCount,
        compacted);
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

TCompactionCounter TIndexTabletState::GetRangeToCompactByGarbage() const
{
    return Impl->CompactionMap.GetTopGarbageScore();
}

TMaybe<TIndexTabletState::TPriorityRange>
TIndexTabletState::NextPriorityRangeForCleanup() const
{
    if (PriorityRangesForCleanup.empty()
            && GetLargeDeletionMarkersCount()
                >= LargeDeletionMarkersCleanupThreshold)
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

TVector<TCompactionRangeInfo> TIndexTabletState::GetTopRangesByCompactionScore(
    ui32 topSize) const
{
    return Impl->CompactionMap.GetTopRangesByCompactionScore(topSize);
}

TVector<TCompactionRangeInfo> TIndexTabletState::GetTopRangesByCleanupScore(
    ui32 topSize) const
{
    return Impl->CompactionMap.GetTopRangesByCleanupScore(topSize);
}

TVector<TCompactionRangeInfo> TIndexTabletState::GetTopRangesByGarbageScore(
    ui32 topSize) const
{
    return Impl->CompactionMap.GetTopRangesByGarbageScore(topSize);
}

void TIndexTabletState::LoadCompactionMap(
    const TVector<TCompactionRangeInfo>& compactionMap)
{
    Impl->CompactionMap.Update(compactionMap);
}

TString TIndexTabletState::EnqueueForcedRangeOperation(
    TEvIndexTabletPrivate::EForcedRangeOperationMode mode,
    TVector<ui32> ranges)
{
    auto operationId = CreateGuidAsString();
    PendingForcedRangeOperations.emplace_back(
        mode,
        std::move(ranges),
        operationId);
    return operationId;
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
    TVector<ui32> ranges,
    TString operationId)
{
    TABLET_VERIFY(!ForcedRangeOperationState.Defined());
    ForcedRangeOperationState.ConstructInPlace(
        mode,
        std::move(ranges),
        std::move(operationId));
}

void TIndexTabletState::CompleteForcedRangeOperation()
{
    Y_DEBUG_ABORT_UNLESS(ForcedRangeOperationState);
    if (ForcedRangeOperationState && ForcedRangeOperationState->OperationId) {
        ForcedRangeOperationState->Current =
            ForcedRangeOperationState->RangesToCompact.size();
        CompletedForcedRangeOperations.push_back(*ForcedRangeOperationState);
    }
    ForcedRangeOperationState.Clear();
}

auto TIndexTabletState::FindForcedRangeOperation(
    const TString& operationId) const -> const TForcedRangeOperationState*
{
    if (ForcedRangeOperationState
            && ForcedRangeOperationState->OperationId == operationId)
    {
        return ForcedRangeOperationState.Get();
    }

    for (const auto& op: CompletedForcedRangeOperations) {
        if (op.OperationId == operationId) {
            return &op;
        }
    }

    return nullptr;
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

////////////////////////////////////////////////////////////////////////////////
// Balancing

NProto::TError TIndexTabletState::SelectShard(ui64 fileSize, TString* shardId)
{
    auto e = Impl->ShardBalancer->SelectShard(fileSize, shardId);
    if (HasError(e)) {
        return e;
    }

    return e;
}

void TIndexTabletState::UpdateShardBalancer(const TVector<TShardStats>& stats)
{
    std::optional<ui64> desiredFreeSpaceReserve;
    std::optional<ui64> minFreeSpaceReserve;
    // TODO: remove this code when all the file systems will be switched to
    // strict mode
    if(GetFileSystem().GetStrictFileSystemSizeEnforcementEnabled()) {
        desiredFreeSpaceReserve = 0;
        minFreeSpaceReserve = 0;
    }

    Impl->ShardBalancer->Update(
        stats,
        desiredFreeSpaceReserve,
        minFreeSpaceReserve);
}

}   // namespace NCloud::NFileStore::NStorage
