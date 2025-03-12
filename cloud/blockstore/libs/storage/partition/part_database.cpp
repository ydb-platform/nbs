#include "part_database.h"

#include "part_schema.h"

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 SafeDistance(ui32 l, ui32 r)
{
    return l < r ? r - l : 0;
}

ui32 SafeAdd(ui32 start, ui32 count)
{
    Y_DEBUG_ABORT_UNLESS(count);
    if (start < TBlockRange32::MaxIndex - (count - 1)) {
        return start + (count - 1);
    } else {
        return TBlockRange32::MaxIndex;
    }
}

static constexpr ui32 ScanRangeSize = 100;

TVector<TBlockRange32> SplitInRanges(
    const TVector<ui32>& blocks,
    ui32 rangeSize = ScanRangeSize)
{
    Y_DEBUG_ABORT_UNLESS(!blocks.empty());
    Y_DEBUG_ABORT_UNLESS(IsSorted(blocks.begin(), blocks.end()));

    TVector<TBlockRange32> result;

    ui32 rangeStart = blocks[0];
    ui32 rangeEnd = blocks[0];

    for (size_t i = 1; i < blocks.size(); ++i) {
        if (blocks[i] - rangeStart < rangeSize) {
            rangeEnd = blocks[i];
        } else {
            result.push_back(
                TBlockRange32::MakeClosedInterval(rangeStart, rangeEnd));
            rangeStart = blocks[i];
            rangeEnd = blocks[i];
        }
    }

    result.push_back(TBlockRange32::MakeClosedInterval(rangeStart, rangeEnd));
    return result;
}

ui16 ProcessCompactionCounter(ui32 value)
{
    return value > Max<ui16>() ? Max<ui16>() : value;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::InitSchema()
{
    Materialize<TPartitionSchema>();

    TSchemaInitializer<TPartitionSchema::TTables>::InitStorage(Database.Alter());
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteMeta(const NProto::TPartitionMeta& meta)
{
    using TTable = TPartitionSchema::Meta;

    Table<TTable>()
        .Key(1)
        .Update(NIceDb::TUpdate<TTable::PartitionMeta>(meta));
}

bool TPartitionDatabase::ReadMeta(TMaybe<NProto::TPartitionMeta>& meta)
{
    using TTable = TPartitionSchema::Meta;

    auto it = Table<TTable>()
        .Key(1)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        meta = it.GetValue<TTable::PartitionMeta>();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteFreshBlock(
    ui32 blockIndex,
    ui64 commitId,
    TBlockDataRef blockContent)
{
    using TTable = TPartitionSchema::FreshBlocksIndex;

    Table<TTable>()
        .Key(blockIndex, ReverseCommitId(commitId))
        .Update(NIceDb::TUpdate<TTable::BlockContent>(blockContent.AsStringBuf()));
}

void TPartitionDatabase::DeleteFreshBlock(ui32 blockIndex, ui64 commitId)
{
    using TTable = TPartitionSchema::FreshBlocksIndex;

    Table<TTable>()
        .Key(blockIndex, ReverseCommitId(commitId))
        .Delete();
}

bool TPartitionDatabase::ReadFreshBlocks(TVector<TOwningFreshBlock>& blocks)
{
    using TTable = TPartitionSchema::FreshBlocksIndex;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        blocks.emplace_back(
            TBlock{
                it.GetValue<TTable::BlockIndex>(),
                ReverseCommitId(it.GetValue<TTable::CommitId>()),
                true  // isStoredInDb
            },
            TString{it.GetValue<TTable::BlockContent>()}
        );

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteMixedBlock(TMixedBlock block)
{
    using TTable = TPartitionSchema::MixedBlocksIndex;

    if (block.CommitId == block.BlobId.CommitId()) {
        Table<TTable>()
            .Key(block.BlockIndex, ReverseCommitId(block.CommitId))
            .Update(
                NIceDb::TUpdate<TTable::BlobId>(block.BlobId.UniqueId()),
                NIceDb::TUpdate<TTable::BlobOffset>(block.BlobOffset));
    } else {
        Table<TTable>()
            .Key(block.BlockIndex, ReverseCommitId(block.CommitId))
            .Update(
                NIceDb::TUpdate<TTable::BlobCommitId>(block.BlobId.CommitId()),
                NIceDb::TUpdate<TTable::BlobId>(block.BlobId.UniqueId()),
                NIceDb::TUpdate<TTable::BlobOffset>(block.BlobOffset));
    }
}

void TPartitionDatabase::WriteMixedBlocks(
    const TPartialBlobId& blobId,
    const TVector<ui32>& blocks)
{
    using TTable = TPartitionSchema::MixedBlocksIndex;

    ui16 blobOffset = 0;
    for (ui32 blockIndex: blocks) {
        Table<TTable>()
            .Key(blockIndex, ReverseCommitId(blobId.CommitId()))
            .Update(
                NIceDb::TUpdate<TTable::BlobId>(blobId.UniqueId()),
                NIceDb::TUpdate<TTable::BlobOffset>(blobOffset));
        ++blobOffset;
    }
}

void TPartitionDatabase::DeleteMixedBlock(ui32 blockIndex, ui64 commitId)
{
    using TTable = TPartitionSchema::MixedBlocksIndex;

    Table<TTable>()
        .Key(blockIndex, ReverseCommitId(commitId))
        .Delete();
}

bool TPartitionDatabase::FindMixedBlocks(
    IBlocksIndexVisitor& visitor,
    const TBlockRange32& readRange,
    bool precharge,
    ui64 maxCommitId)
{
    using TTable = TPartitionSchema::MixedBlocksIndex;

    auto query = Table<TTable>()
        .GreaterOrEqual(readRange.Start)
        .LessOrEqual(readRange.End);

    if (precharge && !query.Precharge()) {
        return false;
    }

    auto it = query.Select();

    if (!it.IsReady()) {
        query.Precharge();

        return false;   // not ready
    }

    while (it.IsValid()) {
        ui32 blockIndex = it.GetValue<TTable::BlockIndex>();
        Y_DEBUG_ABORT_UNLESS(blockIndex >= readRange.Start);

        if (blockIndex > readRange.End) {
            break;  // out of range
        }

        ui64 commitId = ReverseCommitId(it.GetValue<TTable::CommitId>());
        if (commitId <= maxCommitId) {
            ui64 blobCommitId = it.GetValue<TTable::BlobCommitId>();
            auto blobId = MakePartialBlobId(
                blobCommitId ? blobCommitId : commitId,
                it.GetValue<TTable::BlobId>());

            ui16 blobOffset = it.GetValue<TTable::BlobOffset>();

            if (!visitor.Visit(blockIndex, commitId, blobId, blobOffset)) {
                return true;    // interrupted
            }
        }

        if (!it.Next()) {
            query.Precharge();

            return false;   // not ready
        }
    }

    return true;
}

bool TPartitionDatabase::FindMixedBlocks(
    IBlocksIndexVisitor& visitor,
    const TVector<ui32>& blocks,
    ui64 maxCommitId)
{
    using TTable = TPartitionSchema::MixedBlocksIndex;

    auto ranges = SplitInRanges(blocks);
    bool ready = true;

    // read ranges
    size_t i = 0;
    for (const auto& readRange: ranges) {
        auto it = Table<TTable>()
            .GreaterOrEqual(readRange.Start)
            .LessOrEqual(readRange.End)
            .Select();

        if (!it.IsReady()) {
            ready = false;
            break;
        }

        while (it.IsValid()) {
            ui32 blockIndex = it.GetValue<TTable::BlockIndex>();
            while (blocks[i] < blockIndex) {
                // skip blocks
                ++i;
            }

            Y_DEBUG_ABORT_UNLESS(blockIndex <= blocks[i]);
            if (blockIndex == blocks[i]) {
                ui64 commitId = ReverseCommitId(it.GetValue<TTable::CommitId>());
                if (commitId <= maxCommitId) {
                    ui64 blobCommitId = it.GetValue<TTable::BlobCommitId>();
                    auto blobId = MakePartialBlobId(
                        blobCommitId ? blobCommitId : commitId,
                        it.GetValue<TTable::BlobId>());

                    ui16 blobOffset = it.GetValue<TTable::BlobOffset>();

                    if (!visitor.Visit(blockIndex, commitId, blobId, blobOffset)) {
                        return true;    // interrupted
                    }
                }
            }

            if (!it.Next()) {
                ready = false;
                break;
            }
        }

        if (!ready) {
            break;
        }
    }

    if (!ready) {
        for (const auto& readRange: ranges) {
            auto query = Table<TTable>()
                .GreaterOrEqual(readRange.Start)
                .LessOrEqual(readRange.End);

            query.Precharge();
        }
    }

    return ready;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteMergedBlocks(
    const TPartialBlobId& blobId,
    const TBlockRange32& blockRange,
    const TBlockMask& skipMask)
{
    using TTable = TPartitionSchema::MergedBlocksIndex;

    auto value = Table<TTable>()
        .Key(blockRange.End, ReverseCommitId(blobId.CommitId()));

    value.Update(
        NIceDb::TUpdate<TTable::RangeStart>(blockRange.Start),
        NIceDb::TUpdate<TTable::BlobId>(blobId.UniqueId()));

    if (!skipMask.Empty()) {
        value.Update(
            NIceDb::TUpdate<TTable::SkipMask>(BlockMaskAsString(skipMask)));
    }
}

void TPartitionDatabase::DeleteMergedBlocks(
    const TPartialBlobId& blobId,
    const TBlockRange32& blockRange)
{
    using TTable = TPartitionSchema::MergedBlocksIndex;

    Table<TTable>()
        .Key(blockRange.End, ReverseCommitId(blobId.CommitId()))
        .Delete();
}

bool TPartitionDatabase::FindMergedBlocks(
    IBlocksIndexVisitor& visitor,
    const TBlockRange32& readRange,
    bool precharge,
    ui32 maxBlocksInBlob,
    ui64 maxCommitId)
{
    using TTable = TPartitionSchema::MergedBlocksIndex;

    auto query = Table<TTable>()
        .GreaterOrEqual(readRange.Start)
        .LessOrEqual(SafeAdd(readRange.End, maxBlocksInBlob));

    if (precharge && !query.Precharge()) {
        return false;
    }

    auto it = query.Select();

    if (!it.IsReady()) {
        query.Precharge();

        return false;   // not ready
    }

    while (it.IsValid()) {
        auto range = TBlockRange32::MakeClosedInterval(
            it.GetValue<TTable::RangeStart>(),
            it.GetValue<TTable::RangeEnd>());

        Y_DEBUG_ABORT_UNLESS(range.End >= readRange.Start);
        Y_DEBUG_ABORT_UNLESS(range.Size() <= maxBlocksInBlob);

        if (SafeDistance(readRange.End, range.End) > maxBlocksInBlob) {
            break;  // out of range
        }

        ui32 start = Max(range.Start, readRange.Start);
        ui32 end = Min(range.End, readRange.End);

        // if there is no intersection - just skip range
        if (start <= end) {
            ui64 commitId = ReverseCommitId(it.GetValue<TTable::CommitId>());
            if (commitId <= maxCommitId) {
                auto blobId = MakePartialBlobId(
                    commitId,
                    it.GetValue<TTable::BlobId>());

                const auto holeMask = BlockMaskFromString(
                    it.GetValueOrDefault<TTable::HoleMask>());

                const auto skipMask = BlockMaskFromString(
                    it.GetValueOrDefault<TTable::SkipMask>());

                ui32 skipped = 0;
                for (ui32 blockIndex = range.Start; blockIndex < start; ++blockIndex) {
                    ui16 pos = blockIndex - range.Start;
                    skipped += skipMask.Get(pos);
                }

                for (ui32 blockIndex = start; blockIndex <= end; ++blockIndex) {
                    ui16 pos = blockIndex - range.Start;

                    if (skipMask.Get(pos)) {
                        ++skipped;
                        continue;
                    }

                    ui16 blobOffset = pos - skipped;

                    if (holeMask.Get(blobOffset)) {
                        continue;
                    }

                    if (!visitor.Visit(blockIndex, commitId, blobId, blobOffset)) {
                        return true;    // interrupted
                    }
                }
            }
        }

        if (!it.Next()) {
            query.Precharge();

            return false;   // not ready
        }
    }

    return true;
}

bool TPartitionDatabase::FindMergedBlocks(
    IBlocksIndexVisitor& visitor,
    const TVector<ui32>& blocks,
    ui32 maxBlocksInBlob,
    ui64 maxCommitId)
{
    using TTable = TPartitionSchema::MergedBlocksIndex;

    auto ranges = SplitInRanges(blocks);
    bool ready = true;

    // read ranges
    for (const auto& readRange: ranges) {
        auto it = Table<TTable>()
            .GreaterOrEqual(readRange.Start)
            .LessOrEqual(SafeAdd(readRange.End, maxBlocksInBlob))
            .Select();

        if (!it.IsReady()) {
            ready = false;
            break;
        }

        while (it.IsValid()) {
            auto range = TBlockRange32::MakeClosedInterval(
                it.GetValue<TTable::RangeStart>(),
                it.GetValue<TTable::RangeEnd>());

            Y_DEBUG_ABORT_UNLESS(range.End >= readRange.Start);
            Y_DEBUG_ABORT_UNLESS(range.Size() <= maxBlocksInBlob);

            auto start = LowerBound(blocks.begin(), blocks.end(), range.Start);
            auto end = UpperBound(start, blocks.end(), range.End);

            // if there is no intersection - just skip range
            if (start != end) {
                ui64 commitId = ReverseCommitId(it.GetValue<TTable::CommitId>());
                if (commitId <= maxCommitId) {
                    auto blobId = MakePartialBlobId(
                        commitId,
                        it.GetValue<TTable::BlobId>());

                    const auto holeMask = BlockMaskFromString(
                        it.GetValueOrDefault<TTable::HoleMask>());

                    const auto skipMask = BlockMaskFromString(
                        it.GetValueOrDefault<TTable::SkipMask>());

                    ui32 skipped = 0;
                    for (ui32 blockIndex = range.Start; blockIndex < *start; ++blockIndex) {
                        ui16 pos = blockIndex - range.Start;
                        skipped += skipMask.Get(pos);
                    }

                    for (auto it = start; it != end; ++it) {
                        ui16 pos = *it - range.Start;

                        if (skipMask.Get(pos)) {
                            ++skipped;
                            continue;
                        }

                        ui16 blobOffset = pos - skipped;

                        if (holeMask.Get(blobOffset)) {
                            continue;
                        }

                        if (!visitor.Visit(*it, commitId, blobId, blobOffset)) {
                            return true;    // interrupted
                        }
                    }
                }
            }

            if (!it.Next()) {
                ready = false;
                break;
            }
        }

        if (!ready) {
            break;
        }
    }

    if (!ready) {
        for (const auto& readRange: ranges) {
            auto query = Table<TTable>()
                .GreaterOrEqual(readRange.Start)
                .LessOrEqual(SafeAdd(readRange.End, maxBlocksInBlob));

            query.Precharge();
        }
    }

    return ready;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteBlobMeta(
    const TPartialBlobId& blobId,
    const NProto::TBlobMeta& blobMeta)
{
    using TTable = TPartitionSchema::BlobsIndex;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Update<TTable::BlobMeta>(blobMeta);
}

void TPartitionDatabase::DeleteBlobMeta(const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::BlobsIndex;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Delete();
}

bool TPartitionDatabase::ReadBlobMeta(
    const TPartialBlobId& blobId,
    TMaybe<NProto::TBlobMeta>& meta)
{
    using TTable = TPartitionSchema::BlobsIndex;

    auto it = Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        meta = it.GetValue<TTable::BlobMeta>();
    }

    return true;
}

bool TPartitionDatabase::ReadNewBlobs(
    TVector<TPartialBlobId>& blobIds,
    ui64 minCommitId)
{
    using TTable = TPartitionSchema::BlobsIndex;

    auto query = Table<TTable>()
        .GreaterOrEqual(minCommitId);

    if (!query.Precharge()) {
        return false;
    }

    auto it = query.Select();

    if (!it.IsReady()) {
        query.Precharge();
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto blobId = MakePartialBlobId(
            it.GetValue<TTable::CommitId>(),
            it.GetValue<TTable::BlobId>());

        // we are not interested in deletion markers here
        if (!IsDeletionMarker(blobId)) {
            blobIds.push_back(blobId);
        }

        if (!it.Next()) {
            query.Precharge();
            return false;   // not ready
        }
    }

    return true;
}

void TPartitionDatabase::WriteBlockMask(
    const TPartialBlobId& blobId,
    const TBlockMask& blockMask)
{
    using TTable = TPartitionSchema::BlobsIndex;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Update(
            NIceDb::TUpdate<TTable::BlockMask>(BlockMaskAsString(blockMask)));
}

bool TPartitionDatabase::ReadBlockMask(
    const TPartialBlobId& blobId,
    TMaybe<TBlockMask>& blockMask)
{
    using TTable = TPartitionSchema::BlobsIndex;

    auto it = Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Select<TTable::BlockMask>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        blockMask = BlockMaskFromString(
            it.GetValueOrDefault<TTable::BlockMask>());
    }

    return true;
}

enum class EIndexProcResult
{
    Ok,
    NotReady,
    Interrupted,
};

static EIndexProcResult FindBlocksInBlobIndex(
    TPartitionDatabase& db,
    IExtendedBlocksIndexVisitor& visitor,
    const ui32 maxBlocksInBlob,
    const TPartialBlobId& blobId,
    const NProto::TBlobMeta& blobMeta,
    const TBlockMask& blockMask,
    const TBlockRange32& blockRange)
{
    auto visit = [&] (
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset)
    {
        ui16 maskedBlobOffset = blockMask.Get(blobOffset)
            ? InvalidBlobOffset
            : blobOffset;

        return visitor.Visit(
            blockIndex,
            commitId,
            blobId,
            maskedBlobOffset,
            blobOffset < blobMeta.BlockChecksumsSize()
                ? blobMeta.GetBlockChecksums(blobOffset)
                : 0);
    };

    if (blobMeta.HasMixedBlocks()) {
        const auto& mixedBlocks = blobMeta.GetMixedBlocks();

        if (mixedBlocks.CommitIdsSize() == 0) {
            // every block shares the same commitId
            ui64 commitId = blobId.CommitId();

            ui16 blobOffset = 0;
            for (ui32 blockIndex: mixedBlocks.GetBlocks()) {
                if (blockRange.Contains(blockIndex)) {
                    if (!visit(blockIndex, commitId, blobId, blobOffset)) {
                        return EIndexProcResult::Interrupted;
                    }
                }
                ++blobOffset;
            }
        } else {
            // each block has its own commitId
            Y_ABORT_UNLESS(mixedBlocks.BlocksSize() == mixedBlocks.CommitIdsSize());

            ui16 blobOffset = 0;
            for (size_t i = 0; i < mixedBlocks.BlocksSize(); ++i) {
                ui32 blockIndex = mixedBlocks.GetBlocks(i);
                ui64 commitId = mixedBlocks.GetCommitIds(i);

                if (blockRange.Contains(blockIndex)) {
                    if (!visit(blockIndex, commitId, blobId, blobOffset)) {
                        return EIndexProcResult::Interrupted;
                    }
                }
                ++blobOffset;
            }

        }
    } else if (blobMeta.HasMergedBlocks()) {
        const auto& mergedBlocks = blobMeta.GetMergedBlocks();
        const auto blobRange = TBlockRange32::MakeClosedInterval(
            mergedBlocks.GetStart(),
            mergedBlocks.GetEnd());
        if (!blobRange.Overlaps(blockRange)) {
            return EIndexProcResult::Ok;
        }
        const auto intersection = blobRange.Intersect(blockRange);

        struct TMark
        {
            ui64 CommitId = InvalidCommitId;
            ui16 BlobOffset = InvalidBlobOffset;
        };

        struct TVisitor final
            : public IBlocksIndexVisitor
        {
            TBlockRange32 Intersection;
            TPartialBlobId BlobId;

            TVector<TMark> Marks;

            TVisitor(TBlockRange32 intersection, TPartialBlobId blobId)
                : Intersection(intersection)
                , BlobId(blobId)
                , Marks(intersection.Size())
            {
            }

            bool Visit(
                ui32 blockIndex,
                ui64 commitId,
                const TPartialBlobId& blobId,
                ui16 blobOffset) override
            {
                if (blobId == BlobId) {
                    Marks[blockIndex - Intersection.Start] = {
                        commitId,
                        blobOffset,
                    };
                }

                return true;
            }
        } helper(intersection, blobId);

        // just using BlobMeta isn't enough - we need SkipMask to properly
        // iterate blocks belonging to this merged blob
        const bool ready = db.FindMergedBlocks(
            helper,
            blockRange,
            true,   // precharge
            maxBlocksInBlob,
            Max<ui64>());

        if (!ready) {
            return EIndexProcResult::NotReady;
        }

        for (ui32 i = 0; i < intersection.Size(); ++i) {
            const auto res = visit(
                intersection.Start + i,
                helper.Marks[i].CommitId,
                blobId,
                helper.Marks[i].BlobOffset);

            if (!res) {
                return EIndexProcResult::Interrupted;
            }
        }
    }

    return EIndexProcResult::Ok;
}

bool TPartitionDatabase::FindBlocksInBlobsIndex(
    IExtendedBlocksIndexVisitor& visitor,
    const ui32 maxBlocksInBlob,
    const TBlockRange32& blockRange)
{
    using TTable = TPartitionSchema::BlobsIndex;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto blobId = MakePartialBlobId(
            it.GetValue<TTable::CommitId>(),
            it.GetValue<TTable::BlobId>());

        auto blobMeta = it.GetValue<TTable::BlobMeta>();

        auto blockMask = BlockMaskFromString(
            it.GetValueOrDefault<TTable::BlockMask>());

        const auto res = FindBlocksInBlobIndex(
            *this,
            visitor,
            maxBlocksInBlob,
            blobId,
            blobMeta,
            blockMask,
            blockRange);

        switch (res) {
            case EIndexProcResult::Ok: break;
            case EIndexProcResult::NotReady: return false;
            case EIndexProcResult::Interrupted: return true;
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TPartitionDatabase::FindBlocksInBlobsIndex(
    IExtendedBlocksIndexVisitor& visitor,
    const ui32 maxBlocksInBlob,
    const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::BlobsIndex;

    auto it = Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        auto blobMeta = it.GetValue<TTable::BlobMeta>();

        auto blockMask = BlockMaskFromString(
            it.GetValueOrDefault<TTable::BlockMask>());

        const auto res = FindBlocksInBlobIndex(
            *this,
            visitor,
            maxBlocksInBlob,
            blobId,
            blobMeta,
            blockMask,
            TBlockRange32::Max());

        switch (res) {
            case EIndexProcResult::Ok: break;
            case EIndexProcResult::NotReady: return false;
            case EIndexProcResult::Interrupted: return true;
        }
    }

    return true;
}

TPartitionDatabase::EBlobIndexScanProgress TPartitionDatabase::FindBlocksInBlobsIndex(
    IBlobsIndexVisitor& visitor,
    TPartialBlobId startBlobId,
    TPartialBlobId finalBlobId,
    ui64 prechargeRowCount)
{
    using TTable = TPartitionSchema::BlobsIndex;

    auto q = Table<TTable>()
        .GreaterOrEqual(startBlobId.CommitId(), startBlobId.UniqueId())
        .LessOrEqual(finalBlobId.CommitId(), finalBlobId.UniqueId());

    if (!q.Precharge(prechargeRowCount)) {
        return EBlobIndexScanProgress::NotReady;
    }

    auto it = q.Select();

    if (!it.IsReady()) {
        q.Precharge(prechargeRowCount);
        return EBlobIndexScanProgress::NotReady;
    }

    auto result = EBlobIndexScanProgress::Completed;

    while (it.IsValid()) {
        auto commitId = it.GetValue<TTable::CommitId>();
        auto uniqId = it.GetValue<TTable::BlobId>();

        auto blobMeta = it.GetValue<TTable::BlobMeta>();

        auto blockMask =
            it.GetValueOrDefault<TTable::BlockMask>();

        if (!visitor.Visit(commitId, uniqId, blobMeta, blockMask)) {
            break ; // interrupted
        }

        if (!it.Next()) {
            // data was read and we made some progress.
            // however the amount of data is less than we requested.
            result = EBlobIndexScanProgress::Partial;
            break;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteCompactionMap(
    ui32 blockIndex,
    ui32 blobCount,
    ui32 blockCount)
{
    using TTable = TPartitionSchema::CompactionMap;

    Table<TTable>()
        .Key(blockIndex)
        .Update(NIceDb::TUpdate<TTable::BlobCount>(blobCount))
        .Update(NIceDb::TUpdate<TTable::BlockCount>(blockCount));
}

void TPartitionDatabase::DeleteCompactionMap(ui32 blockIndex)
{
    using TTable = TPartitionSchema::CompactionMap;

    Table<TTable>()
        .Key(blockIndex)
        .Delete();
}

bool TPartitionDatabase::ReadCompactionMap(
    TVector<TCompactionCounter>& compactionMap)
{
    using TTable = TPartitionSchema::CompactionMap;

    auto it = Table<TTable>().Range().Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        compactionMap.emplace_back(
            it.GetValue<TTable::BlockIndex>(),
            TRangeStat{
                ProcessCompactionCounter(it.GetValue<TTable::BlobCount>()),
                ProcessCompactionCounter(it.GetValue<TTable::BlockCount>()),
                0,
                0,
                0,
                0,
                false,
                0});

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TPartitionDatabase::ReadCompactionMap(
    TBlockRange32 rangeBlockIndices,
    TVector<TCompactionCounter>& compactionMap)
{
    using TTable = TPartitionSchema::CompactionMap;

    auto it = Table<TTable>().GreaterOrEqual(rangeBlockIndices.Start).Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        const auto blockIndex = it.GetValue<TTable::BlockIndex>();
        if (blockIndex > rangeBlockIndices.End) {
            break;
        }
        compactionMap.emplace_back(
            blockIndex,
            TRangeStat{
                ProcessCompactionCounter(it.GetValue<TTable::BlobCount>()),
                ProcessCompactionCounter(it.GetValue<TTable::BlockCount>()),
                0,
                0,
                0,
                0,
                false,
                0});

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteUsedBlocks(
    const TCompressedBitmap::TSerializedChunk& chunk)
{
    using TTable = TPartitionSchema::UsedBlocks;

    Table<TTable>()
        .Key(chunk.ChunkIdx)
        .Update(NIceDb::TUpdate<TTable::Bitmap>(chunk.Data));
}

bool TPartitionDatabase::ReadUsedBlocks(TCompressedBitmap& usedBlocks)
{
    return ReadUsedBlocksRaw([&usedBlocks](TCompressedBitmap::TSerializedChunk chunk) {
        usedBlocks.Update(chunk);
    });
}

bool TPartitionDatabase::ReadUsedBlocksRaw(std::function<void(TCompressedBitmap::TSerializedChunk)> onChunk)
{
    using TTable = TPartitionSchema::UsedBlocks;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        onChunk({
            it.GetValue<TTable::RangeIndex>(),
            it.GetValue<TTable::Bitmap>()
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteLogicalUsedBlocks(
    const TCompressedBitmap::TSerializedChunk& chunk)
{
    using TTable = TPartitionSchema::LogicalUsedBlocks;

    Table<TTable>()
        .Key(chunk.ChunkIdx)
        .Update(NIceDb::TUpdate<TTable::Bitmap>(chunk.Data));
}

bool TPartitionDatabase::ReadLogicalUsedBlocks(
    TCompressedBitmap& usedBlocks,
    bool& read)
{
    using TTable = TPartitionSchema::LogicalUsedBlocks;

    read = false;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.EndOfSet()) {
        return true;
    }

    while (it.IsValid()) {
        usedBlocks.Update({
            it.GetValue<TTable::RangeIndex>(),
            it.GetValue<TTable::Bitmap>()
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    read = true;
    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteCheckpoint(const TCheckpoint& checkpoint, bool withoutData)
{
    using TTable = TPartitionSchema::Checkpoints;

    Table<TTable>()
        .Key(checkpoint.CheckpointId)
        .Update(
            NIceDb::TUpdate<TTable::CommitId>(checkpoint.CommitId),
            NIceDb::TUpdate<TTable::IdempotenceId>(checkpoint.IdempotenceId),
            NIceDb::TUpdate<TTable::DateCreated>(checkpoint.DateCreated.MicroSeconds()),
            NIceDb::TUpdate<TTable::Stats>(checkpoint.Stats),
            NIceDb::TUpdate<TTable::DataDeleted>(withoutData));
}

void TPartitionDatabase::DeleteCheckpoint(const TString& checkpointId, bool deleteOnlyData)
{
    using TTable = TPartitionSchema::Checkpoints;

    if (deleteOnlyData) {
        Table<TTable>()
            .Key(checkpointId)
            .Update(NIceDb::TUpdate<TTable::DataDeleted>(true));
    } else {
        Table<TTable>()
            .Key(checkpointId)
            .Delete();
    }
}

bool TPartitionDatabase::ReadCheckpoints(
    TVector<TCheckpoint>& checkpoints,
    THashMap<TString, ui64>& checkpointId2CommitId)
{
    using TTable = TPartitionSchema::Checkpoints;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto checkpointId = it.GetValue<TTable::CheckpointId>();
        auto commitId = it.GetValue<TTable::CommitId>();
        bool isDataDeleted = it.GetValue<TTable::DataDeleted>();

        checkpointId2CommitId.emplace(checkpointId, commitId);

        if (!isDataDeleted) {
            checkpoints.emplace_back(
                checkpointId,
                commitId,
                it.GetValue<TTable::IdempotenceId>(),
                TInstant::MicroSeconds(it.GetValue<TTable::DateCreated>()),
                it.GetValue<TTable::Stats>());
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteCleanupQueue(
    const TPartialBlobId& blobId,
    ui64 commitId)
{
    using TTable = TPartitionSchema::CleanupQueue;

    Table<TTable>()
        .Key(commitId, blobId.CommitId(), blobId.UniqueId())
        .Update();
}

void TPartitionDatabase::DeleteCleanupQueue(
    const TPartialBlobId& blobId,
    ui64 commitId)
{
    using TTable = TPartitionSchema::CleanupQueue;

    Table<TTable>()
        .Key(commitId, blobId.CommitId(), blobId.UniqueId())
        .Delete();
}

bool TPartitionDatabase::ReadCleanupQueue(TVector<TCleanupQueueItem>& items)
{
    using TTable = TPartitionSchema::CleanupQueue;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        ui64 commitId = it.GetValue<TTable::DeletionCommitId>();

        auto blobId = MakePartialBlobId(
            it.GetValue<TTable::CommitId>(),
            it.GetValue<TTable::BlobId>());

        items.emplace_back(blobId, commitId);

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteGarbageBlob(const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::GarbageBlobs;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Update();
}

void TPartitionDatabase::DeleteGarbageBlob(const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::GarbageBlobs;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Delete();
}

bool TPartitionDatabase::ReadGarbageBlobs(TVector<TPartialBlobId>& blobIds)
{
    using TTable = TPartitionSchema::GarbageBlobs;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto blobId = MakePartialBlobId(
            it.GetValue<TTable::CommitId>(),
            it.GetValue<TTable::BlobId>());

        blobIds.push_back(blobId);

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteUnconfirmedBlob(
    const TPartialBlobId& blobId,
    const TBlobToConfirm& blob)
{
    using TTable = TPartitionSchema::UnconfirmedBlobs;

    // TODO: persist blob checksums (issue-122)
    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Update(
            NIceDb::TUpdate<TTable::RangeStart>(blob.BlockRange.Start),
            NIceDb::TUpdate<TTable::RangeEnd>(blob.BlockRange.End)
        );
}

void TPartitionDatabase::DeleteUnconfirmedBlob(const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::UnconfirmedBlobs;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Delete();
}

bool TPartitionDatabase::ReadUnconfirmedBlobs(TCommitIdToBlobsToConfirm& blobs)
{
    using TTable = TPartitionSchema::UnconfirmedBlobs;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto commitId = it.GetValue<TTable::CommitId>();
        auto uniqueId = it.GetValue<TTable::BlobId>();
        auto blockRange = TBlockRange32::MakeClosedInterval(
            it.GetValue<TTable::RangeStart>(),
            it.GetValue<TTable::RangeEnd>());

        blobs[commitId].emplace_back(
            uniqueId,
            blockRange,
            TVector<ui32>() /* TODO: checksums */
        );

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
