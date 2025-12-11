#include "mixed_index.h"

#include "alloc.h"

#include <util/generic/algorithm.h>

#include <contrib/libs/sparsehash/src/sparsehash/sparse_hash_map>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBlockRef
{
    ui32 Index;
    TBlockLocation Location;

    bool operator<(const TBlockRef& block) const
    {
        return Index < block.Index;
    }

    bool operator<(const ui32 blockIndex) const
    {
        return Index < blockIndex;
    }
};

constexpr ui32 InvalidBlockIndex = Max();
using TBlockMap = google::sparse_hash_map<
    ui32,
    TBlockLocation,
    SPARSEHASH_HASH<ui32>,
    std::equal_to<ui32>,
    TStlAlloc<std::pair<const ui32, TBlockLocation>>>;

struct TCheckpointData
{
    ui64 CheckpointId;
    // blocks that were present in blob index when the checkpoint was created
    // reside here
    TVector<TBlockRef> Blocks;
    // blocks that belong to this checkpoint but arrived to blob index after the
    // checkpoint was created (e.g. the result of flush) reside here
    TBlockMap BlockMap;

    TCheckpointData()
        : CheckpointId(0)
        , BlockMap(
              0,
              SPARSEHASH_HASH<ui32>(),
              std::equal_to<ui32>(),
              GetAllocatorByTag(EAllocatorTag::MixedIndexBlockMap))
    {}

    bool operator<(ui64 commitId) const
    {
        return CheckpointId < commitId;
    }
};

TBlock Block(ui32 blockIndex, ui64 minCommitId, const TBlockLocation& loc)
{
    return {
        blockIndex,
        minCommitId,
        InvalidCommitId,
        loc.BlobOffset == ZeroBlobOffset   // zeroed
    };
}

bool FillResult(
    ui32 blockIndex,
    ui64 minCommitId,
    const TBlockLocation& loc,
    TBlockAndLocation* result)
{
    result->Location = loc;
    result->Block = Block(blockIndex, minCommitId, loc);
    return loc.BlobOffset != InvalidBlobOffset;
}

bool FillResult(
    ui32 blockIndex,
    const TCheckpointData& cd,
    TVector<TBlockRef>::const_iterator it,
    TBlockAndLocation* result)
{
    result->Location = it->Location;
    result->Block = Block(blockIndex, cd.CheckpointId, it->Location);
    return it->Location.BlobOffset != InvalidBlobOffset;
}

void AddResult(
    ui32 blockIndex,
    TVector<TCheckpointData>::const_reverse_iterator rit,
    const TBlockRange32& range,
    const TBlockLocation& loc,
    const TVector<ui64>& deletionCommits,
    TVector<TBlockAndLocation>& result)
{
    result.push_back(
        {{
             blockIndex,
             rit->CheckpointId,
             deletionCommits[blockIndex - range.Start],
             loc.BlobOffset == ZeroBlobOffset   // zeroed
         },
         loc});
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TMixedIndex::TImpl
{
    TVector<TCheckpointData> CheckpointData;
    TBlockMap Blocks;
    TPartialBlobIdHashSet OverwrittenBlobIds;

    TImpl()
        : Blocks(
              0,
              SPARSEHASH_HASH<ui32>(),
              std::equal_to<ui32>(),
              GetAllocatorByTag(EAllocatorTag::MixedIndexBlockMap))
        , OverwrittenBlobIds(
              GetAllocatorByTag(EAllocatorTag::MixedIndexOverwrittenBlobIds))
    {
        Blocks.set_deleted_key(InvalidBlockIndex);
    }

    void UpdateOverwrittenBlobs(ui32 blockIndex, ui64 commitId)
    {
        TBlockAndLocation oldLocation;
        const auto found = FindBlock(commitId, blockIndex, &oldLocation);
        if (found) {
            Y_ABORT_UNLESS(oldLocation.Location.BlobId);
            OverwrittenBlobIds.insert(oldLocation.Location.BlobId);
        }
    }

    void OverwriteBlock(
        ui32 blockIndex,
        ui64 commitId,
        const TBlockLocation& location,
        TBlockMap& blocks,
        bool replace)
    {
        auto it = blocks.find(blockIndex);
        if (it == blocks.end()) {
            if (CheckpointData.size()) {
                UpdateOverwrittenBlobs(blockIndex, commitId);
            }
            blocks.insert({blockIndex, location});
        } else if (replace) {
            if (it->second.BlobId) {
                OverwrittenBlobIds.insert(it->second.BlobId);
            }
            it->second = location;
        }
    }

    void SetOrUpdateBlock(const TBlockAndLocation& b, bool replace)
    {
        auto rit = LowerBound(
            CheckpointData.begin(),
            CheckpointData.end(),
            b.Block.MinCommitId);

        if (rit == CheckpointData.end()) {
            OverwriteBlock(
                b.Block.BlockIndex,
                b.Block.MinCommitId,
                b.Location,
                Blocks,
                replace);
        } else {
            auto it = LowerBound(
                rit->Blocks.begin(),
                rit->Blocks.end(),
                b.Block.BlockIndex);

            if (it == rit->Blocks.end() || it->Index != b.Block.BlockIndex) {
                Y_ABORT_UNLESS(b.Block.MaxCommitId > rit->CheckpointId);
                // location can either be unset - it means that this block is
                // being added to blob index for the first time (e.g. flush
                // result) or it can be set and needs to be overwritten - e.g.
                // via compaction
                OverwriteBlock(
                    b.Block.BlockIndex,
                    b.Block.MinCommitId,
                    b.Location,
                    rit->BlockMap,
                    replace);
            } else if (replace) {
                if (it->Location.BlobId) {
                    OverwrittenBlobIds.insert(it->Location.BlobId);
                }
                it->Location = b.Location;
            }

            if (b.Block.MaxCommitId != InvalidCommitId) {
                // got a deleted block (e.g. arrived via Flush)
                // need to insert a tombstone if there is no block whose
                // MinCommitId is equivalent to b.Block.MaxCommitId

                SetOrUpdateBlock(
                    {{b.Block.BlockIndex,
                      b.Block.MaxCommitId,
                      InvalidCommitId,
                      false},
                     {{}, InvalidBlobOffset}},
                    false);
            }
        }
    }

    void ClearBlock(ui32 blockIndex)
    {
        OverwriteBlock(
            blockIndex,
            InvalidCommitId,
            {{}, InvalidBlobOffset},
            Blocks,
            true);
    }

    void OnCheckpoint(ui64 checkpointId)
    {
        TCheckpointData snapshot;
        snapshot.CheckpointId = checkpointId;

        for (const auto& x: Blocks) {
            snapshot.Blocks.push_back({x.first, x.second});
        }

        Sort(snapshot.Blocks.begin(), snapshot.Blocks.end());
        CheckpointData.emplace_back(std::move(snapshot));

        Blocks.clear();
    }

    void OnCheckpointDeletion(ui64 checkpointId)
    {
        auto rit = LowerBound(
            CheckpointData.begin(),
            CheckpointData.end(),
            checkpointId);

        if (rit == CheckpointData.end() || rit->CheckpointId != checkpointId) {
            // or verify?
            return;
        }

        auto nextRit = rit + 1;
        if (nextRit == CheckpointData.end()) {
            for (const auto& block: rit->Blocks) {
                if (!Blocks.count(block.Index)) {
                    Blocks[block.Index] = block.Location;
                }
            }

            for (const auto& x: rit->BlockMap) {
                if (!Blocks.count(x.first)) {
                    Blocks[x.first] = x.second;
                }
            }
        } else {
            TVector<TBlockRef> blocks;
            blocks.reserve(rit->Blocks.size() + nextRit->Blocks.size());

            auto o = rit->Blocks.begin();
            auto n = nextRit->Blocks.begin();
            while (n != nextRit->Blocks.end()) {
                while (o != rit->Blocks.end() && o->Index == n->Index) {
                    ++o;
                }

                if (o == rit->Blocks.end()) {
                    break;
                }

                if (o->Index < n->Index) {
                    if (!nextRit->BlockMap.count(o->Index)) {
                        blocks.push_back(*o);
                    }
                    ++o;
                } else {
                    blocks.push_back(*n);
                    ++n;
                }
            }

            while (o != rit->Blocks.end()) {
                if (!nextRit->BlockMap.count(o->Index)) {
                    blocks.push_back(*o);
                }
                ++o;
            }

            while (n != nextRit->Blocks.end()) {
                blocks.push_back(*n);
                ++n;
            }

            for (const auto& x: rit->BlockMap) {
                if (nextRit->BlockMap.count(x.first)) {
                    continue;
                }

                auto it = LowerBound(
                    nextRit->Blocks.begin(),
                    nextRit->Blocks.end(),
                    x.first);

                if (it != nextRit->Blocks.end() && it->Index == x.first) {
                    continue;
                }

                nextRit->BlockMap[x.first] = x.second;
            }

            nextRit->Blocks = std::move(blocks);
        }

        CheckpointData.erase(rit);
    }

    bool FindBlock(ui32 blockIndex, TBlockAndLocation* result) const
    {
        auto it = Blocks.find(blockIndex);

        if (it != Blocks.end()) {
            result->Location = it->second;
            result->Block = {
                blockIndex,
                InvalidCommitId,
                InvalidCommitId,
                it->second.BlobOffset == ZeroBlobOffset   // zeroed
            };
            return it->second.BlobOffset != InvalidBlobOffset;
        }

        if (CheckpointData.empty()) {
            return false;
        }

        return FindBlock(CheckpointData.end() - 1, blockIndex, result);
    }

    bool
    FindBlock(ui64 commitId, ui32 blockIndex, TBlockAndLocation* result) const
    {
        auto rit =
            LowerBound(CheckpointData.begin(), CheckpointData.end(), commitId);

        if (rit == CheckpointData.end()) {
            auto it = Blocks.find(blockIndex);

            if (it != Blocks.end()) {
                return FillResult(
                    blockIndex,
                    InvalidCommitId,
                    it->second,
                    result);
            }

            if (CheckpointData.empty()) {
                return false;
            }

            --rit;
        }

        return FindBlock(rit, blockIndex, result);
    }

    struct TFindResult
    {
        bool Exists = false;
        bool Found = false;
    };

    TFindResult FindBlock(
        const TCheckpointData& cd,
        ui32 blockIndex,
        TBlockAndLocation* result) const
    {
        auto it = LowerBound(cd.Blocks.begin(), cd.Blocks.end(), blockIndex);

        if (it != cd.Blocks.end() && blockIndex == it->Index) {
            return {FillResult(blockIndex, cd, it, result), true};
        }

        auto bmit = cd.BlockMap.find(blockIndex);
        if (bmit != cd.BlockMap.end()) {
            return {
                FillResult(blockIndex, cd.CheckpointId, bmit->second, result),
                true};
        }

        return {false, false};
    }

    bool FindBlock(
        TVector<TCheckpointData>::const_iterator rit,
        ui32 blockIndex,
        TBlockAndLocation* result) const
    {
        while (true) {
            auto f = FindBlock(*rit, blockIndex, result);
            if (f.Found) {
                return f.Exists;
            }

            if (rit == CheckpointData.begin()) {
                break;
            }

            --rit;
        }

        return false;
    }

    TVector<TBlockAndLocation> FindAllBlocks(const TBlockRange32& range) const
    {
        TVector<TBlockAndLocation> result;
        TVector<ui64> deletionCommits(range.Size(), InvalidCommitId);

        for (ui32 i = range.Start; i <= range.End; ++i) {
            auto it = Blocks.find(i);
            if (it != Blocks.end()) {
                if (it->second.BlobOffset != InvalidBlobOffset) {
                    result.push_back(
                        {Block(i, InvalidCommitId, it->second), it->second});
                }

                if (CheckpointData) {
                    deletionCommits[i - range.Start] =
                        CheckpointData.back().CheckpointId + 1;
                }
            }
        }

        for (auto rit = CheckpointData.rbegin(); rit != CheckpointData.rend();
             ++rit)
        {
            auto it =
                LowerBound(rit->Blocks.begin(), rit->Blocks.end(), range.Start);

            while (it != rit->Blocks.end() && it->Index <= range.End) {
                if (it->Location.BlobOffset != InvalidBlobOffset) {
                    AddResult(
                        it->Index,
                        rit,
                        range,
                        it->Location,
                        deletionCommits,
                        result);
                }

                deletionCommits[it->Index - range.Start] = rit->CheckpointId;

                ++it;
            }

            for (const auto& x: rit->BlockMap) {
                if (range.Contains(x.first)) {
                    if (x.second.BlobOffset != InvalidBlobOffset) {
                        AddResult(
                            x.first,
                            rit,
                            range,
                            x.second,
                            deletionCommits,
                            result);
                    }

                    deletionCommits[x.first - range.Start] = rit->CheckpointId;
                }
            }
        }

        return result;
    }

    bool IsEmpty() const
    {
        return Blocks.empty() && CheckpointData.empty();
    }

    TPartialBlobIdHashSet ExtractOverwrittenBlobIds()
    {
        return std::move(OverwrittenBlobIds);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMixedIndex::TMixedIndex()
    : Impl(new TImpl())
{}

TMixedIndex::~TMixedIndex()
{}

void TMixedIndex::SetOrUpdateBlock(const TBlockAndLocation& b)
{
    Impl->SetOrUpdateBlock(b, true);
}

void TMixedIndex::SetOrUpdateBlock(
    ui32 blockIndex,
    const TBlockLocation& location)
{
    Impl->SetOrUpdateBlock(
        {Block(blockIndex, InvalidCommitId, location), location},
        true);
}

void TMixedIndex::ClearBlock(ui32 blockIndex)
{
    Impl->ClearBlock(blockIndex);
}

void TMixedIndex::OnCheckpoint(ui64 checkpointId)
{
    Impl->OnCheckpoint(checkpointId);
}

void TMixedIndex::OnCheckpointDeletion(ui64 checkpointId)
{
    Impl->OnCheckpointDeletion(checkpointId);
}

bool TMixedIndex::FindBlock(ui32 blockIndex, TBlockAndLocation* result) const
{
    return Impl->FindBlock(blockIndex, result);
}

bool TMixedIndex::FindBlock(
    ui64 commitId,
    ui32 blockIndex,
    TBlockAndLocation* result) const
{
    return Impl->FindBlock(commitId, blockIndex, result);
}

TVector<TBlockAndLocation> TMixedIndex::FindAllBlocks(
    const TBlockRange32& range) const
{
    return Impl->FindAllBlocks(range);
}

bool TMixedIndex::IsEmpty() const
{
    return Impl->IsEmpty();
}

TPartialBlobIdHashSet TMixedIndex::ExtractOverwrittenBlobIds()
{
    return Impl->ExtractOverwrittenBlobIds();
}

////////////////////////////////////////////////////////////////////////////////

TMixedIndexBuilder::TMixedIndexBuilder(const TBlockRange32& range)
    : Range(range)
    , Blocks(range.Size())
    , OverwrittenBlobIds(
          GetAllocatorByTag(EAllocatorTag::MixedIndexOverwrittenBlobIds))
{}

void TMixedIndexBuilder::AddBlock(const TBlockAndLocation& b)
{
    if (b.Block.MinCommitId < b.Block.MaxCommitId) {
        Y_ABORT_UNLESS(Range.Contains(b.Block.BlockIndex));
        Blocks[b.Block.BlockIndex - Range.Start].push_back(b);
    }
}

void TMixedIndexBuilder::AddOverwrittenBlob(const TPartialBlobId& blobId)
{
    OverwrittenBlobIds.insert(blobId);
}

std::unique_ptr<TMixedIndex> TMixedIndexBuilder::Build(
    const TVector<ui64>& checkpointIds)
{
    auto index = std::make_unique<TMixedIndex>();
    auto& rodata = index->Impl->CheckpointData;
    auto& blocks = index->Impl->Blocks;

    rodata.resize(checkpointIds.size(), TCheckpointData());
    for (ui32 i = 0; i < rodata.size(); ++i) {
        rodata[i].CheckpointId = checkpointIds[i];
    }

    for (auto& v: Blocks) {
        Sort(
            v.begin(),
            v.end(),
            [](const TBlockAndLocation& l, const TBlockAndLocation& r)
            { return l.Block.MinCommitId < r.Block.MinCommitId; });

        auto checkpoint = checkpointIds.begin();
        for (const auto& b: v) {
            while (checkpoint != checkpointIds.end() &&
                   *checkpoint < b.Block.MinCommitId)
            {
                ++checkpoint;
            }

            auto creationCheckpoint = checkpoint;

            while (checkpoint != checkpointIds.end() &&
                   *checkpoint < b.Block.MaxCommitId)
            {
                ++checkpoint;
            }

            auto deletionCheckpoint = checkpoint;

            if (creationCheckpoint != checkpointIds.end()) {
                if (creationCheckpoint != deletionCheckpoint) {
                    const auto j = std::distance(
                        checkpointIds.begin(),
                        creationCheckpoint);
                    if (rodata[j].Blocks &&
                        rodata[j].Blocks.back().Index == b.Block.BlockIndex)
                    {
                        Y_ABORT_UNLESS(
                            rodata[j].Blocks.back().Location.BlobOffset ==
                            InvalidBlobOffset);
                        rodata[j].Blocks.back().Location = b.Location;
                    } else {
                        rodata[j].Blocks.push_back(
                            {b.Block.BlockIndex, b.Location});
                    }

                    if (deletionCheckpoint != checkpointIds.end()) {
                        const auto k = std::distance(
                            checkpointIds.begin(),
                            deletionCheckpoint);
                        rodata[k].Blocks.push_back(
                            {b.Block.BlockIndex, {{}, InvalidBlobOffset}});
                    } else if (b.Block.MaxCommitId != InvalidCommitId) {
                        blocks[b.Block.BlockIndex] = {{}, InvalidBlobOffset};
                    }
                }
            } else if (b.Block.MaxCommitId == InvalidCommitId) {
                blocks[b.Block.BlockIndex] = b.Location;
            } else {
                blocks[b.Block.BlockIndex] = {{}, InvalidBlobOffset};
            }
        }
    }

    index->Impl->OverwrittenBlobIds = std::move(OverwrittenBlobIds);

    return index;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
