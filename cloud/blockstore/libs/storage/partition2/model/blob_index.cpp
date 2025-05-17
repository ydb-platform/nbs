#include "blob_index.h"

#include "disjoint_range_map.h"
#include "lfu_list.h"
#include "mixed_index.h"

#include <cloud/storage/core/libs/common/alloc.h>

#include <library/cpp/containers/intrusive_rb_tree/rb_tree.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/algorithm.h>
#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

void ApplyTimeDiscount(TDuration d, double& x)
{
    // the discount is 0.9 for 10s, 1/e for 100s and 0.0025 for 10min
    x *= pow(double(0.99999999), d.MicroSeconds());
}

////////////////////////////////////////////////////////////////////////////////

bool CheckSortedWithGaps(const TVector<TBlock>& blocks)
{
    TBlock prevBlock;

    for (const auto& block: blocks) {
        if (block.MinCommitId == block.MaxCommitId) {
            continue;
        }

        if (prevBlock.MinCommitId != prevBlock.MaxCommitId
                && block < prevBlock)
        {
            return false;
        }

        prevBlock = block;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

struct TBlockListNode;
struct TGarbageNode;

////////////////////////////////////////////////////////////////////////////////

struct TBlobNode
    : public TBlob
{
    TBlockListNode* BlockList = nullptr;
    TGarbageNode* Garbage = nullptr;

    using TBlob::TBlob;
};

const TPartialBlobId& GetBlobId(const TPartialBlobId& blobId)
{
    return blobId;
}

const TPartialBlobId& GetBlobId(const TBlobNode& node)
{
    return node.BlobId;
}

////////////////////////////////////////////////////////////////////////////////

class TBlobMap
{
    struct THash
    {
        template <typename T>
        size_t operator ()(const T& l) const
        {
            return GetBlobId(l).GetHash();
        }
    };

    struct TEqual
    {
        template <typename T1, typename T2>
        bool operator ()(const T1& l, const T2& r) const
        {
            return GetBlobId(l) == GetBlobId(r);
        }
    };

    using TData = THashSet<TBlobNode, THash, TEqual, TStlAllocator>;

private:
    TData Data;
    size_t BlockCount = 0;
    size_t CheckpointBlockCount = 0;

public:
    TBlobMap()
        : Data(GetAllocatorByTag(EAllocatorTag::BlobIndexBlobMap))
    {}

    size_t GetCount() const
    {
        return Data.size();
    }

    size_t GetBlockCount() const
    {
        return BlockCount;
    }

    size_t GetCheckpointBlockCount() const
    {
        return CheckpointBlockCount;
    }

    TBlobNode* Add(
        const TPartialBlobId& blobId,
        TBlockRanges blockRanges,
        ui16 blockCount,
        ui16 checkpointBlockCount)
    {
        TData::iterator it;
        bool inserted;

        std::tie(it, inserted) = Data.emplace(
            blobId,
            std::move(blockRanges),
            blockCount,
            checkpointBlockCount
        );

        if (inserted) {
            BlockCount += blockCount;
            return const_cast<TBlobNode*>(&*it);
        }

        return nullptr;
    }

    TBlobNode* Find(const TPartialBlobId& blobId) const
    {
        auto it = Data.find(blobId);
        return it != Data.end() ? const_cast<TBlobNode*>(&*it) : nullptr;
    }

    bool Remove(TBlobNode* blob)
    {
        auto it = Data.find(blob->BlobId);
        if (it != Data.end()) {
            BlockCount -= blob->BlockCount;
            Data.erase(it);
            return true;
        }

        return false;
    }

    using iterator = TData::iterator;
    iterator begin() { return Data.begin(); }
    iterator end() { return Data.end(); }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockRangeNode
{
    TBlobNode* Blob;
    ui32 BlockRangeIdx;

    TBlockRangeNode(TBlobNode* blob, ui32 blockRangeIdx)
        : Blob(blob)
        , BlockRangeIdx(blockRangeIdx)
    {}

    bool operator==(const TBlockRangeNode& rhs) const
    {
        return Blob == rhs.Blob && BlockRangeIdx == rhs.BlockRangeIdx;
    }

    const TBlockRange32& Range() const
    {
        return Blob->BlockRanges[BlockRangeIdx];
    }
};

struct TRangeNode
{
    TStackVec<TBlockRangeNode, 8> Blobs;
};

////////////////////////////////////////////////////////////////////////////////

class TRangeMap
{
    using TData =
        THashMap<ui32, TRangeNode, THash<ui32>, TEqualTo<ui32>, TStlAllocator>;

private:
    const ui32 MaxBlocksInBlob;
    TData Data;

public:
    TRangeMap(ui32 maxBlocksInBlob)
        : MaxBlocksInBlob(maxBlocksInBlob)
        , Data(GetAllocatorByTag(EAllocatorTag::BlobIndexRangeMap))
    {}

    size_t GetCount() const
    {
        return Data.size();
    }

    ui32 Add(TBlockRangeNode node)
    {
        const auto c = Data.size();

        auto b = node.Range().Start / MaxBlocksInBlob;
        auto e = node.Range().End / MaxBlocksInBlob;
        for (ui32 i = b; i <= e; ++i) {
            Data[i].Blobs.push_back(node);
        }

        return Data.size() - c;
    }

    ui32 Remove(TBlockRangeNode node)
    {
        const auto c = Data.size();

        auto b = node.Range().Start / MaxBlocksInBlob;
        auto e = node.Range().End / MaxBlocksInBlob;
        for (ui32 i = b; i <= e; ++i) {
            auto& bucket = Data[i].Blobs;
            auto it = Find(bucket, node);

            if (it != bucket.end()) {
                ui32 idx = std::distance(bucket.begin(), it);
                if (idx != bucket.size() - 1) {
                    DoSwap(bucket[idx], bucket.back());
                }
                bucket.pop_back();

                if (bucket.empty()) {
                    Data.erase(i);
                }
            }
        }

        return c - Data.size();
    }

    template <typename T>
    void Visit(const TBlockRange32& blockRange, T&& visitor) const
    {
        auto b = blockRange.Start / MaxBlocksInBlob;
        auto e = blockRange.End / MaxBlocksInBlob;

        for (ui32 i = b; i <= e; ++i) {
            if (auto it = Data.find(i); it != Data.end()) {
                for (const auto& node: it->second.Blobs) {
                    if (!blockRange.Overlaps(node.Range())) {
                        continue;
                    }

                    if (!visitor(node.Blob)) {
                        return;
                    }
                }
            }
        }
    }

    template <typename T>
    void Visit(T&& visitor) const
    {
        for (const auto& x: Data) {
            for (const auto& node: x.second.Blobs) {
                if (!visitor(node.Blob)) {
                    return;
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockListNode
    : TLFUListItemBase<TBlockListNode>
    , TBlockList
{
    TBlobNode* const Blob;
    ui32 BlockCount;

    TBlockListNode(TBlobNode* blob, ui32 blockCount)
        : Blob(blob)
        , BlockCount(blockCount)
    {}

    ui32 Weight() const
    {
        return BlockCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

using TBlockLists = TLFUList<TBlockListNode>;

////////////////////////////////////////////////////////////////////////////////

struct TGarbageNodeCompare
{
    template <typename T1, typename T2>
    static bool Compare(const T1& l, const T2& r)
    {
        return GetBlockCount(l) > GetBlockCount(r);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TGarbageNode
    : public TRbTreeItem<TGarbageNode, TGarbageNodeCompare>
{
    TBlobNode* const Blob;
    ui16 BlockCount;

    TGarbageNode(TBlobNode* blob, ui16 blockCount)
        : Blob(blob)
        , BlockCount(blockCount)
    {}
};

size_t GetBlockCount(const TGarbageNode& node)
{
    return node.BlockCount;
}

////////////////////////////////////////////////////////////////////////////////

class TGarbageMap
{
    using TData = TRbTree<TGarbageNode, TGarbageNodeCompare>;

private:
    TData Data;
    size_t BlockCount = 0;

public:
    size_t GetBlockCount() const
    {
        return BlockCount;
    }

    void Add(TBlobNode* blob)
    {
        Data.Insert(blob->Garbage);
        BlockCount += blob->Garbage->BlockCount;
    }

    void Remove(TBlobNode* blob)
    {
        Data.Erase(blob->Garbage);
        BlockCount -= blob->Garbage->BlockCount;
    }

    TVector<TBlobCounter> GetTop(size_t maxBlobs, size_t maxBlocks) const
    {
        TVector<TBlobCounter> result;

        size_t blobs = 0;
        size_t blocks = 0;

        for (auto it = Data.Begin(); it != Data.End(); ++it) {
            // count only live blocks
            blocks += it->Blob->BlockCount - it->BlockCount;
            ++blobs;

            if (!result.empty() && (blobs > maxBlobs || blocks > maxBlocks)) {
                break;
            }

            result.emplace_back(it->Blob->BlobId, it->BlockCount);
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDeletedRangeMap
{
private:
    struct TChunk
    {
        const ui64 StartsWithCommitId;
        TVector<TBlobUpdate> BlobUpdates;
        TDisjointRangeMap Ranges;
        ui32 DeletedBlockCount = 0;

        TChunk(ui64 startsWithCommitId, EOptimizationMode mode)
            : StartsWithCommitId(startsWithCommitId)
            , Ranges(mode)
        {}
    };

    EOptimizationMode Mode;
    TDeque<TChunk> Chunks;

public:
    TDeletedRangeMap(EOptimizationMode mode)
        : Mode(mode)
    {
        Chunks.emplace_back(0, Mode);
    }

    void AddUpdate(TBlobUpdate blobUpdate)
    {
        auto chunk = Chunks.begin();
        auto nextChunk = std::next(chunk);

        while (nextChunk != Chunks.end()
            && nextChunk->StartsWithCommitId <= blobUpdate.CommitId)
        {
            ++chunk;
            ++nextChunk;
        }

        Y_ABORT_UNLESS(chunk->StartsWithCommitId <= blobUpdate.CommitId);
        chunk->BlobUpdates.push_back(blobUpdate);
        chunk->Ranges.Mark(blobUpdate.BlockRange, blobUpdate.CommitId);
        chunk->DeletedBlockCount += blobUpdate.BlockRange.Size();
    }

    void GetDeletedBlocks(
        const TBlockRange32& blockRange,
        ui64 commitId,
        TVector<TDeletedBlock>* blocks) const
    {
        const size_t initialSize = blocks->size();

        const auto end = UpperBoundBy(
            Chunks.begin(),
            Chunks.end(),
            commitId,
            [] (const TChunk& chunk) {
                return chunk.StartsWithCommitId;
            }
        );

        for (auto chunk = Chunks.begin(); chunk != end; ++chunk) {
            chunk->Ranges.FindMarks(blockRange, commitId, blocks);
        }

        if (std::distance(Chunks.begin(), end) > 1) {
            Sort(blocks->begin() + initialSize, blocks->end());
        }
    }

    ui32 ApplyUpdates(
        const TBlockRange32& blockRange,
        TVector<TBlock>& blocks) const
    {
        Y_DEBUG_ABORT_UNLESS(CheckSortedWithGaps(blocks));

        ui32 updateCount = 0;

        TVector<TDeletedBlock> deletions;

        for (const auto& chunk: Chunks) {
            deletions.clear();
            chunk.Ranges.FindMarks(blockRange, InvalidCommitId, &deletions);

            auto block = blocks.begin();
            auto deletion = deletions.cbegin();

            while (block != blocks.end() && deletion != deletions.end()) {
                if (block->MinCommitId == block->MaxCommitId) {
                    ++block;
                    continue;
                }

                if (block->BlockIndex < deletion->BlockIndex) {
                    ++block;
                    continue;
                }

                if (block->BlockIndex > deletion->BlockIndex) {
                    ++deletion;
                    continue;
                }

                if (auto next = std::next(deletion); next != deletions.end()) {
                    Y_ABORT_UNLESS(deletion->BlockIndex != next->BlockIndex);
                }

                const auto blockIndex = block->BlockIndex;

                while (block != blocks.end() && block->BlockIndex == blockIndex) {
                    if (deletion->CommitId > block->MinCommitId &&
                        deletion->CommitId < block->MaxCommitId)
                    {
                        block->MaxCommitId = deletion->CommitId;
                        ++updateCount;
                    }

                    ++block;
                }
            }
        }

        return updateCount;
    }

    void Barrier(ui64 commitId)
    {
        Y_ABORT_UNLESS(Chunks.back().StartsWithCommitId < commitId);
        Chunks.emplace_back(commitId, Mode);
    }

    size_t DeletionCount() const
    {
        size_t s = 0;

        for (const auto& chunk: Chunks) {
            s += chunk.DeletedBlockCount;
        }

        return s;
    }

    const auto& GetChunks() const
    {
        return Chunks;
    }

    const auto& TopChunk() const
    {
        return Chunks.front();
    }

    TVector<TBlobUpdate> PopChunk()
    {
        TVector<TBlobUpdate> blobUpdates(std::move(Chunks.front().BlobUpdates));

        if (Chunks.size() == 1) {
            Chunks.front().Ranges.Clear();
            Chunks.front().BlobUpdates.clear();
        } else {
            Chunks.pop_front();
        }

        return blobUpdates;
    }

    size_t ChunkCount() const
    {
        return Chunks.size();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TZone
{
    std::unique_ptr<TBlobMap> Blobs;
    std::unique_ptr<TRangeMap> RangeMap;
    std::unique_ptr<TMixedIndex> MixedIndex;
    std::unique_ptr<TGarbageMap> Garbage;
    std::unique_ptr<TDeletedRangeMap> DeletedRanges;

    mutable double UsageScore = 0;
    TInstant UsageScoreUpdateTimestamp;

    ~TZone()
    {
        if (Blobs) {
            for (auto& blob: *Blobs) {
                DeleteImpl(
                    GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList),
                    blob.BlockList);
                DeleteImpl(
                    GetAllocatorByTag(EAllocatorTag::BlobIndexGarbageMap),
                    blob.Garbage);
            }
        }
    }

    void Init(
        ui32 maxBlocksInBlob,
        EOptimizationMode mode)
    {
        RangeMap = std::make_unique<TRangeMap>(maxBlocksInBlob);
        Blobs = std::make_unique<TBlobMap>();
        Garbage = std::make_unique<TGarbageMap>();
        DeletedRanges = std::make_unique<TDeletedRangeMap>(mode);
    }

    void Reset()
    {
        RangeMap.reset();
        MixedIndex.reset();
        Blobs.reset();
        Garbage.reset();
        DeletedRanges.reset();
        UsageScore = 0;
        UsageScoreUpdateTimestamp = {};
    }

    bool IsInitialized() const
    {
        if (RangeMap || MixedIndex) {
            Y_ABORT_UNLESS(Blobs);
            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCompareByGarbage
{
    bool operator()(const TZone* l, const TZone* r) const
    {
        return std::make_pair(l->Garbage->GetBlockCount(), l) >
            std::make_pair(r->Garbage->GetBlockCount(), r);
    }
};

struct TCompareByDeletions
{
    bool operator()(const TZone* l, const TZone* r) const
    {
        return std::make_pair(l->DeletedRanges->DeletionCount(), l) >
            std::make_pair(r->DeletedRanges->DeletionCount(), r);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TBlobIndex::TImpl
{
private:
    TVector<TZone> Zones;
    TZone* GlobalZone;

    const ui32 ZoneBlockCount;
    const ui32 BlockListCacheSize;
    const ui32 MaxBlocksInBlob;
    const EOptimizationMode OptimizationMode;
    bool PruneBlockListCache = true;
    ui32 MixedZoneCount = 0;
    bool GlobalDataInitializationFinished = false;
    mutable TBlockLists BlockLists;

    TSet<TZone*, TCompareByGarbage> ZonesByGarbage;
    TSet<TZone*, TCompareByDeletions> ZonesByDeletions;

    TZone* ZoneWithPendingDeletionCleanup = nullptr;

    ui32 ZoneBlobCount = 0;
    ui32 TotalRangeCount = 0;
    ui32 TotalBlockCount = 0;
    ui32 TotalGarbageBlockCount = 0;

    mutable double TotalUsageScore = 0;
    TInstant TotalUsageScoreUpdateTimestamp;

public:
    TImpl(
            ui32 zoneCount,
            ui32 zoneBlockCount,
            ui32 blockListCacheSize,
            ui32 maxBlocksInBlob,
            EOptimizationMode optimizationMode)
        : Zones(zoneCount + 1)  // + 1 global
        , GlobalZone(&Zones.back())
        , ZoneBlockCount(zoneBlockCount)
        , BlockListCacheSize(blockListCacheSize)
        , MaxBlocksInBlob(maxBlocksInBlob)
        , OptimizationMode(optimizationMode)
        , BlockLists(GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList))
    {
        GlobalZone->Init(maxBlocksInBlob, optimizationMode);
        ZonesByGarbage.insert(GlobalZone);
        ZonesByDeletions.insert(GlobalZone);
    }

    //
    // Blobs
    //

    size_t GetGlobalBlobCount() const;
    size_t GetZoneBlobCount() const;
    size_t GetRangeCount() const;
    size_t GetBlockCount() const;

    TAddedBlobInfo AddBlob(
        const TPartialBlobId& blobId,
        const TBlockRanges& blockRanges,
        ui16 blockCount,
        ui16 checkpointBlockCount);

    bool RemoveBlob(ui32 zoneHint, const TPartialBlobId& blobId);

    TMutableFoundBlobInfo AccessBlob(ui32 zoneHint, const TPartialBlobId& blobId);
    TFoundBlobInfo FindBlobInfo(ui32 zoneHint, const TPartialBlobId& blobId) const;

    TVector<const TBlob*> FindBlobs(const TBlockRange32& blockRange) const;

    //
    // MixedIndex
    //

    void WriteOrUpdateMixedBlock(const TBlockAndLocation& b);

    TVector<TBlockAndLocation> FindAllMixedBlocks(
        const TBlockRange32& blockRange) const;
    TVector<TBlockAndLocation> FindMixedBlocks(
        const TBlockRange32& blockRange,
        ui64 commitId) const;

    //
    // BlockLists
    //

    size_t GetBlockListCount() const;
    size_t GetBlockListsWeight() const;

    bool AddBlockList(
        ui32 zoneHint,
        const TPartialBlobId& blobId,
        const TBlockList& blocks,
        ui16 blockCount);
    void SetPruneBlockListCache(bool prune);
    void PruneStep();

    const TBlockList* FindBlockList(
        ui32 zoneHint,
        const TPartialBlobId& blobId) const;

    //
    // GarbageBlocks
    //

    size_t GetGarbageBlockCount() const;

    bool AddGarbage(ui32 zoneHint, const TPartialBlobId& blobId, ui16 blockCount);

    size_t FindGarbage(ui32 zoneHint, const TPartialBlobId& blobId) const;

    TGarbageInfo GetTopGarbage(size_t maxBlobs, size_t maxBlocks) const;

    //
    // DeletedBlocks
    //

    ui32 MarkBlocksDeleted(TBlobUpdate blobUpdate);

    ui32 ApplyUpdates(
        const TBlockRange32& blockRange,
        TVector<TBlock>& blocks) const;

    TVector<TDeletedBlock> FindDeletedBlocks(
        const TBlockRange32& blockRange,
        ui64 maxCommitId = InvalidCommitId) const;

    size_t GetPendingUpdates() const;
    ui32 SelectZoneToCleanup();
    void SeparateChunkForCleanup(ui64 commitId);
    TVector<TPartialBlobId> ExtractDirtyBlobs();
    TDeletionsInfo CleanupDirtyRanges();
    void OnCheckpoint(ui64 checkpointId);
    void OnCheckpoint(ui32 z, ui64 checkpointId);
    void OnCheckpointDeletion(ui64 checkpointId);

    //
    // Zones
    //

    ui32 GetZoneCount() const;
    ui32 GetMixedZoneCount() const;

    ui32 ToZone(ui32 blockIndex) const;
    std::pair<ui32, ui32> ToZoneRange(const TBlockRange32& blockRange) const;
    TBlockRange32 ToBlockRange(ui32 z) const;
    bool IsGlobalZone(ui32 z) const;

    void FinishGlobalDataInitialization();
    void InitializeZone(ui32 z);
    void ResetZone(ui32 z);
    bool IsZoneInitialized(ui32 z) const;
    bool IsMixedIndex(ui32 z) const;

    TVector<const TBlob*> FindZoneBlobs(ui32 z) const;

    // Applies time-related discount and fetches zone usage score (discounted
    // request count)
    double UpdateAndGetZoneUsageScore(ui32 z, TInstant now);
    // Applies time-related discount and fetches total usage score (discounted
    // request count)
    double UpdateAndGetTotalUsageScore(TInstant now);
    // Converts range map + block lists to mixed index for this zone
    // All block lists for this zone should be added before this call
    void ConvertToMixedIndex(ui32 z, const TVector<ui64>& checkpointIds);
    // Drops mixed index for this zone
    void ConvertToRangeMap(ui32 z);

private:
    TZone* GetTopZoneByDeletionCount();
    const TZone* GetTopZoneByDeletionCount() const;

    void ExtractBlobsMixed(
        const TBlockRange32& blockRange,
        TPartialBlobIdHashSet& blobIds);

    void FindBlobs(
        const TBlockRange32& blockRange,
        TPartialBlobIdHashSet& blobIds) const;

    TBlobNode* FindBlob(
        ui32 zoneHint,
        const TPartialBlobId& blobId);

    const TBlobNode* FindBlob(
        ui32 zoneHint,
        const TPartialBlobId& blobId) const;

    std::pair<TBlobNode*, TZone*> FindBlobAndZone(
        ui32 zoneHint,
        const TPartialBlobId& blobId);

    std::pair<const TBlobNode*, const TZone*> FindBlobAndZone(
        ui32 zoneHint,
        const TPartialBlobId& blobId) const;

    void PruneStep(size_t limit);

    ui32 ToZoneId(const TZone* zone) const;

    TZone& SelectZone(const TBlockRanges& blockRanges);
};

////////////////////////////////////////////////////////////////////////////////

size_t TBlobIndex::TImpl::GetGlobalBlobCount() const
{
    return GlobalZone->Blobs->GetCount();
}

size_t TBlobIndex::TImpl::GetZoneBlobCount() const
{
    return ZoneBlobCount;
}

size_t TBlobIndex::TImpl::GetRangeCount() const
{
    return TotalRangeCount;
}

size_t TBlobIndex::TImpl::GetBlockCount() const
{
    return TotalBlockCount;
}

TAddedBlobInfo TBlobIndex::TImpl::AddBlob(
    const TPartialBlobId& blobId,
    const TBlockRanges& blockRanges,
    ui16 blockCount,
    ui16 checkpointBlockCount)
{
    TZone& zone = SelectZone(blockRanges);

    // XXX(@qkrorlqr): do it only in debug mode?
    if (&zone != GlobalZone && GlobalZone->Blobs->Find(blobId)) {
        return { false, InvalidZoneId };
    }

    auto* blob = zone.Blobs->Add(
        blobId,
        blockRanges,
        blockCount,
        checkpointBlockCount);

    if (!blob) {
        return { false, InvalidZoneId };
    }

    if (zone.RangeMap) {
        for (ui32 i = 0; i < blockRanges.size(); ++i) {
            TotalRangeCount += zone.RangeMap->Add({blob, i});
        }
    }

    TotalBlockCount += blockCount;
    ZoneBlobCount += (&zone != GlobalZone);

    return { true, ToZoneId(&zone) };
}

bool TBlobIndex::TImpl::RemoveBlob(ui32 zoneHint, const TPartialBlobId& blobId)
{
    auto [blob, zone] = FindBlobAndZone(zoneHint, blobId);
    if (!blob) {
        return false;
    }

    if (blob->BlockList) {
        BlockLists.Remove(blob->BlockList);
        DeleteImpl(
            GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList),
            blob->BlockList);
    }

    if (blob->Garbage) {
        TotalGarbageBlockCount -= blob->Garbage->BlockCount;

        ZonesByGarbage.erase(zone);
        zone->Garbage->Remove(blob);
        DeleteImpl(
            GetAllocatorByTag(EAllocatorTag::BlobIndexGarbageMap),
            blob->Garbage);
        ZonesByGarbage.insert(zone);
    }

    if (zone->RangeMap) {
        for (ui32 i = 0; i < blob->BlockRanges.size(); ++i) {
            TotalRangeCount -= zone->RangeMap->Remove({blob, i});
        }
    }

    TotalBlockCount -= blob->BlockCount;
    ZoneBlobCount -= (zone != GlobalZone);

    zone->Blobs->Remove(blob);

    return true;
}

TFoundBlobInfo TBlobIndex::TImpl::FindBlobInfo(
    ui32 zoneHint,
    const TPartialBlobId& blobId) const
{
    auto [blob, zone] = FindBlobAndZone(zoneHint, blobId);
    return { blob, ToZoneId(zone) };
}

std::pair<TBlobNode*, TZone*> TBlobIndex::TImpl::FindBlobAndZone(
    ui32 zoneHint,
    const TPartialBlobId& blobId)
{
    auto [blob, zone] = std::as_const(*this).FindBlobAndZone(zoneHint, blobId);
    return { const_cast<TBlobNode*>(blob), const_cast<TZone*>(zone) };
}


std::pair<const TBlobNode*, const TZone*> TBlobIndex::TImpl::FindBlobAndZone(
    ui32 zoneHint,
    const TPartialBlobId& blobId) const
{
    if (zoneHint < Zones.size()) {
        const auto& suggestedZone = Zones[zoneHint];
        if (suggestedZone.IsInitialized()) {
            if (TBlobNode* blob = suggestedZone.Blobs->Find(blobId)) {
                return { blob, &suggestedZone };
            }
        }
    }

    if (TBlobNode* blob = GlobalZone->Blobs->Find(blobId)) {
        return { blob, GlobalZone };
    }

    for (const auto& zone: Zones) {
        if (zone.IsInitialized()) {
            if (TBlobNode* blob = zone.Blobs->Find(blobId)) {
                return { blob, &zone };
            }
        }
    }

    return { nullptr, nullptr };
}

TBlobNode* TBlobIndex::TImpl::FindBlob(
    ui32 zoneHint,
    const TPartialBlobId& blobId)
{
    return FindBlobAndZone(zoneHint, blobId).first;
}

const TBlobNode* TBlobIndex::TImpl::FindBlob(
    ui32 zoneHint,
    const TPartialBlobId& blobId) const
{
    return FindBlobAndZone(zoneHint, blobId).first;
}

TMutableFoundBlobInfo TBlobIndex::TImpl::AccessBlob(
    ui32 zoneHint,
    const TPartialBlobId& blobId)
{
    auto info = FindBlobInfo(zoneHint, blobId);
    return {
        const_cast<TBlob*>(info.Blob),
        info.ZoneId
    };
}

TVector<const TBlob*> TBlobIndex::TImpl::FindBlobs(
    const TBlockRange32& blockRange) const
{
    TVector<const TBlob*> result;
    GlobalZone->RangeMap->Visit(blockRange, [&] (const TBlob* blob) {
        result.push_back(blob);
        return true;
    });

    const auto zoneRange = ToZoneRange(blockRange);
    for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
        const auto& zone = Zones[z];
        Y_ABORT_UNLESS(zone.IsInitialized());

        if (zone.RangeMap) {
            zone.RangeMap->Visit(blockRange, [&] (const TBlob* blob) {
                result.push_back(blob);
                return true;
            });

            ++zone.UsageScore;
            ++TotalUsageScore;
        }
    }

    SortUnique(result);
    return result;
}

void TBlobIndex::TImpl::FindBlobs(
    const TBlockRange32& blockRange,
    TPartialBlobIdHashSet& blobIds) const
{
    GlobalZone->RangeMap->Visit(blockRange, [&] (const TBlob* blob) {
        blobIds.insert(blob->BlobId);
        return true;
    });

    const auto zoneRange = ToZoneRange(blockRange);
    for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
        const auto& zone = Zones[z];
        Y_ABORT_UNLESS(zone.IsInitialized());

        if (zone.RangeMap) {
            zone.RangeMap->Visit(blockRange, [&] (const TBlob* blob) {
                blobIds.insert(blob->BlobId);
                return true;
            });
        }
    }
}

void TBlobIndex::TImpl::ExtractBlobsMixed(
    const TBlockRange32& blockRange,
    TPartialBlobIdHashSet& blobIds)
{
    const auto zoneRange = ToZoneRange(blockRange);
    for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
        auto& zone = Zones[z];
        Y_ABORT_UNLESS(zone.IsInitialized());

        if (zone.MixedIndex) {
            auto overwrittenBlobIds =
                zone.MixedIndex->ExtractOverwrittenBlobIds();
            for (const auto& blobId: overwrittenBlobIds) {
                blobIds.insert(blobId);
            }
        }
    }
}

void TBlobIndex::TImpl::WriteOrUpdateMixedBlock(const TBlockAndLocation& b)
{
    auto z = ToZone(b.Block.BlockIndex);
    auto& zone = Zones[z];
    Y_ABORT_UNLESS(zone.IsInitialized());

    if (zone.MixedIndex) {
        zone.MixedIndex->SetOrUpdateBlock(b);
    }
}

TVector<TBlockAndLocation> TBlobIndex::TImpl::FindAllMixedBlocks(
    const TBlockRange32& blockRange) const
{
    TVector<TBlockAndLocation> result;

    auto z = ToZone(blockRange.Start);
    auto lastZone = ToZone(blockRange.End);

    while (z <= lastZone) {
        auto& zone = Zones[z];
        Y_ABORT_UNLESS(zone.IsInitialized());

        if (zone.MixedIndex) {
            auto blocks = zone.MixedIndex->FindAllBlocks(blockRange);
            for (const auto& x: blocks) {
                Y_ABORT_UNLESS(blockRange.Contains(x.Block.BlockIndex));
                result.push_back(x);
            }
        }

        ++z;
    }

    return result;
}

TVector<TBlockAndLocation> TBlobIndex::TImpl::FindMixedBlocks(
    const TBlockRange32& blockRange,
    ui64 commitId) const
{
    TVector<TBlockAndLocation> result;

    auto z = ToZone(blockRange.Start);
    auto lastZone = ToZone(blockRange.End);

    while (z <= lastZone) {
        const auto& zone = Zones[z];
        Y_ABORT_UNLESS(zone.IsInitialized());

        if (zone.MixedIndex) {
            const auto subRange = blockRange.Intersect(ToBlockRange(z));

            for (ui32 i = subRange.Start; i <= subRange.End; ++i) {
                TBlockAndLocation b;
                if (zone.MixedIndex->FindBlock(commitId, i, &b)) {
                    result.push_back(b);
                }
            }

            ++zone.UsageScore;
            ++TotalUsageScore;
        }

        ++z;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

size_t TBlobIndex::TImpl::GetBlockListCount() const
{
    return BlockLists.GetCount();
}

size_t TBlobIndex::TImpl::GetBlockListsWeight() const
{
    return BlockLists.GetWeight();
}

bool TBlobIndex::TImpl::AddBlockList(
    ui32 zoneHint,
    const TPartialBlobId& blobId,
    const TBlockList& blocks,
    ui16 blockCount)
{
    auto* blob = FindBlob(zoneHint, blobId);
    if (blob) {
        PruneStep(BlockListCacheSize - 1);

        if (!blob->BlockList) {
            blob->BlockList = NewImpl<TBlockListNode>(
                GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList),
                blob,
                blockCount);
            BlockLists.Add(blob->BlockList);
        }

        blob->BlockList->Init(
            blocks.EncodedBlocks(),
            blocks.EncodedDeletedBlocks());

        return true;
    }

    return false;
}

void TBlobIndex::TImpl::SetPruneBlockListCache(bool prune)
{
    PruneBlockListCache = prune;
}

void TBlobIndex::TImpl::PruneStep(size_t limit)
{
    // TODO: implement UseCount decay
    // e.g. regularly decrement UseCounts in UpdateIndexStructures

    if (PruneBlockListCache && BlockLists.GetWeight() > limit) {
        auto* x = BlockLists.Prune();
        x->Blob->BlockList = nullptr;
        DeleteImpl(GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList), x);
    }
}

void TBlobIndex::TImpl::PruneStep()
{
    PruneStep(BlockListCacheSize);
}

const TBlockList* TBlobIndex::TImpl::FindBlockList(
    ui32 zoneHint,
    const TPartialBlobId& blobId) const
{
    const auto* blob = FindBlob(zoneHint, blobId);
    if (blob && blob->BlockList) {
        BlockLists.Use(blob->BlockList);
        return blob->BlockList;
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

size_t TBlobIndex::TImpl::GetGarbageBlockCount() const
{
    return TotalGarbageBlockCount;
}

bool TBlobIndex::TImpl::AddGarbage(
    ui32 zoneHint,
    const TPartialBlobId& blobId,
    ui16 blockCount)
{
    auto [blob, zone] = FindBlobAndZone(zoneHint, blobId);
    if (!blob) {
        return false;
    }

    ui32 prevGarbageBlockCount = 0;

    if (blob->Garbage) {
        Y_DEBUG_ABORT_UNLESS(blob->Garbage->BlockCount <= blockCount);
        prevGarbageBlockCount = blob->Garbage->BlockCount;
        zone->Garbage->Remove(blob);
        DeleteImpl(
            GetAllocatorByTag(EAllocatorTag::BlobIndexGarbageMap),
            blob->Garbage);
    }

    blob->Garbage = NewImpl<TGarbageNode>(
        GetAllocatorByTag(EAllocatorTag::BlobIndexGarbageMap),
        blob,
        blockCount);

    ZonesByGarbage.erase(zone);
    zone->Garbage->Add(blob);
    ZonesByGarbage.insert(zone);

    TotalGarbageBlockCount += blockCount - prevGarbageBlockCount;

    return true;
}

size_t TBlobIndex::TImpl::FindGarbage(ui32 zoneHint, const TPartialBlobId& blobId) const
{
    const auto* blob = FindBlob(zoneHint, blobId);
    if (blob && blob->Garbage) {
        return blob->Garbage->BlockCount;
    }

    return 0;
}

TGarbageInfo TBlobIndex::TImpl::GetTopGarbage(
    size_t maxBlobs,
    size_t maxBlocks) const
{
    const auto* topZone = *ZonesByGarbage.begin();
    auto blobCounters = topZone->Garbage->GetTop(maxBlobs, maxBlocks);
    return { std::move(blobCounters), ToZoneId(topZone) };
}

////////////////////////////////////////////////////////////////////////////////

ui32 TBlobIndex::TImpl::MarkBlocksDeleted(TBlobUpdate blobUpdate)
{
    auto addRange = [&](TZone* zone) {
        ZonesByDeletions.erase(zone);
        zone->DeletedRanges->AddUpdate(blobUpdate);
        ZonesByDeletions.insert(zone);

        return ToZoneId(zone);
    };

    const auto zoneRange = ToZoneRange(blobUpdate.BlockRange);
    ui32 zoneCount = zoneRange.second - zoneRange.first + 1;
    if (GlobalDataInitializationFinished) {
        for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
            auto& zone = Zones[z];
            Y_ABORT_UNLESS(zone.IsInitialized());
            if (zone.MixedIndex) {
                auto subRange = blobUpdate.BlockRange.Intersect(ToBlockRange(z));
                for (ui32 j = subRange.Start; j <= subRange.End; ++j) {
                    zone.MixedIndex->ClearBlock(j);
                }
            }
        }
    }

    if (zoneCount == 1 && GlobalDataInitializationFinished) {
        return addRange(&Zones[zoneRange.first]);
    }

    return addRange(GlobalZone);
}

ui32 TBlobIndex::TImpl::ApplyUpdates(
    const TBlockRange32& blockRange,
    TVector<TBlock>& blocks) const
{
    ui32 updateCount = GlobalZone->DeletedRanges->ApplyUpdates(
        blockRange,
        blocks);

    auto zoneRange = ToZoneRange(blockRange);
    for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
        auto& zone = Zones[z];
        Y_ABORT_UNLESS(zone.IsInitialized());

        updateCount += zone.DeletedRanges->ApplyUpdates(blockRange, blocks);
    }

    return updateCount;
}

TVector<TDeletedBlock> TBlobIndex::TImpl::FindDeletedBlocks(
    const TBlockRange32& blockRange,
    ui64 maxCommitId) const
{
    TVector<TDeletedBlock> deletedBlocks;
    GlobalZone->DeletedRanges->GetDeletedBlocks(
        blockRange,
        maxCommitId,
        &deletedBlocks
    );

    ui32 globalSize = deletedBlocks.size();

    auto zoneRange = ToZoneRange(blockRange);
    for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
        auto& zone = Zones[z];
        Y_ABORT_UNLESS(zone.IsInitialized());

        zone.DeletedRanges->GetDeletedBlocks(
            blockRange,
            maxCommitId,
            &deletedBlocks
        );
    }

    // XXX suboptimal
    if (globalSize < deletedBlocks.size()
            || zoneRange.first != zoneRange.second)
    {
        Sort(deletedBlocks.begin(), deletedBlocks.end());
    }

    return deletedBlocks;
}

const TZone* TBlobIndex::TImpl::GetTopZoneByDeletionCount() const
{
    const auto* topZone = *ZonesByDeletions.begin();
    if (topZone != GlobalZone) {
        return topZone;
    }

    bool ready = true;

    const auto& chunks = GlobalZone->DeletedRanges->GetChunks();
    auto chunk = chunks.begin();

    while (std::next(chunk) != chunks.end()
        && chunk->DeletedBlockCount == 0)
    {
        ++chunk;
    }

    chunk->Ranges.Visit(
        [&] (const TBlockRange32& range) {
            auto zoneRange = ToZoneRange(range);
            for (auto z = zoneRange.first; z <= zoneRange.second; ++z) {
                if (!Zones[z].IsInitialized()) {
                    ready = false;
                    break;
                }
            }
        }
    );

    return ready ? GlobalZone : nullptr;
}

TZone* TBlobIndex::TImpl::GetTopZoneByDeletionCount()
{
    return const_cast<TZone*>(std::as_const(*this).GetTopZoneByDeletionCount());
}

TVector<TPartialBlobId> TBlobIndex::TImpl::ExtractDirtyBlobs()
{
    Y_ABORT_UNLESS(ZoneWithPendingDeletionCleanup != nullptr);

    const auto& deletions = *ZoneWithPendingDeletionCleanup->DeletedRanges;
    const auto& top = deletions.TopChunk().Ranges;
    TPartialBlobIdHashSet blobIds(
        GetAllocatorByTag(EAllocatorTag::BlobIndexDirtyBlobs));
    // TODO: don't extract blobs whose blocks are newer than the corresponding
    // deletions
    top.Visit([&] (const TBlockRange32& range) {
        FindBlobs(range, blobIds);
        ExtractBlobsMixed(range, blobIds);
    });

    TVector<TPartialBlobId> blobIdsVec(Reserve(blobIds.size()));
    blobIdsVec.assign(blobIds.begin(), blobIds.end());

    return blobIdsVec;
}

size_t TBlobIndex::TImpl::GetPendingUpdates() const
{
    if (const auto* zone = GetTopZoneByDeletionCount()) {
        return zone->DeletedRanges->DeletionCount();
    }

    return 0;
}

ui32 TBlobIndex::TImpl::SelectZoneToCleanup()
{
    Y_ABORT_UNLESS(ZoneWithPendingDeletionCleanup == nullptr);
    ZoneWithPendingDeletionCleanup = GetTopZoneByDeletionCount();
    return ToZoneId(ZoneWithPendingDeletionCleanup);
}

void TBlobIndex::TImpl::SeparateChunkForCleanup(ui64 commitId)
{
    Y_ABORT_UNLESS(ZoneWithPendingDeletionCleanup != nullptr);
    auto& deletions = *ZoneWithPendingDeletionCleanup->DeletedRanges;

    while (deletions.ChunkCount() > 1
        && deletions.TopChunk().DeletedBlockCount == 0)
    {
        deletions.PopChunk();
    }

    if (deletions.ChunkCount() == 1) {
        deletions.Barrier(commitId);
    }
}

TDeletionsInfo TBlobIndex::TImpl::CleanupDirtyRanges()
{
    Y_ABORT_UNLESS(ZoneWithPendingDeletionCleanup != nullptr);
    auto* zone = ZoneWithPendingDeletionCleanup;

    ZonesByDeletions.erase(zone);
    auto blobUpdates = zone->DeletedRanges->PopChunk();
    ZonesByDeletions.insert(zone);

    ZoneWithPendingDeletionCleanup = nullptr;

    return { std::move(blobUpdates), ToZoneId(zone) };
}

void TBlobIndex::TImpl::OnCheckpoint(ui64 checkpointId)
{
    for (auto& zone: Zones) {
        if (zone.IsInitialized()) {
            zone.DeletedRanges->Barrier(checkpointId);
        }

        if (zone.MixedIndex) {
            zone.MixedIndex->OnCheckpoint(checkpointId);
        }
    }
}

void TBlobIndex::TImpl::OnCheckpoint(ui32 z, ui64 checkpointId)
{
    auto& zone = Zones[z];
    Y_ABORT_UNLESS(zone.IsInitialized());
    zone.DeletedRanges->Barrier(checkpointId);

    if (zone.MixedIndex) {
        zone.MixedIndex->OnCheckpoint(checkpointId);
    }
}

void TBlobIndex::TImpl::OnCheckpointDeletion(ui64 checkpointId)
{
    for (auto& zone: Zones) {
        if (zone.MixedIndex) {
            zone.MixedIndex->OnCheckpointDeletion(checkpointId);
        }
    }
}

ui32 TBlobIndex::TImpl::GetZoneCount() const
{
    return Zones.size();
}

ui32 TBlobIndex::TImpl::GetMixedZoneCount() const
{
    return MixedZoneCount;
}

ui32 TBlobIndex::TImpl::ToZone(ui32 blockIndex) const
{
    return blockIndex / ZoneBlockCount;
}

ui32 TBlobIndex::TImpl::ToZoneId(const TZone* zone) const
{
    if (zone == nullptr) {
        return InvalidZoneId;
    }
    if (zone == GlobalZone) {
        return GlobalZoneId;
    }
    return static_cast<ui32>(std::distance(&Zones.front(), zone));
}

std::pair<ui32, ui32> TBlobIndex::TImpl::ToZoneRange(
    const TBlockRange32& blockRange) const
{
    return std::make_pair(
        blockRange.Start / ZoneBlockCount,
        blockRange.End / ZoneBlockCount);
}

TBlockRange32 TBlobIndex::TImpl::ToBlockRange(const ui32 zone) const
{

    return TBlockRange32::WithLength(zone * ZoneBlockCount, ZoneBlockCount);
}

bool TBlobIndex::TImpl::IsGlobalZone(ui32 z) const
{
    Y_ABORT_UNLESS(z != InvalidZoneId);
    return z == GlobalZoneId;
}

void TBlobIndex::TImpl::FinishGlobalDataInitialization()
{
    GlobalDataInitializationFinished = true;
}

void TBlobIndex::TImpl::InitializeZone(ui32 z)
{
    Zones[z].Init(MaxBlocksInBlob, OptimizationMode);
}

void TBlobIndex::TImpl::ResetZone(ui32 z)
{
    Zones[z].Reset();
}

bool TBlobIndex::TImpl::IsZoneInitialized(ui32 z) const
{
    return Zones[z].IsInitialized();
}

bool TBlobIndex::TImpl::IsMixedIndex(ui32 z) const
{
    const auto& zone = Zones[z];
    Y_ABORT_UNLESS(zone.IsInitialized());
    return !!zone.MixedIndex;
}

TVector<const TBlob*> TBlobIndex::TImpl::FindZoneBlobs(ui32 z) const
{
    TVector<const TBlob*> result;

    const auto& zone = Zones[z];
    Y_ABORT_UNLESS(zone.IsInitialized());
    Y_ABORT_UNLESS(zone.RangeMap);

    zone.RangeMap->Visit([&] (const TBlob* blob) {
        result.push_back(blob);
        return true;
    });

    // TODO: filter by zone block range
    // don't forget to modify ConvertToMixedIndex after this (Y_ABORT_UNLESS for global
    // blob's blocklists)
    GlobalZone->RangeMap->Visit([&] (const TBlob* blob) {
        result.push_back(blob);
        return true;
    });

    SortUnique(result);
    return result;
}

double TBlobIndex::TImpl::UpdateAndGetZoneUsageScore(ui32 z, TInstant now)
{
    auto& zone = Zones[z];
    Y_ABORT_UNLESS(zone.IsInitialized());

    if (zone.UsageScoreUpdateTimestamp.GetValue()) {
        const auto d = now - zone.UsageScoreUpdateTimestamp;
        ApplyTimeDiscount(d, zone.UsageScore);
    }
    zone.UsageScoreUpdateTimestamp = now;

    return zone.UsageScore;
}

double TBlobIndex::TImpl::UpdateAndGetTotalUsageScore(TInstant now)
{
    if (TotalUsageScoreUpdateTimestamp.GetValue()) {
        const auto d = now - TotalUsageScoreUpdateTimestamp;
        ApplyTimeDiscount(d, TotalUsageScore);
    }
    TotalUsageScoreUpdateTimestamp = now;

    return TotalUsageScore;
}

void TBlobIndex::TImpl::ConvertToMixedIndex(ui32 z, const TVector<ui64>& checkpointIds)
{
    auto& zone = Zones[z];
    Y_ABORT_UNLESS(zone.IsInitialized());

    if (zone.MixedIndex) {
        return;
    }

    const auto range = ToBlockRange(z);
    TMixedIndexBuilder builder(range);

    auto deletedBlocks = FindDeletedBlocks(range, InvalidCommitId);

    auto onBlob = [&] (const TBlobNode* blob) {
        Y_ABORT_UNLESS(blob->BlockList);
        auto nodeBlocks = blob->BlockList->GetBlocks();
        ui16 blobOffset = 0;

        for (auto& block: nodeBlocks) {
            if (range.Contains(block.BlockIndex)) {
                auto it = UpperBound(
                    deletedBlocks.begin(),
                    deletedBlocks.end(),
                    TDeletedBlock(block.BlockIndex, block.MinCommitId)
                );

                if (it != deletedBlocks.end()
                        && it->BlockIndex == block.BlockIndex
                        && it->CommitId < block.MaxCommitId)
                {
                    block.MaxCommitId = it->CommitId;
                }

                if (block.Zeroed) {
                    builder.AddBlock({
                        block,
                        {blob->BlobId, ZeroBlobOffset}
                    });
                } else {
                    builder.AddBlock({
                        block,
                        {blob->BlobId, blobOffset}
                    });
                }
            }

            if (!block.Zeroed) {
                ++blobOffset;
            }
        }

        // marking all zone blobs for simplicity
        // TODO: mark only those blobs that intersect with DeletedRanges
        builder.AddOverwrittenBlob(blob->BlobId);
    };

    for (const auto& blob: *GlobalZone->Blobs) {
        onBlob(&blob);
    }

    for (const auto& blob: *zone.Blobs) {
        onBlob(&blob);
    }

    zone.RangeMap.reset();
    zone.MixedIndex = builder.Build(checkpointIds);
    ++MixedZoneCount;
}

void TBlobIndex::TImpl::ConvertToRangeMap(ui32 z)
{
    auto& zone = Zones[z];
    Y_ABORT_UNLESS(zone.IsInitialized());

    if (zone.RangeMap) {
        return;
    }

    zone.MixedIndex.reset();

    zone.RangeMap = std::make_unique<TRangeMap>(MaxBlocksInBlob);
    const auto range = ToBlockRange(z);

    for (auto& x: *zone.Blobs) {
        for (ui32 i = 0; i < x.BlockRanges.size(); ++i) {
            const auto& blockRange = x.BlockRanges[i];
            Y_ABORT_UNLESS(range.Overlaps(blockRange));
            zone.RangeMap->Add({const_cast<TBlobNode*>(&x), i});
        }
    }

    --MixedZoneCount;
}

TZone& TBlobIndex::TImpl::SelectZone(const TBlockRanges& blockRanges)
{
    Y_ABORT_UNLESS(!blockRanges.empty());

    if (!GlobalDataInitializationFinished) {
        return *GlobalZone;
    }

    ui32 zoneCount = 0;
    ui32 lastZoneId = InvalidZoneId;

    for (const auto& blockRange: blockRanges) {
        const auto zoneRange = ToZoneRange(blockRange);
        for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
            if (lastZoneId == InvalidZoneId || lastZoneId != z) {
                lastZoneId = z;
                ++zoneCount;

                Y_ABORT_UNLESS(Zones[z].IsInitialized());
            }
        }
    }

    return zoneCount == 1 ? Zones[lastZoneId] : *GlobalZone;
}

////////////////////////////////////////////////////////////////////////////////
// TBlobIndex

TBlobIndex::TBlobIndex(
        ui32 zoneCount,
        ui32 zoneBlockCount,
        ui32 blockListCacheSize,
        ui32 maxBlocksInBlob,
        EOptimizationMode optimizationMode)
    : Impl(new TImpl(
        zoneCount,
        zoneBlockCount,
        blockListCacheSize,
        maxBlocksInBlob,
        optimizationMode
    ))
{}

TBlobIndex::~TBlobIndex()
{}

////////////////////////////////////////////////////////////////////////////////

size_t TBlobIndex::GetGlobalBlobCount() const
{
    return GetImpl().GetGlobalBlobCount();
}

size_t TBlobIndex::GetZoneBlobCount() const
{
    return GetImpl().GetZoneBlobCount();
}

size_t TBlobIndex::GetRangeCount() const
{
    return GetImpl().GetRangeCount();
}

size_t TBlobIndex::GetBlockCount() const
{
    return GetImpl().GetBlockCount();
}

TAddedBlobInfo TBlobIndex::AddBlob(
    const TPartialBlobId& blobId,
    const TBlockRanges& blockRanges,
    ui16 blockCount,
    ui16 checkpointBlockCount)
{
    return GetImpl().AddBlob(blobId, blockRanges, blockCount, checkpointBlockCount);
}

bool TBlobIndex::RemoveBlob(ui32 zoneHint, const TPartialBlobId& blobId)
{
    return GetImpl().RemoveBlob(zoneHint, blobId);
}

TFoundBlobInfo TBlobIndex::FindBlob(
    ui32 zoneHint,
    const TPartialBlobId& blobId) const
{
    return GetImpl().FindBlobInfo(zoneHint, blobId);
}

TMutableFoundBlobInfo TBlobIndex::AccessBlob(
    ui32 zoneHint,
    const TPartialBlobId& blobId)
{
    return GetImpl().AccessBlob(zoneHint, blobId);
}

TVector<const TBlob*> TBlobIndex::FindBlobs(const TBlockRange32& blockRange) const
{
    return GetImpl().FindBlobs(blockRange);
}

void TBlobIndex::WriteOrUpdateMixedBlock(const TBlockAndLocation& b)
{
    GetImpl().WriteOrUpdateMixedBlock(b);
}

TVector<TBlockAndLocation> TBlobIndex::FindAllMixedBlocks(
    const TBlockRange32& blockRange) const
{
    return GetImpl().FindAllMixedBlocks(blockRange);
}

TVector<TBlockAndLocation> TBlobIndex::FindMixedBlocks(
    const TBlockRange32& blockRange,
    ui64 commitId) const
{
    return GetImpl().FindMixedBlocks(blockRange, commitId);
}

////////////////////////////////////////////////////////////////////////////////

size_t TBlobIndex::GetBlockListCount() const
{
    return GetImpl().GetBlockListCount();
}

size_t TBlobIndex::GetBlockListsWeight() const
{
    return GetImpl().GetBlockListsWeight();
}

bool TBlobIndex::AddBlockList(
    ui32 zoneHint,
    const TPartialBlobId& blobId,
    const TBlockList& blocks,
    ui16 blockCount)
{
    return GetImpl().AddBlockList(zoneHint, blobId, blocks, blockCount);
}

void TBlobIndex::SetPruneBlockListCache(bool prune)
{
    GetImpl().SetPruneBlockListCache(prune);
}

void TBlobIndex::PruneStep()
{
    GetImpl().PruneStep();
}

const TBlockList* TBlobIndex::FindBlockList(
    ui32 zoneHint,
    const TPartialBlobId& blobId) const
{
    return GetImpl().FindBlockList(zoneHint, blobId);
}

////////////////////////////////////////////////////////////////////////////////

size_t TBlobIndex::GetGarbageBlockCount() const
{
    return GetImpl().GetGarbageBlockCount();
}

bool TBlobIndex::AddGarbage(
    ui32 zoneHint,
    const TPartialBlobId& blobId,
    ui16 blockCount)
{
    return GetImpl().AddGarbage(zoneHint, blobId, blockCount);
}

size_t TBlobIndex::FindGarbage(ui32 zoneHint, const TPartialBlobId& blobId) const
{
    return GetImpl().FindGarbage(zoneHint, blobId);
}

TGarbageInfo TBlobIndex::GetTopGarbage(
    size_t maxBlobs,
    size_t maxBlocks) const
{
    return GetImpl().GetTopGarbage(maxBlobs, maxBlocks);
}

////////////////////////////////////////////////////////////////////////////////

ui32 TBlobIndex::MarkBlocksDeleted(TBlobUpdate blobUpdate)
{
    return GetImpl().MarkBlocksDeleted(blobUpdate);
}

ui32 TBlobIndex::ApplyUpdates(
    const TBlockRange32& blockRange,
    TVector<TBlock>& blocks) const
{
    return GetImpl().ApplyUpdates(blockRange, blocks);
}

TVector<TDeletedBlock> TBlobIndex::FindDeletedBlocks(
    const TBlockRange32& blockRange,
    ui64 maxCommitId) const
{
    return GetImpl().FindDeletedBlocks(blockRange, maxCommitId);
}

TVector<TPartialBlobId> TBlobIndex::ExtractDirtyBlobs()
{
    return GetImpl().ExtractDirtyBlobs();
}

size_t TBlobIndex::GetPendingUpdates() const
{
    return GetImpl().GetPendingUpdates();
}

ui32 TBlobIndex::SelectZoneToCleanup()
{
    return GetImpl().SelectZoneToCleanup();
}

void TBlobIndex::SeparateChunkForCleanup(ui64 commitId)
{
    return GetImpl().SeparateChunkForCleanup(commitId);
}

TDeletionsInfo TBlobIndex::CleanupDirtyRanges()
{
    return GetImpl().CleanupDirtyRanges();
}

void TBlobIndex::OnCheckpoint(ui64 checkpointId)
{
    GetImpl().OnCheckpoint(checkpointId);
}

void TBlobIndex::OnCheckpoint(ui32 z, ui64 checkpointId)
{
    GetImpl().OnCheckpoint(z, checkpointId);
}

void TBlobIndex::OnCheckpointDeletion(ui64 checkpointId)
{
    GetImpl().OnCheckpointDeletion(checkpointId);
}

ui32 TBlobIndex::GetZoneCount() const
{
    return GetImpl().GetZoneCount();
}

ui32 TBlobIndex::GetMixedZoneCount() const
{
    return GetImpl().GetMixedZoneCount();
}

std::pair<ui32, ui32> TBlobIndex::ToZoneRange(const TBlockRange32& blockRange) const
{
    return GetImpl().ToZoneRange(blockRange);
}

TBlockRange32 TBlobIndex::ToBlockRange(const ui32 zone) const
{
    return GetImpl().ToBlockRange(zone);
}

bool TBlobIndex::IsGlobalZone(ui32 z) const
{
    return GetImpl().IsGlobalZone(z);
}

void TBlobIndex::FinishGlobalDataInitialization()
{
    GetImpl().FinishGlobalDataInitialization();
}

void TBlobIndex::InitializeZone(ui32 z)
{
    GetImpl().InitializeZone(z);
}

void TBlobIndex::ResetZone(ui32 z)
{
    GetImpl().ResetZone(z);
}

bool TBlobIndex::IsZoneInitialized(ui32 z) const
{
    return GetImpl().IsZoneInitialized(z);
}

bool TBlobIndex::IsMixedIndex(ui32 z) const
{
    return GetImpl().IsMixedIndex(z);
}

TVector<const TBlob*> TBlobIndex::FindZoneBlobs(ui32 z) const
{
    return GetImpl().FindZoneBlobs(z);
}

double TBlobIndex::UpdateAndGetZoneUsageScore(ui32 z, TInstant now)
{
    return GetImpl().UpdateAndGetZoneUsageScore(z, now);
}

double TBlobIndex::UpdateAndGetTotalUsageScore(TInstant now)
{
    return GetImpl().UpdateAndGetTotalUsageScore(now);
}

void TBlobIndex::ConvertToMixedIndex(ui32 z, const TVector<ui64>& checkpointIds)
{
    return GetImpl().ConvertToMixedIndex(z, checkpointIds);
}

void TBlobIndex::ConvertToRangeMap(ui32 z)
{
    return GetImpl().ConvertToRangeMap(z);
}

////////////////////////////////////////////////////////////////////////////////

TBlobIndex::TImpl& TBlobIndex::GetImpl() {
    return *Impl;
}

const TBlobIndex::TImpl& TBlobIndex::GetImpl() const {
    return *Impl;
}

////////////////////////////////////////////////////////////////////////////////

TBlockRanges BuildRanges(const TVector<TBlock>& blocks, ui32 maxRanges)
{
    Y_ABORT_UNLESS(blocks.size());

    if (maxRanges <= 1) {
        return {TBlockRange32::MakeClosedInterval(
            blocks.front().BlockIndex,
            blocks.back().BlockIndex)};
    }

    struct THole
    {
        ui32 FirstBlock;
        ui32 Width;

        bool operator<(const THole& rhs) const
        {
            return Width > rhs.Width;
        }
    };

    TPriorityQueue<THole> holes;
    for (ui32 i = 0; i < blocks.size() - 1; ++i) {
        const auto blockIndex = blocks[i].BlockIndex;
        const auto nextIndex = blocks[i + 1].BlockIndex;
        if (blockIndex + 1 < nextIndex) {
            holes.push({i, blocks[i + 1].BlockIndex - blocks[i].BlockIndex - 1});
            if (holes.size() == maxRanges) {
                holes.pop();
            }
        }
    }

    TBlockRanges holeRanges;
    while (!holes.empty()) {
        auto start = blocks[holes.top().FirstBlock].BlockIndex + 1;
        auto end = blocks[holes.top().FirstBlock + 1].BlockIndex;
        holeRanges.push_back(TBlockRange32::WithLength(start, end - start));
        holes.pop();
    }

    Sort(holeRanges.begin(), holeRanges.end(), [] (const auto& l, const auto& r) {
        return l.Start < r.Start;
    });

    TBlockRanges blockRanges;
    ui32 blockIndex = blocks.front().BlockIndex;
    for (const auto& holeRange: holeRanges) {
        blockRanges.push_back(TBlockRange32::WithLength(
            blockIndex,
            holeRange.Start - blockIndex));
        blockIndex = holeRange.End + 1;
    }
    blockRanges.push_back(TBlockRange32::MakeClosedInterval(
        blockIndex,
        blocks.back().BlockIndex));

    return blockRanges;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
