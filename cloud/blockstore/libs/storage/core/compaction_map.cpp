#include "compaction_map.h"

#include <library/cpp/containers/intrusive_rb_tree/rb_tree.h>

#include <util/generic/algorithm.h>
#include <util/generic/intrlist.h>

#include <array>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

const float COMPACTED_RANGE_SCORE = -1000;

////////////////////////////////////////////////////////////////////////////////

struct TCompactionMap::TImpl
{
    struct TCompareByBlockIndex
    {
        template <typename T1, typename T2>
        static bool Compare(const T1& l, const T2& r)
        {
            return GetBlockIndex(l) < GetBlockIndex(r);
        }
    };

    struct TCompareByScore
    {
        template <typename T1, typename T2>
        static bool Compare(const T1& l, const T2& r)
        {
            return GetScore(l) > GetScore(r);
        }
    };

    struct TCompareByGarbageBlockCount
    {
        template <typename T1, typename T2>
        static bool Compare(const T1& l, const T2& r)
        {
            return GetGarbageBlockCount(l) > GetGarbageBlockCount(r);
        }
    };

    struct TCompareByGarbageIgnoringZeroed
    {
        template <typename T1, typename T2>
        static bool Compare(const T1& l, const T2& r)
        {
            return GetGarbageIgnoringZeroed(l) > GetGarbageIgnoringZeroed(r);
        }
    };

    struct TCompareByMixedBlockCount
    {
        template <typename T1, typename T2>
        static bool Compare(const T1& l, const T2& r)
        {
            const auto maxMixedBlockCountL = GetMaxMixedBlockCount(l);
            const auto maxMixedBlockCountR = GetMaxMixedBlockCount(r);
            const auto usedBlockCountForRangeWithMaxMixedBlockCountL =
                GetUsedBlockCountForRangeWithMaxMixedBlockCount(l);
            const auto usedBlockCountForRangeWithMaxMixedBlockCountR =
                GetUsedBlockCountForRangeWithMaxMixedBlockCount(r);
            return std::tie(
                       maxMixedBlockCountL,
                       usedBlockCountForRangeWithMaxMixedBlockCountL) >
                   std::tie(
                       maxMixedBlockCountR,
                       usedBlockCountForRangeWithMaxMixedBlockCountR);
        }
    };

    struct TGroupByBlockIndexNode
        : public TRbTreeItem<TGroupByBlockIndexNode, TCompareByBlockIndex>
    {};

    struct TGroupByScoreNode
        : public TRbTreeItem<TGroupByScoreNode, TCompareByScore>
    {};

    struct TGroupByGarbageBlockCountNode
        : public TRbTreeItem<
          TGroupByGarbageBlockCountNode,
          TCompareByGarbageBlockCount
        >
    {};

    struct TGroupByGarbageIgnoringZeroedNode
        : public TRbTreeItem<
          TGroupByGarbageIgnoringZeroedNode,
          TCompareByGarbageIgnoringZeroed
        >
    {};

    struct TGroupByMixedBlockCountNode
        : public TRbTreeItem<
              TGroupByMixedBlockCountNode,
              TCompareByMixedBlockCount>
    {
    };

    struct TGroupNode
        : public TIntrusiveListItem<TGroupNode>
        , public TGroupByBlockIndexNode
        , public TGroupByScoreNode
        , public TGroupByGarbageBlockCountNode
        , public TGroupByGarbageIgnoringZeroedNode
        , public TGroupByMixedBlockCountNode
    {
        ui32 BlockIndex = 0;
        float Score = 0;
        ui16 GarbageBlockCount = 0;
        ui16 GarbageIgnoringZeroed = 0;

        ui16 MaxMixedBlockCountInGroup = 0;
        ui32 RangeWithMaxMixedBlockCount = 0;

        ui32 Range = 0;

        std::array<TRangeStat, GroupSize> Stats {};
    };

    using TGroupList = TIntrusiveListWithAutoDelete<TGroupNode, TDelete>;
    using TGroupByBlockIndexTree = TRbTree<TGroupByBlockIndexNode, TCompareByBlockIndex>;
    using TGroupByScoreTree = TRbTree<TGroupByScoreNode, TCompareByScore>;
    using TGroupByGarbageBlockCountTree =
        TRbTree<TGroupByGarbageBlockCountNode, TCompareByGarbageBlockCount>;
    using TGroupByGarbageIgnoringZeroedTree =
        TRbTree<TGroupByGarbageIgnoringZeroedNode, TCompareByGarbageIgnoringZeroed>;
    using TGroupByMixedBlockCountTree =
        TRbTree<TGroupByMixedBlockCountNode, TCompareByMixedBlockCount>;

    const ui32 RangeSize;
    const ICompactionPolicyPtr Policy;

    TGroupList Groups;
    TGroupByBlockIndexTree GroupByBlockIndex;
    TGroupByScoreTree GroupByScore;
    TGroupByGarbageBlockCountTree GroupByGarbageBlockCount;
    TGroupByGarbageIgnoringZeroedTree GroupByGarbageIgnoringZeroed;
    TGroupByMixedBlockCountTree GroupByMixedBlockCount;
    ui32 NonEmptyRangeCount = 0;
    ui64 MixedBlocksCountPerDisk = 0;

    TImpl(ui32 rangeSize, ICompactionPolicyPtr policy)
        : RangeSize(rangeSize)
        , Policy(std::move(policy))
    {
    }

    TGroupNode* FindGroup(ui32 groupStart) const
    {
        return static_cast<TGroupNode*>(GroupByBlockIndex.Find(groupStart));
    }

    const TGroupNode* GetTopGroup() const
    {
        if (!GroupByScore.Empty()) {
            return static_cast<const TGroupNode*>(&*GroupByScore.Begin());
        }
        return nullptr;
    }

    TVector<TCompactionCounter> GetTopsFromGroups(size_t groupCount) const
    {
        TVector<TCompactionCounter> tops(Reserve(groupCount));

        auto it = GroupByScore.Begin();
        while (it != GroupByScore.End() && tops.size() < groupCount) {
            auto& g = static_cast<const TGroupNode&>(*it);

            if (g.Stats[g.Range].BlobCount) {
                tops.push_back({
                    g.BlockIndex + g.Range * RangeSize,
                    g.Stats[g.Range]
                });
            }

            ++it;

            /*
                Idea: advance iterator by 2 to guarantee that we won't get ranges
                that intersect by blobs

                ++it;
            */
        }

        return tops;
    }

    const TGroupNode* GetTopGroupByGarbageBlockCount() const
    {
        if (!GroupByGarbageBlockCount.Empty()) {
            return static_cast<const TGroupNode*>(&*GroupByGarbageBlockCount.Begin());
        }
        return nullptr;
    }

    const TGroupNode* GetTopGroupByGarbageIgnoringZeroed() const
    {
        if (!GroupByGarbageIgnoringZeroed.Empty()) {
            return static_cast<const TGroupNode*>(
                &*GroupByGarbageIgnoringZeroed.Begin());
        }
        return nullptr;
    }

    const TGroupNode* GetTopGroupByMixedBlockCount() const
    {
        if (!GroupByMixedBlockCount.Empty()) {
            return static_cast<const TGroupNode*>(
                &*GroupByMixedBlockCount.Begin());
        }
        return nullptr;
    }

    void InitGroupScores(TGroupNode* group)
    {
        auto score = Policy->CalculateScore({}).Score;
        group->Score = score;

        for (ui32 i = 0; i < group->Stats.size(); ++i) {
            group->Stats[i].CompactionScore = group->Score;
        }
    }

    void RecalculateGroupScore(TGroupNode* group)
    {
        group->Score = -Max<float>();
        for (ui32 i = 0; i < group->Stats.size(); ++i) {
            auto& stat = group->Stats[i].CompactionScore;
            if (stat.Score > group->Score) {
                group->Score = stat.Score;
                group->Range = i;
            }
        }
    }

    void RecalculateGroupGarbageBlockCount(TGroupNode* group)
    {
        group->GarbageBlockCount = 0;
        for (ui32 i = 0; i < group->Stats.size(); ++i) {
            const auto& stat = group->Stats[i];
            if (!stat.Compacted) {
                group->GarbageBlockCount =
                    Max(group->GarbageBlockCount, stat.GarbageBlockCount());
            }
        }
    }

    void RecalculateGroupGarbageIgnoringZeroed(TGroupNode* group)
    {
        group->GarbageIgnoringZeroed = 0;
        for (ui32 i = 0; i < group->Stats.size(); ++i) {
            const auto& stat = group->Stats[i];
            if (!stat.Compacted) {
                group->GarbageIgnoringZeroed =
                    Max(group->GarbageIgnoringZeroed,
                        stat.GarbageIgnoringZeroed());
            }
        }
    }

    void RecalculateGroupMaxMixedBlockCount(TGroupNode* group)
    {
        group->MaxMixedBlockCountInGroup = 0;

        for (ui32 i = 0; i < group->Stats.size(); ++i) {
            const auto& stat = group->Stats[i];

            const ui64 curMixedBlockCount = GetMaxMixedBlockCount(*group);
            const ui64 curUsedBlockCount =
                GetUsedBlockCountForRangeWithMaxMixedBlockCount(*group);

            const ui64 rangeMixedBlockCount = stat.MixedBlockCount;
            const ui64 rangeUsedBlockCount = stat.UsedBlockCount;

            if (!stat.Compacted &&
                std::tie(rangeMixedBlockCount, rangeUsedBlockCount) >
                    std::tie(curMixedBlockCount, curUsedBlockCount))
            {
                group->MaxMixedBlockCountInGroup = stat.MixedBlockCount;
                group->RangeWithMaxMixedBlockCount = i;
            }
        }
    }

    TGroupNode* AddGroup(ui32 blockIndex)
    {
        const auto groupStart = GetGroupStart(blockIndex, RangeSize);
        auto* group = FindGroup(groupStart);
        if (!group) {
            group = new TGroupNode();
            group->BlockIndex = groupStart;
            InitGroupScores(group);

            Groups.PushBack(group);
            GroupByBlockIndex.Insert(group);
        }

        return group;
    }

    TGroupNode* Update(
        ui32 blockIndex,
        ui32 blobCount,
        ui32 blockCount,
        ui32 usedBlockCount,
        ui32 newlyZeroedBlocks,
        ui32 mixedBlockCount,
        bool compacted)
    {
        auto* group = AddGroup(blockIndex);

        const size_t index = (blockIndex - group->BlockIndex) / RangeSize;
        const auto prev = group->Stats[index];
        if (prev.BlobCount != blobCount
                || prev.BlockCount != blockCount
                || prev.UsedBlockCount != usedBlockCount
                || prev.NewlyZeroedBlocks != newlyZeroedBlocks
                || prev.MixedBlockCount != mixedBlockCount
                || prev.Compacted != compacted)
        {
            if (blobCount && !prev.BlobCount) {
                ++NonEmptyRangeCount;
            } else if (!blobCount && prev.BlobCount) {
                --NonEmptyRangeCount;
            }

            UpdateCompactionCounter(blobCount, &group->Stats[index].BlobCount);
            UpdateCompactionCounter(blockCount, &group->Stats[index].BlockCount);
            UpdateCompactionCounter(
                usedBlockCount,
                &group->Stats[index].UsedBlockCount
            );
            UpdateCompactionCounter(
                newlyZeroedBlocks,
                &group->Stats[index].NewlyZeroedBlocks);
            UpdateCompactionCounter(
                mixedBlockCount,
                &group->Stats[index].MixedBlockCount);

            if (compacted) {
                group->Stats[index].ReadRequestCount = 0;
                group->Stats[index].ReadRequestBlobCount = 0;
                group->Stats[index].ReadRequestBlockCount = 0;
            }
            group->Stats[index].Compacted = compacted;

            const auto newScore = compacted
                ? COMPACTED_RANGE_SCORE
                : Policy->CalculateScore(group->Stats[index]);
            group->Stats[index].CompactionScore = newScore;

            if (prev.CompactionScore.Score <= newScore.Score) {
                if (group->Score < newScore.Score) {
                    group->Score = newScore.Score;
                    group->Range = index;
                }
            } else if (index == group->Range) {
                // Score in the top range has decreased, need to recalculate the
                // maximal score.
                RecalculateGroupScore(group);
            }

            const auto newGarbageBlockCount =
                compacted ? 0 : group->Stats[index].GarbageBlockCount();

            if (prev.GarbageBlockCount() <= newGarbageBlockCount) {
                if (GetGarbageBlockCount(*group) < newGarbageBlockCount) {
                    group->GarbageBlockCount = newGarbageBlockCount;
                }
            } else if (prev.GarbageBlockCount() == GetGarbageBlockCount(*group)) {
                // Garbage block count in the top range has decreased, need to
                // recalculate the maximal garbage block count.
                RecalculateGroupGarbageBlockCount(group);
            }

            const auto newGarbageIgnoringZeroed =
                compacted ? 0 : group->Stats[index].GarbageIgnoringZeroed();

            if (prev.GarbageIgnoringZeroed() <= newGarbageIgnoringZeroed) {
                if (GetGarbageIgnoringZeroed(*group) < newGarbageIgnoringZeroed)
                {
                    group->GarbageIgnoringZeroed = newGarbageIgnoringZeroed;
                }
            } else if (
                prev.GarbageIgnoringZeroed() == group->GarbageIgnoringZeroed)
            {
                // GarbageIgnoringZeroed block count in the top range has
                // decreased, need to recalculate the maximal
                // GarbageIgnoringZeroed block count.
                RecalculateGroupGarbageIgnoringZeroed(group);
            }

            const ui16 prevMixedBlockCount =
                prev.Compacted ? 0 : prev.MixedBlockCount;
            const ui16 prevUsedBlocks = prev.UsedBlockCount;

            const ui16 newMixedBlockCount =
                compacted ? 0 : group->Stats[index].MixedBlockCount;
            const auto newUsedBlockCount = group->Stats[index].UsedBlockCount;

            if (std::tie(prevMixedBlockCount, prevUsedBlocks) <=
                std::tie(newMixedBlockCount, newUsedBlockCount))
            {
                const auto prevMaxMixedBlockCount =
                    group->MaxMixedBlockCountInGroup;
                const auto prevUsedBlockCountForMaxRange =
                    group->Stats[group->RangeWithMaxMixedBlockCount]
                        .UsedBlockCount;

                if (std::tie(
                        prevMaxMixedBlockCount,
                        prevUsedBlockCountForMaxRange) <
                    std::tie(newMixedBlockCount, newUsedBlockCount))
                {
                    group->MaxMixedBlockCountInGroup = newMixedBlockCount;
                    group->RangeWithMaxMixedBlockCount = index;
                }
            } else if (index == group->RangeWithMaxMixedBlockCount) {
                // The selected range's mixed/used block tuple decreased,
                // so recalculate the group maximum.
                RecalculateGroupMaxMixedBlockCount(group);
            }

            MixedBlocksCountPerDisk -= prev.MixedBlockCount;
            MixedBlocksCountPerDisk += mixedBlockCount;
        }

        return group;
    }

    void RegisterRead(
        TGroupNode* group,
        size_t index,
        ui32 blobCount,
        ui32 blockCount)
    {
        auto& stat = group->Stats[index];
        const auto prev = stat;
        UpdateCompactionCounter(
            stat.ReadRequestCount + 1,
            &stat.ReadRequestCount
        );
        UpdateCompactionCounter(
            stat.ReadRequestBlobCount + blobCount,
            &stat.ReadRequestBlobCount
        );
        UpdateCompactionCounter(
            stat.ReadRequestBlockCount + blockCount,
            &stat.ReadRequestBlockCount
        );

        if (!group->Stats[index].Compacted) {
            auto newScore = Policy->CalculateScore(group->Stats[index]);
            group->Stats[index].CompactionScore = newScore;

            if (prev.CompactionScore.Score <= newScore.Score) {
                if (group->Score < newScore.Score) {
                    group->Score = newScore.Score;
                    group->Range = index;
                }
            } else if (index == group->Range) {
                // Score in the top range has decreased, need to recalculate the
                // maximal score.
                RecalculateGroupScore(group);
            }
        }
    }

    static ui32 GetBlockIndex(ui32 blockIndex)
    {
        return blockIndex;
    }

    template <typename T>
    static ui32 GetBlockIndex(const T& node)
    {
        return static_cast<const TGroupNode&>(node).BlockIndex;
    }

    static float GetScore(float score)
    {
        return score;
    }

    template <typename T>
    static float GetScore(const T& node)
    {
        return static_cast<const TGroupNode&>(node).Score;
    }

    static ui16 GetGarbageBlockCount(ui16 garbageBlockCount)
    {
        return garbageBlockCount;
    }

    template <typename T>
    static ui16 GetGarbageBlockCount(const T& node)
    {
        return static_cast<const TGroupNode&>(node).GarbageBlockCount;
    }

    static ui16 GetGarbageIgnoringZeroed(ui16 garbageIgnoringZeroed)
    {
        return garbageIgnoringZeroed;
    }

    template <typename T>
    static ui16 GetGarbageIgnoringZeroed(const T& node)
    {
        return static_cast<const TGroupNode&>(node).GarbageIgnoringZeroed;
    }

    static ui16 GetMaxMixedBlockCount(ui16 maxMixedBlockCount)
    {
        return maxMixedBlockCount;
    }

    template <typename T>
    static ui16 GetMaxMixedBlockCount(const T& node)
    {
        return static_cast<const TGroupNode&>(node)
            .MaxMixedBlockCountInGroup;
    }

    template <typename T>
    static ui16 GetUsedBlockCountForRangeWithMaxMixedBlockCount(const T& node)
    {
        const auto& group = static_cast<const TGroupNode&>(node);
        return group.Stats[group.RangeWithMaxMixedBlockCount].UsedBlockCount;
    }

    ui32 GetNonEmptyRangeCount() const
    {
        return NonEmptyRangeCount;
    }

    ui64 GetMixedBlocksCountPerDisk() const
    {
        return MixedBlocksCountPerDisk;
    }
};

////////////////////////////////////////////////////////////////////////////////

TCompactionMap::TCompactionMap(ui32 rangeSize, ICompactionPolicyPtr policy)
    : Impl(new TImpl(rangeSize, std::move(policy)))
{}

TCompactionMap::~TCompactionMap()
{}

void TCompactionMap::Update(
    const TVector<TCompactionCounter>& counters,
    const TCompressedBitmap* used)
{
    if (counters.empty()) {
        return;
    }

    for (const auto& c: counters) {
        auto usedBlockCount = c.Stat.BlockCount;

        if (used) {
            usedBlockCount = used->Count(
                c.BlockIndex,
                Min(
                    static_cast<ui64>(c.BlockIndex + Impl->RangeSize),
                    used->Capacity()
                )
            );
        }

        Impl->Update(
            c.BlockIndex,
            c.Stat.BlobCount,
            c.Stat.BlockCount,
            usedBlockCount,
            c.Stat.NewlyZeroedBlocks,
            c.Stat.MixedBlockCount,
            c.Stat.BlobCount < 2   // compacted
        );
    }

    for (auto group = Impl->Groups.Begin(); group != Impl->Groups.End();
         ++group)
    {
        Impl->GroupByScore.Insert(*group);
        Impl->GroupByGarbageBlockCount.Insert(*group);
        Impl->GroupByGarbageIgnoringZeroed.Insert(*group);
        Impl->GroupByMixedBlockCount.Insert(*group);
    }
}

void TCompactionMap::Update(
    ui32 blockIndex,
    ui32 blobCount,
    ui32 blockCount,
    ui32 usedBlockCount,
    ui32 newlyZeroedBlocks,
    ui32 mixedBlockCount,
    bool compacted)
{
    auto* group = Impl->Update(
        blockIndex,
        blobCount,
        blockCount,
        usedBlockCount,
        newlyZeroedBlocks,
        mixedBlockCount,
        compacted);

    Impl->GroupByScore.Insert(group);
    Impl->GroupByGarbageBlockCount.Insert(group);
    Impl->GroupByGarbageIgnoringZeroed.Insert(group);
    Impl->GroupByMixedBlockCount.Insert(group);
}

void TCompactionMap::RegisterRead(ui32 blockIndex, ui32 blobCount, ui32 blockCount)
{
    ui32 groupStart = GetGroupStart(blockIndex, Impl->RangeSize);

    auto* group = Impl->FindGroup(groupStart);
    if (!group) {
        return;
    }

    Impl->RegisterRead(
        group,
        (blockIndex - groupStart) / Impl->RangeSize,
        blobCount,
        blockCount
    );

    Impl->GroupByScore.Insert(group);
}

void TCompactionMap::Clear()
{
    Impl->GroupByBlockIndex.Clear();
    Impl->GroupByScore.Clear();
    Impl->GroupByGarbageBlockCount.Clear();
    Impl->GroupByGarbageIgnoringZeroed.Clear();
    Impl->GroupByMixedBlockCount.Clear();
    Impl->MixedBlocksCountPerDisk = 0;
    Impl->Groups.Clear();
}

TRangeStat TCompactionMap::Get(ui32 blockIndex) const
{
    ui32 groupStart = GetGroupStart(blockIndex, Impl->RangeSize);

    if (auto* group = Impl->FindGroup(groupStart)) {
        return group->Stats[(blockIndex - groupStart) / Impl->RangeSize];
    }

    return {};
}

TCompactionCounter TCompactionMap::GetTop() const
{
    if (auto* group = Impl->GetTopGroup()) {
        return {
            group->BlockIndex + group->Range * Impl->RangeSize,
            group->Stats[group->Range]
        };
    }

    return {0, {}};
}

TVector<TCompactionCounter> TCompactionMap::GetTopsFromGroups(size_t groupCount) const
{
    return Impl->GetTopsFromGroups(groupCount);
}

TCompactionCounter TCompactionMap::GetTopByGarbageBlockCount() const
{
    if (auto* group = Impl->GetTopGroupByGarbageBlockCount()) {
        ui32 range = 0;
        TRangeStat stat;
        for (ui32 i = 0; i < GroupSize; ++i) {
            if (!group->Stats[i].Compacted
                    && group->Stats[i].GarbageBlockCount() > stat.GarbageBlockCount())
            {
                stat = group->Stats[i];
                range = i;
            }
        }

        return {
            group->BlockIndex + range * Impl->RangeSize,
            stat
        };
    }

    return {0, {}};
}

TCompactionCounter TCompactionMap::GetTopByGarbageIgnoringZeroed() const
{
    if (auto* group = Impl->GetTopGroupByGarbageIgnoringZeroed()) {
        ui32 range = 0;
        TRangeStat stat;
        for (ui32 i = 0; i < GroupSize; ++i) {
            if (!group->Stats[i].Compacted &&
                group->Stats[i].GarbageIgnoringZeroed() >
                    stat.GarbageIgnoringZeroed())
            {
                stat = group->Stats[i];
                range = i;
            }
        }

        return {group->BlockIndex + range * Impl->RangeSize, stat};
    }

    return {0, {}};
}

TCompactionCounter TCompactionMap::GetTopByMixedBlockCount() const
{
    if (auto* group = Impl->GetTopGroupByMixedBlockCount()) {
        return {
            group->BlockIndex +
                group->RangeWithMaxMixedBlockCount * Impl->RangeSize,
            group->Stats[group->RangeWithMaxMixedBlockCount]};
    }
    return {0, {}};
}

TVector<TCompactionCounter> TCompactionMap::GetTop(size_t count) const
{
    TVector<TCompactionCounter> result(Reserve(Impl->Groups.Size() * GroupSize));

    for (const auto& group: Impl->Groups) {
        for (ui32 i = 0; i < group.Stats.size(); ++i) {
            if (group.Stats[i].BlobCount > 0) {
                result.emplace_back(
                    group.BlockIndex + (i * Impl->RangeSize),
                    group.Stats[i]);
            }
        }
    }

    Sort(result, [] (const auto& l, const auto& r) {
        return l.Stat.CompactionScore.Score > r.Stat.CompactionScore.Score;
    });

    result.crop(count);
    return result;
}

TVector<TCompactionCounter> TCompactionMap::GetTopByGarbageBlockCount(
    size_t count) const
{
    TVector<TCompactionCounter> result(Reserve(Impl->Groups.Size() * GroupSize));

    for (const auto& group: Impl->Groups) {
        for (ui32 i = 0; i < group.Stats.size(); ++i) {
            if (group.Stats[i].BlobCount > 0) {
                result.emplace_back(
                    group.BlockIndex + (i * Impl->RangeSize),
                    group.Stats[i]);
            }
        }
    }

    Sort(result, [] (const auto& l, const auto& r) {
        if (l.Stat.Compacted != r.Stat.Compacted) {
            return r.Stat.Compacted;
        }

        return l.Stat.GarbageBlockCount() > r.Stat.GarbageBlockCount();
    });

    result.crop(count);
    return result;
}

TVector<TCompactionCounter> TCompactionMap::GetTopByGarbageIgnoringZeroed(
    size_t count) const
{
    TVector<TCompactionCounter> result(
        Reserve(Impl->Groups.Size() * GroupSize));

    for (const auto& group: Impl->Groups) {
        for (ui32 i = 0; i < group.Stats.size(); ++i) {
            if (group.Stats[i].BlobCount > 0) {
                result.emplace_back(
                    group.BlockIndex + (i * Impl->RangeSize),
                    group.Stats[i]);
            }
        }
    }

    Sort(
        result,
        [](const auto& l, const auto& r)
        {
            if (l.Stat.Compacted != r.Stat.Compacted) {
                return r.Stat.Compacted;
            }

            return l.Stat.GarbageIgnoringZeroed() >
                   r.Stat.GarbageIgnoringZeroed();
        });

    result.crop(count);
    return result;
}

TVector<TCompactionCounter> TCompactionMap::GetTopByMixedBlockCount(
    size_t count) const
{
    TVector<TCompactionCounter> result(
        Reserve(Impl->Groups.Size() * GroupSize));
    for (const auto& group: Impl->Groups) {
        for (ui32 i = 0; i < group.Stats.size(); ++i) {
            if (group.Stats[i].BlobCount > 0) {
                result.emplace_back(
                    group.BlockIndex + (i * Impl->RangeSize),
                    group.Stats[i]);
            }
        }
    }
    Sort(
        result,
        [](const auto& l, const auto& r)
        {
            if (l.Stat.Compacted != r.Stat.Compacted) {
                return r.Stat.Compacted;
            }

            return std::tie(l.Stat.MixedBlockCount, l.Stat.UsedBlockCount) >
                   std::tie(r.Stat.MixedBlockCount, r.Stat.UsedBlockCount);
        });
    result.crop(count);
    return result;
}

TVector<ui32> TCompactionMap::GetNonEmptyRanges() const
{
    TVector<ui32> result(Reserve(Impl->Groups.Size() * GroupSize));

    for (const auto& group: Impl->Groups) {
        for (ui32 i = 0; i < group.Stats.size(); ++i) {
            if (group.Stats[i].BlobCount > 0) {
                result.push_back(group.BlockIndex + (i * Impl->RangeSize));
            }
        }
    }

    return result;
}

ui32 TCompactionMap::GetNonEmptyRangeCount() const
{
    return Impl->GetNonEmptyRangeCount();
}

ui64 TCompactionMap::GetMixedBlocksCountPerDisk() const
{
    return Impl->GetMixedBlocksCountPerDisk();
}

ui32 TCompactionMap::GetRangeStart(ui32 blockIndex) const
{
    return GetRangeStart(blockIndex, Impl->RangeSize);
}

ui32 TCompactionMap::GetRangeIndex(ui32 blockIndex) const
{
    return blockIndex / Impl->RangeSize;
}

ui32 TCompactionMap::GetRangeIndex(TBlockRange32 blockRange) const
{
    Y_ABORT_UNLESS(blockRange.Start % Impl->RangeSize == 0);
    return blockRange.Start / Impl->RangeSize;
}

TBlockRange32 TCompactionMap::GetBlockRange(ui32 rangeIdx) const
{
    return TBlockRange32::WithLength(
        rangeIdx * Impl->RangeSize,
        Impl->RangeSize);
}

ui32 TCompactionMap::GetRangeSize() const
{
    return Impl->RangeSize;
}

}   // namespace NCloud::NBlockStore::NStorage
