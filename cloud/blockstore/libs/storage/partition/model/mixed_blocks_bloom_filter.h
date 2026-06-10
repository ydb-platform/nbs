#pragma once

#include "cloud/blockstore/libs/common/block_range.h"

#include <cloud/blockstore/libs/storage/core/blocks_bloom_filter.h>

#include <util/generic/vector.h>
#include <util/generic/hash.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage::NPartition {

class TMixedBlocksBloomFilter
{
    struct TPerRangeBloomFilters
        : public TIntrusiveListItem<TPerRangeBloomFilters>
    {
        std::deque<TBlo
        std::optional<TBlocksBloomFilter> PrimaryBloomFilter;
        std::optional<TBlocksBloomFilter> SecondaryBloomFilter;
    };

    using TBloomFiltersList =
        TIntrusiveListWithAutoDelete<TPerRangeBloomFilters, TDelete>;

private:
    TBloomFiltersList BloomFilters;
    THashMap<ui32, TPerRangeBloomFilters*> BloomFiltersIndex;

    ui64 BlocksPerRange = 0;
    ui64 MixedBlocksPerRange = 0;
    double ErrorRate = 0;

public:
    TMixedBlocksBloomFilter(
            ui64 blocksPerRange,
            ui64 mixedBlocksPerRange,
            double errorRate)
        : BlocksPerRange(blocksPerRange)
        , MixedBlocksPerRange(mixedBlocksPerRange)
        , ErrorRate(errorRate)
    {}

    [[nodiscard]] bool HasRangeInMixedIndex(TBlockRange32 range) const
    {
        for (ui32 blockIndex = range.Start; blockIndex <= range.End;
             ++blockIndex)
        {
            ui32 rangeIndex = blockIndex / BlocksPerRange;
            const auto* bloomFilters = FindPerRangeBloomFilters(rangeIndex);
            if (!bloomFilters) {
                return true;
            }
            if (!bloomFilters->PrimaryBloomFilter) {
                return true;
            }
            if (bloomFilters->PrimaryBloomFilter->Test(blockIndex)) {
                return true;
            }
        }
        return false;
    }

    void AddBlocks(TBlockRange32 range)
    {
        for (ui32 blockIndex = range.Start; blockIndex <= range.End;
             ++blockIndex)
        {
            ui32 rangeIndex = blockIndex / BlocksPerRange;
            auto& bloomFilters = AccessPerRangeBloomFilters(rangeIndex);

            if (bloomFilters.PrimaryBloomFilter) {
                bloomFilters.PrimaryBloomFilter->Add(blockIndex);
            }

            if (bloomFilters.SecondaryBloomFilter) {
                bloomFilters.SecondaryBloomFilter->Add(blockIndex);
            }
        }
    }

    void AddSecondaryBloomFilter(ui32 rangeIndex)
    {
        auto& filters = AccessPerRangeBloomFilters(rangeIndex);
        // Y_ABORT_UNLESS(!filters.SecondaryBloomFilter);

        filters.SecondaryBloomFilter =
            TBlocksBloomFilter(MixedBlocksPerRange, ErrorRate);
    }

    void PromoteSecondaryBloomFilter(ui32 rangeIndex)
    {
        auto& filters = AccessPerRangeBloomFilters(rangeIndex);
        if (!filters.SecondaryBloomFilter) {
            return;
        }
        // Y_ABORT_UNLESS(filters.SecondaryBloomFilter);

        std::swap(filters.PrimaryBloomFilter, filters.SecondaryBloomFilter);
        filters.SecondaryBloomFilter = std::nullopt;
    }

    void DropSecondaryBloomFilter(ui32 rangeIndex)
    {
        auto& filters = AccessPerRangeBloomFilters(rangeIndex);
        if (!filters.SecondaryBloomFilter) {
            return;
        }
        // Y_ABORT_UNLESS(filters.SecondaryBloomFilter);

        filters.SecondaryBloomFilter = std::nullopt;
    }

private:
    [[nodiscard]] const TPerRangeBloomFilters* FindPerRangeBloomFilters(
        ui32 rangeIndex) const
    {
        const auto* bloomFilters = BloomFiltersIndex.FindPtr(rangeIndex);
        if (!bloomFilters) {
            return nullptr;
        }

        return *bloomFilters;
    }

    [[nodiscard]] TPerRangeBloomFilters& AccessPerRangeBloomFilters(
        ui32 rangeIndex)
    {
        auto* bloomFilters = BloomFiltersIndex.FindPtr(rangeIndex);
        if (bloomFilters) {
            Y_ABORT_UNLESS(*bloomFilters);
            return **bloomFilters;
        }

        auto* newBloomFilters = new TPerRangeBloomFilters();

        BloomFilters.PushBack(newBloomFilters);
        BloomFiltersIndex.insert({rangeIndex, newBloomFilters});

        return *newBloomFilters;
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
