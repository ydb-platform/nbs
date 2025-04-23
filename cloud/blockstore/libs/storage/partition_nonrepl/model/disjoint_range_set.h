#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/map.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Data structure that maintains a collection of non-overlapping intervals.
class TDisjointRangeSet
{
private:
    TMap<ui64, ui64> EndToStart;

public:
    TDisjointRangeSet();
    ~TDisjointRangeSet();

    [[nodiscard]] bool TryInsert(TBlockRange64 range);
    [[nodiscard]] bool Remove(TBlockRange64 range);
    [[nodiscard]] bool Empty() const;
    [[nodiscard]] size_t Size() const;
    [[nodiscard]] TBlockRange64 LeftmostRange() const;

private:
    friend class TDisjointRangeSetIterator;
};

// Iterator to the TDisjointRangeSet. The user should not change the container
// while iterating over it.
class TDisjointRangeSetIterator
{
private:
    const TDisjointRangeSet& RangeSet;
    decltype(TDisjointRangeSet::EndToStart)::const_iterator Pos;

public:
    explicit TDisjointRangeSetIterator(const TDisjointRangeSet& rangeSet);
    ~TDisjointRangeSetIterator() = default;

    [[nodiscard]] bool HasNext() const;
    TBlockRange64 Next();
};

}   // namespace NCloud::NBlockStore::NStorage
