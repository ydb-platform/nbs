#include "disjoint_range_set.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDisjointRangeSet::TDisjointRangeSet() = default;
TDisjointRangeSet::~TDisjointRangeSet() = default;

bool TDisjointRangeSet::TryInsert(TBlockRange64 range)
{
    auto it = EndToStart.lower_bound(range.Start);
    if (it != EndToStart.end()) {
        auto found = TBlockRange64::MakeClosedInterval(it->second, it->first);
        if (found.Overlaps(range)) {
            return false;
        }
    }

    EndToStart[range.End] = range.Start;
    return true;
}

bool TDisjointRangeSet::Remove(TBlockRange64 range)
{
    auto it = EndToStart.find(range.End);
    if (it == EndToStart.end()) {
        return false;
    }

    EndToStart.erase(it);
    return true;
}

bool TDisjointRangeSet::Empty() const
{
    return EndToStart.empty();
}

size_t TDisjointRangeSet::Size() const
{
    return EndToStart.size();
}

TBlockRange64 TDisjointRangeSet::LeftmostRange() const
{
    if (EndToStart.empty()) {
        Y_DEBUG_ABORT_UNLESS(false);
        return {};
    }
    return TBlockRange64::MakeClosedInterval(
        EndToStart.begin()->second,
        EndToStart.begin()->first);
}

////////////////////////////////////////////////////////////////////////////////

TDisjointRangeSetIterator::TDisjointRangeSetIterator(
        const TDisjointRangeSet& rangeSet)
    : RangeSet(rangeSet)
    , Pos(RangeSet.EndToStart.cbegin())
{}

bool TDisjointRangeSetIterator::HasNext() const
{
    return RangeSet.EndToStart.end() != Pos;
}

TBlockRange64 TDisjointRangeSetIterator::Next()
{
    Y_DEBUG_ABORT_UNLESS(HasNext());
    auto result = TBlockRange64::MakeClosedInterval(Pos->second, Pos->first);
    ++Pos;
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
