#include "barrier.h"
#include <util/datetime/base.h>

#include <util/generic/ylimits.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

void TBarriers::AcquireBarrier(ui64 commitId)
{
    AcquireBarrierN(commitId, 1);
}

void TBarriers::AcquireBarrierN(ui64 commitId, ui32 refCount)
{
    auto it = Barriers.find(TBarrier(commitId, 0));
    if (it != Barriers.end()) {
        const_cast<TBarrier&>(*it).RefCount += refCount;
    } else {
        Barriers.emplace(commitId, refCount);
    }
}

void TBarriers::ReleaseBarrier(ui64 commitId)
{
    ReleaseBarrierN(commitId, 1);
}

void TBarriers::ReleaseBarrierN(ui64 commitId, ui32 refCount)
{
    Cerr << "ReleaseBarrierN commitId " << commitId << " refCount " << refCount << Endl;
    auto it = Barriers.find(TBarrier(commitId, 0));

    Cerr << "prev ReleaseBarrierN Barriers ";
    for (const auto& el : Barriers) {
        Cerr << el.CommitId << ' ' << el.RefCount << Endl;
    }
    Cerr << Endl;

    Y_ABORT_UNLESS(it != Barriers.end());
    Y_ABORT_UNLESS(it->RefCount >= refCount);
    if ((const_cast<TBarrier&>(*it).RefCount -= refCount) == 0) {
        Barriers.erase(it);
    }

    Cerr << "new ReleaseBarrierN Barriers ";
    for (const auto& el : Barriers) {
        Cerr << el.CommitId << ' ' << el.RefCount << Endl;
    }
    Cerr << Endl;
}

ui64 TBarriers::GetMinCommitId() const
{
    if (Barriers.empty()) {
        return Max();
    }

    return Barriers.begin()->CommitId;
}

ui64 TBarriers::GetMaxCommitId() const
{
    if (Barriers.empty()) {
        return Min();
    }

    return Barriers.rbegin()->CommitId;
}

void TBarriers::GetCommitIds(TVector<ui64>& result) const
{
    for (const auto& barrier: Barriers) {
        result.push_back(barrier.CommitId);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
