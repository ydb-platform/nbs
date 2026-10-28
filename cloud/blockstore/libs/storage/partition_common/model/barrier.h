#pragma once

#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBarriers
{
private:
    struct TBarrier
    {
        ui64 CommitId;
        ui32 RefCount;

        TBarrier(ui64 commitId, ui32 refCount)
            : CommitId(commitId)
            , RefCount(refCount)
        {
        }

        bool operator<(const TBarrier& rhs) const
        {
            return CommitId < rhs.CommitId;
        }
    };

    TSet<TBarrier> Barriers;

public:
    void AcquireBarrier(ui64 commitId);
    void AcquireBarrierN(ui64 commitId, ui32 refCount);
    // Return true when the last reference to commitId is released.
    bool ReleaseBarrier(ui64 commitId);
    bool ReleaseBarrierN(ui64 commitId, ui32 refCount);

    ui64 GetMinCommitId() const;
    ui64 GetMaxCommitId() const;

    void GetCommitIds(TVector<ui64>& result) const;

    // Appends referenced commit ids in [minCommitId, maxCommitId], ascending.
    void GetCommitIds(
        ui64 minCommitId,
        ui64 maxCommitId,
        TVector<ui64>& result) const;
};

}   // namespace NCloud::NBlockStore::NStorage
