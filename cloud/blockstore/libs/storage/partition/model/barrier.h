#pragma once

#include "public.h"

#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

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
        {}

        bool operator<(const TBarrier& rhs) const
        {
            return CommitId < rhs.CommitId;
        }
    };

    TSet<TBarrier> Barriers;

public:
    void AcquireBarrier(ui64 commitId);
    void AcquireBarrierN(ui64 commitId, ui32 refCount);
    void ReleaseBarrier(ui64 commitId);
    void ReleaseBarrierN(ui64 commitId, ui32 refCount);

    ui64 GetMinCommitId() const;
    ui64 GetMaxCommitId() const;

    void GetCommitIds(TVector<ui64>& result) const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
