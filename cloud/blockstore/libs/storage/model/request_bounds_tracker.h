#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TRequestBoundsTracker
{
    struct TRangeInfo
    {
        ui64 RequestCount = 0;
    };

private:
    THashMap<ui64, TRangeInfo> RangesWithRequests;
    const ui64 BlockCountPerRange;

public:
    explicit TRequestBoundsTracker(ui64 blockSize);

    void AddRequest(TBlockRange64 r);

    void RemoveRequest(TBlockRange64 r);

    [[nodiscard]] bool OverlapsWithRequest(TBlockRange64 r) const;
};

}   // namespace NCloud::NBlockStore::NStorage
