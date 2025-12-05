#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/block_range_map.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TRequestsInFlight
{
private:
    TBlockRangeMap<ui64> Requests;

public:
    static constexpr ui64 InvalidRequestId = Max<ui64>();

    struct TAddResult
    {
        bool Added = false;
        ui64 DuplicateRequestId = InvalidRequestId;
    };
    TAddResult TryAddRequest(ui64 requestId, TBlockRange64 blockRange);
    void RemoveRequest(ui64 requestId);

    [[nodiscard]] size_t Size() const;
};

}   // namespace NCloud::NBlockStore::NStorage
