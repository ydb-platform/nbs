#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TRequestsInFlight
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TRequestsInFlight();
    ~TRequestsInFlight();

    static constexpr ui64 InvalidRequestId = Max<ui64>();

    struct TAddResult
    {
        bool Added = false;
        ui64 DuplicateRequestId = InvalidRequestId;
    };
    TAddResult TryAddRequest(ui64 requestId, TBlockRange64 blockRange);
    void RemoveRequest(ui64 requestId);

    size_t Size() const;
};

}   // namespace NCloud::NBlockStore::NStorage
