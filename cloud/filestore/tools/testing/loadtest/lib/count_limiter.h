#pragma once

#include <cloud/filestore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TCountLimiter
{
private:
    const ui64 MaxCount;
    std::atomic<ui64> ReservedCount = 0;

    bool TryReserve()
    {
        if (!MaxCount) {
            // MaxCount == 0 means unlimited.
            return true;
        }

        ui64 count = ReservedCount.load(std::memory_order_relaxed);
        while (count < MaxCount){
            if (ReservedCount.compare_exchange_weak(
                count,
                count + 1,
                std::memory_order_relaxed))
            {
                return true;
            }
        }
        return count < MaxCount;
    }

public:
    explicit TCountLimiter(ui64 maxCount)
        : MaxCount(maxCount)
    {}

    bool TryReserveHandle()
    {
        return TryReserve();
    }

    bool TryReserveNode()
    {
        return TryReserve();
    }

    void Release()
    {
        if (MaxCount) {
            ReservedCount.fetch_sub(1, std::memory_order_relaxed);
        }
    }
};

using TCountLimiterPtr = std::shared_ptr<TCountLimiter>;

}   // namespace NCloud::NFileStore::NLoadTest
