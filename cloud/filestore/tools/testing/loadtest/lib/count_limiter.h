#pragma once

#include <cloud/filestore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TCountLimiter
{
private:
    const ui64 MaxFileCount;
    std::atomic<ui64> ReservedFileCount = 0;

public:
    explicit TCountLimiter(ui64 maxFileCount)
        : MaxFileCount(maxFileCount)
    {}

    bool TryReserve()
    {
        if (!MaxFileCount) {
            // MaxFileCount == 0 means unlimited.
            return true;
        }

        ui64 count = ReservedFileCount.load(std::memory_order_relaxed);
        while (count < MaxFileCount){
            if (ReservedFileCount.compare_exchange_weak(
                count,
                count + 1,
                std::memory_order_relaxed))
            {
                return true;
            }
        }
        return count < MaxFileCount;
    }

    void Release()
    {
        if (MaxFileCount) {
            ReservedFileCount.fetch_sub(1, std::memory_order_relaxed);
        }
    }
};

using TCountLimiterPtr = std::shared_ptr<TCountLimiter>;

}   // namespace NCloud::NFileStore::NLoadTest
