#pragma once

#include <cloud/filestore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TFileCreationLimiter
{
private:
    const ui64 MaxFileCount;
    std::atomic<ui64> ReservedFileCount = 0;

public:
    explicit TFileCreationLimiter(ui64 maxFileCount)
        : MaxFileCount(maxFileCount)
    {}

    bool TryReserve()
    {
        if (!MaxFileCount) {
            return true;
        }

        auto count = ReservedFileCount.load(std::memory_order_relaxed);
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

using TFileCreationLimiterPtr = std::shared_ptr<TFileCreationLimiter>;

}   // namespace NCloud::NFileStore::NLoadTest
