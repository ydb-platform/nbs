#pragma once

#include "public.h"

#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <util/datetime/base.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// The instance of this class is shared between all mirror partitions of the
// process. This allows to track the budget for direct write requests across all
// mirror disks.
// Note: methods of this class must be thread-safe.
class TPartitionBudgetManager
{
private:
    TStorageConfigPtr Config;
    TAdaptiveLock Lock;
    TLeakyBucket DirectWriteBandwidthQuota{1.0, 1.0, 1.0};

public:
    explicit TPartitionBudgetManager(TStorageConfigPtr config);
    ~TPartitionBudgetManager();

    // Checks if there is enough budget for a direct write request for a
    // mirrored disk.
    [[nodiscard]] bool HasEnoughDirectWriteBudget(TInstant ts, ui32 byteCount);
};

}   // namespace NCloud::NBlockStore::NStorage
