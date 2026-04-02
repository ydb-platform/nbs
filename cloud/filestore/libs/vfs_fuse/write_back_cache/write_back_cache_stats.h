#pragma once

#include "node_state_holder_stats.h"
#include "persistent_storage_stats.h"
#include "write_back_cache_state_stats.h"
#include "write_data_request_manager_stats.h"

#include <cloud/filestore/libs/diagnostics/public.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

enum class EReadDataRequestCacheStatus
{
    // A request wasn't served from the cache
    Miss,

    // A request was partially served from the cache
    PartialHit,

    // A request was fully served from the cache
    FullHit
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheInternalStats
{
    virtual ~IWriteBackCacheInternalStats() = default;

    virtual void ResetNonDerivativeCounters() = 0;

    virtual void FlushStarted() = 0;
    virtual void FlushCompleted() = 0;
    virtual void FlushFailed() = 0;

    virtual void AddReadDataStats(EReadDataRequestCacheStatus status) = 0;
};

using IWriteBackCacheInternalStatsPtr =
    std::shared_ptr<IWriteBackCacheInternalStats>;

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStats
{
    virtual ~IWriteBackCacheStats() = default;

    virtual IWriteBackCacheInternalStatsPtr
    GetWriteBackCacheInternalStats() const = 0;

    virtual IWriteBackCacheStateStatsPtr
    GetWriteBackCacheStateStats() const = 0;

    virtual INodeStateHolderStatsPtr GetNodeStateHolderStats() const = 0;

    virtual IWriteDataRequestManagerStatsPtr
    GetWriteDataRequestManagerStats() const = 0;

    virtual IPersistentStorageStatsPtr GetPersistentStorageStats() const = 0;

    virtual void ResetNonDerivativeCounters() = 0;
};

using IWriteBackCacheStatsPtr = std::shared_ptr<IWriteBackCacheStats>;

IWriteBackCacheStatsPtr CreateDummyWriteBackCacheStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
