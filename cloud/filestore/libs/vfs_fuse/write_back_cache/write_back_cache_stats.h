#pragma once

#include "node_state_holder_stats.h"
#include "persistent_storage_stats.h"
#include "write_back_cache_state_stats.h"
#include "write_data_request_manager_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/public.h>
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

struct TWriteBackCacheInternalMetrics
{
    struct TReadDataMetrics
    {
        NMetrics::IMetricPtr CacheFullHitCount;
        NMetrics::IMetricPtr CachePartialHitCount;
        NMetrics::IMetricPtr CacheMissCount;
    };

    TReadDataMetrics ReadData;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheInternalStats
{
    virtual ~IWriteBackCacheInternalStats() = default;

    virtual void AddReadDataStats(EReadDataRequestCacheStatus status) = 0;

    virtual TWriteBackCacheInternalMetrics CreateMetrics() const = 0;
};

using IWriteBackCacheInternalStatsPtr =
    std::shared_ptr<IWriteBackCacheInternalStats>;

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCacheMetrics
    : public TWriteBackCacheInternalMetrics
    , public TWriteBackCacheStateMetrics
    , public TNodeStateHolderMetrics
    , public TWriteDataRequestManagerMetrics
    , public TPersistentStorageMetrics
{
    TWriteBackCacheMetrics() = default;

    TWriteBackCacheMetrics(
        TWriteBackCacheInternalMetrics writeBackCacheInternalMetrics,
        TWriteBackCacheStateMetrics writeBackCacheStateMetrics,
        TNodeStateHolderMetrics nodeStateHolderMetrics,
        TWriteDataRequestManagerMetrics writeDataRequestManagerMetrics,
        TPersistentStorageMetrics persistentStorageMetrics)
        : TWriteBackCacheInternalMetrics(
              std::move(writeBackCacheInternalMetrics))
        , TWriteBackCacheStateMetrics(std::move(writeBackCacheStateMetrics))
        , TNodeStateHolderMetrics(std::move(nodeStateHolderMetrics))
        , TWriteDataRequestManagerMetrics(
              std::move(writeDataRequestManagerMetrics))
        , TPersistentStorageMetrics(std::move(persistentStorageMetrics))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStats
{
    virtual ~IWriteBackCacheStats() = default;

    virtual IWriteBackCacheInternalStatsPtr
    GetWriteBackCacheInternalStats() = 0;

    virtual IWriteBackCacheStateStatsPtr GetWriteBackCacheStateStats() = 0;

    virtual INodeStateHolderStatsPtr GetNodeStateHolderStats() = 0;

    virtual IWriteDataRequestManagerStatsPtr
    GetWriteDataRequestManagerStats() = 0;

    virtual IPersistentStorageStatsPtr GetPersistentStorageStats() = 0;

    virtual TWriteBackCacheMetrics CreateMetrics() const = 0;
};

using IWriteBackCacheStatsPtr = std::shared_ptr<IWriteBackCacheStats>;

IWriteBackCacheStatsPtr CreateWriteBackCacheStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
