#pragma once

#include "node_state_holder_stats.h"
#include "persistent_storage_stats.h"
#include "write_back_cache_state_stats.h"
#include "write_data_request_manager_stats.h"

#include <cloud/filestore/libs/diagnostics/public.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

struct IStatsRegistry;

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheInternalStats
{
    virtual ~IWriteBackCacheInternalStats() = default;

    virtual void IncrementReadDataCacheFullHit() = 0;
    virtual void IncrementReadDataCachePartialHit() = 0;
    virtual void IncrementReadDataCacheMiss() = 0;
};

using IWriteBackCacheInternalStatsPtr =
    std::shared_ptr<IWriteBackCacheInternalStats>;

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

    void Register(
        NMetrics::IMetricsRegistry& localMetricsRegistry,
        NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const;
};

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

    virtual TWriteBackCacheMetrics CreateMetrics() const = 0;
};

using IWriteBackCacheStatsPtr = std::shared_ptr<IWriteBackCacheStats>;

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateWriteBackCacheStats();

IModuleStatsPtr CreateWriteBackCacheModuleStats(
    IWriteBackCacheStatsPtr stats,
    std::function<void(TInstant now)> updateStatsFunc);

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
