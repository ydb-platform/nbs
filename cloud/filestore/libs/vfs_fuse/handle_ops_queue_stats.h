#pragma once

#include <cloud/filestore/libs/diagnostics/module_stats.h>
#include <cloud/filestore/libs/vfs_fuse/counters/relaxed_counters.h>

#include <util/system/types.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class THandleOpsQueueStats final
    : public std::enable_shared_from_this<THandleOpsQueueStats>
    , public IModuleStats
{
private:
    const ui64 MaxSize;
    TRelaxedCounter Size;
    TRelaxedCounter OverflowErrorCount;
    TRelaxedCounter SerializationErrorCount;
    TRelaxedCounter ParseErrorCount;

public:
    explicit THandleOpsQueueStats(ui64 maxSize);

    TStringBuf GetName() const override;

    void RegisterCounters(
        const NMetrics::IMetricsRegistryPtr& localMetricsRegistry,
        const NMetrics::IMetricsRegistryPtr& aggregatableMetricsRegistry)
        override;

    void UpdateStats(TInstant now) override;

    void SetSize(ui64 size);
    void IncrementOverflowErrorCount();
    void IncrementSerializationErrorCount();
    void IncrementParseErrorCount();
};

using THandleOpsQueueStatsPtr = std::shared_ptr<THandleOpsQueueStats>;

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueueStatsPtr CreateHandleOpsQueueStats(ui64 maxSize);

}   // namespace NCloud::NFileStore::NFuse
