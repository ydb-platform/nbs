#include "persistent_storage_stats.h"

#include "relaxed_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/metric.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPersistentStorageStats
    : public std::enable_shared_from_this<TPersistentStorageStats>
    , public IPersistentStorageStats
{
private:
    TRelaxedCounter RawCapacityBytesCounter;
    TRelaxedCombinedMaxCounter<> RawUsedBytesCounter;
    TRelaxedCombinedMaxCounter<> EntryCounter;
    TRelaxedCounter CorruptedCounter;

public:
    void SetPersistentStorageCounters(
        ui64 rawCapacityBytesCount,
        ui64 rawUsedBytesCount,
        ui64 entryCount,
        bool isCorrupted) override
    {
        RawCapacityBytesCounter.Set(static_cast<i64>(rawCapacityBytesCount));
        RawUsedBytesCounter.Set(static_cast<i64>(rawUsedBytesCount));
        EntryCounter.Set(static_cast<i64>(entryCount));
        CorruptedCounter.Set(isCorrupted ? 1 : 0);
    }

    TPersistentStorageMetrics CreatePersistentStorageMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .Storage = {
                .RawCapacityByteCount = CreateMetric(
                    [self] { return self->RawCapacityBytesCounter.Get(); }),
                .RawUsedByteCount = CreateMetric(
                    [self] { return self->RawUsedBytesCounter.GetCurrent(); }),
                .RawUsedByteMaxCount = CreateMetric(
                    [self] { return self->RawUsedBytesCounter.GetMax(); }),
                .EntryCount = CreateMetric(
                    [self] { return self->EntryCounter.GetCurrent(); }),
                .EntryMaxCount = CreateMetric(
                    [self] { return self->EntryCounter.GetMax(); }),
                .Corrupted = CreateMetric(
                    [self] { return self->CorruptedCounter.Get(); }),
            }};
    }

    void UpdatePersistentStorageStats() override
    {
        RawUsedBytesCounter.Update();
        EntryCounter.Update();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IPersistentStorageStatsPtr CreatePersistentStorageStats()
{
    return std::make_shared<TPersistentStorageStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
