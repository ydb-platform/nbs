#include "merged_blob_compression_policy.h"

#include "merged_blob_compression.h"

#include <cloud/blockstore/libs/storage/core/config.h>

#include <array>
#include <algorithm>
#include <chrono>
#include <mutex>

namespace NCloud::NBlockStore::NStorage::NPartition {

bool SelectMergedBlobCompression(
    const TStorageConfig& config,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    ui64 commitId,
    ui32 ordinal,
    bool background)
{
    if (!config.IsMergedBlobCompressionFeatureEnabled(
            cloudId, folderId, diskId))
    {
        return false;
    }
    const ui32 direct = config.GetDirectMergedBlobCompressionPercentage();
    const ui32 compaction = config.GetCompactionMergedBlobCompressionPercentage();
    if (direct > 100 || compaction > 100 ||
        config.GetMergedBlobCompressionCodec() != "lz4" ||
        config.GetMergedBlobCompressionChunkSize() != MergedBlobCompressionChunkSize ||
        config.GetMergedBlobCompressionMinSavingsPercentage() > 100)
    {
        return false;
    }

    // Explicit stable byte order, independent of physical size/channel and
    // standard library hash implementation. Decision precedes Patch selection.
    ui64 hash = 14695981039346656037ULL;
    const auto add = [&](ui8 byte) {
        hash = (hash ^ byte) * 1099511628211ULL;
    };
    for (unsigned char byte: diskId) {
        add(byte);
    }
    add(0);
    for (ui32 i = 0; i < 8; ++i) {
        add(commitId >> (i * 8));
    }
    for (ui32 i = 0; i < 4; ++i) {
        add(ordinal >> (i * 8));
    }
    add(background);
    return hash % 100 < (background ? compaction : direct);
}


namespace {

struct TBudget
{
    std::mutex Mutex;
    ui64 Bytes = 0;
    ui32 Slots = 0;
    ui64 PeakBytes = 0;
    ui32 PeakSlots = 0;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    NMonitoring::TDynamicCounters::TCounterPtr ReservedBytes;
    NMonitoring::TDynamicCounters::TCounterPtr ActiveOperations;
    NMonitoring::TDynamicCounters::TCounterPtr PeakReservedBytes;
    NMonitoring::TDynamicCounters::TCounterPtr PeakActiveOperations;
    NMonitoring::TDynamicCounters::TCounterPtr AdmissionAttempts;
    NMonitoring::TDynamicCounters::TCounterPtr AdmissionRejected;
    NMonitoring::TDynamicCounters::TCounterPtr AdmissionWaitMicros;

    void Publish()
    {
        if (Counters) {
            ReservedBytes->Set(Bytes);
            ActiveOperations->Set(Slots);
            PeakReservedBytes->Set(PeakBytes);
            PeakActiveOperations->Set(PeakSlots);
        }
    }
};

std::array<TBudget, 4>& GetBudgets()
{
    static std::array<TBudget, 4> pools;
    return pools;
}

struct TCompatibilityTelemetry
{
    std::mutex Mutex;
    NMonitoring::TDynamicCounters::TCounterPtr Rejections;
};

TCompatibilityTelemetry& GetCompatibilityTelemetry()
{
    static TCompatibilityTelemetry telemetry;
    return telemetry;
}

ui64 GetByteLimit(bool background)
{
    return ui64(background ? 256 : 512) * 1024 * 1024;
}

ui32 GetSlotLimit(bool background)
{
    return background ? 2 : 4;
}

}   // namespace

void RegisterMergedBlobCompressionCounters(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters)
{
    {
        auto& telemetry = GetCompatibilityTelemetry();
        std::lock_guard guard(telemetry.Mutex);
        telemetry.Rejections = counters->GetCounter("CompatibilityRejections", true);
    }
    for (ui32 i = 0; i < 4; ++i) {
        auto& pool = GetBudgets()[i];
        std::lock_guard guard(pool.Mutex);
        pool.Counters = counters->GetSubgroup("scope", i / 2 ? "background" : "foreground")
            ->GetSubgroup("operation", i % 2 ? "decode" : "encode");
        pool.Counters->GetCounter("ReservationLimitBytes")->Set(GetByteLimit(i / 2));
        pool.Counters->GetCounter("ActiveOperationLimit")->Set(GetSlotLimit(i / 2));
        // There is no compression work queue: rejection is immediate.
        pool.Counters->GetCounter("QueuedOperations")->Set(0);
        pool.ReservedBytes = pool.Counters->GetCounter("ReservedBytes");
        pool.ActiveOperations = pool.Counters->GetCounter("ActiveOperations");
        pool.PeakReservedBytes = pool.Counters->GetCounter("PeakReservedBytes");
        pool.PeakActiveOperations = pool.Counters->GetCounter("PeakActiveOperations");
        pool.AdmissionAttempts = pool.Counters->GetCounter("AdmissionAttempts", true);
        pool.AdmissionRejected = pool.Counters->GetCounter("AdmissionRejected", true);
        pool.AdmissionWaitMicros = pool.Counters->GetCounter("AdmissionWaitMicros", true);
        pool.Publish();
    }
}

void ReportMergedBlobCompatibilityRejection()
{
    auto& telemetry = GetCompatibilityTelemetry();
    std::lock_guard guard(telemetry.Mutex);
    if (telemetry.Rejections) {
        telemetry.Rejections->Inc();
    }
}

std::shared_ptr<void> TryAcquireMergedBlobBudget(
    bool background,
    bool read,
    ui64 bytes)
{
    auto& pool = GetBudgets()[ui32(background) * 2 + ui32(read)];
    const auto start = std::chrono::steady_clock::now();
    std::unique_lock guard(pool.Mutex);
    if (pool.Counters) {
        pool.AdmissionAttempts->Inc();
        pool.AdmissionWaitMicros->Add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count());
    }
    if (!bytes || bytes > GetByteLimit(background) ||
        pool.Slots >= GetSlotLimit(background) ||
        pool.Bytes > GetByteLimit(background) - bytes)
    {
        if (pool.Counters) {
            pool.AdmissionRejected->Inc();
        }
        return {};
    }

    pool.Bytes += bytes;
    ++pool.Slots;
    pool.PeakBytes = std::max(pool.PeakBytes, pool.Bytes);
    pool.PeakSlots = std::max(pool.PeakSlots, pool.Slots);
    pool.Publish();
    guard.unlock();
    const auto release = [bytes](void* value) {
        auto* p = static_cast<TBudget*>(value);
        std::lock_guard lock(p->Mutex);
        p->Bytes -= bytes;
        --p->Slots;
        p->Publish();
    };
    // shared_ptr invokes the deleter itself if control-block allocation fails.
    return std::shared_ptr<void>(&pool, release);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
