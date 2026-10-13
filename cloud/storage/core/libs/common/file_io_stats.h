#pragma once

#include "public.h"

#include <util/generic/array_ref.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <array>
#include <atomic>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum class EFileIORequest: ui8
{
    Read,
    Write,
    MAX
};

constexpr size_t FileIORequestCount = static_cast<size_t>(EFileIORequest::MAX);

TStringBuf GetFileIORequestName(EFileIORequest request);

////////////////////////////////////////////////////////////////////////////////

struct TFileIORequestStats
{
    // Successfully completed operations
    ui64 Count = 0;
    // Operations completed with an error
    ui64 Errors = 0;
    // Sum of elapsed times of completed operations, in microseconds
    ui64 Time = 0;
    // Sum of requested bytes of completed operations
    ui64 RequestBytes = 0;
    // Started operations that have not completed yet
    ui64 InProgress = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Accumulates statistics of file I/O backend operations. Recording is
// lock-free and can be done concurrently from any thread.
class TFileIOStats
{
private:
    struct alignas(64) TCounters
    {
        std::atomic<ui64> Count = 0;
        std::atomic<ui64> Errors = 0;
        std::atomic<ui64> Time = 0;
        std::atomic<ui64> RequestBytes = 0;
        std::atomic<ui64> InProgress = 0;
        std::atomic<ui64> MaxTime = 0;
    };

    std::array<TCounters, FileIORequestCount> Counters;

public:
    // Returns the start timestamp (in cycles) of the operation
    ui64 RequestStarted(EFileIORequest request);

    void RequestCompleted(
        EFileIORequest request,
        ui64 startCycles,
        ui64 requestBytes,
        bool failed);

    [[nodiscard]] TFileIORequestStats GetStats(EFileIORequest request) const;

    // Returns the maximum operation time (in microseconds) since the previous
    // call and resets it
    ui64 ResetMaxTime(EFileIORequest request);

private:
    TCounters& GetCounters(EFileIORequest request);
    const TCounters& GetCounters(EFileIORequest request) const;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
ui64 GetTotalBufferSize(TArrayRef<const TArrayRef<T>> buffers)
{
    ui64 size = 0;
    for (const auto& buffer: buffers) {
        size += buffer.size();
    }
    return size;
}

////////////////////////////////////////////////////////////////////////////////

struct TFileIOStatsEntry
{
    TString Backend;
    TString ServiceId;
    TFileIOStatsPtr Stats;
};

// Process-wide registry of file I/O backend instances' statistics
class TFileIOStatsRegistry
{
private:
    mutable TMutex Lock;
    TVector<TFileIOStatsEntry> Entries;
    THashMap<TString, ui32> NextServiceIds;

public:
    // Creates statistics for a new backend instance with a unique (among the
    // instances of the backend) service identifier
    TFileIOStatsPtr Register(const TString& backend);

    // Returns the entries registered after the first `offset` ones. Entries
    // are never removed, so the order of entries is stable.
    TVector<TFileIOStatsEntry> GetEntries(size_t offset = 0) const;
};

TFileIOStatsPtr RegisterFileIOStats(
    const TFileIOStatsRegistryPtr& registry,
    const TString& backend);

}   // namespace NCloud
