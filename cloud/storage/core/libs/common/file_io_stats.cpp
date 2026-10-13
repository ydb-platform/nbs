#include "file_io_stats.h"

#include <util/datetime/cputimer.h>
#include <util/string/cast.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetFileIORequestName(EFileIORequest request)
{
    switch (request) {
        case EFileIORequest::Read:
            return "ReadBlocks";
        case EFileIORequest::Write:
            return "WriteBlocks";
        case EFileIORequest::MAX:
            break;
    }

    Y_ABORT("unexpected file I/O request: %d", static_cast<int>(request));
}

////////////////////////////////////////////////////////////////////////////////

ui64 TFileIOStats::RequestStarted(EFileIORequest request)
{
    GetCounters(request).InProgress.fetch_add(1, std::memory_order_relaxed);

    return GetCycleCount();
}

void TFileIOStats::RequestCompleted(
    EFileIORequest request,
    ui64 startCycles,
    ui64 requestBytes,
    bool failed)
{
    const ui64 time =
        CyclesToDurationSafe(GetCycleCount() - startCycles).MicroSeconds();

    auto& counters = GetCounters(request);

    if (failed) {
        counters.Errors.fetch_add(1, std::memory_order_relaxed);
    } else {
        counters.Count.fetch_add(1, std::memory_order_relaxed);
    }

    counters.Time.fetch_add(time, std::memory_order_relaxed);
    counters.RequestBytes.fetch_add(requestBytes, std::memory_order_relaxed);

    ui64 maxTime = counters.MaxTime.load(std::memory_order_relaxed);
    while (maxTime < time && !counters.MaxTime.compare_exchange_weak(
                                 maxTime,
                                 time,
                                 std::memory_order_relaxed))
    {
    }

    counters.InProgress.fetch_sub(1, std::memory_order_relaxed);
}

TFileIORequestStats TFileIOStats::GetStats(EFileIORequest request) const
{
    const auto& counters = GetCounters(request);

    return {
        .Count = counters.Count.load(std::memory_order_relaxed),
        .Errors = counters.Errors.load(std::memory_order_relaxed),
        .Time = counters.Time.load(std::memory_order_relaxed),
        .RequestBytes = counters.RequestBytes.load(std::memory_order_relaxed),
        .InProgress = counters.InProgress.load(std::memory_order_relaxed),
    };
}

ui64 TFileIOStats::ResetMaxTime(EFileIORequest request)
{
    return GetCounters(request).MaxTime.exchange(0, std::memory_order_relaxed);
}

TFileIOStats::TCounters& TFileIOStats::GetCounters(EFileIORequest request)
{
    return Counters[static_cast<size_t>(request)];
}

const TFileIOStats::TCounters& TFileIOStats::GetCounters(
    EFileIORequest request) const
{
    return Counters[static_cast<size_t>(request)];
}

////////////////////////////////////////////////////////////////////////////////

TFileIOStatsPtr TFileIOStatsRegistry::Register(const TString& backend)
{
    auto stats = std::make_shared<TFileIOStats>();

    with_lock (Lock) {
        const ui32 id = NextServiceIds[backend]++;
        Entries.push_back({
            .Backend = backend,
            .ServiceId = ToString(id),
            .Stats = stats,
        });
    }

    return stats;
}

TVector<TFileIOStatsEntry> TFileIOStatsRegistry::GetEntries(size_t offset) const
{
    with_lock (Lock) {
        if (offset >= Entries.size()) {
            return {};
        }

        return {Entries.begin() + offset, Entries.end()};
    }
}

////////////////////////////////////////////////////////////////////////////////

TFileIOStatsPtr RegisterFileIOStats(
    const TFileIOStatsRegistryPtr& registry,
    const TString& backend)
{
    if (registry) {
        return registry->Register(backend);
    }

    return std::make_shared<TFileIOStats>();
}

}   // namespace NCloud
