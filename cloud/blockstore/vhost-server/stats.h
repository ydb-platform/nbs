#pragma once

#include "public.h"

#include "critical_event.h"
#include "histogram.h"

#include <cloud/blockstore/libs/common/latency_sli.h>
#include <util/datetime/cputimer.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <array>
#include <atomic>
#include <optional>

class IOutputStream;

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

using TCpuCycles = ui64;

template <typename T>
using TTimeHistogram = THistogram<T, 7>;

template <typename T>
using TSizeHistogram = THistogram<T, 3>;

template <typename T>
struct TRequestStats
{
    T Count = {};
    T Bytes = {};
    T Errors = {};
    T Unaligned = {};
    T LatencyGood = {};
    T LatencyBad = {};
    T LatencyUnknown = {};

    TRequestStats() = default;

    template <typename U>
    explicit TRequestStats(const TRequestStats<U>& rhs) noexcept
        : Count{rhs.Count}
        , Bytes{rhs.Bytes}
        , Errors{rhs.Errors}
        , Unaligned{rhs.Unaligned}
        , LatencyGood{rhs.LatencyGood}
        , LatencyBad{rhs.LatencyBad}
        , LatencyUnknown{rhs.LatencyUnknown}
    {}

    template <typename U>
    TRequestStats& operator=(const TRequestStats<U>& rhs) noexcept
    {
        Count = rhs.Count;
        Bytes = rhs.Bytes;
        Errors = rhs.Errors;
        Unaligned = rhs.Unaligned;
        LatencyGood = rhs.LatencyGood;
        LatencyBad = rhs.LatencyBad;
        LatencyUnknown = rhs.LatencyUnknown;

        return *this;
    }

    template <typename U>
    TRequestStats& operator+=(const TRequestStats<U>& rhs) noexcept
    {
        Count += rhs.Count;
        Bytes += rhs.Bytes;
        Errors += rhs.Errors;
        Unaligned += rhs.Unaligned;
        LatencyGood += rhs.LatencyGood;
        LatencyBad += rhs.LatencyBad;
        LatencyUnknown += rhs.LatencyUnknown;

        return *this;
    }
};

template <typename T>
TRequestStats<T> operator-(TRequestStats<T> lhs, TRequestStats<T>& rhs) noexcept
{
    lhs.Count -= rhs.Count;
    lhs.Bytes -= rhs.Bytes;
    lhs.Errors -= rhs.Errors;
    lhs.Unaligned -= rhs.Unaligned;
    lhs.LatencyGood -= rhs.LatencyGood;
    lhs.LatencyBad -= rhs.LatencyBad;
    lhs.LatencyUnknown -= rhs.LatencyUnknown;

    return lhs;
}

template <typename T>
struct TStats
{
    T Dequeued = {};
    T Submitted = {};
    T SubFailed = {};
    T Completed = {};
    T CompFailed = {};
    T EncryptorErrors = {};

    std::array<TRequestStats<T>, 2> Requests = {};
    std::array<TTimeHistogram<T>, 2> Times = {};
    std::array<TSizeHistogram<T>, 2> Sizes = {};

    // Set once before workers start; never copied into monitoring snapshots.
    TLatencySliConfigPtr LatencySli;

    void SetLatencySli(TLatencySliConfigPtr config)
    {
        LatencySli = config && config->Enabled ? std::move(config) : nullptr;
    }

    void RecordLatency(
        bool write,
        ui64 bytes,
        ui64 elapsedCycles,
        bool failed)
    {
        if (!LatencySli) {
            return;
        }
        // AIO and direct-agent RDMA have no volume throttler. Retries and
        // device waits are included in the original request's elapsed time.
        const auto result = LatencySli->Classify(
            write, bytes, failed ? 0 : CyclesToDurationSafe(elapsedCycles).MicroSeconds(),
            failed);
        auto& stats = Requests[write];
        auto& counter = result == ELatencySliResult::Good ? stats.LatencyGood
                      : result == ELatencySliResult::Bad ? stats.LatencyBad
                                                        : stats.LatencyUnknown;
        ++counter;
    }

    TStats() = default;

    TStats(const TStats& rhs) noexcept
        requires std::is_same_v<T, ui64>
    {
        operator=<T>(rhs);
    }

    TStats& operator=(const TStats& rhs) noexcept
        requires std::is_same_v<T, ui64>
    {
        return operator=<T>(rhs);
    }

    template <typename U>
    explicit TStats(const TStats<U>& rhs) noexcept
        : Dequeued{rhs.Dequeued}
        , Submitted{rhs.Submitted}
        , SubFailed{rhs.SubFailed}
        , Completed{rhs.Completed}
        , CompFailed{rhs.CompFailed}
        , EncryptorErrors{rhs.EncryptorErrors}
        , Requests{TRequestStats<T>(rhs.Requests[0]), TRequestStats<T>(rhs.Requests[1])}
        , Times{TTimeHistogram<T>(rhs.Times[0]), TTimeHistogram<T>(rhs.Times[1])}
        , Sizes{TSizeHistogram<T>(rhs.Sizes[0]), TSizeHistogram<T>(rhs.Sizes[1])}
    {}

    template <typename U>
    TStats& operator=(const TStats<U>& rhs) noexcept
    {
        Dequeued = rhs.Dequeued;
        Submitted = rhs.Submitted;
        SubFailed = rhs.SubFailed;
        Completed = rhs.Completed;
        CompFailed = rhs.CompFailed;
        EncryptorErrors = rhs.EncryptorErrors;
        for (size_t i = 0; i != Requests.size(); ++i) {
            Requests[i].template operator=<U>(rhs.Requests[i]);
            Times[i].template operator=<U>(rhs.Times[i]);
            Sizes[i].template operator=<U>(rhs.Sizes[i]);
        }
        LatencySli.reset();

        return *this;
    }

    template <typename U>
    TStats& operator+=(const TStats<U>& rhs) noexcept
    {
        Dequeued += rhs.Dequeued;
        Submitted += rhs.Submitted;
        SubFailed += rhs.SubFailed;
        Completed += rhs.Completed;
        CompFailed += rhs.CompFailed;
        EncryptorErrors += rhs.EncryptorErrors;

        for (size_t i = 0; i != Requests.size(); ++i) {
            Requests[i] += rhs.Requests[i];
        }

        for (size_t i = 0; i != Times.size(); ++i) {
            Times[i] += rhs.Times[i];
        }

        for (size_t i = 0; i != Sizes.size(); ++i) {
            Sizes[i] += rhs.Sizes[i];
        }

        return *this;
    }
};

using TAtomicStats = TStats<std::atomic<ui64>>;
using TSimpleStats = TStats<ui64>;

////////////////////////////////////////////////////////////////////////////////
struct TCompleteStats {
    TSimpleStats SimpleStats;
    TCriticalEvents CriticalEvents;
};

////////////////////////////////////////////////////////////////////////////////

struct ICompletionStats
{
    virtual ~ICompletionStats() = default;

    virtual std::optional<TSimpleStats> Get(TDuration timeout) = 0;

    virtual void Sync(const TAtomicStats& stats) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICompletionStatsPtr CreateCompletionStats();

void DumpStats(
    const TCompleteStats& completeStats,
    TSimpleStats& old,
    TDuration elapsed,
    IOutputStream& stream,
    ui64 cyclesPerMs,
    const TLatencySliConfig* latencySli = nullptr);

}   // namespace NCloud::NBlockStore::NVHostServer
