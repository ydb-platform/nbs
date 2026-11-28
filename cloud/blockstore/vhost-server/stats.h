#pragma once

#include "public.h"

#include "critical_event.h"
#include "histogram.h"

#include <cloud/blockstore/libs/common/latency_sli.h>

#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/system/types.h>

#include <array>
#include <atomic>

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
    T FinalCount = {};
    T FinalErrors = {};

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
        , FinalCount{rhs.FinalCount}
        , FinalErrors{rhs.FinalErrors}
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
        FinalCount = rhs.FinalCount;
        FinalErrors = rhs.FinalErrors;

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
        FinalCount += rhs.FinalCount;
        FinalErrors += rhs.FinalErrors;

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
    lhs.FinalCount -= rhs.FinalCount;
    lhs.FinalErrors -= rhs.FinalErrors;

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

    void RecordLatency(bool write, ui64 bytes, ui64 elapsedCycles, bool failed,
                       const TLatencySliConfig* latencySli,
                       bool measurable = true) noexcept
    {
        auto& stats = Requests[write];
        ++(failed ? stats.FinalErrors : stats.FinalCount);
        if (!latencySli || !latencySli->Enabled) {
            return;
        }
        if (!measurable) {
            ++stats.LatencyUnknown;
            return;
        }
        // Measure the original backend request, including its parts and
        // decryption, using the same interval as the legacy timing histogram.
        const auto result = latencySli->Classify(
            write,
            bytes,
            failed ? 0 : CyclesToDurationSafe(elapsedCycles).MicroSeconds(),
            failed);
        auto& counter = result == ELatencySliResult::Good ? stats.LatencyGood
                      : result == ELatencySliResult::Bad ? stats.LatencyBad
                                                        : stats.LatencyUnknown;
        ++counter;
    }

    TStats() = default;

    template <typename U>
    explicit TStats(const TStats<U>& rhs) noexcept
        : Dequeued{rhs.Dequeued}
        , Submitted{rhs.Submitted}
        , SubFailed{rhs.SubFailed}
        , Completed{rhs.Completed}
        , CompFailed{rhs.CompFailed}
        , EncryptorErrors{rhs.EncryptorErrors}
        , Requests{rhs.Requests[0], rhs.Requests[1]}
        , Times{rhs.Times[0], rhs.Times[1]}
        , Sizes{rhs.Sizes[0], rhs.Sizes[1]}
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
        Requests = rhs.Requests;
        Times = rhs.Times;
        Sizes = rhs.Sizes;

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
    bool Fresh = true;
};

////////////////////////////////////////////////////////////////////////////////

void DumpStats(
    const TCompleteStats& completeStats,
    TSimpleStats& old,
    TDuration elapsed,
    IOutputStream& stream,
    ui64 cyclesPerMs,
    const TLatencySliConfig* latencySli = nullptr, bool final = false);

}   // namespace NCloud::NBlockStore::NVHostServer
