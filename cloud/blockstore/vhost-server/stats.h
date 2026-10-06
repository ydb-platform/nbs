#pragma once

#include "public.h"

#include "critical_event.h"
#include "histogram.h"

#include <util/datetime/base.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>
#include <util/system/types.h>

#include <array>
#include <atomic>
#include <optional>
#include <type_traits>
#include <utility>

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
    T IoSizeCount = {};
    T IoSizeBytes = {};
    T Errors = {};
    T Unaligned = {};

    TRequestStats() = default;

    TRequestStats(const TRequestStats& rhs) noexcept
        : TRequestStats()
    {
        *this = rhs;
    }

    template <typename U>
    explicit TRequestStats(const TRequestStats<U>& rhs) noexcept
        : Count{static_cast<ui64>(rhs.Count)}
        , Bytes{static_cast<ui64>(rhs.Bytes)}
        , Errors{static_cast<ui64>(rhs.Errors)}
        , Unaligned{static_cast<ui64>(rhs.Unaligned)}
    {
        const auto [count, bytes] = rhs.GetIoSize();
        IoSizeCount = count;
        IoSizeBytes = bytes;
    }

    TRequestStats& operator=(const TRequestStats& rhs) noexcept
    {
        return operator= <T>(rhs);
    }

    template <typename U>
    TRequestStats& operator=(const TRequestStats<U>& rhs) noexcept
    {
        Count = static_cast<ui64>(rhs.Count);
        Bytes = static_cast<ui64>(rhs.Bytes);
        Errors = static_cast<ui64>(rhs.Errors);
        Unaligned = static_cast<ui64>(rhs.Unaligned);

        const auto [count, bytes] = rhs.GetIoSize();
        with_lock (IoSizeLock) {
            IoSizeCount = count;
            IoSizeBytes = bytes;
        }

        return *this;
    }

    template <typename U>
    TRequestStats& operator+=(const TRequestStats<U>& rhs) noexcept
    {
        Count += rhs.Count;
        Bytes += rhs.Bytes;
        Errors += rhs.Errors;
        Unaligned += rhs.Unaligned;

        const auto [count, bytes] = rhs.GetIoSize();
        with_lock (IoSizeLock) {
            IoSizeCount += count;
            IoSizeBytes += bytes;
        }

        return *this;
    }

    void AddIoSize(ui64 bytes) noexcept
    {
        with_lock (IoSizeLock) {
            IoSizeCount += 1;
            IoSizeBytes += bytes;
        }
    }

    [[nodiscard]] std::pair<ui64, ui64> GetIoSize() const noexcept
    {
        auto guard = Guard(IoSizeLock);
        return {IoSizeCount, IoSizeBytes};
    }

private:
    // AIO completion workers and the snapshot thread share the atomic stats.
    // Both counters must be updated and copied under the same lock. Plain stats
    // are owned by one thread and do not need synchronization.
    mutable std::conditional_t<
        std::is_same_v<T, std::atomic<ui64>>,
        TAdaptiveLock,
        TFakeMutex>
        IoSizeLock;
};

template <typename T>
TRequestStats<T> operator-(TRequestStats<T> lhs, TRequestStats<T>& rhs) noexcept
{
    lhs.Count -= rhs.Count;
    lhs.Bytes -= rhs.Bytes;
    lhs.IoSizeCount -= rhs.IoSizeCount;
    lhs.IoSizeBytes -= rhs.IoSizeBytes;
    lhs.Errors -= rhs.Errors;
    lhs.Unaligned -= rhs.Unaligned;

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
};

////////////////////////////////////////////////////////////////////////////////

struct ICompletionStats
{
    virtual ~ICompletionStats() = default;

    virtual std::optional<TSimpleStats> Get(TDuration timeout) = 0;

    virtual void Sync(const TSimpleStats& stats) = 0;

    virtual void Sync(const TAtomicStats& stats) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICompletionStatsPtr CreateCompletionStats();

void DumpStats(
    const TCompleteStats& completeStats,
    TSimpleStats& old,
    TDuration elapsed,
    IOutputStream& stream,
    ui64 cyclesPerMs);

}   // namespace NCloud::NBlockStore::NVHostServer
