#include "stats.h"

#include "critical_event.h"

#include <library/cpp/json/json_writer.h>

#include <util/datetime/cputimer.h>
#include <util/stream/output.h>
#include <util/system/event.h>

#include <numeric>

namespace NCloud::NBlockStore::NVHostServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompletionStats
    : public ICompletionStats
{
private:
    TSimpleStats CompletionStats;
    bool IsCompletionStatsWaitTimeout = false;
    std::atomic_bool NeedUpdateCompletionStats = false;
    TAutoEvent CompletionStatsEvent;

public:
    std::optional<TSimpleStats> Get(TDuration timeout) override
    {
        if (!IsCompletionStatsWaitTimeout) {
            NeedUpdateCompletionStats = true;
        }

        bool signaled =
            CompletionStatsEvent.WaitT(timeout);
        if (!signaled) {
            IsCompletionStatsWaitTimeout = true;
            return {};
        }

        IsCompletionStatsWaitTimeout = false;

        return CompletionStats;
    }

    void Sync(const TSimpleStats& stats) override
    {
        if (!NeedUpdateCompletionStats) {
            return;
        }

        CompletionStats.Completed = stats.Completed;
        CompletionStats.CompFailed = stats.CompFailed;
        CompletionStats.EncryptorErrors = stats.EncryptorErrors;

        CompletionStats.Requests = stats.Requests;
        CompletionStats.Times = stats.Times;
        CompletionStats.Sizes = stats.Sizes;

        NeedUpdateCompletionStats = false;
        CompletionStatsEvent.Signal();
    }
};

////////////////////////////////////////////////////////////////////////////////

void WriteTimes(
    NJsonWriter::TBuf& buf,
    const auto& hist,
    const auto& prevHist,
    ui64 cyclesPerMs)
{
    buf.WriteKey("times");
    buf.BeginList();

    auto bucket = [&] (ui64 time, ui64 count) {
        buf.BeginList();
        buf.WriteULongLong(time);
        buf.WriteULongLong(count);
        buf.EndList();
    };

    ui64 prevTime = 0;
    ui64 prevCount = 0;

    hist.IterateDiffBuckets(prevHist, [&] (ui64 start, ui64 end, ui64 count) {
        const ui64 m = std::midpoint(start, end);
        const ui64 v = m * 1000 / cyclesPerMs;

        if (v == prevTime) {
            prevCount += count;
        } else {
            if (prevTime) {
                bucket(prevTime, prevCount);
            }

            prevTime = v;
            prevCount = count;
        }
    });

    if (prevTime) {
        bucket(prevTime, prevCount);
    }

    buf.EndList();
}

void WriteSizes(NJsonWriter::TBuf& buf, const auto& hist, const auto& prevHist)
{
    buf.WriteKey("sizes");
    buf.BeginList();

    hist.IterateDiffBuckets(prevHist, [&] (ui64 start, ui64 end, ui64 count) {
        Y_UNUSED(end);

        buf.BeginList();
        buf.WriteULongLong(start);
        buf.WriteULongLong(count);
        buf.EndList();
    });

    buf.EndList();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void DumpStats(
    const TCompleteStats& completeStats,
    TSimpleStats& old,
    TDuration elapsed,
    IOutputStream& stream,
    ui64 cyclesPerMs)
{
    const auto & stats = completeStats.SimpleStats;

    NJsonWriter::TBuf buf {NJsonWriter::HEM_DONT_ESCAPE_HTML, &stream};

    auto write = [&] (TStringBuf key, ui64 value) {
        buf.WriteKey(key);
        buf.WriteULongLong(value);
    };

    auto request = [&] (int kind, TStringBuf key) {
        const auto r = stats.Requests[kind] - old.Requests[kind];

        buf.WriteKey(key);
        buf.BeginObject();
        write("count", r.Count);
        write("bytes", r.Bytes);
        write("errors", r.Errors);
        write("unaligned", r.Unaligned);

        WriteTimes(buf, stats.Times[kind], old.Times[kind], cyclesPerMs);
        WriteSizes(buf, stats.Sizes[kind], old.Sizes[kind]);

        buf.EndObject();
    };

    buf.BeginObject();

    write("elapsed_ms", elapsed.MilliSeconds());
    write("dequeued", stats.Dequeued - old.Dequeued);
    write("submitted", stats.Submitted - old.Submitted);
    write("submission_failed", stats.SubFailed - old.SubFailed);
    write("completed", stats.Completed - old.Completed);
    write("failed", stats.CompFailed - old.CompFailed);
    write("encryptor_errors", stats.EncryptorErrors - old.EncryptorErrors);

    if (completeStats.CriticalEvents) {
        buf.WriteKey("crit_events");
        buf.BeginList();
        for (const auto& criticalEvent: completeStats.CriticalEvents) {
            buf.BeginObject();
            buf.WriteKey("name");
            buf.WriteString(criticalEvent.SensorName);
            buf.WriteKey("message");
            buf.WriteString(criticalEvent.Message);
            buf.EndObject();
        }
        buf.EndList();
    }

    request(0, "read");
    request(1, "write");

    buf.EndObject();
    stream << Endl;

    old = stats;
}

ICompletionStatsPtr CreateCompletionStats()
{
    return std::make_shared<TCompletionStats>();
}

}   // namespace NCloud::NBlockStore::NVHostServer
