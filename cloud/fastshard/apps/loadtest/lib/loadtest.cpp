#include "loadtest.h"

#include "context.h"

#include <cloud/fastshard/libs/common/context.h>
#include <cloud/fastshard/libs/sn/client/durable.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <memory>

namespace NCloud::NFastShard::NLoadTest {

using silk::FiberFuture;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Records fetched per ReadJournalTail call while looking for the newest
// lsn the device holds.
constexpr ui32 TailBatch = 1024;

// How often the stopper fiber checks whether the run has stopped.
constexpr TDuration StopPollInterval = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

struct TActionStats
{
    const TString Name;

    std::atomic<ui64> Count{0};
    std::atomic<ui64> Errors{0};
    std::atomic<ui64> Bytes{0};
    // Retriable failures that were followed by another attempt; counted
    // by the durable client.
    std::atomic<ui64>& Retries;

    TAdaptiveLock Lock;
    TLatencyHistogram Hist;
    TString FirstError;

    TActionStats(TString name, std::atomic<ui64>& retries)
        : Name(std::move(name))
        , Retries(retries)
    {}

    void Record(TDuration elapsed, ui64 bytes)
    {
        Count.fetch_add(1);
        Bytes.fetch_add(bytes);
        with_lock (Lock) {
            Hist.RecordValue(elapsed);
        }
    }

    void RecordError(const NCloud::NProto::TError& error)
    {
        Errors.fetch_add(1);
        with_lock (Lock) {
            if (!FirstError) {
                FirstError = FormatError(error);
            }
        }
    }

    void Fill(NFileStore::NProto::TTestStats::TStats& stats)
    {
        stats.SetAction(Name);
        stats.SetCount(Count.load());
        stats.SetRequestBytes(Bytes.load());

        with_lock (Lock) {
            auto& latency = *stats.MutableLatency();
            latency.SetP50(Hist.GetValueAtPercentile(50));
            latency.SetP90(Hist.GetValueAtPercentile(90));
            latency.SetP95(Hist.GetValueAtPercentile(95));
            latency.SetP99(Hist.GetValueAtPercentile(99));
            latency.SetP999(Hist.GetValueAtPercentile(99.9));
            latency.SetMin(Hist.GetMin());
            latency.SetMax(Hist.GetMax());
            latency.SetMean(Hist.GetMean());
            latency.SetStdDeviation(Hist.GetStdDeviation());
        }
    }

    TString Summary(TDuration elapsed)
    {
        const ui64 count = Count.load();
        const double seconds = Max(elapsed.SecondsFloat(), 1e-9);

        TStringBuilder out;
        with_lock (Lock) {
            out << Name << ": " << count << " ok, " << Errors.load()
                << " errors, " << Retries.load() << " retries, "
                << Sprintf("%.1f", count / seconds) << " req/s, "
                << Sprintf("%.2f", Bytes.load() / seconds / 1_MB) << " MB/s";
            if (count) {
                out << ", latency us: mean " << Sprintf("%.0f", Hist.GetMean())
                    << " p50 " << Hist.GetValueAtPercentile(50) << " p90 "
                    << Hist.GetValueAtPercentile(90) << " p99 "
                    << Hist.GetValueAtPercentile(99) << " max "
                    << Hist.GetMax();
            }
            if (FirstError) {
                out << ", first error: " << FirstError;
            }
        }
        return out;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLoadTest final: public ILoadTest
{
private:
    const TAppContext& Ctx;
    const TLoadTestConfig Config;
    const IStorageNodePtr Client;
    const TDurableClientMetricsPtr Metrics;

    // Requests that give up once the run stops: reads and advances. Set-up
    // uses it too, but runs before the stopper starts.
    TFastShardContext StoppableCtx{0};
    // Requests that complete past the stop: writes (see DoWrite) and the
    // release.
    TFastShardContext PersistentCtx{1};

    // One page of payload, copied into every written page.
    const TString PagePattern;

    TInstant Deadline = TInstant::Max();

    // Requests handed out so far against Config.Requests.
    std::atomic<ui64> RequestsIssued{0};

    // Lsn of the newest record written or found in the journal; the
    // next record continues from it.
    std::atomic<ui64> LastLsn{0};

    // Set once every worker has exited; stops the reporter and the
    // stopper.
    std::atomic<bool> Done{false};

    // Cleared when the storage node answers AdvanceLsnLowWatermark with
    // E_NOT_IMPLEMENTED, so the run does not keep failing on it.
    std::atomic<bool> AdvanceSupported{true};

    TActionStats Writes{"WriteLogRecord", Metrics->WriteLogRecordRetries};
    TActionStats Reads{"ReadPages", Metrics->ReadPagesRetries};
    TActionStats Advances{
        "AdvanceLsnLowWatermark",
        Metrics->AdvanceLsnLowWatermarkRetries};

    TInstant Started;
    TInstant Finished;

    // Set-up failure (acquire, journal tail); the run does not start.
    TString SetupError;

public:
    TLoadTest(
            const TAppContext& ctx,
            TLoadTestConfig config,
            IStorageNodePtr client,
            TDurableClientMetricsPtr clientMetrics)
        : Ctx(ctx)
        , Config(std::move(config))
        , Client(std::move(client))
        , Metrics(std::move(clientMetrics))
        , PagePattern(Config.PageSize, 'x')
    {}

    NFileStore::NProto::TTestStats Run() override
    {
        //
        // The whole run happens in fibers: the sn client can only be
        // used from fiber context. The driver fiber does the set-up,
        // spawns the workers and waits for them; the calling thread
        // waits on the driver's future.
        //

        FiberFuture done;
        Spawn(&TLoadTest::DriverMain, &done);
        done.wait();

        return BuildStats();
    }

private:
    struct TFiberParams
    {
        TLoadTest* Self;
    };

    static_assert(sizeof(TFiberParams) <= silk::FIBER_PARAMETERS_SIZE);

    static int DriverMain(TFiberParams* params) noexcept
    {
        params->Self->RunDriver();
        return 0;
    }

    static int WorkerMain(TFiberParams* params) noexcept
    {
        params->Self->RunWorker();
        return 0;
    }

    static int ReporterMain(TFiberParams* params) noexcept
    {
        params->Self->RunReporter();
        return 0;
    }

    static int StopperMain(TFiberParams* params) noexcept
    {
        params->Self->RunStopper();
        return 0;
    }

    void Spawn(int (*main)(TFiberParams*) noexcept, FiberFuture* future)
    {
        const int r =
            FiberScheduler::run(main, TFiberParams{.Self = this}, future);
        Y_ABORT_UNLESS(r == 0, "failed to start fiber: %s", ::strerror(r));
    }

    void RunDriver()
    {
        const bool acquired = Acquire();
        if (acquired && FindLastLsn()) {
            RunLoad();
        }

        if (acquired) {
            Release();
        }
    }

    bool Acquire()
    {
        if (Config.NoAcquire) {
            return true;
        }

        NCloud::NProto::TAcquireDevicesRequest request;
        PrepareHeaders(*request.MutableHeaders());
        request.AddDeviceUUIDs(Config.DeviceUUID);
        request.SetGeneration(Config.Generation);
        auto response =
            Client->AcquireDevices(StoppableCtx, std::move(request));
        if (HasError(response)) {
            SetupError = "AcquireDevices: " + FormatError(response.GetError());
            return false;
        }
        return true;
    }

    void RunLoad()
    {
        Started = TInstant::Now();
        if (Config.Duration) {
            Deadline = Started + Config.Duration;
        }

        FiberFuture stopper;
        Spawn(&TLoadTest::StopperMain, &stopper);

        std::unique_ptr<FiberFuture[]> workers(
            new FiberFuture[Config.IoDepth]);
        for (ui32 i = 0; i < Config.IoDepth; ++i) {
            Spawn(&TLoadTest::WorkerMain, &workers[i]);
        }

        FiberFuture reporter;
        if (Config.ReportInterval) {
            Spawn(&TLoadTest::ReporterMain, &reporter);
        }

        for (ui32 i = 0; i < Config.IoDepth; ++i) {
            workers[i].wait();
        }
        Finished = TInstant::Now();
        Done.store(true);

        if (Config.ReportInterval) {
            reporter.wait();
        }
        stopper.wait();
    }

    // Continues the chain from the newest record the journal holds (or
    // from the lsn low watermark if it holds none: every record the
    // journal has dropped was below it).
    bool FindLastLsn()
    {
        ui64 last = 0;
        ui64 after = 0;
        for (;;) {
            NCloud::NProto::TReadJournalTailRequest request;
            PrepareHeaders(*request.MutableHeaders());
            request.SetDeviceUUID(Config.DeviceUUID);
            request.SetAfterLogSequenceNumber(after);
            request.SetMaxRecordCount(TailBatch);
            auto response =
                Client->ReadJournalTail(StoppableCtx, std::move(request));
            if (response.GetError().GetCode() == E_NOT_IMPLEMENTED) {
                // TODO(#6956): drop once every storage node serves the tail
                SILK_WARN(
                    "ReadJournalTail is not implemented by the storage node; "
                    "assuming an empty journal, records start at lsn 1");
                break;
            }
            if (HasError(response)) {
                SetupError =
                    "ReadJournalTail: " + FormatError(response.GetError());
                return false;
            }

            last = Max(last, response.GetLsnLowWatermark());
            for (const auto& record: response.GetRecords()) {
                last = Max(last, record.GetLogSequenceNumber());
            }
            if (response.RecordsSize() < TailBatch) {
                break;
            }
            after = last;
        }

        LastLsn.store(last);
        return true;
    }

    void Release()
    {
        if (Config.NoAcquire) {
            return;
        }

        NCloud::NProto::TReleaseDevicesRequest request;
        PrepareHeaders(*request.MutableHeaders());
        request.AddDeviceUUIDs(Config.DeviceUUID);
        auto response =
            Client->ReleaseDevices(PersistentCtx, std::move(request));
        if (HasError(response)) {
            SILK_ERROR(
                "ReleaseDevices: %s",
                FormatError(response.GetError()).c_str());
        }
    }

    // The run was stopped or ran out of time.
    bool StopPending() const
    {
        return Ctx.ShouldStop.load() || TInstant::Now() >= Deadline;
    }

    // Cancels StoppableCtx once the run stops, so a request retried under
    // it gives up.
    void RunStopper()
    {
        while (!Done.load()) {
            FiberScheduler::sleep(StopPollInterval.NanoSeconds());
            if (StopPending()) {
                StoppableCtx.Cancel.set();
            }
        }
    }

    // A read or advance the stop cut short is neither ok nor an error.
    bool CutShortByStop(const NCloud::NProto::TError& error) const
    {
        return StoppableCtx.Cancel.isSet() &&
               GetErrorKind(error) == EErrorKind::ErrorRetriable;
    }

    // Takes one request from the --requests budget. Returns false when the
    // budget is used up.
    bool TryTakeRequest()
    {
        return !Config.Requests ||
               RequestsIssued.fetch_add(1) < Config.Requests;
    }

    void RunWorker()
    {
        while (!StopPending() && TryTakeRequest()) {
            if (RandomNumber<ui32>(100) < Config.ReadPercent) {
                DoRead();
            } else {
                DoWrite();
            }
        }
    }

    void DoWrite()
    {
        // Allocating the lsn and its predecessor together keeps the
        // chain intact however the concurrent requests are reordered.
        const ui64 lsn = LastLsn.fetch_add(1) + 1;

        NCloud::NProto::TWriteLogRecordRequest request;
        PrepareHeaders(*request.MutableHeaders());
        request.SetDeviceUUID(Config.DeviceUUID);
        request.SetLogSequenceNumber(lsn);
        request.SetPrevLogSequenceNumber(lsn - 1);

        auto* group = request.AddPageGroups();
        group->SetFirstPageNo(RandomPageNo(Config.WritePages));
        for (ui32 i = 0; i < Config.WritePages; ++i) {
            group->AddContent(PagePattern);
        }

        // A rejected record is retried under the same lsn, even past the
        // stop: a skipped lsn strands every later record of the chain.
        const TInstant started = TInstant::Now();
        auto response =
            Client->WriteLogRecord(PersistentCtx, std::move(request));
        const TDuration latency = TInstant::Now() - started;
        if (HasError(response)) {
            Writes.RecordError(response.GetError());
            return;
        }
        Writes.Record(
            latency,
            static_cast<ui64>(Config.WritePages) * Config.PageSize);

        if (Config.AdvanceEvery && lsn % Config.AdvanceEvery == 0 &&
            AdvanceSupported.load())
        {
            DoAdvance(lsn);
        }
    }

    void DoRead()
    {
        NCloud::NProto::TReadPagesRequest request;
        PrepareHeaders(*request.MutableHeaders());
        request.SetDeviceUUID(Config.DeviceUUID);

        auto* ref = request.AddPageGroupRefs();
        ref->SetFirstPageNo(RandomPageNo(Config.ReadPages));
        ref->SetPageCount(Config.ReadPages);
        ref->SetPageSize(Config.PageSize);

        const TInstant started = TInstant::Now();
        auto response = Client->ReadPages(StoppableCtx, std::move(request));
        const TDuration latency = TInstant::Now() - started;
        if (HasError(response)) {
            if (!CutShortByStop(response.GetError())) {
                Reads.RecordError(response.GetError());
            }
            return;
        }

        ui64 bytes = 0;
        for (const auto& group: response.GetPageGroups()) {
            for (const auto& page: group.GetContent()) {
                bytes += page.size();
            }
        }
        Reads.Record(latency, bytes);
    }

    void DoAdvance(ui64 lsn)
    {
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request;
        PrepareHeaders(*request.MutableHeaders());
        request.SetDeviceUUID(Config.DeviceUUID);
        request.SetLsnLowWatermark(lsn);

        const TInstant started = TInstant::Now();
        auto response =
            Client->AdvanceLsnLowWatermark(StoppableCtx, std::move(request));
        const TDuration latency = TInstant::Now() - started;
        if (response.GetError().GetCode() == E_NOT_IMPLEMENTED) {
            // TODO(#6956): drop once every storage node serves it
            if (AdvanceSupported.exchange(false)) {
                SILK_WARN(
                    "AdvanceLsnLowWatermark is not implemented by the "
                    "storage node; the journal will not be trimmed");
            }
            return;
        }
        if (HasError(response)) {
            if (!CutShortByStop(response.GetError())) {
                Advances.RecordError(response.GetError());
            }
            return;
        }
        Advances.Record(latency, 0);
    }

    void RunReporter()
    {
        ui64 lastWrites = 0;
        ui64 lastReads = 0;
        TInstant last = Started;

        while (!Done.load()) {
            FiberScheduler::sleep(Config.ReportInterval.NanoSeconds());

            const TInstant now = TInstant::Now();
            const ui64 writes = Writes.Count.load();
            const ui64 reads = Reads.Count.load();
            const double seconds = Max((now - last).SecondsFloat(), 1e-9);

            SILK_INFO(
                "%6.0fs: write %.0f req/s, read %.0f req/s, "
                "errors %lu, retries %lu",
                (now - Started).SecondsFloat(),
                (writes - lastWrites) / seconds,
                (reads - lastReads) / seconds,
                static_cast<unsigned long>(
                    Writes.Errors.load() + Reads.Errors.load() +
                    Advances.Errors.load()),
                static_cast<unsigned long>(
                    Writes.Retries.load() + Reads.Retries.load() +
                    Advances.Retries.load()));

            lastWrites = writes;
            lastReads = reads;
            last = now;
        }
    }

    ui64 RandomPageNo(ui32 pages) const
    {
        return RandomNumber<ui64>(Config.PageCount - pages + 1);
    }

    void PrepareHeaders(NCloud::NProto::TDeviceRequestHeaders& headers) const
    {
        headers.SetClientId(Config.ClientId);
        if (Config.RequestTimeoutMs) {
            headers.SetRequestTimeout(Config.RequestTimeoutMs);
        }
    }

    NFileStore::NProto::TTestStats BuildStats()
    {
        NFileStore::NProto::TTestStats stats;
        stats.SetName(Config.Name);

        if (SetupError) {
            SILK_ERROR("%s", SetupError.c_str());
            stats.SetSuccess(false);
            return stats;
        }

        const TDuration elapsed = Finished - Started;
        ui64 errors = 0;
        for (auto* action: {&Writes, &Reads, &Advances}) {
            errors += action->Errors.load();
            if (action->Count.load() || action->Errors.load()) {
                action->Fill(*stats.AddStats());
                SILK_INFO("%s", action->Summary(elapsed).c_str());
            }
        }

        stats.SetSuccess(errors == 0);
        return stats;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ILoadTestPtr CreateLoadTest(
    const TAppContext& ctx,
    TLoadTestConfig config,
    IStorageNodePtr client,
    TDurableClientMetricsPtr clientMetrics)
{
    return std::make_shared<TLoadTest>(
        ctx,
        std::move(config),
        std::move(client),
        std::move(clientMetrics));
}

}   // namespace NCloud::NFastShard::NLoadTest
