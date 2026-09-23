#include "loadtest.h"

#include <cloud/fastshard/sn/client/client.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>
#include <cloud/fastshard/protos/device.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/stream/output.h>
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

////////////////////////////////////////////////////////////////////////////////

struct TActionStats
{
    const TString Name;

    std::atomic<ui64> Count{0};
    std::atomic<ui64> Errors{0};
    std::atomic<ui64> Bytes{0};

    TAdaptiveLock Lock;
    TLatencyHistogram Hist;
    TString FirstError;

    explicit TActionStats(TString name)
        : Name(std::move(name))
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

    void PrintSummary(IOutputStream& out, TDuration elapsed)
    {
        const ui64 count = Count.load();
        const double seconds = Max(elapsed.SecondsFloat(), 1e-9);

        with_lock (Lock) {
            out << Name << ": " << count << " ok, " << Errors.load()
                << " errors, " << Sprintf("%.1f", count / seconds) << " req/s, "
                << Sprintf("%.2f", Bytes.load() / seconds / 1_MB) << " MB/s";
            if (count) {
                out << ", latency us: p50 " << Hist.GetValueAtPercentile(50)
                    << " p90 " << Hist.GetValueAtPercentile(90) << " p99 "
                    << Hist.GetValueAtPercentile(99) << " max "
                    << Hist.GetMax();
            }
            if (FirstError) {
                out << ", first error: " << FirstError;
            }
            out << Endl;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLoadTest final: public ILoadTest
{
private:
    const TOptions Options;
    IStorageNodePtr Client;

    // One page of payload, copied into every written page.
    const TString PagePattern;

    std::atomic<bool> StopRequested{false};
    TInstant Deadline = TInstant::Max();

    // Requests handed out so far against Options.Requests.
    std::atomic<ui64> RequestsIssued{0};

    // Lsn of the newest record written or found in the journal; the
    // next record continues from it.
    std::atomic<ui64> LastLsn{0};

    // Set once every worker has exited; stops the progress reporter.
    std::atomic<bool> WorkersDone{false};

    // Cleared when the storage node answers AdvanceLsnLowWatermark with
    // E_NOT_IMPLEMENTED, so the run does not keep failing on it.
    std::atomic<bool> AdvanceSupported{true};

    TActionStats Writes{"WriteLogRecord"};
    TActionStats Reads{"ReadPages"};
    TActionStats Advances{"AdvanceLsnLowWatermark"};

    TInstant Started;
    TInstant Finished;

    // Set-up failure (acquire, journal tail); the run does not start.
    TString SetupError;

public:
    TLoadTest(TOptions options, IStorageNodePtr client)
        : Options(std::move(options))
        , Client(std::move(client))
        , PagePattern(Options.PageSize, 'x')
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
        const int r = FiberScheduler::run(
            &TLoadTest::DriverMain,
            TFiberParams{.Self = this},
            &done);
        if (r) {
            ythrow yexception() << "failed to start fiber: " << ::strerror(r);
        }
        done.wait();

        return BuildStats();
    }

    void Stop() override
    {
        StopRequested.store(true);
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

    void RunDriver()
    {
        if (!Client) {
            Client = CreateStorageNodeClient(Options.Host, Options.Port);
        }

        if (!Options.NoAcquire) {
            NCloud::NProto::TAcquireDevicesRequest request;
            PrepareHeaders(*request.MutableHeaders());
            request.AddDeviceUUIDs(Options.DeviceUUID);
            request.SetGeneration(Options.Generation);
            auto response = Client->AcquireDevices(std::move(request));
            if (HasError(response)) {
                SetupError =
                    "AcquireDevices: " + FormatError(response.GetError());
                return;
            }
        }

        if (!FindLastLsn()) {
            Release();
            return;
        }

        Started = TInstant::Now();
        if (Options.Duration) {
            Deadline = Started + Options.Duration;
        }

        std::unique_ptr<FiberFuture[]> workers(
            new FiberFuture[Options.IoDepth]);
        for (ui32 i = 0; i < Options.IoDepth; ++i) {
            const int r = FiberScheduler::run(
                &TLoadTest::WorkerMain,
                TFiberParams{.Self = this},
                &workers[i]);
            Y_ABORT_UNLESS(r == 0, "failed to start fiber: %s", ::strerror(r));
        }

        FiberFuture reporter;
        if (Options.ReportInterval) {
            const int r = FiberScheduler::run(
                &TLoadTest::ReporterMain,
                TFiberParams{.Self = this},
                &reporter);
            Y_ABORT_UNLESS(r == 0, "failed to start fiber: %s", ::strerror(r));
        }

        for (ui32 i = 0; i < Options.IoDepth; ++i) {
            workers[i].wait();
        }
        Finished = TInstant::Now();
        WorkersDone.store(true);

        if (Options.ReportInterval) {
            reporter.wait();
        }

        Release();
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
            request.SetDeviceUUID(Options.DeviceUUID);
            request.SetAfterLogSequenceNumber(after);
            request.SetMaxRecordCount(TailBatch);
            auto response = Client->ReadJournalTail(std::move(request));
            if (response.GetError().GetCode() == E_NOT_IMPLEMENTED) {
                // TODO(#6956): drop once every storage node serves the tail
                Cerr << "ReadJournalTail is not implemented by the storage "
                        "node; assuming an empty journal, records start at "
                        "lsn 1"
                     << Endl;
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
        if (Options.NoAcquire) {
            return;
        }

        NCloud::NProto::TReleaseDevicesRequest request;
        PrepareHeaders(*request.MutableHeaders());
        request.AddDeviceUUIDs(Options.DeviceUUID);
        auto response = Client->ReleaseDevices(std::move(request));
        if (HasError(response)) {
            Cerr << "ReleaseDevices: " << FormatError(response.GetError())
                 << Endl;
        }
    }

    bool ShouldStop()
    {
        if (StopRequested.load()) {
            return true;
        }
        if (TInstant::Now() >= Deadline) {
            return true;
        }
        if (Options.Requests && RequestsIssued.fetch_add(1) >= Options.Requests)
        {
            return true;
        }
        return false;
    }

    void RunWorker()
    {
        while (!ShouldStop()) {
            if (RandomNumber<ui32>(100) < Options.ReadPercent) {
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
        request.SetDeviceUUID(Options.DeviceUUID);
        request.SetLogSequenceNumber(lsn);
        request.SetPrevLogSequenceNumber(lsn - 1);

        auto* group = request.AddPageGroups();
        group->SetFirstPageNo(RandomPageNo(Options.WritePages));
        for (ui32 i = 0; i < Options.WritePages; ++i) {
            group->AddContent(PagePattern);
        }

        const TInstant started = TInstant::Now();
        auto response = Client->WriteLogRecord(std::move(request));
        if (HasError(response)) {
            Writes.RecordError(response.GetError());
            return;
        }
        Writes.Record(
            TInstant::Now() - started,
            static_cast<ui64>(Options.WritePages) * Options.PageSize);

        if (Options.AdvanceEvery && lsn % Options.AdvanceEvery == 0 &&
            AdvanceSupported.load())
        {
            DoAdvance(lsn);
        }
    }

    void DoRead()
    {
        NCloud::NProto::TReadPagesRequest request;
        PrepareHeaders(*request.MutableHeaders());
        request.SetDeviceUUID(Options.DeviceUUID);

        auto* ref = request.AddPageGroupRefs();
        ref->SetFirstPageNo(RandomPageNo(Options.ReadPages));
        ref->SetPageCount(Options.ReadPages);
        ref->SetPageSize(Options.PageSize);

        const TInstant started = TInstant::Now();
        auto response = Client->ReadPages(std::move(request));
        if (HasError(response)) {
            Reads.RecordError(response.GetError());
            return;
        }

        ui64 bytes = 0;
        for (const auto& group: response.GetPageGroups()) {
            for (const auto& page: group.GetContent()) {
                bytes += page.size();
            }
        }
        Reads.Record(TInstant::Now() - started, bytes);
    }

    void DoAdvance(ui64 lsn)
    {
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request;
        PrepareHeaders(*request.MutableHeaders());
        request.SetDeviceUUID(Options.DeviceUUID);
        request.SetLsnLowWatermark(lsn);

        const TInstant started = TInstant::Now();
        auto response = Client->AdvanceLsnLowWatermark(std::move(request));
        if (response.GetError().GetCode() == E_NOT_IMPLEMENTED) {
            // TODO(#6956): drop once every storage node serves it
            if (AdvanceSupported.exchange(false)) {
                Cerr << "AdvanceLsnLowWatermark is not implemented by the "
                        "storage node; the journal will not be trimmed"
                     << Endl;
            }
            return;
        }
        if (HasError(response)) {
            Advances.RecordError(response.GetError());
            return;
        }
        Advances.Record(TInstant::Now() - started, 0);
    }

    void RunReporter()
    {
        ui64 lastWrites = 0;
        ui64 lastReads = 0;
        TInstant last = Started;

        while (!WorkersDone.load()) {
            FiberScheduler::sleep(Options.ReportInterval.NanoSeconds());

            const TInstant now = TInstant::Now();
            const ui64 writes = Writes.Count.load();
            const ui64 reads = Reads.Count.load();
            const double seconds = Max((now - last).SecondsFloat(), 1e-9);

            Cerr << Sprintf(
                        "%6.0fs: write %.0f req/s, read %.0f req/s, "
                        "errors %lu",
                        (now - Started).SecondsFloat(),
                        (writes - lastWrites) / seconds,
                        (reads - lastReads) / seconds,
                        static_cast<unsigned long>(
                            Writes.Errors.load() + Reads.Errors.load() +
                            Advances.Errors.load()))
                 << Endl;

            lastWrites = writes;
            lastReads = reads;
            last = now;
        }
    }

    ui64 RandomPageNo(ui32 pages) const
    {
        return RandomNumber<ui64>(Options.PageCount - pages + 1);
    }

    void PrepareHeaders(NCloud::NProto::TDeviceRequestHeaders& headers) const
    {
        headers.SetClientId(Options.ClientId);
        if (Options.RequestTimeoutMs) {
            headers.SetRequestTimeout(Options.RequestTimeoutMs);
        }
    }

    NFileStore::NProto::TTestStats BuildStats()
    {
        NFileStore::NProto::TTestStats stats;
        stats.SetName(Options.Name);

        if (SetupError) {
            Cerr << SetupError << Endl;
            stats.SetSuccess(false);
            return stats;
        }

        const TDuration elapsed = Finished - Started;
        ui64 errors = 0;
        for (auto* action: {&Writes, &Reads, &Advances}) {
            errors += action->Errors.load();
            if (action->Count.load() || action->Errors.load()) {
                action->Fill(*stats.AddStats());
                action->PrintSummary(Cerr, elapsed);
            }
        }

        stats.SetSuccess(errors == 0);
        return stats;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ILoadTestPtr CreateLoadTest(TOptions options, IStorageNodePtr client)
{
    return std::make_shared<TLoadTest>(std::move(options), std::move(client));
}

}   // namespace NCloud::NFastShard::NLoadTest
