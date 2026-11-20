#include "trace_processor.h"

#include "monitoring.h"
#include "trace_processor_mon.h"
#include "trace_reader.h"

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NCloud {

#define TRACE_VIEW_UT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)             \
    PROBE(                                                                     \
        ViewRequest,                                                           \
        GROUPS(), TYPES(ui64, TString), NAMES("requestId", "diskId"))          \
    PROBE(ViewMissing, GROUPS(), TYPES(TString), NAMES("diskId"))              \
    PROBE(                                                                     \
        ViewInvalid,                                                           \
        GROUPS(), TYPES(TString, TString), NAMES("requestId", "diskId"))
LWTRACE_DECLARE_PROVIDER(TRACE_VIEW_UT_PROVIDER)
LWTRACE_DEFINE_PROVIDER(TRACE_VIEW_UT_PROVIDER)
LWTRACE_USING(TRACE_VIEW_UT_PROVIDER)

namespace {

struct TReader final: ITraceReader
{
    TVector<TEntry> Entries;

    explicit TReader(TString id = "st_trace_logger")
        : ITraceReader(std::move(id))
    {}

    void Push(TThread::TId, const NLWTrace::TTrackLog& log) override
    {
        TEntry entry;
        entry.Ts = TInstant::Seconds(100 + Entries.size());
        entry.TrackLog = log;
        entry.Date = log.Items.front().TimestampCycles;
        entry.Tag = "tag<&>\"'";
        Entries.push_back(std::move(entry));
    }

    void ForEach(std::function<void(const TEntry&)> fn) override
    {
        for (const auto& entry: Entries) {
            fn(entry);
        }
    }

    void Reset() override
    {
        Entries.clear();
    }
};

struct TProcessor final: ITraceProcessor
{
    TVector<ITraceReaderPtr> Readers;
    bool Started = false;

    void Start() override
    {
        Started = true;
    }

    void Stop() override
    {
        Started = false;
    }

    void ForEach(std::function<void(ITraceReaderPtr)> fn) override
    {
        for (const auto& reader: Readers) {
            fn(reader);
        }
    }
};

struct TRequest final: NMonitoring::TMonService2HttpRequest
{
    TCgiParameters Params;

    TRequest(IOutputStream& out, const TString& query)
        : TMonService2HttpRequest(&out, nullptr, nullptr, nullptr, {}, nullptr)
        , Params(query)
    {}

    const TCgiParameters& GetParams() const override
    {
        return Params;
    }
};

struct TEnv
{
    NLWTrace::TManager Manager{*Singleton<NLWTrace::TProbeRegistry>(), true};
    std::shared_ptr<TReader> Reader = std::make_shared<TReader>();
    std::shared_ptr<TProcessor> Processor = std::make_shared<TProcessor>();
    IMonitoringServicePtr Monitoring = CreateMonitoringServiceStub();
    ITraceProcessorPtr Mon;

    explicit TEnv(
        TString url = "https://tracing.example/trace/{trace_id}?a=1&b=2")
    {
        NLWTrace::TQuery query;
        for (const auto* probe: {"ViewRequest", "ViewMissing", "ViewInvalid"}) {
            auto* block = query.AddBlocks();
            block->MutableProbeDesc()->SetName(probe);
            block->MutableProbeDesc()->SetProvider("TRACE_VIEW_UT_PROVIDER");
            block->AddAction()->MutableRunLogShuttleAction();
        }
        Manager.New("View", query);
        Reader->Entries.emplace_back();   // Empty tracks must be harmless.
        for (const auto& [id, disk]:
             {std::pair<ui64, TString>{7311, "disk-1"},
              {7312, "disk-2"},
              {0, "zero"}})
        {
            {
                NLWTrace::TOrbit orbit;
                LWTRACK(ViewRequest, orbit, id, disk);
            }
            Manager.ReadDepot("View", *Reader);
        }
        {
            NLWTrace::TOrbit orbit;
            LWTRACK(ViewMissing, orbit, TString("missing"));
        }
        {
            NLWTrace::TOrbit orbit;
            LWTRACK(ViewInvalid, orbit, TString("bad"), TString("invalid"));
        }
        Manager.ReadDepot("View", *Reader);
        Processor->Readers.push_back(Reader);
        Processor->Readers.push_back(std::make_shared<TReader>("unrelated"));
        Mon = CreateTraceProcessorMon(Monitoring, Processor, std::move(url));
    }

    TString Render(const TString& page, const TString& query = {})
    {
        auto rootPage = Monitoring->GetMonPage("tracelogs");
        UNIT_ASSERT(rootPage);
        auto* monPage =
            static_cast<NMonitoring::TIndexMonPage&>(*rootPage).FindPage(page);
        UNIT_ASSERT(monPage);
        TStringStream out;
        TRequest request(out, query);
        if (page == "json") {
            monPage->Output(request);
        } else {
            static_cast<NMonitoring::THtmlMonPage&>(*monPage).OutputContent(
                request);
        }
        return out.Str();
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TraceMonitoring)
{
    Y_UNIT_TEST(ShouldLinkOnlySelectedDiskAndInterval)
    {
        TEnv env;
        const auto html = env.Render(
            "random",
            "diskId=disk-1&from=1970-01-01T00:01:41Z&to=1970-01-01T00:01:41Z");
        UNIT_ASSERT(
            html.Contains("https://tracing.example/trace/"
                          "8f1c0000000000000000000000000000?a=1&amp;b=2"));
        UNIT_ASSERT(html.Contains("Open trace"));
        UNIT_ASSERT(!html.Contains("disk-2"));
        UNIT_ASSERT(html.Contains("tag&lt;&amp;&gt;"));
        UNIT_ASSERT(!html.Contains("tag<&>"));
        UNIT_ASSERT(!env.Render("slow").Contains("Open trace"));
        UNIT_ASSERT(
            !env.Render("random", "diskId=absent").Contains("Open trace"));
        UNIT_ASSERT(!env.Render("random", "from=1970-01-01T00:10:00Z")
                         .Contains("Open trace"));
        UNIT_ASSERT(!env.Render("random", "to=1970-01-01T00:00:01Z")
                         .Contains("Open trace"));
        UNIT_ASSERT(env.Render("random", "fsId=disk-1").Contains("Open trace"));
        env.Mon->Start();
        UNIT_ASSERT(env.Processor->Started);
        ui32 readers = 0;
        env.Mon->ForEach([&](ITraceReaderPtr) { ++readers; });
        UNIT_ASSERT_VALUES_EQUAL(readers, 2);
        env.Mon->Stop();
        UNIT_ASSERT(!env.Processor->Started);
    }

    Y_UNIT_TEST(ShouldKeepJsonContractAndRejectInvalidQueries)
    {
        TEnv env;
        const auto json = env.Render("json", "traceType=random&diskId=disk-1");
        UNIT_ASSERT(json.Contains("200"));
        UNIT_ASSERT(json.Contains("disk-1"));
        UNIT_ASSERT(!json.Contains("disk-2"));
        UNIT_ASSERT(!json.Contains("Open trace"));
        UNIT_ASSERT(env.Render("json").Contains("disk-2"));
        for (const auto* query:
             {"traceType=invalid",
              "from=bad",
              "to=bad", "from=2026-02-02T00:00:00Z&to=2026-01-01T00:00:00Z"})
        {
            const auto result = env.Render("json", query);
            UNIT_ASSERT(result.Contains("400"));
            UNIT_ASSERT(!result.Contains("200"));
            UNIT_ASSERT(!result.Contains("ViewRequest"));
        }
        UNIT_ASSERT(env.Render("json", "traceType=slow").Contains("[]"));
        UNIT_ASSERT(
            env.Render("random", "from=bad").Contains("Invalid time interval"));
    }

    Y_UNIT_TEST(ShouldNotLinkInvalidContextOrTemplate)
    {
        TEnv env;
        for (const auto* disk: {"zero", "missing", "invalid"}) {
            UNIT_ASSERT(!env.Render("random", TString("diskId=") + disk)
                             .Contains("Open trace"));
        }
        for (const auto* url:
             {"",
              "javascript:{trace_id}",
              "https://example/trace", "https://example/{trace_id}/{trace_id}"})
        {
            TEnv invalid(url);
            UNIT_ASSERT(!invalid.Render("random").Contains("Open trace"));
        }
        TEnv http("http://example/{trace_id}");
        UNIT_ASSERT(
            http.Render("random", "diskId=disk-1").Contains("Open trace"));
        const auto html = env.Render("random", "diskId=%22%3C%26%3E%27");
        UNIT_ASSERT(html.Contains("&quot;&lt;&amp;&gt;&#39;"));
        UNIT_ASSERT(!html.Contains("value=\"\"<&>'"));
    }

    Y_UNIT_TEST(ShouldFilterByRequestTimeInsteadOfCollectionTime)
    {
        TEnv env;
        for (const bool future: {false, true}) {
            auto log = env.Reader->Entries[1].TrackLog;
            const auto before = TInstant::Now();
            const auto cycles = GetCycleCount();
            const auto delta = DurationToCyclesSafe(TDuration::Seconds(5));
            log.Items[0].TimestampCycles =
                future ? cycles + delta : cycles - delta;
            TTraceReaderWithRingBuffer reader("st_trace_logger", "test");
            reader.Push(0, log);
            ui32 count = 0;
            reader.ForEach(
                [&](const TEntry& entry)
                {
                    ++count;
                    if (future) {
                        UNIT_ASSERT(entry.Ts >= before);
                        UNIT_ASSERT(entry.Ts <= TInstant::Now());
                    } else {
                        UNIT_ASSERT(entry.Ts >= before - TDuration::Seconds(6));
                        UNIT_ASSERT(entry.Ts <= before - TDuration::Seconds(4));
                    }
                });
            UNIT_ASSERT_VALUES_EQUAL(count, 1);
        }
    }

    Y_UNIT_TEST(ShouldPreserveDefaultMonitoring)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto mon =
            CreateTraceProcessorMon(monitoring, CreateTraceProcessorStub());
        auto rootPage = monitoring->GetMonPage("tracelogs");
        UNIT_ASSERT(rootPage);
        UNIT_ASSERT(
            static_cast<NMonitoring::TIndexMonPage&>(*rootPage).FindPage(
                "random"));
        mon->Start();
        mon->Stop();
    }
}

}   // namespace NCloud
