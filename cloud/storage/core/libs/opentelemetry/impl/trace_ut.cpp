#include "helpers.h"
#include "trace_convert.h"

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/protos/lwtrace.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum ESimpleEnum
{
    ValueA,
    ValueB,
};

enum class EEnumClass
{
    ValueC,
    ValueD,
};

#define LWTRACE_UT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)                \
    PROBE(NoParam, GROUPS("Group"), TYPES(), NAMES())                          \
    PROBE(IntParam, GROUPS("Group"), TYPES(ui32), NAMES("value"))              \
    PROBE(                                                                     \
        TwoIntParam,                                                           \
        GROUPS("Group"), TYPES(ui32, ui32), NAMES("value1", "value2"))         \
    PROBE(                                                                     \
        RequestStarted,                                                        \
        GROUPS("Group"), TYPES(ui64), NAMES("requestStartTimeUs"))             \
    PROBE(StringParam, GROUPS("Group"), TYPES(TString), NAMES("svalue"))       \
    PROBE(                                                                     \
        SymbolParam,                                                           \
        GROUPS("Group"), TYPES(NLWTrace::TSymbol), NAMES("symbol"))            \
    PROBE(                                                                     \
        CheckParam, GROUPS("Group"), TYPES(NLWTrace::TCheck), NAMES("value"))  \
    PROBE(                                                                     \
        EnumParams,                                                            \
        GROUPS("Group"),                                                       \
        TYPES(ESimpleEnum, EEnumClass), NAMES("simpleEnum", "enumClass"))      \
    PROBE(InstantParam, GROUPS("Group"), TYPES(TInstant), NAMES("value"))      \
    PROBE(DurationParam, GROUPS("Group"), TYPES(TDuration), NAMES("value"))    \
    PROBE(                                                                     \
        ProtoEnum,                                                             \
        GROUPS("Group"), TYPES(NLWTrace::EOperatorType), NAMES("value"))       \
    PROBE(                                                                     \
        IntIntParams,                                                          \
        GROUPS("Group"), TYPES(ui32, ui64), NAMES("value1", "value2"))         \
    PROBE(                                                                     \
        InvalidContext,                                                        \
        GROUPS("Group"),                                                       \
        TYPES(TString, ui64, ui64, TString, TString),                          \
        NAMES(                                                                 \
            "requestId", "requestType", "diskId", "mediaKind", "requestSize")) \
    PROBE(                                                                     \
        ValidContext,                                                          \
        GROUPS("Group"),                                                       \
        TYPES(ui64, TString, TString, ui32, ui64),                             \
        NAMES(                                                                 \
            "requestId", "requestType", "diskId", "mediaKind", "requestSize")) \
    /**/

LWTRACE_DECLARE_PROVIDER(LWTRACE_UT_PROVIDER)
LWTRACE_DEFINE_PROVIDER(LWTRACE_UT_PROVIDER)
LWTRACE_USING(LWTRACE_UT_PROVIDER)

using namespace NLWTrace;

Y_UNIT_TEST_SUITE(TraceConverter)
{
    Y_UNIT_TEST(ShouldConvertSimpleSpan)
    {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(
            R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    RunLogShuttleAction { }
                }
            }
            )END",
            &q);

        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);

        {
            TOrbit a;

            LWTRACK(NoParam, a);
            LWTRACK(IntParam, a, 1);
            LWTRACK(TwoIntParam, a, 3, 4);
            LWTRACK(StringParam, a, "string");
        }

        struct
        {
            void Push(TThread::TId, const TTrackLog& tl)
            {
                auto spans = ConvertToOpenTelemetrySpans(tl).Spans;

                UNIT_ASSERT(spans.size() == 1);

                const auto& span = spans.front();
                const auto secondExport = ConvertToOpenTelemetrySpans(tl).Spans;
                UNIT_ASSERT_VALUES_UNEQUAL(
                    span.span_id(), secondExport.front().span_id());

                UNIT_ASSERT_VALUES_UNEQUAL(span.span_id(), TString(8, '\0'));
                UNIT_ASSERT_VALUES_UNEQUAL(span.trace_id(), TString(16, '\0'));

                UNIT_ASSERT_VALUES_EQUAL(span.events().size(), 4);

                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(0).name(),
                    "NoParam");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(0).attributes().size(),
                    0);

                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(1).name(),
                    "IntParam");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(1).attributes().size(),
                    1);
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(1).attributes(0).key(),
                    "value");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(1)
                        .attributes(0)
                        .value()
                        .int_value(),
                    1);

                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(2).name(),
                    "TwoIntParam");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(2).attributes().size(),
                    2);
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(2).attributes(0).key(),
                    "value1");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(2)
                        .attributes(0)
                        .value()
                        .int_value(),
                    3);
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(2).attributes(1).key(),
                    "value2");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(2)
                        .attributes(1)
                        .value()
                        .int_value(),
                    4);

                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(3).name(),
                    "StringParam");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(3).attributes().size(),
                    1);
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(3).attributes(0).key(),
                    "svalue");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(3)
                        .attributes(0)
                        .value()
                        .string_value(),
                    "string");
            }
        } reader;
        mngr.ReadDepot("Query1", reader);
    }

    Y_UNIT_TEST(ShouldAnchorDelayedTraceToRequestStart)
    {
        TManager manager(*Singleton<TProbeRegistry>(), true);
        TQuery query;
        auto* block = query.AddBlocks();
        block->MutableProbeDesc()->SetName("NoParam");
        block->MutableProbeDesc()->SetProvider("LWTRACE_UT_PROVIDER");
        block->AddAction()->MutableRunLogShuttleAction();
        manager.New("TimeAnchor", query);
        {
            TOrbit root;
            LWTRACK(NoParam, root);
            TOrbit child;
            root.Fork(child);
            LWTRACK(RequestStarted, child, ui64{10000000});
            root.Join(child);
            LWTRACK(RequestStarted, root, ui64{20000000});
            LWTRACK(IntParam, root, 1);
        }

        struct
        {
            size_t Count = 0;

            void Push(TThread::TId, const TTrackLog& log)
            {
                ++Count;
                const auto& items = log.Items;
                const auto& anchor = items[items.size() - 2];
                UNIT_ASSERT_VALUES_EQUAL(
                    anchor.Probe->Event.Name, "RequestStarted");
                const auto spans = ConvertToOpenTelemetrySpans(log).Spans;
                const auto epoch = TInstant::MicroSeconds(20000000);
                const auto& root = spans.front();
                UNIT_ASSERT_VALUES_EQUAL(
                    root.events(1).name(), "RequestStarted");
                UNIT_ASSERT_VALUES_EQUAL(
                    root.events(1).time_unix_nano(), epoch.NanoSeconds());
                UNIT_ASSERT_VALUES_EQUAL(
                    root.start_time_unix_nano(),
                    (epoch - CyclesToDurationSafe(
                                 anchor.TimestampCycles -
                                 items.front().TimestampCycles))
                        .NanoSeconds());
                UNIT_ASSERT_VALUES_EQUAL(
                    root.end_time_unix_nano(),
                    (epoch +
                     CyclesToDurationSafe(
                         items.back().TimestampCycles - anchor.TimestampCycles))
                        .NanoSeconds());
            }
        } reader;

        manager.ReadDepot("TimeAnchor", reader);
        UNIT_ASSERT_VALUES_EQUAL(reader.Count, 1);
    }

    Y_UNIT_TEST(ShouldPreserveSymbolAndCheckAttributes)
    {
        TManager manager(*Singleton<TProbeRegistry>(), true);
        TQuery query;
        auto* block = query.AddBlocks();
        block->MutableProbeDesc()->SetName("NoParam");
        block->MutableProbeDesc()->SetProvider("LWTRACE_UT_PROVIDER");
        block->AddAction()->MutableRunLogShuttleAction();
        manager.New("Attributes", query);
        {
            TOrbit orbit;
            LWTRACK(NoParam, orbit);
            LWTRACK(SymbolParam, orbit, LWTRACE_INLINE_SYMBOL("WriteBlocks"));
            LWTRACK(CheckParam, orbit, TCheck(-17));
        }

        struct
        {
            ui32 Count = 0;

            void Push(TThread::TId, const TTrackLog& log)
            {
                ++Count;
                const auto spans = ConvertToOpenTelemetrySpans(log).Spans;
                UNIT_ASSERT_VALUES_EQUAL(spans.size(), 1);
                const auto& events = spans.front().events();
                UNIT_ASSERT_VALUES_EQUAL(events.size(), 3);
                const auto& symbol = events[1].attributes(0).value();
                const auto& check = events[2].attributes(0).value();
                UNIT_ASSERT(symbol.has_string_value());
                UNIT_ASSERT_VALUES_EQUAL(symbol.string_value(), "WriteBlocks");
                UNIT_ASSERT(check.has_int_value());
                UNIT_ASSERT_VALUES_EQUAL(check.int_value(), -17);
            }
        } reader;

        manager.ReadDepot("Attributes", reader);
        UNIT_ASSERT_VALUES_EQUAL(reader.Count, 1);
    }

    Y_UNIT_TEST(ShouldRejectWrongContextTypes)
    {
        TManager manager(*Singleton<TProbeRegistry>(), true);
        TQuery query;
        auto* block = query.AddBlocks();
        block->MutableProbeDesc()->SetName("InvalidContext");
        block->MutableProbeDesc()->SetProvider("LWTRACE_UT_PROVIDER");
        block->AddAction()->MutableRunLogShuttleAction();
        manager.New("InvalidContext", query);
        {
            TOrbit orbit;
            LWTRACK(
                InvalidContext,
                orbit,
                TString("7311"),
                ui64{1}, ui64{2}, TString("3"), TString("4096"));
        }

        struct
        {
            ui32 Count = 0;

            void Push(TThread::TId, const TTrackLog& log)
            {
                ++Count;
                const auto info = ConvertToOpenTelemetrySpans(log);
                UNIT_ASSERT_VALUES_EQUAL(info.RequestId, 0);
                UNIT_ASSERT_VALUES_EQUAL(info.RequestSize, 0);
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(info.MediaKind), 0);
                UNIT_ASSERT(info.DiskId.empty());
                UNIT_ASSERT_VALUES_EQUAL(info.Spans.size(), 1);
                const auto& span = info.Spans.front();
                UNIT_ASSERT(span.name().empty());
                UNIT_ASSERT_VALUES_UNEQUAL(span.trace_id(), TString(16, '\0'));
                UNIT_ASSERT_VALUES_EQUAL(span.events_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(span.events(0).attributes_size(), 5);
                UNIT_ASSERT_VALUES_EQUAL(
                    span.events(0).attributes(0).value().string_value(),
                    "7311");
            }
        } reader;

        manager.ReadDepot("InvalidContext", reader);
        UNIT_ASSERT_VALUES_EQUAL(reader.Count, 1);
    }

    Y_UNIT_TEST(ShouldPreserveTypedContext)
    {
        TManager manager(*Singleton<TProbeRegistry>(), true);
        TQuery query;
        auto* block = query.AddBlocks();
        block->MutableProbeDesc()->SetName("ValidContext");
        block->MutableProbeDesc()->SetProvider("LWTRACE_UT_PROVIDER");
        block->AddAction()->MutableRunLogShuttleAction();
        manager.New("ValidContext", query);
        {
            TOrbit orbit;
            LWTRACK(
                ValidContext,
                orbit,
                ui64{7311},
                TString("ReadBlocks"),
                TString("disk-1"), ui32{NProto::STORAGE_MEDIA_SSD}, ui64{4096});
        }

        struct
        {
            ui32 Count = 0;

            void Push(TThread::TId, const TTrackLog& log)
            {
                ++Count;
                const auto info = ConvertToOpenTelemetrySpans(log);
                UNIT_ASSERT_VALUES_EQUAL(info.RequestId, 7311);
                UNIT_ASSERT_VALUES_EQUAL(info.RequestSize, 4096);
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<int>(info.MediaKind),
                    static_cast<int>(NProto::STORAGE_MEDIA_SSD));
                UNIT_ASSERT_VALUES_EQUAL(info.DiskId, "disk-1");
                UNIT_ASSERT_VALUES_EQUAL(info.Spans.size(), 1);
                const auto& span = info.Spans.front();
                UNIT_ASSERT_VALUES_EQUAL(span.name(), "ReadBlocks");
                const ui64 traceId[] = {7311, 0};
                UNIT_ASSERT_VALUES_EQUAL(
                    span.trace_id(),
                    TString(
                        reinterpret_cast<const char*>(traceId),
                        sizeof(traceId)));
                UNIT_ASSERT_VALUES_EQUAL(span.events_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(span.events(0).attributes_size(), 5);
            }
        } reader;

        manager.ReadDepot("ValidContext", reader);
        UNIT_ASSERT_VALUES_EQUAL(reader.Count, 1);
    }

    Y_UNIT_TEST(ShouldConvertForkJoinSpan)
    {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(
            R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    RunLogShuttleAction { }
                }
            }
            )END",
            &q);

        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);

        {
            TOrbit a, b, c, d;

            // Graph:
            //         c
            //        / \
            //     b-f-b-j-b
            //    /         \
            // a-f-a-f-a-j-a-j-a
            //        \ /
            //         d
            //
            // Merged track:
            //   a-f(b)-a-f(d)-a-j(d,1)-d-a-j(b,6)-b-f(c)-b-j(c,1)-c-b-a

            LWTRACK(NoParam, a);
            a.Fork(b);
            LWTRACK(IntParam, a, 1);
            a.Fork(d);
            LWTRACK(IntParam, a, 2);

            LWTRACK(IntParam, b, 3);
            b.Fork(c);
            LWTRACK(IntParam, b, 4);

            LWTRACK(IntParam, c, 5);
            b.Join(c);
            LWTRACK(IntParam, b, 6);

            LWTRACK(IntParam, d, 7);
            a.Join(d);
            LWTRACK(IntParam, a, 8);

            a.Join(b);
            LWTRACK(IntParam, a, 9);
        }

        struct
        {
            void Push(TThread::TId, const TTrackLog& tl)
            {
                const auto spans = ConvertToOpenTelemetrySpans(tl).Spans;

                UNIT_ASSERT_VALUES_EQUAL(spans.size(), 4);
                const auto second = ConvertToOpenTelemetrySpans(tl).Spans;
                THashSet<TString> ids;
                for (const auto* converted: {&spans, &second}) {
                    for (const auto& span: *converted) {
                        UNIT_ASSERT(ids.insert(span.span_id()).second);
                    }
                }

                auto getValues = [](const auto& span)
                {
                    TVector<int> values;
                    for (const auto& event: span.events()) {
                        if (event.name() == "NoParam") {
                            continue;
                        }
                        UNIT_ASSERT_VALUES_EQUAL(event.name(), "IntParam");
                        UNIT_ASSERT_VALUES_EQUAL(event.attributes().size(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(
                            event.attributes(0).key(),
                            "value");
                        values.push_back(
                            event.attributes(0).value().int_value());
                    }

                    return values;
                };

                auto firstTraceId = spans[0].trace_id();
                UNIT_ASSERT_VALUES_EQUAL(16, firstTraceId.size());

                UNIT_ASSERT(AllOf(
                    spans,
                    [&](const auto& span)
                    { return span.trace_id() == firstTraceId; }));

                UNIT_ASSERT(!spans[0].parent_span_id());
                UNIT_ASSERT_VALUES_EQUAL(
                    getValues(spans[0]),
                    (TVector<int>{1, 2, 8, 9}));
                UNIT_ASSERT_VALUES_EQUAL(8, spans[0].span_id().size());

                UNIT_ASSERT_VALUES_EQUAL(
                    spans[0].span_id(),
                    spans[1].parent_span_id());
                UNIT_ASSERT_VALUES_EQUAL(
                    (TVector<int>{7}),
                    getValues(spans[1]));
                UNIT_ASSERT_VALUES_EQUAL(8, spans[1].span_id().size());

                UNIT_ASSERT_VALUES_EQUAL(
                    spans[0].span_id(),
                    spans[2].parent_span_id());
                UNIT_ASSERT_VALUES_EQUAL(
                    getValues(spans[2]),
                    (TVector<int>{3, 4, 6}));
                UNIT_ASSERT_VALUES_EQUAL(8, spans[2].span_id().size());

                UNIT_ASSERT_VALUES_EQUAL(
                    spans[2].span_id(),
                    spans[3].parent_span_id());
                UNIT_ASSERT_VALUES_EQUAL(
                    (TVector<int>{5}),
                    getValues(spans[3]));
                UNIT_ASSERT_VALUES_EQUAL(8, spans[3].span_id().size());

                auto getTime = [](const auto& spans, int i, int j)
                {
                    return spans[i].events(j).time_unix_nano();
                };

                TVector eventTimes{
                    getTime(spans, 0, 0),
                    spans[2].start_time_unix_nano(),
                    getTime(spans, 0, 1),
                    spans[1].start_time_unix_nano(),
                    getTime(spans, 0, 2),
                    getTime(spans, 2, 0),
                    spans[3].start_time_unix_nano(),
                    getTime(spans, 2, 1),
                    getTime(spans, 3, 0),
                    spans[3].end_time_unix_nano(),
                    getTime(spans, 2, 2),
                    getTime(spans, 1, 0),
                    spans[1].end_time_unix_nano(),
                    getTime(spans, 0, 3),
                    spans[2].end_time_unix_nano(),
                    getTime(spans, 0, 4)};

                for (size_t i = 0; i < eventTimes.size() - 1; ++i) {
                    UNIT_ASSERT(eventTimes[i] <= eventTimes[i + 1]);
                }
            }
        } reader;
        mngr.ReadDepot("Query1", reader);
    }

    Y_UNIT_TEST(ShouldConvertTraceWithInvalidSpanIds)
    {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(
            R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    RunLogShuttleAction { }
                }
            }
            )END",
            &q);

        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);

        {
            TOrbit a, b, c, d;

            // Graph:
            //         c
            //        / \
            //     b-f-b-j-b
            //    /         \
            // a-f-a-f-a-j-a-j-a
            //        \ /
            //         d
            //
            // Merged track:
            //   a-f(b)-a-f(d)-a-j(d,1)-d-a-j(b,6)-b-f(c)-b-j(c,1)-c-b-a

            LWTRACK(NoParam, a);
            a.Fork(b);
            LWTRACK(IntParam, a, 1);
            a.Fork(d);
            LWTRACK(IntParam, a, 2);

            LWTRACK(IntParam, b, 3);
            b.Fork(c);
            LWTRACK(IntParam, b, 4);

            LWTRACK(IntParam, c, 5);
            b.Join(c);
            LWTRACK(IntParam, b, 6);

            LWTRACK(IntParam, d, 7);
            a.Join(d);
            LWTRACK(IntParam, a, 8);

            a.Join(b);
            LWTRACK(IntParam, a, 9);
        }

        struct
        {
            void Push(TThread::TId, const TTrackLog& tl)
            {
                // Simulate that spanids for fork joins were corrupted
                auto& tlCorrupted = const_cast<TTrackLog&>(tl);
                for (auto& item: tlCorrupted.Items) {
                    const auto* name = item.Probe->Event.Name;
                    if (TStringBuf(name) == "Fork" ||
                        TStringBuf(name) == "Join")
                    {
                        auto& param = item.Params.Param[0];
                        memset(param.Data, 0, LWTRACE_MAX_PARAM_SIZE);
                    }
                }

                auto spans = ConvertToOpenTelemetrySpans(tl).Spans;

                UNIT_ASSERT_VALUES_EQUAL(4, spans.size());

                // No spans with same spanId.
                THashSet<TString> spanIds;
                for (const auto& span: spans) {
                    auto [_, inserted] = spanIds.insert(span.span_id());
                    UNIT_ASSERT(inserted);
                }

                auto getTime = [](const auto& spans, int i, int j)
                {
                    return spans[i].events(j).time_unix_nano();
                };

                // Right order between events and end of spans is saved.
                TVector eventTimes{
                    getTime(spans, 0, 0),
                    getTime(spans, 0, 1),
                    getTime(spans, 0, 2),
                    getTime(spans, 2, 0),
                    getTime(spans, 2, 1),
                    getTime(spans, 3, 0),
                    spans[3].end_time_unix_nano(),
                    getTime(spans, 2, 2),
                    getTime(spans, 1, 0),
                    spans[1].end_time_unix_nano(),
                    getTime(spans, 0, 3),
                    spans[2].end_time_unix_nano(),
                    getTime(spans, 0, 4)};

                for (size_t i = 0; i < eventTimes.size() - 1; ++i) {
                    UNIT_ASSERT(eventTimes[i] <= eventTimes[i + 1]);
                }
            }
        } reader;
        mngr.ReadDepot("Query1", reader);
    }

    Y_UNIT_TEST(ShouldSeparateReusedShuttlesAfterSequentialJoins)
    {
        TManager manager(*Singleton<TProbeRegistry>(), true);
        TQuery query;
        auto* block = query.AddBlocks();
        block->MutableProbeDesc()->SetName("NoParam");
        block->MutableProbeDesc()->SetProvider("LWTRACE_UT_PROVIDER");
        block->AddAction()->MutableRunLogShuttleAction();
        manager.New("Repeated", query);
        {
            TOrbit parent;
            LWTRACK(NoParam, parent);
            for (ui32 attempt = 0; attempt < 4; ++attempt) {
                TOrbit child;
                parent.Fork(child);
                LWTRACK(IntParam, child, attempt);
                parent.Join(child);
            }
        }

        struct
        {
            ui32 Count = 0;

            void Push(TThread::TId, const TTrackLog& log)
            {
                ++Count;
                const auto spans = ConvertToOpenTelemetrySpans(log).Spans;
                UNIT_ASSERT_VALUES_EQUAL(spans.size(), 5);
                THashSet<TString> ids;
                for (const auto& span: spans) {
                    UNIT_ASSERT(ids.insert(span.span_id()).second);
                }
                for (ui32 i = 1; i != spans.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        spans[i].parent_span_id(), spans[0].span_id());
                    UNIT_ASSERT_VALUES_EQUAL(
                        spans[i].events(0).attributes(0).value().int_value(),
                        i - 1);
                }
            }
        } reader;

        manager.ReadDepot("Repeated", reader);
        UNIT_ASSERT_VALUES_EQUAL(reader.Count, 1);
    }

    Y_UNIT_TEST(ShouldConvertBigTraces)
    {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(
            R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    RunLogShuttleAction { }
                }
            }
            )END",
            &q);

        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);

        {
            TOrbit first;

            LWTRACK(NoParam, first);

            TVector<TOrbit> orbits;

            size_t forks = 98;

            for (size_t i = 0; i < forks; ++i) {
                orbits.emplace_back();
                first.Fork(orbits.back());
            }

            for (size_t i = 0; i < forks; ++i) {
                LWTRACK(IntParam, orbits[i], 1);
                LWTRACK(IntParam, orbits[i], 2);
                LWTRACK(IntParam, orbits[i], 3);
                LWTRACK(IntParam, orbits[i], 4);
                LWTRACK(IntParam, orbits[i], 5);

                first.Join(orbits[i]);
            }
        }

        struct
        {
            void Push(TThread::TId, const TTrackLog& tl)
            {
                ConvertToOpenTelemetrySpans(tl);
            }
        } reader;
        mngr.ReadDepot("Query1", reader);
    }
}

}   // namespace NCloud
