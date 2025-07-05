#include "helpers.h"
#include "trace_convert.h"

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/protos/lwtrace.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

namespace NCloud {

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

#define LWTRACE_UT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)             \
    PROBE(NoParam, GROUPS("Group"), TYPES(), NAMES())                       \
    PROBE(IntParam, GROUPS("Group"), TYPES(ui32), NAMES("value"))           \
    PROBE(                                                                  \
        TwoIntParam,                                                        \
        GROUPS("Group"),                                                    \
        TYPES(ui32, ui32),                                                  \
        NAMES("value1", "value2"))                                          \
    PROBE(StringParam, GROUPS("Group"), TYPES(TString), NAMES("svalue"))    \
    PROBE(                                                                  \
        SymbolParam,                                                        \
        GROUPS("Group"),                                                    \
        TYPES(NLWTrace::TSymbol),                                           \
        NAMES("symbol"))                                                    \
    PROBE(                                                                  \
        CheckParam,                                                         \
        GROUPS("Group"),                                                    \
        TYPES(NLWTrace::TCheck),                                            \
        NAMES("value"))                                                     \
    PROBE(                                                                  \
        EnumParams,                                                         \
        GROUPS("Group"),                                                    \
        TYPES(ESimpleEnum, EEnumClass),                                     \
        NAMES("simpleEnum", "enumClass"))                                   \
    PROBE(InstantParam, GROUPS("Group"), TYPES(TInstant), NAMES("value"))   \
    PROBE(DurationParam, GROUPS("Group"), TYPES(TDuration), NAMES("value")) \
    PROBE(                                                                  \
        ProtoEnum,                                                          \
        GROUPS("Group"),                                                    \
        TYPES(NLWTrace::EOperatorType),                                     \
        NAMES("value"))                                                     \
    PROBE(                                                                  \
        IntIntParams,                                                       \
        GROUPS("Group"),                                                    \
        TYPES(ui32, ui64),                                                  \
        NAMES("value1", "value2"))                                          \
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
                auto spans = ConvertToOpenTelemetrySpans(tl);

                UNIT_ASSERT(spans.size() == 1);

                const auto& span = spans.front();

                UNIT_ASSERT_VALUES_EQUAL(span.Getspan_id(), ToHexString8(0));

                UNIT_ASSERT_VALUES_EQUAL(span.Getevents().size(), 4);

                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[0].Getname(),
                    "NoParam");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[0].Getattributes().size(),
                    0);

                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[1].Getname(),
                    "IntParam");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[1].Getattributes().size(),
                    1);
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[1].Getattributes()[0].Getkey(),
                    "value");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[1]
                        .Getattributes()[0]
                        .Getvalue()
                        .Getint_value(),
                    1);

                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[2].Getname(),
                    "TwoIntParam");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[2].Getattributes().size(),
                    2);
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[2].Getattributes()[0].Getkey(),
                    "value1");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[2]
                        .Getattributes()[0]
                        .Getvalue()
                        .Getint_value(),
                    3);
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[2].Getattributes()[1].Getkey(),
                    "value2");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[2]
                        .Getattributes()[1]
                        .Getvalue()
                        .Getint_value(),
                    4);

                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[3].Getname(),
                    "StringParam");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[3].Getattributes().size(),
                    1);
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[3].Getattributes()[0].Getkey(),
                    "svalue");
                UNIT_ASSERT_VALUES_EQUAL(
                    span.Getevents()[3]
                        .Getattributes()[0]
                        .Getvalue()
                        .Getstring_value(),
                    "string");
            }
        } reader;
        mngr.ReadDepot("Query1", reader);
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
                auto spans = ConvertToOpenTelemetrySpans(tl);

                UNIT_ASSERT_VALUES_EQUAL(spans.size(), 4);

                auto getValues = [](const auto& span)
                {
                    TVector<int> values;
                    for (const auto& event: span.Getevents()) {
                        if (event.Getname() == "NoParam") {
                            continue;
                        }
                        UNIT_ASSERT_VALUES_EQUAL(event.Getname(), "IntParam");
                        UNIT_ASSERT_VALUES_EQUAL(
                            event.Getattributes().size(),
                            1);
                        UNIT_ASSERT_VALUES_EQUAL(
                            event.Getattributes()[0].Getkey(),
                            "value");
                        values.push_back(
                            event.Getattributes()[0].Getvalue().Getint_value());
                    }

                    return values;
                };

                auto firstTraceId = spans[0].Gettrace_id();
                UNIT_ASSERT_VALUES_EQUAL(16, firstTraceId.size());

                UNIT_ASSERT(AllOf(
                    spans,
                    [&](const auto& span)
                    { return span.Gettrace_id() == firstTraceId; }));

                UNIT_ASSERT(!spans[0].Getparent_span_id());
                UNIT_ASSERT_VALUES_EQUAL(
                    getValues(spans[0]),
                    (TVector<int>{1, 2, 8, 9}));
                UNIT_ASSERT_VALUES_EQUAL(8, spans[0].Getspan_id().size());

                UNIT_ASSERT_VALUES_EQUAL(
                    spans[0].Getspan_id(),
                    spans[1].Getparent_span_id());
                UNIT_ASSERT_VALUES_EQUAL(
                    (TVector<int>{7}),
                    getValues(spans[1]));
                UNIT_ASSERT_VALUES_EQUAL(8, spans[1].Getspan_id().size());

                UNIT_ASSERT_VALUES_EQUAL(
                    spans[0].Getspan_id(),
                    spans[2].Getparent_span_id());
                UNIT_ASSERT_VALUES_EQUAL(
                    getValues(spans[2]),
                    (TVector<int>{3, 4, 6}));
                UNIT_ASSERT_VALUES_EQUAL(8, spans[2].Getspan_id().size());

                UNIT_ASSERT_VALUES_EQUAL(
                    spans[2].Getspan_id(),
                    spans[3].Getparent_span_id());
                UNIT_ASSERT_VALUES_EQUAL(
                    (TVector<int>{5}),
                    getValues(spans[3]));
                UNIT_ASSERT_VALUES_EQUAL(8, spans[3].Getspan_id().size());

                auto getTime = [](const auto& spans, int i, int j)
                {
                    return spans[i].Getevents()[j].Gettime_unix_nano();
                };

                TVector eventTimes{
                    getTime(spans, 0, 0),
                    spans[2].Getstart_time_unix_nano(),
                    getTime(spans, 0, 1),
                    spans[1].Getstart_time_unix_nano(),
                    getTime(spans, 0, 2),
                    getTime(spans, 2, 0),
                    spans[3].Getstart_time_unix_nano(),
                    getTime(spans, 2, 1),
                    getTime(spans, 3, 0),
                    spans[3].Getend_time_unix_nano(),
                    getTime(spans, 2, 2),
                    getTime(spans, 1, 0),
                    spans[1].Getend_time_unix_nano(),
                    getTime(spans, 0, 3),
                    spans[2].Getend_time_unix_nano(),
                    getTime(spans, 0, 4)};

                for (size_t i = 0; i < eventTimes.size() - 1; ++i) {
                    UNIT_ASSERT(eventTimes[i] < eventTimes[i + 1]);
                }
            }
        } reader;
        mngr.ReadDepot("Query1", reader);
    }
}

}   // namespace NCloud
