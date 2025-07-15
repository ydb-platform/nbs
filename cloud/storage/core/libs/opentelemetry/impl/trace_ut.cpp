#include "helpers.h"
#include "trace_convert.h"

#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/common/v1/common.pb.h>
#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/trace/v1/trace.pb.h>
#include <contrib/libs/protobuf/src/google/protobuf/util/message_differencer.h>

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/protos/lwtrace.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

namespace NCloud {

using namespace opentelemetry::proto::trace::v1;
using namespace opentelemetry::proto::common::v1;

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

                UNIT_ASSERT_VALUES_EQUAL(span.span_id(), ToHexString8(0));

                struct TExpectedEvent
                {
                    TString Name;
                    TVector<AnyValue> Attributes;
                    TVector<TString> AttributeNames;
                };

                auto constructIntAnyValue = [](auto value)
                {
                    AnyValue val;
                    val.set_int_value(value);
                    return val;
                };

                auto constructStringAnyValue = [](auto value)
                {
                    AnyValue val;
                    val.set_string_value(value);
                    return val;
                };

                TVector<TExpectedEvent> expectedEvents{
                    {.Name = "NoParam", .Attributes = {}, .AttributeNames = {}},
                    {.Name = "IntParam",
                     .Attributes = {constructIntAnyValue(1)},
                     .AttributeNames = {"value"}},
                    {.Name = "TwoIntParam",
                     .Attributes =
                         {constructIntAnyValue(3), constructIntAnyValue(4)},
                     .AttributeNames = {"value1", "value2"}},
                    {.Name = "StringParam",
                     .Attributes = {constructStringAnyValue("string")},
                     .AttributeNames = {"svalue"}}};

                UNIT_ASSERT_VALUES_EQUAL(
                    expectedEvents.size(),
                    span.events().size());

                for (size_t i = 0; i < expectedEvents.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        expectedEvents[i].Name,
                        span.events(i).name());

                    UNIT_ASSERT_VALUES_EQUAL(
                        expectedEvents[i].Attributes.size(),
                        span.events(i).attributes().size());

                    auto diff = std::make_unique<
                        google::protobuf::util::MessageDifferencer>();
                    for (size_t j = 0; j < expectedEvents[i].Attributes.size();
                         ++j)
                    {
                        UNIT_ASSERT(diff->Equals(
                            expectedEvents[i].Attributes[j],
                            span.events(i).attributes(j).value()));
                        UNIT_ASSERT_VALUES_EQUAL(
                            expectedEvents[i].AttributeNames[j],
                            span.events(i).attributes(j).key());
                    }
                }
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

                TVector<TVector<int>> expectedValues{
                    {1, 2, 8, 9},
                    {7},
                    {3, 4, 6},
                    {5}};

                UNIT_ASSERT_VALUES_EQUAL(expectedValues.size(), spans.size());

                for (size_t i = 0; i < spans.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        expectedValues[i],
                        getValues(spans[i]));
                    UNIT_ASSERT_VALUES_EQUAL(8, spans[i].span_id().size());
                }

                UNIT_ASSERT(!spans[0].parent_span_id());

                UNIT_ASSERT_VALUES_EQUAL(
                    spans[0].span_id(),
                    spans[1].parent_span_id());

                UNIT_ASSERT_VALUES_EQUAL(
                    spans[0].span_id(),
                    spans[2].parent_span_id());

                UNIT_ASSERT_VALUES_EQUAL(
                    spans[2].span_id(),
                    spans[3].parent_span_id());

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
}

}   // namespace NCloud
