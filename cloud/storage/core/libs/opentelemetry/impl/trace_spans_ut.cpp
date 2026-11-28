#include "trace_spans.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>

namespace NCloud {

namespace {

using namespace opentelemetry::proto::trace::v1;

TVector<Span> MakeTrace(ui64 requestId = 1)
{
    Span span;
    const ui64 id = 1;
    const ui64 traceId[2] = {requestId, 0};
    span.set_span_id(&id, sizeof(id));
    span.set_trace_id(traceId, sizeof(traceId));
    span.set_start_time_unix_nano(10);
    span.set_end_time_unix_nano(1000);
    return {span};
}

Span_Event* AddEvent(
    Span& span,
    TStringBuf name,
    ui64 time, TStringBuf operation = "WriteBlocks", i64 requestId = 1)
{
    auto* event = span.add_events();
    event->set_name(name.data(), name.size());
    event->set_time_unix_nano(time);
    auto* id = event->add_attributes();
    id->set_key("requestId");
    id->mutable_value()->set_int_value(requestId);
    auto* type = event->add_attributes();
    type->set_key("requestType");
    type->mutable_value()->set_string_value(operation.data(), operation.size());
    return event;
}

void SetError(Span_Event& event, ui32 error)
{
    auto* attribute = event.add_attributes();
    attribute->set_key("error");
    attribute->mutable_value()->set_int_value(error);
}

}   // namespace

Y_UNIT_TEST_SUITE(DiagnosticSpans)
{
    Y_UNIT_TEST(ShouldCoverAllDataOperationsAndErrors)
    {
        for (const TString operation:
             {"ReadBlocks", "WriteBlocks", "ZeroBlocks"})
        {
            for (const ui32 error: {0U, 1U, 0x80000001U}) {
                auto spans = MakeTrace();
                auto& root = spans.front();
                AddEvent(root, "RequestStarted", 10, operation);
                AddEvent(root, "RequestReceived_Service", 20, operation);
                AddEvent(root, "RequestReceived_Throttler", 30, operation);
                AddEvent(root, "RequestCompleted_Throttler", 40, operation);
                AddEvent(
                    root, "RequestSent_PartitionWorker_DSProxy", 50, operation);
                SetError(
                    *AddEvent(
                        root,
                        "ResponseSent_PartitionWorker_DSProxy", 60, operation),
                    error);
                AddEvent(root, "ResponseReceived_Proxy", 70, operation);
                SetError(
                    *AddEvent(root, "RequestCompleted", 80, operation), error);

                AddDiagnosticSpans(spans);

                UNIT_ASSERT_VALUES_EQUAL(spans.size(), 5);
                THashSet<TString> ids;
                for (const auto& span: spans) {
                    UNIT_ASSERT(ids.insert(span.span_id()).second);
                    UNIT_ASSERT_VALUES_UNEQUAL(
                        span.span_id(), TString(8, '\0'));
                    UNIT_ASSERT_VALUES_EQUAL(
                        span.trace_id(), spans[0].trace_id());
                }
                const TString stages[] = {
                    "Server",
                    "Service",
                    "Throttler",
                    "BlobStorage"};
                const ui64 starts[] = {10, 20, 30, 50};
                const ui64 ends[] = {80, 70, 40, 60};
                for (size_t i = 1; i < spans.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        spans[i].name(), stages[i - 1] + "." + operation);
                    UNIT_ASSERT_VALUES_EQUAL(
                        spans[i].parent_span_id(), spans[0].span_id());
                    UNIT_ASSERT_VALUES_EQUAL(
                        spans[i].start_time_unix_nano(), starts[i - 1]);
                    UNIT_ASSERT_VALUES_EQUAL(
                        spans[i].end_time_unix_nano(), ends[i - 1]);
                    UNIT_ASSERT_VALUES_EQUAL(spans[i].events_size(), 2);
                }
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<int>(spans[1].status().code()),
                    error & 0x80000000U
                        ? static_cast<int>(Status::STATUS_CODE_ERROR)
                        : static_cast<int>(Status::STATUS_CODE_UNSET));
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<int>(spans[4].status().code()),
                    static_cast<int>(spans[1].status().code()));
                UNIT_ASSERT_VALUES_EQUAL(spans[0].events_size(), 8);
            }
        }
    }

    Y_UNIT_TEST(ShouldWaitForServiceResponseAtProxy)
    {
        for (const TString operation:
             {"ReadBlocks", "WriteBlocks", "ZeroBlocks"})
        {
            auto spans = MakeTrace();
            AddEvent(spans[0], "RequestReceived_Service", 20, operation);
            AddEvent(spans[0], "ResponseSent_Service", 25, operation);
            AddEvent(spans[0], "ResponseReceived_Proxy", 30, operation);
            AddDiagnosticSpans(spans);
            UNIT_ASSERT_VALUES_EQUAL(spans.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(spans[1].name(), "Service." + operation);
            UNIT_ASSERT_VALUES_EQUAL(spans[1].start_time_unix_nano(), 20);
            UNIT_ASSERT_VALUES_EQUAL(spans[1].end_time_unix_nano(), 30);
        }
    }

    Y_UNIT_TEST(ShouldKeepPointEventsAndUnfinishedStages)
    {
        auto spans = MakeTrace();
        for (const TString name:
             {"RequestRetry",
              "ForkFailed",
              "FaultInjection",
              "TxPrepareRestarted", "RequestReceived_Service"})
        {
            AddEvent(spans[0], name, 20);
        }
        AddEvent(spans[0], "RequestCompleted", 30);
        AddDiagnosticSpans(spans);
        UNIT_ASSERT_VALUES_EQUAL(spans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(spans[0].events_size(), 6);
    }

    Y_UNIT_TEST(ShouldMatchConcurrentRequestsAndRetries)
    {
        auto spans = MakeTrace();
        AddEvent(spans[0], "RequestReceived_Service", 20, "ReadBlocks", 11);
        AddEvent(spans[0], "RequestReceived_Service", 25, "ReadBlocks", 22);
        AddEvent(spans[0], "ResponseReceived_Proxy", 30, "ReadBlocks", 11);
        AddEvent(spans[0], "ResponseReceived_Proxy", 40, "ReadBlocks", 22);
        AddEvent(spans[0], "RequestReceived_Service", 50, "ReadBlocks", 11);
        AddEvent(spans[0], "ResponseReceived_Proxy", 60, "ReadBlocks", 11);
        AddDiagnosticSpans(spans);
        UNIT_ASSERT_VALUES_EQUAL(spans.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(spans[1].start_time_unix_nano(), 20);
        UNIT_ASSERT_VALUES_EQUAL(spans[1].end_time_unix_nano(), 30);
        UNIT_ASSERT_VALUES_EQUAL(spans[2].start_time_unix_nano(), 25);
        UNIT_ASSERT_VALUES_EQUAL(spans[2].end_time_unix_nano(), 40);
        UNIT_ASSERT_VALUES_EQUAL(spans[3].start_time_unix_nano(), 50);
        UNIT_ASSERT_VALUES_EQUAL(spans[3].end_time_unix_nano(), 60);
    }

    Y_UNIT_TEST(ShouldNotMatchDifferentOperationsOrOrbits)
    {
        auto spans = MakeTrace();
        AddEvent(spans[0], "RequestReceived_Service", 20, "ReadBlocks");
        AddEvent(spans[0], "ResponseReceived_Proxy", 30, "WriteBlocks");
        Span child;
        const ui64 childId = 2;
        child.set_span_id(&childId, sizeof(childId));
        child.set_trace_id(spans[0].trace_id());
        child.set_parent_span_id(spans[0].span_id());
        AddEvent(child, "ResponseReceived_Proxy", 40, "ReadBlocks");
        spans.push_back(child);
        AddDiagnosticSpans(spans);
        UNIT_ASSERT_VALUES_EQUAL(spans.size(), 2);
    }

    Y_UNIT_TEST(ShouldRejectMissingOrInvalidContext)
    {
        for (ui32 invalid = 0; invalid < 6; ++invalid) {
            auto spans = MakeTrace();
            auto* begin = AddEvent(spans[0], "RequestReceived_Service", 20);
            switch (invalid) {
                case 0:
                    begin->clear_attributes();
                    break;
                case 1:
                    begin->mutable_attributes(0)
                        ->mutable_value()
                        ->set_int_value(0);
                    break;
                case 2:
                    begin->mutable_attributes(0)
                        ->mutable_value()
                        ->set_string_value("bad id");
                    break;
                case 3:
                    begin->mutable_attributes(1)
                        ->mutable_value()
                        ->set_string_value("");
                    break;
                case 4:
                    begin->mutable_attributes(1)
                        ->mutable_value()
                        ->set_int_value(1);
                    break;
                case 5:
                    begin->mutable_attributes()->RemoveLast();
                    break;
            }
            AddEvent(spans[0], "ResponseReceived_Proxy", 30);
            AddDiagnosticSpans(spans);
            UNIT_ASSERT_VALUES_EQUAL(spans.size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldNotInventDurationFromReversedOrRepeatedBoundaries)
    {
        auto spans = MakeTrace();
        AddEvent(spans[0], "RequestReceived_Service", 20);
        AddEvent(spans[0], "ResponseReceived_Proxy", 10);
        AddEvent(spans[0], "RequestReceived_Service", 30);
        AddEvent(spans[0], "RequestReceived_Service", 40);
        AddEvent(spans[0], "ResponseReceived_Proxy", 50);
        AddEvent(spans[0], "RequestReceived_Service", 60);
        AddEvent(spans[0], "ResponseReceived_Proxy", 70);
        AddDiagnosticSpans(spans);
        UNIT_ASSERT_VALUES_EQUAL(spans.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(spans[1].start_time_unix_nano(), 60);
        UNIT_ASSERT_VALUES_EQUAL(spans[1].end_time_unix_nano(), 70);
    }

    Y_UNIT_TEST(ShouldMeasureQueuedThrottlingAndKeepForeignErrorsAsEvents)
    {
        auto spans = MakeTrace();
        AddEvent(spans[0], "RequestPostponed_Volume", 20);
        auto* end = AddEvent(spans[0], "RequestAdvanced_Volume", 50);
        auto* error = end->add_attributes();
        error->set_key("error");
        error->mutable_value()->set_string_value("unknown");
        AddDiagnosticSpans(spans);
        UNIT_ASSERT_VALUES_EQUAL(spans.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            spans[1].name(), "Throttler.Queue.WriteBlocks");
        UNIT_ASSERT_VALUES_EQUAL(spans[1].start_time_unix_nano(), 20);
        UNIT_ASSERT_VALUES_EQUAL(spans[1].end_time_unix_nano(), 50);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(spans[1].status().code()),
            static_cast<int>(Status::STATUS_CODE_UNSET));
    }

    Y_UNIT_TEST(ShouldAcceptUnsignedRequestIdsWithoutMixingTraces)
    {
        auto first = MakeTrace(11);
        auto second = MakeTrace(22);
        for (auto* spans: {&first, &second}) {
            AddEvent((*spans)[0], "RequestStarted", 10, "ZeroBlocks", -1);
            AddEvent((*spans)[0], "RequestCompleted", 20, "ZeroBlocks", -1);
            AddDiagnosticSpans(*spans);
            UNIT_ASSERT_VALUES_EQUAL(spans->size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(
                (*spans)[1].trace_id(), (*spans)[0].trace_id());
        }
        UNIT_ASSERT_VALUES_UNEQUAL(first[1].trace_id(), second[1].trace_id());
    }

    Y_UNIT_TEST(ShouldNotReuseSyntheticIdsAcrossExportsOfOneRequest)
    {
        THashSet<TString> ids;
        for (ui32 attempt = 0; attempt != 32; ++attempt) {
            auto spans = MakeTrace(7311);
            AddEvent(spans[0], "RequestStarted", 10, "WriteBlocks", 7311);
            AddEvent(spans[0], "RequestCompleted", 20, "WriteBlocks", 7311);
            AddDiagnosticSpans(spans);
            UNIT_ASSERT_VALUES_EQUAL(spans.size(), 2);
            UNIT_ASSERT(ids.insert(spans[1].span_id()).second);
        }
    }

    Y_UNIT_TEST(ShouldKeepExactWireParentsAndRejectAmbiguousIds)
    {
        for (ui32 scenario = 0; scenario < 5; ++scenario) {
            auto spans = MakeTrace();
            auto* begin =
                AddEvent(spans[0], "RequestSent_PartitionWorker_DSProxy", 20);
            if (scenario != 0) {
                auto* attr = begin->add_attributes();
                attr->set_key("spanId");
                if (scenario == 1) {
                    attr->mutable_value()->set_string_value("bad");
                } else {
                    attr->mutable_value()->set_int_value(
                        scenario == 2   ? 0
                        : scenario == 3 ? 1
                                        : -1);
                }
            }
            AddEvent(spans[0], "ResponseSent_PartitionWorker_DSProxy", 30);
            AddDiagnosticSpans(spans);
            UNIT_ASSERT_VALUES_EQUAL(spans.size(), 2);
            const ui64 wireId = Max<ui64>();
            const TString bytes(reinterpret_cast<const char*>(&wireId), 8);
            UNIT_ASSERT_VALUES_EQUAL(
                spans[1].parent_span_id(), spans[0].span_id());
            if (scenario == 4) {
                UNIT_ASSERT_VALUES_EQUAL(spans[1].span_id(), bytes);
            } else {
                UNIT_ASSERT_VALUES_UNEQUAL(
                    spans[1].span_id(), spans[0].span_id());
            }
        }

        auto spans = MakeTrace();
        const ui64 duplicateId = 42;
        const TString bytes(reinterpret_cast<const char*>(&duplicateId), 8);
        for (ui64 time: {20, 40}) {
            auto* event =
                AddEvent(spans[0], "RequestSent_PartitionWorker_DSProxy", time);
            auto* attr = event->add_attributes();
            attr->set_key("spanId");
            attr->mutable_value()->set_int_value(duplicateId);
            AddEvent(
                spans[0], "ResponseSent_PartitionWorker_DSProxy", time + 10);
        }
        AddDiagnosticSpans(spans);
        UNIT_ASSERT_VALUES_EQUAL(spans.size(), 3);
        UNIT_ASSERT_VALUES_UNEQUAL(spans[1].span_id(), bytes);
        UNIT_ASSERT_VALUES_UNEQUAL(spans[2].span_id(), bytes);
        UNIT_ASSERT_VALUES_UNEQUAL(spans[1].span_id(), spans[2].span_id());
    }

    Y_UNIT_TEST(ShouldHandleEmptyTrace)
    {
        TVector<Span> spans;
        AddDiagnosticSpans(spans);
        UNIT_ASSERT(spans.empty());
    }
}

}   // namespace NCloud
