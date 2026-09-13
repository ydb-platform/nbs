#include "request_timing.h"

#include "request_timing_collector.h"
#include "request_timing_graph_builder.h"
#include "request_timing_journal.h"

#include <cloud/storage/core/protos/request_timing.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <future>
#include <thread>

namespace NCloud {
namespace {

TTimingStage Stage(
    ui64 begin,
    ui64 end,
    TVector<TTimingWait> waits = {},
    TVector<TTimingDependency> dependencies = {}, ui64 notBefore = 0)
{
    TTimingStage s;
    s.Begin = begin;
    s.End = end;
    s.Waits = std::move(waits);
    s.Dependencies = std::move(dependencies);
    s.NotBefore = notBefore;
    return s;
}

void Check(
    const TVector<TTimingStage>& stages,
    ui32 node, ui32 mask, ui64 total, ui64 without)
{
    const auto r = TRequestTiming::Calculate(stages, node, mask, total);
    UNIT_ASSERT_C(r.TimeWithoutWaits, r.IncompleteReason);
    UNIT_ASSERT(r.WaitImpact);
    UNIT_ASSERT_VALUES_EQUAL(r.TotalTime.MicroSeconds(), total);
    UNIT_ASSERT_VALUES_EQUAL(r.TimeWithoutWaits->MicroSeconds(), without);
    UNIT_ASSERT_VALUES_EQUAL(r.WaitImpact->MicroSeconds(), total - without);
}

void Incomplete(
    const TVector<TTimingStage>& stages, ui32 node, ui32 mask, ui64 total)
{
    const auto r = TRequestTiming::Calculate(stages, node, mask, total);
    UNIT_ASSERT(!r.TimeWithoutWaits);
    UNIT_ASSERT(!r.WaitImpact);
    UNIT_ASSERT(!r.IncompleteReason.empty());
    UNIT_ASSERT_VALUES_EQUAL(r.TotalTime.MicroSeconds(), total);
}

NJson::TJsonValue Json(TString s)
{
    NJson::TJsonValue json;
    UNIT_ASSERT(NJson::ReadJsonTree(s, &json, true));
    return json;
}

}   // namespace

Y_UNIT_TEST_SUITE(TRequestTimingTest)
{
    Y_UNIT_TEST(ShouldRecalculateParallelAndDependentRequests)
    {
        Check(
            {Stage(0, 90, {{0, 80, 1}}),
             Stage(0, 60), Stage(90, 90, {}, {{0, 0}, {1, 0}})}, 2, 7, 90, 60);
        Check(
            {Stage(0, 90, {{0, 80, 1}}),
             Stage(0, 90, {{0, 80, 1}}), Stage(90, 90, {}, {{0, 0}, {1, 0}})},
            2, 7, 90, 10);
        Check(
            {Stage(0, 90, {{0, 80, 1}}), Stage(95, 115, {}, {{0, 5}})},
            1, 7, 115, 35);
        Check(
            {Stage(0, 90, {{0, 80, 1}}),
             Stage(20, 80, {}, {}, 20), Stage(90, 90, {}, {{0, 0}, {1, 0}})},
            2, 7, 90, 80);
        Check(
            {Stage(0, 90, {{0, 80, 1}}),
             Stage(30, 50, {}, {}, 30), Stage(90, 90, {}, {{0, 0}, {1, 0}})},
            2, 7, 90, 50);
        Check(
            {Stage(0, 10, {{0, 10, 1}}), Stage(20, 25, {}, {{0, 5}}, 20)},
            1, 7, 25, 25);
    }

    Y_UNIT_TEST(ShouldCountCommonAndNestedStagesOnce)
    {
        Check(
            {Stage(0, 10),
             Stage(10, 35, {{10, 30, 1}}, {{0, 0}}),
             Stage(35, 35, {}, {{1, 0}}),
             Stage(35, 50, {{35, 45, 2}}, {{2, 0}}),
             Stage(50, 57, {}, {{3, 0}})}, 4, 7, 57, 27);
        Check(
            {Stage(0, 10),
             Stage(10, 100, {{10, 90, 1}}, {{0, 0}}),
             Stage(10, 70, {}, {{0, 0}}),
             Stage(100, 107, {}, {{1, 0}, {2, 0}})}, 3, 7, 107, 77);
        Check(
            {Stage(0, 10), Stage(0, 20), Stage(20, 20, {}, {{0, 0}, {1, 0}})},
            2, 7, 20, 20);
        Check({Stage(0, 0)}, 0, 7, 0, 0);
    }

    Y_UNIT_TEST(ShouldUnionDuplicateAndOverlappingCategories)
    {
        const TVector<TTimingStage> stages = {
            Stage(0, 90, {{0, 50, 1}, {30, 80, 2}, {0, 50, 1}})};
        for (const auto& [mask, expected]: TVector<std::pair<ui32, ui64>>{
                 {0, 90},
                 {1, 40},
                 {2, 40},
                 {3, 10},
                 {4, 90},
                 {7, 10}})
        {
            Check(stages, 0, mask, 90, expected);
        }
        Check(
            {Stage(0, 90, {{30, 80, 1}, {0, 30, 1}, {0, 80, 1}, {80, 80, 1}})},
            0, 1, 90, 10);
        Check({Stage(0, 90, {{0, 80, 3}})}, 0, 2, 90, 10);
    }

    Y_UNIT_TEST(ShouldReevaluateCriticalPathForEverySelection)
    {
        const TVector<TTimingStage> stages = {
            Stage(0, 90, {{0, 80, 1}}),
            Stage(0, 85, {{0, 70, 2}}),
            Stage(90, 90, {}, {{0, 0}, {1, 0}})};
        Check(stages, 2, 1, 90, 85);
        Check(stages, 2, 2, 90, 90);
        Check(stages, 2, 3, 90, 15);
        // Neither input node order nor repeated dependencies affect the result.
        Check(
            {Stage(90, 90, {}, {{2, 0}, {1, 0}, {2, 0}}),
             Stage(0, 60), Stage(0, 90, {{0, 80, 1}})}, 0, 7, 90, 60);
    }

    Y_UNIT_TEST(ShouldUseOnlyCausalAncestorsForEarlyError)
    {
        auto unfinished = Stage(0, 90);
        unfinished.IncompleteReason = "cancelled without future completion";
        unfinished.Dependencies = {{2, 0}};
        Check(
            {Stage(0, 90, {{0, 80, 1}}),
             Stage(90, 90, {}, {{0, 0}}), unfinished}, 1, 7, 90, 10);
    }

    Y_UNIT_TEST(ShouldExposeMissingDataWithoutInventingZero)
    {
        auto stage = Stage(0, 90, {{20, 30, 2}});
        stage.MissingCategories = 1;
        Check({stage}, 0, 2, 90, 80);
        Check({stage}, 0, 0, 90, 90);
        Incomplete({stage}, 0, 1, 90);
        Incomplete({stage}, 0, 3, 90);
        stage.IncompleteReason = "unknown launch dependency";
        Incomplete({stage}, 0, 0, 90);
    }

    Y_UNIT_TEST(ShouldRejectInvalidGraphsAndIntervals)
    {
        Incomplete({}, 0, 7, 0);
        Incomplete({Stage(0, 10)}, 1, 7, 10);
        Incomplete({Stage(0, 0, {}, {{7, 0}})}, 0, 7, 0);
        Incomplete({Stage(0, 0, {}, {{0, 0}})}, 0, 7, 0);
        Incomplete(
            {Stage(0, 0, {}, {{1, 0}}), Stage(0, 0, {}, {{0, 0}})}, 0, 7, 0);
        Incomplete({Stage(0, 10)}, 0, 7, 11);
        Incomplete({Stage(5, 10)}, 0, 0, 10);
        Incomplete({Stage(10, 9)}, 0, 7, 9);
        Incomplete({Stage(0, 11), Stage(10, 10, {}, {{0, 0}})}, 1, 7, 10);
        for (auto s:
             {Stage(0, 10, {{5, 4, 1}}),
              Stage(5, 10, {{4, 6, 1}}, {}, 5), Stage(0, 10, {{4, 11, 1}})})
        {
            Incomplete({s}, 0, 0, 10);
        }
        Incomplete({Stage(0, 10)}, 0, 8, 10);
        const auto max = std::numeric_limits<ui64>::max();
        Incomplete({Stage(0, max), Stage(max, max, {}, {{0, 1}})}, 1, 7, max);
        Check({Stage(0, max, {{0, max, 1}, {0, max, 2}})}, 0, 3, max, 0);
    }

    Y_UNIT_TEST(ShouldRoundTripCompactTimingWithIndependentOracle)
    {
        // Literal oracles: hidden wait (90->60), both wait (90->10),
        // and dependent launch (115->35). Serialization is part of the test.
        for (const ui32 shape: {0u, 1u, 2u}) {
            TRequestTimingCollector timing(7772);
            const auto fork = timing.Fork(0, 0);
            const auto a = timing.Start(fork, 0);
            timing.Wait(a, 1, 0, 80);
            timing.Finish(a, 90);
            if (shape < 2) {
                const auto b = timing.Start(fork, 0);
                if (shape == 1) {
                    timing.Wait(b, 1, 0, 80);
                }
                timing.Finish(b, shape == 0 ? 60 : 90);
                timing.Join(0, {a, b}, 90);
            } else {
                timing.Join(0, {a}, 90);
                // Five microseconds of parent work before the dependent fork.
                const auto next = timing.Fork(0, 95);
                const auto b = timing.Start(next, 95);
                timing.Finish(b, 115);
                timing.Join(0, {b}, 115);
            }
            timing.Freeze(shape == 2 ? 115 : 90, 1, 7);
            NProto::TRequestTimingTrace encoded;
            timing.FillTrace(encoded);
            UNIT_ASSERT(!encoded.GetImplicitRoot());
            NProto::TRequestTimingTrace restored;
            UNIT_ASSERT(restored.ParseFromString(encoded.SerializeAsString()));
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(
                FormatRequestTimingTrace(restored), &json, true));
            UNIT_ASSERT(json["complete"].GetBoolean());
            UNIT_ASSERT_VALUES_EQUAL(json["request_id"].GetUInteger(), 7772);
            UNIT_ASSERT_VALUES_EQUAL(json["error_code"].GetUInteger(), 7);
            UNIT_ASSERT_VALUES_EQUAL(
                json["total_us"].GetUInteger(), shape == 2 ? 115 : 90);
            UNIT_ASSERT_VALUES_EQUAL(
                json["without_waits_us"].GetUInteger(),
                shape == 0   ? 60
                : shape == 1 ? 10
                             : 35);
            UNIT_ASSERT_VALUES_EQUAL(
                json["wait_impact_us"].GetUInteger(), shape == 0 ? 30 : 80);
        }
    }

    Y_UNIT_TEST(ShouldRejectMalformedCompactTiming)
    {
        TRequestTimingGraphBuilder timing(7772);
        timing.Freeze(90, 7);
        NProto::TRequestTimingTrace original;
        timing.FillTrace(original);
        for (ui32 shape = 0; shape < 13; ++shape) {
            auto trace = original;
            switch (shape) {
                case 0:
                    trace.SetVersion(99);
                    break;
                case 1:
                    trace.SetImplicitRoot(true);
                    break;
                case 2:
                    trace.ClearCompletionNode();
                    break;
                case 3:
                    trace.MutableStages(0)->SetPart(99);
                    break;
                case 4:
                    trace.MutableStages(0)->ClearEnd();
                    break;
                case 5: {
                    auto* dep = trace.MutableStages(0)->AddDependencies();
                    dep->SetNode(0);
                    dep->SetLag(0);
                    break;
                }
                case 6: {
                    auto* dep = trace.MutableStages(0)->AddDependencies();
                    dep->SetNode(99);
                    dep->SetLag(0);
                    break;
                }
                case 7:
                    for (size_t i = 0;
                         i <= 4 * TRequestTimingCollector::MaxEvents;
                         ++i)
                    {
                        trace.AddParts();
                    }
                    break;
                case 8:
                    for (size_t i = 0;
                         i <= 4 * TRequestTimingCollector::MaxEvents;
                         ++i)
                    {
                        trace.MutableStages(0)->AddDependencies();
                    }
                    break;
                case 9:
                    trace.MutableStages(0)->AddDependencies()->SetNode(0);
                    break;
                case 10: {
                    auto* wait = trace.MutableStages(0)->AddWaits();
                    wait->SetBegin(0);
                    wait->SetCategories(1);
                    break;
                }
                case 11:
                    trace.MutableParts(0)->SetParent(0);
                    break;
                case 12:
                    trace.MutableStages(0)->MutableUnlocatedWaits()->Clear();
                    break;
            }
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(
                FormatRequestTimingTrace(trace), &json, true));
            UNIT_ASSERT(!json["complete"].GetBoolean());
            UNIT_ASSERT(json["without_waits_us"].IsNull());
            UNIT_ASSERT(json["wait_impact_us"].IsNull());
            UNIT_ASSERT(!json["reason"].GetString().empty());
            UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
        }
    }

    Y_UNIT_TEST(ShouldReplayJournalAsOriginalGraphBuilder)
    {
        for (ui32 scenario = 0; scenario != 5; ++scenario) {
            const ui64 requestId = 1000 + scenario;
            const ui32 error = scenario == 3 ? 7 : 0;
            TRequestTimingCollector journal(requestId);
            TRequestTimingGraphBuilder graph(requestId);
            const auto exercise = [scenario](auto& collector)
            {
                TVector<ui32> ids;
                if (scenario == 0 || scenario == 3) {
                    collector.Wait(0, 1, 10, 40);
                } else if (scenario == 4) {
                    collector.Wait(999, 1, 10, 40);
                    collector.Missing(999, 1, 1);
                    collector.Finish(999, 50);
                    collector.Cancel(999, 60);
                    collector.Incomplete("invalid input");
                    collector.Incomplete("invalid input");
                } else {
                    const auto outerNode = collector.Fork(0, 0);
                    const auto outer = collector.Start(outerNode, 0);
                    const auto innerNode = collector.Fork(outer, 10);
                    const auto inner = collector.Start(innerNode, 10);
                    ids = {outerNode, outer, innerNode, inner};
                    collector.Wait(inner, 1, 15, 25);
                    if (scenario == 2) {
                        collector.Cancel(inner, 40);
                        collector.Cancel(inner, 40);
                    } else {
                        collector.Finish(inner, 40);
                    }
                    collector.Finish(inner, 40);
                    collector.Join(outer, TVector<ui32>{inner}, 40);
                    if (scenario == 2) {
                        collector.Join(outer, TVector<ui32>{inner}, 40);
                    }
                    collector.Finish(outer, 80);
                    collector.Finish(outer, 80);
                    collector.Join(0, TVector<ui32>{outer}, 80);
                }
                return ids;
            };
            const auto expectedIds = exercise(graph);
            const auto actualIds = exercise(journal);
            UNIT_ASSERT(actualIds == expectedIds);
            const auto expected = graph.Complete(90, 1, error);
            const auto actual = journal.Complete(90, 1, error);
            UNIT_ASSERT_VALUES_EQUAL(actual, expected);
            NProto::TRequestTimingTrace trace;
            journal.FillTrace(trace);
            UNIT_ASSERT_VALUES_EQUAL(trace.GetVersion(), 2);
            const auto json = Json(actual);
            UNIT_ASSERT_VALUES_EQUAL(
                json["request_id"].GetUInteger(), requestId);
            UNIT_ASSERT_VALUES_EQUAL(json["error_code"].GetUInteger(), error);
            if (scenario == 0 || scenario == 3) {
                UNIT_ASSERT(json["complete"].GetBoolean());
                UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
                UNIT_ASSERT_VALUES_EQUAL(
                    json["without_waits_us"].GetUInteger(), 60);
                UNIT_ASSERT_VALUES_EQUAL(
                    json["wait_impact_us"].GetUInteger(), 30);
                UNIT_ASSERT_VALUES_EQUAL(
                    json["selected_categories"].GetUInteger(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(journal.Complete(190, 7, 19), actual);
            UNIT_ASSERT_VALUES_EQUAL(graph.Complete(190, 7, 19), expected);
        }
    }

    Y_UNIT_TEST(ShouldPreserveIdsAfterRejectedJournalOperations)
    {
        const auto exercise = [](auto& collector)
        {
            TVector<ui32> ids;
            const auto fork = collector.Fork(0, 10);
            ids.push_back(fork);
            ids.push_back(collector.Start(fork, 9));
            const auto child = collector.Start(fork, 10);
            ids.push_back(child);
            collector.Finish(child, 20);
            collector.Join(0, {child, child}, 20);
            collector.Join(0, {child}, 19);
            ids.push_back(collector.Fork(0, 19));
            const auto next = collector.Fork(0, 30);
            ids.push_back(next);
            const auto later = collector.Start(next, 30);
            ids.push_back(later);
            collector.Finish(later, 40);
            collector.Join(0, {later, child}, 40);
            // The rejected join has already resumed the parent and retained
            // its partial Begin. The next node ordinal must still agree.
            ids.push_back(collector.Fork(0, 50));
            collector.Cancel(later, 50);
            collector.Cancel(child, 50);
            collector.Incomplete("rejected operations");
            return ids;
        };
        TRequestTimingCollector journal(7772);
        TRequestTimingGraphBuilder graph(7772);
        const auto expectedIds = exercise(graph);
        const auto actualIds = exercise(journal);
        UNIT_ASSERT(actualIds == expectedIds);
        UNIT_ASSERT_VALUES_EQUAL(
            actualIds[1], TRequestTimingCollector::InvalidId);
        UNIT_ASSERT_VALUES_EQUAL(
            actualIds[3], TRequestTimingCollector::InvalidId);
        UNIT_ASSERT_VALUES_EQUAL(
            journal.Complete(60, 7), graph.Complete(60, 7));
    }

    Y_UNIT_TEST(ShouldPreserveGraphAndIdsAfterJournalOverflow)
    {
        const ui64 requestId = 777;
        const auto prefix = [](auto& collector)
        {
            const auto node = collector.Fork(0, 0);
            const auto part = collector.Start(node, 0);
            collector.Finish(part, 10);
            collector.Join(0, TVector<ui32>{part}, 10);
            return TVector<ui32>{node, part};
        };
        TRequestTimingCollector probe(requestId);
        prefix(probe);
        probe.Freeze(10000, 1, 7);
        NProto::TRequestTimingTrace beforeOverflow;
        probe.FillTrace(beforeOverflow);
        UNIT_ASSERT_VALUES_EQUAL(beforeOverflow.GetVersion(), 2);

        TRequestTimingCollector journal(requestId);
        TRequestTimingGraphBuilder graph(requestId);
        const auto expectedPrefixIds = prefix(graph);
        const auto actualPrefixIds = prefix(journal);
        UNIT_ASSERT(actualPrefixIds == expectedPrefixIds);
        constexpr ui32 MissingCount = 2048;
        for (ui32 i = 0; i != MissingCount; ++i) {
            journal.Missing(0, 1, 1);
            graph.Missing(0, 1, 1);
        }
        const auto expectedNode = graph.Fork(0, 5000);
        const auto actualNode = journal.Fork(0, 5000);
        UNIT_ASSERT_VALUES_EQUAL(actualNode, expectedNode);
        const auto expectedPart = graph.Start(expectedNode, 5000);
        const auto actualPart = journal.Start(actualNode, 5000);
        UNIT_ASSERT_VALUES_EQUAL(actualPart, expectedPart);
        graph.Finish(expectedPart, 6000);
        journal.Finish(actualPart, 6000);
        graph.Join(0, TVector<ui32>{expectedPart}, 6000);
        journal.Join(0, TVector<ui32>{actualPart}, 6000);
        graph.Incomplete("final reason");
        journal.Incomplete("final reason");
        graph.Freeze(10000, 1, 7);
        journal.Freeze(10000, 1, 7);
        NProto::TRequestTimingTrace afterOverflow;
        journal.FillTrace(afterOverflow);
        UNIT_ASSERT_VALUES_EQUAL(afterOverflow.GetVersion(), 1);
        const auto expected = graph.Complete(10000, 1, 7);
        const auto actual = journal.Complete(10000, 1, 7);
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
        const auto json = Json(actual);
        UNIT_ASSERT(!json["complete"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(json["reason"].GetString(), "final reason");
        UNIT_ASSERT_VALUES_EQUAL(json["request_id"].GetUInteger(), 777);
        UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 10000);
        UNIT_ASSERT_VALUES_EQUAL(json["selected_categories"].GetUInteger(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["error_code"].GetUInteger(), 7);
        ui64 unlocated = 0;
        for (const auto& stage: json["stages"].GetArray()) {
            unlocated += stage["unlocated_wait_us"][0].GetUInteger();
        }
        UNIT_ASSERT_VALUES_EQUAL(unlocated, MissingCount);
    }

    Y_UNIT_TEST(ShouldRejectMalformedTimingJournal)
    {
        TRequestTimingCollector timing(7772);
        timing.Wait(0, 1, 0, 80);
        timing.Freeze(90, 1, 7);
        NProto::TRequestTimingTrace original;
        timing.FillTrace(original);
        UNIT_ASSERT_VALUES_EQUAL(original.GetVersion(), 2);
        UNIT_ASSERT(original.JournalSize() > 0);
        UNIT_ASSERT(
            Json(FormatRequestTimingTrace(original))["complete"].GetBoolean());

        for (ui32 shape = 0; shape < 12; ++shape) {
            auto trace = original;
            switch (shape) {
                case 0:
                    trace.AddParts();
                    break;
                case 1:
                    trace.SetImplicitRoot(false);
                    break;
                case 2:
                    trace.ClearJournal();
                    trace.AddJournal(99);
                    break;
                case 3:
                    trace.ClearJournal();
                    trace.AddJournal(static_cast<ui64>(ERequestTimingOp::Wait));
                    trace.AddJournal(0);
                    break;
                case 4:
                    trace.ClearJournal();
                    trace.AddJournal(
                        static_cast<ui64>(ERequestTimingOp::Finish));
                    trace.AddJournal(ui64{1} << 32);
                    trace.AddJournal(90);
                    break;
                case 5:
                    trace.ClearJournal();
                    for (ui64 word:
                         {ui64{4},
                          ui64{0}, ui64{90}, std::numeric_limits<ui64>::max()})
                    {
                        trace.AddJournal(word);
                    }
                    break;
                case 6:
                    trace.ClearJournal();
                    trace.AddJournal(
                        static_cast<ui64>(ERequestTimingOp::Incomplete));
                    trace.AddJournal(std::numeric_limits<ui64>::max());
                    break;
                case 7:
                    trace.ClearJournal();
                    trace.AddJournal(
                        static_cast<ui64>(ERequestTimingOp::Incomplete));
                    trace.AddJournal(1);
                    trace.AddJournal(0x100);   // nonzero unused byte
                    break;
                case 8:
                    trace.ClearJournal();
                    for (size_t i = 0; i <= MaxRequestTimingJournalWords; ++i) {
                        trace.AddJournal(0);
                    }
                    break;
                case 9:
                    trace.AddJournal(
                        static_cast<ui64>(ERequestTimingOp::Finish));
                    break;
                case 10:
                    trace.SetVersion(1);   // mixed graph/journal representation
                    break;
                case 11:
                    trace.ClearErrorCode();
                    break;
            }
            const auto json = Json(FormatRequestTimingTrace(trace));
            UNIT_ASSERT_C(!json["complete"].GetBoolean(), shape);
            UNIT_ASSERT_VALUES_EQUAL(
                json["reason"].GetString(), "invalid_timing_trace");
            UNIT_ASSERT(json["without_waits_us"].IsNull());
            UNIT_ASSERT(json["wait_impact_us"].IsNull());
            UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
        }
    }

    Y_UNIT_TEST(ShouldSerializeFrozenGraphConcurrently)
    {
        TRequestTimingCollector timing(7772);
        const auto fork = timing.Fork(0, 0);
        const auto child = timing.Start(fork, 0);
        timing.Wait(child, 1, 0, 80);
        timing.Finish(child, 90);
        timing.Join(0, {child}, 90);
        timing.Freeze(90, 1, 7);

        // Nothing below may change the frozen graph, even before its first
        // serialization on another thread.
        timing.Wait(0, 1, 0, 90);
        timing.Missing(0, 4, 5);
        timing.Join(0, {}, 999);
        timing.Cancel(child, 999);
        timing.Finish(child, 999);
        UNIT_ASSERT_VALUES_EQUAL(
            timing.Start(fork, 999), TRequestTimingCollector::InvalidId);

        std::atomic<bool> go{false};
        std::thread lateWriter(
            [&]
            {
                while (!go.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (ui32 i = 0; i < 128; ++i) {
                    timing.Wait(0, 7, 0, 999);
                    timing.Missing(0, 4, 5);
                    timing.Incomplete("late");
                    timing.Cancel(child, 999);
                }
            });
        TVector<TString> snapshots(8);
        TVector<std::thread> readers;
        for (size_t i = 0; i < snapshots.size(); ++i) {
            readers.emplace_back(
                [&, i]
                {
                    while (!go.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    if (i < 4) {
                        snapshots[i] = timing.Complete(999, 7, 99);
                    } else {
                        NProto::TRequestTimingTrace trace;
                        timing.FillTrace(trace);
                        snapshots[i] = FormatRequestTimingTrace(trace);
                    }
                });
        }
        go.store(true, std::memory_order_release);
        for (auto& reader: readers) {
            reader.join();
        }
        lateWriter.join();
        for (const auto& snapshot: snapshots) {
            UNIT_ASSERT_VALUES_EQUAL(snapshot, snapshots[0]);
        }
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(snapshots[0], &json, true));
        UNIT_ASSERT(json["complete"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
        UNIT_ASSERT_VALUES_EQUAL(json["without_waits_us"].GetUInteger(), 10);
        UNIT_ASSERT_VALUES_EQUAL(json["wait_impact_us"].GetUInteger(), 80);
        UNIT_ASSERT_VALUES_EQUAL(json["selected_categories"].GetUInteger(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["error_code"].GetUInteger(), 7);
    }

    Y_UNIT_TEST(ShouldRecordParallelWaitsAndFreezeSnapshot)
    {
        TRequestTimingCollector timing;
        const auto fork = timing.Fork(0, 0);
        const auto a = timing.Start(fork, 0);
        const auto b = timing.Start(fork, 0);
        timing.Wait(a, 1, 0, 80);
        timing.Finish(a, 90);
        timing.Finish(b, 60);
        timing.Join(0, {a, b}, 90);
        const auto snapshot = timing.Complete(90, 7);
        const auto json = Json(snapshot);
        UNIT_ASSERT(json["complete"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(json["without_waits_us"].GetUInteger(), 60);
        UNIT_ASSERT_VALUES_EQUAL(json["wait_impact_us"].GetUInteger(), 30);
        timing.Missing(a, 7, 100);
        timing.Wait(b, 1, 0, 100);
        timing.Fork(0, 200);
        UNIT_ASSERT_VALUES_EQUAL(snapshot, timing.Complete(200, 7));
    }

    Y_UNIT_TEST(ShouldFreezeConsistentSnapshotWhileRecording)
    {
        // Exercise both forced orders, then allow Complete to race with the
        // writer. Every result must correspond to one whole prefix of events.
        for (ui32 trial = 0; trial < 18; ++trial) {
            TRequestTimingCollector timing;
            std::promise<void> ready;
            std::promise<void> start;
            auto startSignal = start.get_future();
            auto writer = std::async(
                std::launch::async,
                [&]
                {
                    ready.set_value();
                    startSignal.wait();
                    timing.Wait(0, 1, 0, 80);
                    timing.Missing(0, 4, 5);
                });
            ready.get_future().wait();
            TString snapshot;
            if (trial == 0) {
                start.set_value();
                writer.get();
                snapshot = timing.Complete(90, 7);
            } else if (trial == 1) {
                // Release the worker even if snapshot construction throws.
                try {
                    snapshot = timing.Complete(90, 7);
                } catch (...) {
                    start.set_value();
                    throw;
                }
                start.set_value();
                writer.get();
            } else {
                start.set_value();
                snapshot = timing.Complete(90, 7);
                writer.get();
            }

            const auto json = Json(snapshot);
            UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
            const auto& stages = json["stages"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(stages.size(), 1);
            const auto& waits = stages[0]["waits"].GetArray();
            const auto missing = stages[0]["missing_categories"].GetUInteger();
            UNIT_ASSERT(waits.size() <= 1);
            UNIT_ASSERT(missing == 0 || missing == 4);
            UNIT_ASSERT(!missing || waits.size() == 1);
            const auto& unlocated = stages[0]["unlocated_wait_us"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(unlocated.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(unlocated[0].GetUInteger(), 0);
            UNIT_ASSERT_VALUES_EQUAL(unlocated[1].GetUInteger(), 0);
            UNIT_ASSERT_VALUES_EQUAL(
                unlocated[2].GetUInteger(), missing ? 5 : 0);
            if (!waits.empty()) {
                UNIT_ASSERT_VALUES_EQUAL(waits[0]["begin_us"].GetUInteger(), 0);
                UNIT_ASSERT_VALUES_EQUAL(waits[0]["end_us"].GetUInteger(), 80);
                UNIT_ASSERT_VALUES_EQUAL(
                    waits[0]["categories"].GetUInteger(), 1);
            }
            if (missing) {
                UNIT_ASSERT(!json["complete"].GetBoolean());
                UNIT_ASSERT(!json["reason"].GetString().empty());
                UNIT_ASSERT(json["without_waits_us"].IsNull());
                UNIT_ASSERT(json["wait_impact_us"].IsNull());
            } else {
                UNIT_ASSERT(json["complete"].GetBoolean());
                UNIT_ASSERT(json["reason"].GetString().empty());
                UNIT_ASSERT_VALUES_EQUAL(
                    json["without_waits_us"].GetUInteger(),
                    waits.empty() ? 90 : 10);
                UNIT_ASSERT_VALUES_EQUAL(
                    json["wait_impact_us"].GetUInteger(),
                    waits.empty() ? 0 : 80);
            }
            if (trial == 0) {
                UNIT_ASSERT_VALUES_EQUAL(missing, 4);
            } else if (trial == 1) {
                UNIT_ASSERT(waits.empty());
                UNIT_ASSERT_VALUES_EQUAL(missing, 0);
            }

            timing.Wait(0, 1, 0, 100);
            timing.Missing(0, 4, 10);
            UNIT_ASSERT_VALUES_EQUAL(snapshot, timing.Complete(200, 0, 42));
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidCollectorLaunchAndJoin)
    {
        const auto checkIncomplete =
            [](const TString& snapshot, const TString& reason)
        {
            const auto json = Json(snapshot);
            UNIT_ASSERT(!json["complete"].GetBoolean());
            UNIT_ASSERT_VALUES_EQUAL(json["reason"].GetString(), reason);
            UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
            UNIT_ASSERT(json["without_waits_us"].IsNull());
            UNIT_ASSERT(json["wait_impact_us"].IsNull());
        };

        TRequestTimingCollector launch;
        const auto fork = launch.Fork(0, 10);
        UNIT_ASSERT_VALUES_EQUAL(
            launch.Start(fork, 9), TRequestTimingCollector::InvalidId);
        // Finish the otherwise valid graph, preserving the rejected event.
        const auto child = launch.Start(fork, 10);
        launch.Finish(child, 20);
        launch.Join(0, {child}, 20);
        checkIncomplete(launch.Complete(90, 7), "invalid_launch_time");

        TRequestTimingCollector duplicate;
        const auto duplicateFork = duplicate.Fork(0, 0);
        const auto duplicateChild = duplicate.Start(duplicateFork, 0);
        duplicate.Finish(duplicateChild, 20);
        duplicate.Join(0, {duplicateChild, duplicateChild}, 20);
        duplicate.Join(0, {duplicateChild}, 20);
        checkIncomplete(duplicate.Complete(90, 7), "duplicate_join_child");

        TRequestTimingCollector join;
        const auto joinFork = join.Fork(0, 0);
        const auto joinChild = join.Start(joinFork, 0);
        join.Finish(joinChild, 20);
        join.Join(0, {joinChild}, 19);
        checkIncomplete(join.Complete(90, 7), "invalid_join_time");
    }

    Y_UNIT_TEST(ShouldKeepFutureObservationsOutsideResponseBoundary)
    {
        for (const bool futureStage: {false, true}) {
            TRequestTimingCollector timing;
            if (futureStage) {
                const auto fork = timing.Fork(0, 0);
                const auto child = timing.Start(fork, 100);
                timing.Wait(child, 1, 100, 120);
            } else {
                timing.Wait(0, 1, 100, 120);
            }
            const auto json = Json(timing.Complete(90, 7));
            UNIT_ASSERT(!json["complete"].GetBoolean());
            UNIT_ASSERT(!json["reason"].GetString().empty());
            UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
            UNIT_ASSERT(json["without_waits_us"].IsNull());
            UNIT_ASSERT(json["wait_impact_us"].IsNull());
            for (const auto& stage: json["stages"].GetArray()) {
                UNIT_ASSERT(stage["begin_us"].GetUInteger() <= 90);
                UNIT_ASSERT(stage["end_us"].GetUInteger() <= 90);
                UNIT_ASSERT(stage["waits"].GetArray().empty());
            }
        }
    }

    Y_UNIT_TEST(ShouldNotWaitForCancelledSibling)
    {
        TRequestTimingCollector timing;
        const auto fork = timing.Fork(0, 0);
        const auto a = timing.Start(fork, 0);
        const auto b = timing.Start(fork, 0);
        timing.Wait(a, 1, 0, 80);
        timing.Wait(b, 1, 0, std::numeric_limits<ui64>::max());
        timing.Finish(a, 90);
        timing.Cancel(b, 90);
        timing.Join(0, {a}, 90);
        const auto json = Json(timing.Complete(90, 7));
        UNIT_ASSERT(json["complete"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(json["without_waits_us"].GetUInteger(), 10);
        UNIT_ASSERT_VALUES_EQUAL(json["stages"].GetArray().size(), 4);
    }

    Y_UNIT_TEST(ShouldExposeMissingIntervalsAndJoin)
    {
        TRequestTimingCollector missingWait;
        missingWait.Missing(0, 1, 30);
        const auto json = Json(missingWait.Complete(90, 7));
        UNIT_ASSERT(!json["complete"].GetBoolean());
        UNIT_ASSERT(json["without_waits_us"].IsNull());
        UNIT_ASSERT(json["wait_impact_us"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
        UNIT_ASSERT_VALUES_EQUAL(
            json["stages"][0]["unlocated_wait_us"][0].GetUInteger(), 30);

        TRequestTimingCollector missingJoin;
        missingJoin.Fork(0, 0);
        UNIT_ASSERT(
            !Json(missingJoin.Complete(90, 7))["complete"].GetBoolean());
    }

    Y_UNIT_TEST(ShouldBoundRecordingMemory)
    {
        TRequestTimingCollector timing;
        for (size_t i = 0; i < TRequestTimingCollector::MaxEvents + 1; ++i) {
            timing.Wait(0, 1, 0, 1);
        }
        const auto json = Json(timing.Complete(2, 7));
        UNIT_ASSERT(!json["complete"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 2);
    }
    Y_UNIT_TEST(ShouldPreserveEvidenceWhenRecordingCannotBeCompleted)
    {
        TRequestTimingCollector timing;
        const auto fork = timing.Fork(0, 10);
        const auto child = timing.Start(fork, 10);
        timing.Missing(0, 1, 5);
        timing.Wait(0, 1, 10, 20);
        timing.Finish(child, 30);
        timing.Join(0, {child}, 30);
        const auto json = Json(timing.Complete(40, 7));
        UNIT_ASSERT(!json["complete"].GetBoolean());
        bool found = false;
        for (const auto& s: json["stages"].GetArray()) {
            found |= s["unlocated_wait_us"][0].GetUInteger() == 5;
        }
        UNIT_ASSERT(found);

        TRequestTimingCollector open;
        open.Wait(0, 1, 0, std::numeric_limits<ui64>::max());
        const auto f = open.Fork(0, 10);
        const auto c = open.Start(f, 10);
        open.Finish(c, 20);
        open.Join(0, {c}, 20);
        UNIT_ASSERT(!Json(open.Complete(30, 7))["complete"].GetBoolean());
    }

    Y_UNIT_TEST(ShouldRejectUnboundedOrUnrelatedJoinDependencies)
    {
        TRequestTimingCollector timing;
        const auto fork = timing.Fork(0, 0);
        const auto child = timing.Start(fork, 0);
        timing.Finish(child, 10);
        timing.Join(
            0,
            TVector<ui32>(TRequestTimingCollector::MaxEvents + 1, child), 10);
        const auto json = Json(timing.Complete(20, 7));
        UNIT_ASSERT(!json["complete"].GetBoolean());

        TRequestTimingCollector unrelated;
        const auto first = unrelated.Fork(0, 0);
        const auto a = unrelated.Start(first, 0);
        unrelated.Finish(a, 10);
        unrelated.Join(0, {a}, 10);
        unrelated.Fork(0, 20);
        unrelated.Join(0, {a}, 20);
        UNIT_ASSERT(!Json(unrelated.Complete(30, 7))["complete"].GetBoolean());
    }

    Y_UNIT_TEST(ShouldRecalculateNestedForks)
    {
        TRequestTimingCollector timing;
        const auto fork = timing.Fork(0, 10);
        const auto a = timing.Start(fork, 10);
        const auto b = timing.Start(fork, 10);
        const auto inner = timing.Fork(a, 20);
        const auto a1 = timing.Start(inner, 20);
        const auto a2 = timing.Start(inner, 20);
        timing.Wait(a1, 1, 20, 70);
        timing.Finish(a1, 80);
        timing.Finish(a2, 60);
        timing.Join(a, {a1, a2}, 80);
        timing.Finish(a, 90);
        timing.Finish(b, 85);
        timing.Join(0, {a, b}, 90);
        const auto json = Json(timing.Complete(100, 7));
        UNIT_ASSERT_C(
            json["complete"].GetBoolean(), json["reason"].GetString());
        UNIT_ASSERT_VALUES_EQUAL(json["without_waits_us"].GetUInteger(), 95);
    }
}

}   // namespace NCloud
