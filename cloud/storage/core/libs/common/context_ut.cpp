#include "context.h"

#include "request_timing_collector.h"

#include <cloud/storage/core/protos/request_timing.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

#include <atomic>
#include <limits>
#include <thread>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCallContext)
{
    void CheckCalcRequestTime(
        ui64 cycles,
        TCallContextBasePtr context, TRequestTime requestTimeAnswer)
    {
        auto requestTime = context->CalcRequestTime(cycles);
        UNIT_ASSERT_VALUES_EQUAL(
            requestTimeAnswer.ExecutionTime, requestTime.ExecutionTime);
        UNIT_ASSERT_VALUES_EQUAL(
            requestTimeAnswer.TotalTime, requestTime.TotalTime);
    }

    Y_UNIT_TEST(ShouldMatchOrdinarySnapshotToCollectorSchema)
    {
        for (const ui64 total:
             {ui64{0}, ui64{90}, std::numeric_limits<ui64>::max()})
        {
            for (const ui32 error: {ui32{0}, ui32{7}}) {
                const auto requestId = std::numeric_limits<ui64>::max();
                auto context = MakeIntrusive<TCallContextBase>(requestId);
                context->SetRequestStartedCycles(10);
                context->EnableRequestTiming();
                const auto text = context
                                      ->FreezeRequestTiming(
                                          TDuration::MicroSeconds(total), error)
                                      .Serialize();
                TRequestTimingCollector reference(requestId);
                NJson::TJsonValue actual;
                NJson::TJsonValue expected;
                UNIT_ASSERT(NJson::ReadJsonTree(text, &actual, true));
                UNIT_ASSERT(NJson::ReadJsonTree(
                    reference.Complete(
                        total, TRequestTiming::SupportedWaitCategories, error),
                    &expected, true));
                UNIT_ASSERT(actual == expected);
                UNIT_ASSERT(actual["complete"].GetBoolean());
                UNIT_ASSERT_VALUES_EQUAL(
                    actual["total_us"].GetUInteger(), total);
                UNIT_ASSERT_VALUES_EQUAL(
                    actual["without_waits_us"].GetUInteger(), total);
                UNIT_ASSERT_VALUES_EQUAL(
                    actual["wait_impact_us"].GetUInteger(), 0);
                UNIT_ASSERT_VALUES_EQUAL(
                    actual["error_code"].GetUInteger(), error);
                NProto::TRequestTimingTrace compact;
                context
                    ->FreezeRequestTiming(TDuration::MicroSeconds(total), error)
                    .FillTrace(compact);
                UNIT_ASSERT(compact.GetImplicitRoot());
                UNIT_ASSERT_VALUES_EQUAL(compact.StagesSize(), 0);
                UNIT_ASSERT_VALUES_EQUAL(compact.PartsSize(), 0);
            }
        }
    }

    Y_UNIT_TEST(ShouldInitializeChildWhileParentFreezes)
    {
        for (ui32 iteration = 0; iteration < 32; ++iteration) {
            auto parent = MakeIntrusive<TCallContextBase>(ui64{7772});
            parent->SetRequestStartedCycles(10);
            parent->EnableRequestTiming();
            const auto fork = parent->ForkRequestTiming(10);
            auto child = MakeIntrusive<TCallContextBase>(ui64{7772}, parent);
            std::atomic<bool> go{false};
            std::thread freeze(
                [&]
                {
                    while (!go.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    parent->FreezeRequestTiming(TDuration::MicroSeconds(90), 7);
                });
            go.store(true, std::memory_order_release);
            child->InitChildRequestTiming(parent, fork, 20);
            freeze.join();
            const auto expected =
                parent->CompleteRequestTiming(TDuration::MicroSeconds(999), 99);
            const auto actual =
                child->CompleteRequestTiming(TDuration::MicroSeconds(999), 99);
            UNIT_ASSERT_VALUES_EQUAL(actual, expected);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(actual, &json, true));
            UNIT_ASSERT_VALUES_EQUAL(json["request_id"].GetUInteger(), 7772);
            UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
            UNIT_ASSERT_VALUES_EQUAL(json["error_code"].GetUInteger(), 7);
        }
    }

    Y_UNIT_TEST(ShouldRetainOrdinaryTimingAfterContextDestruction)
    {
        TRequestTimingSnapshot snapshot;
        {
            auto context = MakeIntrusive<TCallContextBase>(ui64{7772});
            context->SetRequestStartedCycles(10);
            context->EnableRequestTiming();
            snapshot =
                context->FreezeRequestTiming(TDuration::MicroSeconds(90), 7);
            context->AddTime(
                EProcessingStage::Shaping, TDuration::MicroSeconds(5));
            context->MarkRequestTimingIncomplete("late observation");
            UNIT_ASSERT_VALUES_EQUAL(
                context->ForkRequestTiming(100),
                TRequestTimingCollector::InvalidId);
            UNIT_ASSERT_VALUES_EQUAL(
                context->FreezeRequestTiming(TDuration::MicroSeconds(1000), 99)
                    .Serialize(), snapshot.Serialize());
        }
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(snapshot.Serialize(), &json, true));
        UNIT_ASSERT(json["complete"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(json["request_id"].GetUInteger(), 7772);
        UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
        UNIT_ASSERT_VALUES_EQUAL(json["error_code"].GetUInteger(), 7);
        UNIT_ASSERT_VALUES_EQUAL(json["without_waits_us"].GetUInteger(), 90);
        UNIT_ASSERT_VALUES_EQUAL(json["wait_impact_us"].GetUInteger(), 0);
        UNIT_ASSERT_VALUES_EQUAL(json["stages"].GetArray().size(), 1);
    }

    Y_UNIT_TEST(ShouldFreezeWhileFirstTimingEventIsMaterialized)
    {
        for (ui32 attempt = 0; attempt < 18; ++attempt) {
            auto context = MakeIntrusive<TCallContextBase>(attempt);
            context->SetRequestStartedCycles(10);
            context->EnableRequestTiming();
            std::atomic<bool> go{false};
            auto record = [&]
            {
                context->AddTime(
                    EProcessingStage::Shaping, TDuration::MicroSeconds(5));
            };
            TRequestTimingSnapshot snapshot;
            if (attempt == 0) {
                record();
                snapshot =
                    context->FreezeRequestTiming(TDuration::MicroSeconds(90));
            } else if (attempt == 1) {
                snapshot =
                    context->FreezeRequestTiming(TDuration::MicroSeconds(90));
                record();
            } else {
                std::thread writer(
                    [&]
                    {
                        while (!go.load(std::memory_order_acquire)) {
                            std::this_thread::yield();
                        }
                        record();
                    });
                go.store(true, std::memory_order_release);
                snapshot =
                    context->FreezeRequestTiming(TDuration::MicroSeconds(90));
                writer.join();
            }
            const auto first = snapshot.Serialize();
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(first, &json, true));
            const bool complete = json["complete"].GetBoolean();
            if (attempt < 2) {
                UNIT_ASSERT_VALUES_EQUAL(complete, attempt == 1);
            }
            if (complete) {
                UNIT_ASSERT_VALUES_EQUAL(
                    json["without_waits_us"].GetUInteger(), 90);
            } else {
                UNIT_ASSERT(json["without_waits_us"].IsNull());
                UNIT_ASSERT_VALUES_EQUAL(
                    json["reason"].GetString(),
                    "Missing interval positions for selected wait categories");
                const auto& stage = json["stages"].GetArray().front();
                UNIT_ASSERT_VALUES_EQUAL(
                    stage["missing_categories"].GetUInteger(), 4);
                UNIT_ASSERT_VALUES_EQUAL(
                    stage["unlocated_wait_us"].GetArray()[2].GetUInteger(), 5);
            }
            context->MarkRequestTimingIncomplete("too late");
            UNIT_ASSERT_VALUES_EQUAL(
                context->CompleteRequestTiming(TDuration::MicroSeconds(999), 7),
                first);
        }
    }

    Y_UNIT_TEST(ShouldFreezeOrdinaryOnceConcurrently)
    {
        auto context = MakeIntrusive<TCallContextBase>(ui64{7772});
        context->SetRequestStartedCycles(10);
        context->EnableRequestTiming();
        std::atomic<bool> go{false};
        TVector<TString> snapshots(4);
        TVector<std::thread> readers;
        for (ui32 i = 0; i < snapshots.size(); ++i) {
            readers.emplace_back(
                [&, i]
                {
                    while (!go.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    snapshots[i] = context
                                       ->FreezeRequestTiming(
                                           TDuration::MicroSeconds(100 + i), i)
                                       .Serialize();
                });
        }
        go.store(true, std::memory_order_release);
        for (auto& reader: readers) {
            reader.join();
        }
        for (const auto& snapshot: snapshots) {
            UNIT_ASSERT_VALUES_EQUAL(snapshot, snapshots[0]);
        }
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(snapshots[0], &json, true));
        const auto total = json["total_us"].GetUInteger();
        UNIT_ASSERT(total >= 100 && total < 104);
        UNIT_ASSERT_VALUES_EQUAL(json["error_code"].GetUInteger(), total - 100);
        UNIT_ASSERT(json["complete"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(json["wait_impact_us"].GetUInteger(), 0);
    }

    Y_UNIT_TEST(ShouldKeepFrozenEmptyChildAfterRepeatedInitialization)
    {
        auto parent = MakeIntrusive<TCallContextBase>(ui64{7772});
        parent->SetRequestStartedCycles(10);
        parent->EnableRequestTiming();
        const auto first =
            parent->FreezeRequestTiming(TDuration::MicroSeconds(90), 7)
                .Serialize();
        auto child = MakeIntrusive<TCallContextBase>(ui64{7773}, parent);
        child->InitChildRequestTiming(
            parent, TRequestTimingCollector::InvalidId, 10);
        child->AddTime(EProcessingStage::Shaping, TDuration::MicroSeconds(5));
        auto other = MakeIntrusive<TCallContextBase>(ui64{9999});
        other->SetRequestStartedCycles(10);
        other->EnableRequestTiming();
        child->InitChildRequestTiming(
            other, TRequestTimingCollector::InvalidId, 10);
        UNIT_ASSERT_VALUES_EQUAL(
            child->FreezeRequestTiming(TDuration::MicroSeconds(999), 99)
                .Serialize(), first);
        NProto::TRequestTimingTrace trace;
        child->FreezeRequestTiming(TDuration::MicroSeconds(999), 99)
            .FillTrace(trace);
        UNIT_ASSERT_VALUES_EQUAL(trace.GetRequestId(), 7772);
        UNIT_ASSERT_VALUES_EQUAL(trace.GetTotalMicros(), 90);
        UNIT_ASSERT_VALUES_EQUAL(trace.GetErrorCode(), 7);
    }

    Y_UNIT_TEST(CheckCalcRequestTime)
    {
        auto callContext =
            MakeIntrusive<TCallContextBase>(static_cast<ui64>(0));
        callContext->SetRequestStartedCycles(5);
        CheckCalcRequestTime(
            3,
            callContext,
            {TDuration::Zero(), TDuration::Zero()});

        CheckCalcRequestTime(
            20,
            callContext,
            TRequestTime{
                .TotalTime = CyclesToDurationSafe(15),
                .ExecutionTime = CyclesToDurationSafe(15)});

        callContext->SetResponseSentCycles(13);

        CheckCalcRequestTime(
            20,
            callContext,
            TRequestTime{
                .TotalTime = CyclesToDurationSafe(15),
                .ExecutionTime = CyclesToDurationSafe(7)});

        callContext->SetResponseSentCycles(0);

        CheckCalcRequestTime(
            20,
            callContext,
            TRequestTime{
                .TotalTime = CyclesToDurationSafe(15),
                .ExecutionTime = CyclesToDurationSafe(2)});
    }
}

}   // namespace NCloud
