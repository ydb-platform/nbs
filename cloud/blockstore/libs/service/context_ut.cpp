#include <cloud/blockstore/libs/service/context.h>

#include <library/cpp/json/json_reader.h>
#include <util/datetime/cputimer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TContextTest)
{
    Y_UNIT_TEST(ShouldCastBaseCallContextToBlockStoreCallContext)
    {
        auto concrete = CreateCallContext(42);
        auto* raw = concrete.Get();

        TCallContextBasePtr base = concrete;
        UNIT_ASSERT_VALUES_EQUAL(concrete.RefCount(), 2);

        concrete.Reset();
        UNIT_ASSERT_VALUES_EQUAL(base.RefCount(), 1);

        auto restored = ToBlockStoreCallContext(std::move(base));

        UNIT_ASSERT(!base);
        UNIT_ASSERT(restored);
        UNIT_ASSERT_VALUES_EQUAL(restored.Get(), raw);
        UNIT_ASSERT_VALUES_EQUAL(restored.RefCount(), 1);
    }
    Y_UNIT_TEST(ShouldKeepLegacyCountersFlagsAndTraceShared)
    {
        auto root = CreateCallContext(42);
        const ui64 start = GetCycleCount();
        root->SetRequestStartedCycles(start);
        root->EnableRequestTiming();
        const auto fork = root->ForkRequestTiming(start);
        auto a = root->CreateChild(fork, start);
        auto b = root->CreateChild(fork, start);
        UNIT_ASSERT(a.Get() != root.Get());
        UNIT_ASSERT(b.Get() != a.Get());
        UNIT_ASSERT_VALUES_EQUAL(&root->LWOrbit, &a->LWOrbit);
        UNIT_ASSERT_VALUES_EQUAL(&root->LWOrbit, &b->LWOrbit);

        a->AddTime(EProcessingStage::Postponed, TDuration::MicroSeconds(80));
        b->AddTime(EProcessingStage::Postponed, TDuration::MicroSeconds(80));
        UNIT_ASSERT_VALUES_EQUAL(
            root->Time(EProcessingStage::Postponed).MicroSeconds(), 160);
        UNIT_ASSERT_VALUES_EQUAL(
            b->Time(EProcessingStage::Postponed).MicroSeconds(), 160);
        a->SetHasUncountableRejects();
        UNIT_ASSERT(root->GetHasUncountableRejects());
        UNIT_ASSERT(b->GetHasUncountableRejects());
        root->SetSilenceRetriableErrors(true);
        UNIT_ASSERT(a->GetSilenceRetriableErrors());
        b->SetPossiblePostponeDuration(TDuration::MicroSeconds(5));
        UNIT_ASSERT_VALUES_EQUAL(
            root->GetPossiblePostponeDuration().MicroSeconds(), 5);
        UNIT_ASSERT_VALUES_EQUAL(a->GetRequestStartedCycles(), start);
        UNIT_ASSERT_VALUES_EQUAL(
            a->GetPossiblePostponeDuration().MicroSeconds(), 5);
        b->SetSilenceRetriableErrors(false);
        UNIT_ASSERT(!root->GetSilenceRetriableErrors());
        UNIT_ASSERT(!a->GetSilenceRetriableErrors());
        a->SetSilenceRetriableErrors(true);
        UNIT_ASSERT(b->GetSilenceRetriableErrors());
        b->SetResponseSentCycles(start + 1);
        UNIT_ASSERT_VALUES_EQUAL(root->GetResponseSentCycles(), start + 1);
        UNIT_ASSERT_VALUES_EQUAL(a->GetResponseSentCycles(), start + 1);
        a->SetRequestStartedCycles(start + 2);
        UNIT_ASSERT_VALUES_EQUAL(root->GetRequestStartedCycles(), start + 2);
        UNIT_ASSERT_VALUES_EQUAL(b->GetRequestStartedCycles(), start + 2);
    }

    Y_UNIT_TEST(ShouldShareNestedLiveRequestTiming)
    {
        auto root = CreateCallContext();
        const ui64 start = GetCycleCount();
        const auto cycles = [start](ui64 us) {
            return start + DurationToCyclesSafe(TDuration::MicroSeconds(us));
        };
        root->SetRequestStartedCycles(start);
        root->EnableRequestTiming();
        auto child = root->CreateChild(root->ForkRequestTiming(start), start);
        auto grandchild = child->CreateChild(
            child->ForkRequestTiming(start), start);

        grandchild->Postpone(cycles(20000));
        UNIT_ASSERT_VALUES_EQUAL(
            root->CalcRequestTime(cycles(100000)).ExecutionTime,
            CyclesToDurationSafe(cycles(20000) - start));
        grandchild->Advance(cycles(50000));
        const auto total = CyclesToDurationSafe(cycles(100000) - start);
        const auto postponed = CyclesToDurationSafe(
            cycles(50000) - cycles(20000));
        UNIT_ASSERT_VALUES_EQUAL(
            root->Time(EProcessingStage::Postponed), postponed);
        UNIT_ASSERT_VALUES_EQUAL(
            root->CalcRequestTime(cycles(100000)).ExecutionTime,
            total - postponed);
        UNIT_ASSERT_VALUES_EQUAL(
            grandchild->CalcRequestTime(cycles(100000)).ExecutionTime,
            total - postponed);
    }

    Y_UNIT_TEST(ShouldReportChangedMeasurementStartIncomplete)
    {
        auto root = CreateCallContext();
        const ui64 start = GetCycleCount();
        root->SetRequestStartedCycles(start);
        root->EnableRequestTiming();
        root->SetRequestStartedCycles(start + 1);
        root->EnableRequestTiming();

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(
            root->CompleteRequestTiming(TDuration::MicroSeconds(90)),
            &json,
            true));
        UNIT_ASSERT(!json["complete"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(
            json["reason"].GetString(), "measurement_start_changed");
        UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(), 90);
        UNIT_ASSERT(json["without_waits_us"].IsNull());
        UNIT_ASSERT(json["wait_impact_us"].IsNull());
    }

    Y_UNIT_TEST(ShouldRecordOverlappingChildPostponements)
    {
        auto root = CreateCallContext();
        const ui64 start = GetCycleCount();
        const auto cycles = [start](ui64 us) {
            return start + DurationToCyclesSafe(TDuration::MicroSeconds(us));
        };
        root->SetRequestStartedCycles(start);
        root->EnableRequestTiming();
        const auto fork = root->ForkRequestTiming(start);
        auto a = root->CreateChild(fork, start);
        auto b = root->CreateChild(fork, start);
        a->Postpone(start);
        b->Postpone(start);
        a->Advance(cycles(80000));
        b->Advance(cycles(80000));
        a->FinishRequestTiming(cycles(90000));
        b->FinishRequestTiming(cycles(90000));
        root->JoinRequestTiming({a, b}, cycles(90000));
        const auto total = CyclesToDurationSafe(cycles(90000) - start);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(
            root->CompleteRequestTiming(total), &json, true));
        UNIT_ASSERT_C(json["complete"].GetBoolean(), json["reason"].GetString());
        // Conversion from cycles truncates each event to whole microseconds.
        const ui64 withoutWaits = json["without_waits_us"].GetUInteger();
        UNIT_ASSERT_C(
            withoutWaits >= 9998 && withoutWaits <= 10002,
            withoutWaits);
        const ui64 postponed =
            root->Time(EProcessingStage::Postponed).MicroSeconds();
        UNIT_ASSERT_C(
            postponed >= 159998 && postponed <= 160002,
            postponed);
    }

    Y_UNIT_TEST(ShouldAvoidAllocatingChildWhenRecordingDisabled)
    {
        auto root = CreateCallContext();
        auto child = root->CreateChild(0, GetCycleCount());
        UNIT_ASSERT_VALUES_EQUAL(root.Get(), child.Get());
        UNIT_ASSERT(root->CompleteRequestTiming(TDuration::Seconds(1)).empty());
    }

}

}   // namespace NCloud::NBlockStore
