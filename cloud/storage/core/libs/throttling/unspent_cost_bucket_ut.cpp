#include "unspent_cost_bucket.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUnspentCostBucketTest)
{
#define REG_AND_CHECK(resultMcs, nowMcs, costMcs, spentMcs) \
    UNIT_ASSERT_VALUES_EQUAL(                               \
        TDuration::MicroSeconds(resultMcs),                 \
        bucket.Register(                                    \
            TInstant::MicroSeconds(nowMcs),                 \
            TDuration::MicroSeconds(costMcs),               \
            TDuration::MicroSeconds(spentMcs)));            \
// REG_AND_CHECK
#define GET_SHARE_AND_CHECK(currentShare, nowMcs) \
    UNIT_ASSERT_DOUBLES_EQUAL(                    \
        currentShare,                             \
        bucket.CalculateCurrentSpentBudgetShare(  \
            TInstant::MicroSeconds(nowMcs)),      \
        1e-10);                                   \
    // GET_SHARE_AND_CHECK

    // maxBudget=100ms, refillTime=1s (rate=0.1), spendRate=2 (factor=0.5),
    // spentShapingBudgetShare=0.0 (full budget). Coverage scales with
    // Smootherstep(CurrentBudget / MaxBudget).
    Y_UNIT_TEST(ShouldRegister)
    {
        TUnspentCostBucket bucket(
            TDuration::MilliSeconds(100),
            TDuration::Seconds(1),
            2.0,
            0.0);

        // Full budget => discount=1 => cover full gap (40k), return 0
        REG_AND_CHECK(0, 100'000, 40'000, 0);
        // 70k budget, partial discount => covered 36738, return 3262
        REG_AND_CHECK(3262, 200'000, 40'000, 0);
        // +10k refill, partial spend with spent=20k
        REG_AND_CHECK(12'496, 300'000, 40'000, 20'000);
        // spent >= cost => 0
        REG_AND_CHECK(0, 400'000, 40'000, 40'000);
        // +40k refill to cap, large cost, partial coverage
        REG_AND_CHECK(144'242, 500'000, 200'000, 0);
        // nearly empty budget => small covered portion
        REG_AND_CHECK(40'000, 600'000, 50'000, 0);
        // full refill then full coverage of 50k gap
        REG_AND_CHECK(0, 1'600'000, 50'000, 0);
        // spent >= cost => 0
        REG_AND_CHECK(0, 1'700'000, 50'000, 50'000);
    }

    // maxBudget=100ms, refillTime=1s (rate=0.1), spendRate=2 (factor=0.5),
    // spentShapingBudgetShare=1.0 (empty budget)
    Y_UNIT_TEST(ShouldRegisterWithCustomInitialBudget)
    {
        TUnspentCostBucket bucket(
            TDuration::MilliSeconds(100),
            TDuration::Seconds(1),
            2.0,
            1.0);

        // budget=0, no coverage => full cost returned
        REG_AND_CHECK(40'000, 100'000, 40'000, 0);
        // budget=0+40k=40k, Smootherstep scales desired spend
        REG_AND_CHECK(13'651, 500'000, 40'000, 0);
        // refill + scaled coverage for cost=60k
        REG_AND_CHECK(36'349, 600'000, 60'000, 0);
        // budget=0+100k=100k (full refill), maxSpend=25k, minSpend=0,
        // need=25k, covered=25k => budget=75k
        REG_AND_CHECK(0, 1'600'000, 50'000, 25'000);
    }

    // maxBudget=100ms, refillTime=0 (rate=0), spendRate=2 (factor=0.5),
    // spentShapingBudgetShare=0.0 -- once budget depletes, it never refills
    Y_UNIT_TEST(ShouldRegisterWithZeroRefillRate)
    {
        TUnspentCostBucket bucket(
            TDuration::MilliSeconds(100),
            TDuration::Zero(),
            2.0,
            0.0);

        // full budget => full gap covered
        REG_AND_CHECK(0, 100'000, 40'000, 0);
        // no refill: Smootherstep from 60k budget
        REG_AND_CHECK(6349, 500'000, 40'000, 0);
        // no refill: remainder after partial coverage
        REG_AND_CHECK(173'651, 600'000, 200'000, 0);
        // no refill: budget depleted, no coverage
        REG_AND_CHECK(40'000, 1'600'000, 40'000, 0);
        // still empty
        REG_AND_CHECK(40'000, 10'000'000, 40'000, 0);
    }

    // spendRate=1.0 makes BudgetSpendRate=1.0, so minBudgetSpend equals
    // maxBudgetSpend; the whole gap is the "minimum" spend from budget.
    Y_UNIT_TEST(ShouldRegisterWithDisabledSpendRate)
    {
        TUnspentCostBucket bucket(
            TDuration::MilliSeconds(100),
            TDuration::Seconds(1),
            1.0,
            0.0);

        // min=max=gap => desired always equals gap; budget covers it => 0 delay
        REG_AND_CHECK(0, 100'000, 40'000, 0);
        REG_AND_CHECK(0, 200'000, 40'000, 0);
        // spent >= cost => 0
        REG_AND_CHECK(0, 300'000, 40'000, 40'000);
    }

    // spendRate=10.0 makes BudgetSpendRate=0.1, so budget covers 90% of cost
    Y_UNIT_TEST(ShouldRegisterWithHighSpendRate)
    {
        TUnspentCostBucket bucket(
            TDuration::MilliSeconds(100),
            TDuration::Seconds(1),
            10.0,
            0.0);

        // full budget => cover up to min(200k desired, 100k budget)
        REG_AND_CHECK(0, 100'000, 100'000, 0);
        // 10k budget, low discount => covered 10k of 90k need
        REG_AND_CHECK(90'000, 200'000, 100'000, 0);
        // full refill => full coverage of 90k need
        REG_AND_CHECK(0, 1'200'000, 100'000, 0);
    }

    Y_UNIT_TEST(ShouldSpentBudgetShare)
    {
        TUnspentCostBucket bucket(
            TDuration::MilliSeconds(100),
            TDuration::Seconds(1),
            2.0,
            0.0);

        // budget=100k (no LastUpdateTs yet), spent share=1-100k/100k=0
        GET_SHARE_AND_CHECK(0.0, 50'000);

        REG_AND_CHECK(0, 100'000, 40'000, 0);   // budget=60k after

        // 60k+10k refill => 70k budget, spent share=0.3
        GET_SHARE_AND_CHECK(0.3, 200'000);
        // +15k refill => 75k budget
        GET_SHARE_AND_CHECK(0.25, 250'000);

        // +25k refill to cap, large cost, return 100k of 200k gap
        REG_AND_CHECK(100'000, 500'000, 200'000, 0);

        // budget=0, spent share=1-0=1.0
        GET_SHARE_AND_CHECK(1.0, 500'000);
        // budget=0+50k=50k, spent share=1-0.5=0.5
        GET_SHARE_AND_CHECK(0.5, 1'000'000);
        // budget=0+100k=100k (capped), spent share=1-1=0
        GET_SHARE_AND_CHECK(0.0, 1'500'000);
    }

    Y_UNIT_TEST(ShouldSpentBudgetShareWithCustomInitialBudget)
    {
        TUnspentCostBucket bucket(
            TDuration::MilliSeconds(100),
            TDuration::Seconds(1),
            2.0,
            1.0);

        // budget=0 (no LastUpdateTs yet), spent share=1-0=1.0
        GET_SHARE_AND_CHECK(1.0, 50'000);

        // budget=0
        REG_AND_CHECK(40'000, 100'000, 40'000, 0);

        // budget=0+10k=10k, spent share=1-0.1=0.9
        GET_SHARE_AND_CHECK(0.9, 200'000);
        // budget=0+50k=50k, spent share=1-0.5=0.5
        GET_SHARE_AND_CHECK(0.5, 600'000);
        // budget=0+100k=100k (capped), spent share=1-1=0
        GET_SHARE_AND_CHECK(0.0, 1'100'000);
    }

#undef REG_AND_CHECK
#undef GET_SHARE_AND_CHECK

    Y_UNIT_TEST(Smootherstep)
    {
        constexpr double Eps = 1e-10;
        UNIT_ASSERT_DOUBLES_EQUAL(
            0.0,
            TUnspentCostBucket::Smootherstep(0.0),
            Eps);
        UNIT_ASSERT_DOUBLES_EQUAL(
            1.0,
            TUnspentCostBucket::Smootherstep(1.0),
            Eps);

        for (int i = 0; i < 100'000'000; i++) {
            const double x = RandomNumber<double>();
            const double result = TUnspentCostBucket::Smootherstep(x);

            UNIT_ASSERT_LE_C(
                result,
                1.0 + Eps,
                Sprintf("Smootherstep(%f) = %f", x, result).c_str());
            UNIT_ASSERT_GE_C(
                result,
                0.0,
                Sprintf("Smootherstep(%f) = %f", x, result).c_str());

            if (x <= 0.5) {
                UNIT_ASSERT_LE_C(
                    result,
                    0.5 + Eps,
                    Sprintf("Smootherstep(%f) = %f", x, result).c_str());
            } else {
                UNIT_ASSERT_GE_C(
                    result,
                    0.5 - Eps,
                    Sprintf("Smootherstep(%f) = %f", x, result).c_str());
            }
        }
    }
}

}   // namespace NCloud
