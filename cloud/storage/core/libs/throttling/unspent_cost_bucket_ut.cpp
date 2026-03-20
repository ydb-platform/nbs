#include "unspent_cost_bucket.h"

#include <library/cpp/testing/unittest/registar.h>

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
    // spentShapingBudgetShare=0.0 (full budget)
    Y_UNIT_TEST(ShouldRegister)
    {
        TUnspentCostBucket bucket(
            TDuration::MilliSeconds(100),
            TDuration::Seconds(1),
            2.0,
            0.0);

        // budget=100k, cost*0.5=20k, need=20k, covered=20k => budget=80k
        REG_AND_CHECK(20'000, 100'000, 40'000, 0);
        // budget=80k+10k=90k, cost*0.5=20k, need=20k, covered=20k => budget=70k
        REG_AND_CHECK(20'000, 200'000, 40'000, 0);
        // budget=70k+10k=80k, maxSpend=20k, minSpend=0, need=20k => budget=60k
        REG_AND_CHECK(0, 300'000, 40'000, 20'000);
        // spent >= cost => 0
        REG_AND_CHECK(0, 400'000, 40'000, 40'000);
        // budget=70k+10k=80k, maxSpend=200k, minSpend=100k, need=100k,
        // covered=80k => budget=0
        REG_AND_CHECK(120'000, 500'000, 200'000, 0);
        // budget=0+10k=10k, maxSpend=50k, minSpend=25k, need=25k,
        // covered=10k => budget=0
        REG_AND_CHECK(40'000, 600'000, 50'000, 0);
        // budget=0+100k=100k (full refill), maxSpend=50k, minSpend=25k,
        // need=25k, covered=25k => budget=75k
        REG_AND_CHECK(25'000, 1'600'000, 50'000, 0);
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
        // budget=0+40k=40k, need=20k, covered=20k => budget=20k
        REG_AND_CHECK(20'000, 500'000, 40'000, 0);
        // budget=20k+10k=30k, maxSpend=60k, minSpend=30k, need=30k,
        // covered=30k => budget=0
        REG_AND_CHECK(30'000, 600'000, 60'000, 0);
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

        // budget=100k, need=20k, covered=20k => budget=80k
        REG_AND_CHECK(20'000, 100'000, 40'000, 0);
        // no refill: budget=80k, need=20k, covered=20k => budget=60k
        REG_AND_CHECK(20'000, 500'000, 40'000, 0);
        // budget=60k, maxSpend=200k, minSpend=100k, need=100k,
        // covered=60k => budget=0
        REG_AND_CHECK(140'000, 600'000, 200'000, 0);
        // no refill: budget=0, no coverage => full cost
        REG_AND_CHECK(40'000, 1'600'000, 40'000, 0);
        // still empty
        REG_AND_CHECK(40'000, 10'000'000, 40'000, 0);
    }

    // spendRate=1.0 makes BudgetSpendRate=1.0, so minBudgetSpend equals
    // maxBudgetSpend and budget is never consumed
    Y_UNIT_TEST(ShouldRegisterWithDisabledSpendRate)
    {
        TUnspentCostBucket bucket(
            TDuration::MilliSeconds(100),
            TDuration::Seconds(1),
            1.0,
            0.0);

        // minBudgetSpend = cost*1.0 = cost, neededBudgetSpend = 0
        REG_AND_CHECK(40'000, 100'000, 40'000, 0);
        REG_AND_CHECK(40'000, 200'000, 40'000, 0);
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

        // budget=100k, minSpend=10k, need=90k, covered=90k => budget=10k
        REG_AND_CHECK(10'000, 100'000, 100'000, 0);
        // budget=10k+10k=20k, need=90k, covered=20k => budget=0
        REG_AND_CHECK(80'000, 200'000, 100'000, 0);
        // budget=0+100k=100k (full refill), need=90k, covered=90k => budget=10k
        REG_AND_CHECK(10'000, 1'200'000, 100'000, 0);
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

        REG_AND_CHECK(20'000, 100'000, 40'000, 0);   // budget=80k

        // budget=80k+10k=90k, spent share=1-0.9=0.1
        GET_SHARE_AND_CHECK(0.1, 200'000);
        // budget=80k+15k=95k, spent share=1-0.95=0.05
        GET_SHARE_AND_CHECK(0.05, 250'000);

        // budget=80k+40k=100k (capped), maxSpend=200k, minSpend=100k,
        // need=100k, covered=100k => budget=0
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
}

}   // namespace NCloud
