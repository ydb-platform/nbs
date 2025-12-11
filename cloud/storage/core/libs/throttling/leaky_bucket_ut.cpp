#include "leaky_bucket.h"

#include "helpers.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLeakyBucketTest)
{
#define REG_AND_CHECK(budgetDiff, nowMcs, update)             \
    UNIT_ASSERT_VALUES_EQUAL(                                 \
        budgetDiff,                                           \
        lb.Register(TInstant::MicroSeconds(nowMcs), update)); \
// REG_AND_CHECK
#define REG_AND_CHECK_D(delayMcs, nowMcs, update) \
    REG_AND_CHECK(                                \
        TDuration::MicroSeconds(delayMcs),        \
        nowMcs,                                   \
        TDuration::MicroSeconds(update))          \
// REG_AND_CHECK_D
#define GET_SHARE_AND_CHECK(currentShare, nowMcs)                             \
    UNIT_ASSERT_VALUES_EQUAL(                                                 \
        currentShare,                                                         \
        lb.CalculateCurrentSpentBudgetShare(TInstant::MicroSeconds(nowMcs))); \
    // GET_SHARE_AND_CHECK

    Y_UNIT_TEST(ShouldRegister)
    {
        TLeakyBucket lb(100, 200, 200);

        REG_AND_CHECK(0, 100'000, 110);   // 200 - 110 = 90
        REG_AND_CHECK(0, 500'000, 70);    // 90 + 0.4 * 100 - 70 = 60
        REG_AND_CHECK(0, 600'000, 40);    // 60 + 0.1 * 100 - 40 = 30
        REG_AND_CHECK(10, 800'000, 60);   // 30 + 0.2 * 100 = 50
        REG_AND_CHECK(0, 900'000, 60);    // 50 + 0.1 * 100 - 60 = 0
        REG_AND_CHECK(
            0,
            10'000'000,
            500);   // min(max(0, 200 - 500), 0 + 9.1 * 100 - 500) = 0
        REG_AND_CHECK(
            0,
            20'000'000,
            1);   // 0 + min(max(0, 200 - 1), 10 * 100) = 199
        REG_AND_CHECK(1, 20'000'000, 200);   // 199 + 0 = 199
        REG_AND_CHECK(0, 20'010'000, 200);   // 199. + 0.01 * 100 - 200 = 0
    }

    Y_UNIT_TEST(ShouldRegisterWithCustomInitialBudget)
    {
        TLeakyBucket lb(100, 200, 10);

        REG_AND_CHECK(100, 100'000, 110);   // 10
        REG_AND_CHECK(20, 500'000, 70);     // 10 + 0.4 * 100 = 50
        REG_AND_CHECK(0, 600'000, 40);      // 50 + 0.1 * 100 - 40 = 20
        REG_AND_CHECK(20, 800'000, 60);     // 20 + 0.2 * 100 = 40
        REG_AND_CHECK(10, 900'000, 60);     // 40 + 0.1 * 100 = 50
        REG_AND_CHECK(
            0,
            10'000'000,
            500);   // min(max(0, 200 - 500), 40 + 9.1 * 100 - 500) = 0
        REG_AND_CHECK(
            0,
            20'000'000,
            1);   // min(max(0, 200 - 1), 0 + 10 * 100 - 1) = 199
        REG_AND_CHECK(1, 20'000'000, 200);   // 199 + 0 = 199
        REG_AND_CHECK(0, 20'010'000, 200);   // 199. + 0.01 * 100 - 200 = 0
    }

    Y_UNIT_TEST(ShouldSpentBudgetShare)
    {
        TLeakyBucket lb(100, 200, 200);

        GET_SHARE_AND_CHECK(0, 50'000);   // share = (200 - 200) / 200 = 0
        REG_AND_CHECK(0, 100'000, 110);   // budget = 200 - 110 = 90
        GET_SHARE_AND_CHECK(
            0.5,
            200'000);   // share = (200 - (90 + 0.1 * 100)) / 200 = 0.5
        GET_SHARE_AND_CHECK(
            0.475,
            250'000);   // share = (200 - (90 + 0.15 * 100)) / 200 = 0.475
        REG_AND_CHECK(0, 500'000, 70);   // budget = 90 + 0.4 * 100 - 70 = 60
        GET_SHARE_AND_CHECK(
            0.575,
            750'000);   // share = (200 - (60 + 0.25 * 100)) / 200 = 0.575
        GET_SHARE_AND_CHECK(
            0,
            1'900'000);   // share = (200 - (60 + 1.4 * 100)) / 200 = 0
        REG_AND_CHECK(0, 2'000'000, 200);   // budget = 60 + 1.4 * 100 - 200 = 0
        GET_SHARE_AND_CHECK(
            1,
            2'000'000);   // share = (200 - (0 + 0 * 100)) / 200 = 1
        GET_SHARE_AND_CHECK(
            0.75,
            2'500'000);   // share = (200 - (0 + 0.5 * 100)) = 0.75
    }

    Y_UNIT_TEST(ShouldSpentBudgetShareWithCustomInitialBudget)
    {
        TLeakyBucket lb(100, 200, 10);

        GET_SHARE_AND_CHECK(0.95, 50'000);   // share = (200 - 10) / 200 = 0.95
        REG_AND_CHECK(100, 100'000, 110);    // budget = 10
        GET_SHARE_AND_CHECK(
            0.9,
            200'000);   // share = (200 - (10 + 0.1 * 100)) / 200 = 0.9
        GET_SHARE_AND_CHECK(
            0.875,
            250'000);   // share = (200 - (10 + 0.15 * 100)) / 200 = 0.875
        REG_AND_CHECK(20, 500'000, 70);   // budget = 10 + 0.4 * 100 = 50
        GET_SHARE_AND_CHECK(
            0.625,
            750'000);   // share = (200 - (50 + 0.25 * 100)) / 200 = 0.625
        GET_SHARE_AND_CHECK(
            0.05,
            1'900'000);   // share = (200 - (50 + 1.4 * 100)) / 200 = 0.05
        REG_AND_CHECK(0, 2'000'000, 200);   // budget = 50 + 1.5 * 100 - 200 = 0
        GET_SHARE_AND_CHECK(
            1.0,
            2'000'000);   // share = (200 - (0 + 0 * 100)) / 200 = 1
        GET_SHARE_AND_CHECK(
            0.75,
            2'500'000);   // share = (200 - (0 + 0.5 * 100)) / 200 = 0.75
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateBoostedTimeBucket)
    {
        TBoostedTimeBucket lb(
            TDuration::MilliSeconds(100),
            11,
            TDuration::Seconds(2),
            TDuration::Seconds(20),
            TDuration::Seconds(2));

        // random +1 mcs errors happen due to floating point arithmetic errors
        REG_AND_CHECK_D(0, 100'000, 100'000);   // 0.1 - 0.1 = 0
        REG_AND_CHECK_D(
            0,
            500'000,
            1'300'000);   // 0 + 0.4 - 1.3 + 0.9 = 0, b = 2 - 0.9 = 1.1
        REG_AND_CHECK_D(
            0,
            600'000,
            210'000);   // 0 + 0.1 - 0.21 + 0.11 (boost) = 0, b = 1.1 - 0.11 +
                        // 0.01 = 1
        REG_AND_CHECK_D(100'000, 600'000, 1'100'000);   // nop
        REG_AND_CHECK_D(0, 700'000, 1'100'000);   // 0 + 0.1 - 1.1 + 1 (boost) =
                                                  // 0, b = 1 - 1 + 0.01 = 0.01
        REG_AND_CHECK_D(
            0,
            800'000,
            120'000);   // 0 + 0.1 - 0.12 + 0.02 (boost) = 0, b = 0.01 - 0.02 +
                        // 0.01 = 0
        REG_AND_CHECK_D(
            500'001,
            1800'000,
            1'650'000);   // 0 + 1 + 0.1 (boost) = 1.1, b = 0 - 0.1 + 0.1 = 0
        REG_AND_CHECK_D(
            0,
            2300'000,
            1'650'000);   // 1.1 + 0.5 - 1.65 + 0.05 (boost) = 0, b = 0 - 0.05 +
                          // 0.05 = 0
        REG_AND_CHECK_D(
            100'001,
            2400'000,
            220'000);   // 0 + 0.1 + 0.01 (boost) = 0.11, b = 0 - 0.01 + 0.01 =
                        // 0
        REG_AND_CHECK_D(
            0,
            2500'000,
            220'000);   // 0.11 + 0.1 - 0.22 + 0.01 (boost) = 0, b = 0 - 0.01 +
                        // 0.01 = 0
        REG_AND_CHECK_D(
            0,
            52500'000,
            30'000'000);   // 0 + min(max(0, 0.1 - 30), 50 - 30) = 0, b = 0 + 2
                           // (max) = 2
        REG_AND_CHECK_D(100'000, 52500'000, 1'100'000);   // nop
        REG_AND_CHECK_D(
            0,
            52600'000,
            1'100'000);   // 0 + 0.1 - 1.1 + 1 (boost) = 0, b = 2 - 1 = 1
        REG_AND_CHECK_D(
            0,
            52700'000,
            1'100'000);   // 0 + 0.1 - 1.1 + 1 (boost) = 0, b = 1 - 1 + 0.01 =
                          // 0.01
        REG_AND_CHECK_D(
            0,
            52800'000,
            120'000);   // 0 + 0.1 - 0.12 + 0.02 (boost) = 0, b = 0.01 - 0.02 +
                        // 0.01 = 0
        REG_AND_CHECK_D(
            9'091,
            52900'000,
            120'000);   // 0 + 0.1 + 0.01 (boost) = 0.11, b = 0 - 0.01 + 0.01 =
                        // 0
    }

    Y_UNIT_TEST(
        ShouldCorrectlyCalculateBoostedTimeBucketWithCustomInitialBudget)
    {
        TBoostedTimeBucket lb(
            TDuration::MilliSeconds(100),
            11,
            TDuration::Seconds(2),
            TDuration::Seconds(20),
            TDuration::Seconds(0));

        // random +1 mcs errors happen due to floating point arithmetic errors
        REG_AND_CHECK_D(0, 100'000, 100'000);   // standard = 0.0, boost = 0.0
        REG_AND_CHECK_D(
            81'8182,
            500'000,
            1'300'000);                         // standard = 0.4, boost = 0.0
        REG_AND_CHECK_D(0, 600'000, 210'000);   // standard = 0.0, boost = 0.0
        REG_AND_CHECK_D(
            990'910,
            600'000,
            1'100'000);   // standard = 0.0, boost = 0.01
        REG_AND_CHECK_D(
            890'910,
            700'000,
            1'100'000);                         // standard = 0.1, boost = 0.02
        REG_AND_CHECK_D(0, 800'000, 120'000);   // standard = 0.0, boost = 0.02
        REG_AND_CHECK_D(
            472'728,
            1800'000,
            1'650'000);   // standard = 1.0, boost = 0.13
        REG_AND_CHECK_D(
            0,
            2300'000,
            1'650'000);   // standard = 0.0, boost = 0.03
        REG_AND_CHECK_D(
            72'728,
            2400'000,
            220'000);                            // standard = 0.1, boost = 0.04
        REG_AND_CHECK_D(0, 2500'000, 220'000);   // standard = 0.0, boost = 0.03
        REG_AND_CHECK_D(
            0,
            52500'000,
            30'000'000);   // standard = 0.0, boost = 0.03
        REG_AND_CHECK_D(
            100'000,
            52500'000,
            1'100'000);   // standard = 0.0, boost = 2.0
        REG_AND_CHECK_D(
            0,
            52600'000,
            1'100'000);   // standard = 0.0, boost = 1.0
        REG_AND_CHECK_D(
            0,
            52700'000,
            1'100'000);                           // standard = 0.0, boost = 0.1
        REG_AND_CHECK_D(0, 52800'000, 120'000);   // standard = 0.0, boost = 0.0
        REG_AND_CHECK_D(
            9'091,
            52900'000,
            120'000);   // standard = 0.1, boost = 0.01
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateBoostedTimeBucketZeroBoostRefillRate)
    {
        TBoostedTimeBucket lb(
            TDuration::MilliSeconds(100),
            11,
            TDuration::Seconds(2),
            TDuration::Zero(),
            TDuration::Seconds(2));

        // random +1 mcs errors happen due to floating point arithmetic errors
        REG_AND_CHECK_D(0, 100'000, 100'000);   // 0.1 - 0.1 = 0
        REG_AND_CHECK_D(
            0,
            500'000,
            1'300'000);   // 0 + 0.4 - 1.3 + 0.9 = 0, b = 2 - 0.9 = 1.1
        REG_AND_CHECK_D(0, 600'000, 210'000);   // 0 + 0.1 - 0.21 + 0.11 (boost)
                                                // = 0, b = 1.1 - 0.11 = 0.99
        REG_AND_CHECK_D(100'001, 600'000, 1'090'000);   // nop
        REG_AND_CHECK_D(
            0,
            700'000,
            1'090'000);   // 0 + 0.1 - 1.09 + 0.99 (boost) = 0, b = 0.99 - 0.99
                          // = 0 no boost after this point
        REG_AND_CHECK_D(20'001, 800'000, 120'000);     // 0 + 0.1 = 0.1
        REG_AND_CHECK_D(0, 820'000, 120'000);          // 0.1 + 0.1 - 0.2 = 0
        REG_AND_CHECK_D(100'001, 1000'000, 280'000);   // 0 + 0.18 = 0.18
    }

    Y_UNIT_TEST(
        ShouldCorrectlyCalculateBoostedTimeBucketZeroBoostRefillRateWithCustomInitialBudget)
    {
        TBoostedTimeBucket lb(
            TDuration::MilliSeconds(100),
            11,
            TDuration::Seconds(2),
            TDuration::Zero(),
            TDuration::Seconds(1));

        // random +1 mcs errors happen due to floating point arithmetic errors
        REG_AND_CHECK_D(0, 100'000, 100'000);   // 0.1 - 0.1 = 0
        REG_AND_CHECK_D(
            0,
            500'000,
            1'300'000);   // 0 + 0.4 - 1.3 + 0.9 = 0, b = 1 - 0.9 = 0.1
        REG_AND_CHECK_D(
            0,
            600'000,
            200'000);   // 0 + 0.1 - 0.2 + 0.1 (boost) = 0, b = 0.1 - 0.1 = 0
                        // no boost after this point
        REG_AND_CHECK_D(
            990'001,
            700'000,
            1'090'000);   // 0 + 0.1 - 1.09 = -0.99
        REG_AND_CHECK_D(
            0,
            800'000,
            100'000);   // Min(0.1 + 0.1 - 0.1, 0.1 - 0.1) = 0
        REG_AND_CHECK_D(100'001, 820'000, 120'000);   // 0 + 0.02 - 0.12 = -0.1
        REG_AND_CHECK_D(
            80'001,
            1000'000,
            280'000);   // 0.02 + 0.18 - 0.28 = -0.08
    }

#define TEST_NO_BOOST                           \
    REG_AND_CHECK_D(0, 100'000, 100'000);       \
    REG_AND_CHECK_D(100'000, 100'000, 100'000); \
    REG_AND_CHECK_D(200'000, 200'000, 300'000); \
    REG_AND_CHECK_D(0, 400'000, 300'000);       \
    // TEST_NO_BOOST

    Y_UNIT_TEST(ShouldCorrectlyCalculateBoostedTimeBucketNoBoost)
    {
        TBoostedTimeBucket lb(TDuration::MilliSeconds(100));
        TEST_NO_BOOST;
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateBoostedTimeBucketBoostSmallerThanRate)
    {
        TBoostedTimeBucket lb(
            TDuration::MilliSeconds(100),
            0.9,
            TDuration::Seconds(2),
            TDuration::Seconds(20),
            TDuration::Seconds(2));
        TEST_NO_BOOST;
    }

#undef REG_AND_CHECK
#undef REG_AND_CHECK_D
#undef GET_SHARE_AND_CHECK
#undef TEST_NO_BOOST
}

}   // namespace NCloud
