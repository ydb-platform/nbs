#include "throttling_policy.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TThrottlerConfig MakeConfig(
    ui64 maxReadBandwidth,
    ui64 maxWriteBandwidth,
    ui32 maxReadIops,
    ui32 maxWriteIops,
    ui32 burstPercentage,
    ui32 boostTime,
    ui32 boostRefillTime,
    ui32 boostPercentage,
    ui64 maxPostponedWeight,
    TDuration maxPostponedTime,
    ui32 maxWriteCostMultiplier,
    ui64 defaultPostponedRequestWeight)
{
    TThrottlerConfig config;

    config.DefaultParameters.MaxReadBandwidth = maxReadBandwidth;
    config.DefaultParameters.MaxWriteBandwidth = maxWriteBandwidth;
    config.DefaultParameters.MaxReadIops = maxReadIops;
    config.DefaultParameters.MaxWriteIops = maxWriteIops;
    config.BoostParameters.BoostTime = TDuration::MilliSeconds(boostTime);
    config.BoostParameters.BoostRefillTime = TDuration::MilliSeconds(boostRefillTime);
    config.BoostParameters.BoostPercentage = boostPercentage;
    config.DefaultThresholds.MaxPostponedWeight = maxPostponedWeight;
    config.DefaultThresholds.MaxPostponedTime = maxPostponedTime;
    config.DefaultThresholds.MaxWriteCostMultiplier = maxWriteCostMultiplier;
    config.BurstPercentage = burstPercentage;
    config.DefaultPostponedRequestWeight = defaultPostponedRequestWeight;

    return config;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletThrottlingPolicyTest)
{
    using EOpType = TThrottlingPolicy::EOpType;

#define DO_TEST_V(tp, expectedDelayMcs, nowMcs, byteCount, opType, cv)         \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        TDuration::MicroSeconds(expectedDelayMcs),                             \
        tp.SuggestDelay(                                                       \
            TInstant::MicroSeconds(nowMcs),                                    \
            TDuration::Zero(),                                                 \
            {byteCount, opType, cv}                                            \
        ).GetOrElse(TDuration::Max())                                          \
    )                                                                          \
// DO_TEST_V

#define DO_TEST(tp, expectedDelayMcs, nowMcs, byteCount, opType)               \
    DO_TEST_V(tp, expectedDelayMcs, nowMcs, byteCount, opType, 1)              \
// DO_TEST

#define DO_TEST_REJECT_V(tp, nowMcs, byteCount, opType, cv)                    \
    UNIT_ASSERT(                                                               \
        !tp.SuggestDelay(                                                      \
            TInstant::MicroSeconds(nowMcs),                                    \
            TDuration::Zero(),                                                 \
            {byteCount, opType, cv}                                            \
        ).Defined()                                                            \
    )                                                                          \
// DO_TEST_REJECT_V

#define DO_TEST_REJECT(tp, nowMcs, byteCount, opType)                          \
    DO_TEST_REJECT_V(tp, nowMcs, byteCount, opType, 1)                         \
// DO_TEST

    Y_UNIT_TEST(Params)
    {
        const auto config = MakeConfig(
            100_MB,           // maxReadBandwidth
            200_MB,           // maxWriteBandwidth
            1000,             // maxReadIops
            5000,             // maxWriteIops
            200,              // burstPercentage
            0,                // boostTime
            0,                // boostRefillTime
            0,                // boostPercentage
            1000,             // maxPostponedWeight
            TDuration::Max(), // maxPostponedTime
            Max<ui32>(),      // maxWriteCostMultiplier
            1                 // defaultPostponedRequestWeight
        );
        TThrottlingPolicy tp(config);

        UNIT_ASSERT_VALUES_EQUAL(1039, tp.C1(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(102, tp.C2(EOpType::Read) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(5535, tp.C1(EOpType::Write));
        UNIT_ASSERT_VALUES_EQUAL(201, tp.C2(EOpType::Write) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(0, tp.GetCurrentBoostBudget().MilliSeconds());
    }

    Y_UNIT_TEST(InconsistentParams)
    {
        const auto config = MakeConfig(
            4_MB,             // maxReadBandwidth
            20_MB,            // maxWriteBandwidth
            1024,             // maxReadIops
            5 * 1024,         // maxWriteIops
            200,              // burstPercentage
            0,                // boostTime
            0,                // boostRefillTime
            0,                // boostPercentage
            1000,             // maxPostponedWeight
            TDuration::Max(), // maxPostponedTime
            Max<ui32>(),      // maxWriteCostMultiplier
            1                 // defaultPostponedRequestWeight
        );
        TThrottlingPolicy tp(config);

        UNIT_ASSERT_VALUES_EQUAL(1024, tp.C1(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(4, tp.C2(EOpType::Read) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(5 * 1024, tp.C1(EOpType::Write));
        UNIT_ASSERT_VALUES_EQUAL(19, tp.C2(EOpType::Write) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(0, tp.GetCurrentBoostBudget().MilliSeconds());
    }

    Y_UNIT_TEST(BasicThrottling)
    {
        const auto config = MakeConfig(
            1_MB,             // maxReadBandwidth
            0,                // maxWriteBandwidth
            10,               // maxReadIops
            0,                // maxWriteIops
            100,              // burstPercentage
            0,                // boostTime
            0,                // boostRefillTime
            0,                // boostPercentage
            8_KB,             // maxPostponedWeight
            TDuration::Max(), // maxPostponedTime
            Max<ui32>(),      // maxWriteCostMultiplier
            1_KB              // defaultPostponedRequestWeight
        );
        TThrottlingPolicy tp(config);

        // the results are not exactly equal to what's actually expected due to
        // rounding errors

        // small requests test

        for (ui32 i = 0; i < 9; ++i) {
            DO_TEST(tp, 0, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        }
        DO_TEST(tp, 38'131, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 38'131, 10'000, 4_KB, static_cast<ui32>(EOpType::Write));
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Read), 1});
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Write), 1});
        DO_TEST(tp, 0, 48'131, 4_KB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 103'813, 48'131, 4_KB, static_cast<ui32>(EOpType::Write));
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Write), 1});
        DO_TEST(tp, 0, 151'944, 4_KB, static_cast<ui32>(EOpType::Write));

        // MaxPostponedWeight test

        for (ui32 i = 0; i < 8; ++i) {
            DO_TEST(tp, 103'813, 151'944, 4_KB, static_cast<ui32>(EOpType::Read));
        }

        DO_TEST_REJECT(tp, 151'944, 4_KB, static_cast<ui32>(EOpType::Write));
        DO_TEST_REJECT(tp, 151'944, 4_KB, static_cast<ui32>(EOpType::Read));

        for (ui32 i = 0; i < 8; ++i) {
            tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Read), 1});
        }

        DO_TEST(tp, 103'813, 151'944, 4_KB, static_cast<ui32>(EOpType::Write));
        DO_TEST(tp, 103'813, 151'944, 4_KB, static_cast<ui32>(EOpType::Write));
        DO_TEST_REJECT(tp, 151'944, 4_KB, static_cast<ui32>(EOpType::Write));
        DO_TEST_REJECT(tp, 151'944, 4_KB, static_cast<ui32>(EOpType::Read));
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Write), 1});
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Write), 1});

        // MaxBurst test

        for (ui32 i = 0; i < 9; ++i) {
            DO_TEST(tp, 0, 10'000'000, 4_KB, static_cast<ui32>(EOpType::Read));
        }
        DO_TEST(tp, 38'131, 10'000'000, 4_KB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 38'131, 10'000'000, 4_KB, static_cast<ui32>(EOpType::Write));
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Read), 1});
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Write), 1});
        DO_TEST(tp, 0, 10'038'131, 4_KB, static_cast<ui32>(EOpType::Read));
    }

    Y_UNIT_TEST(ThrottlingByBandwidth)
    {
        const auto config = MakeConfig(
            1_MB,             // maxReadBandwidth
            0,                // maxWriteBandwidth
            10,               // maxReadIops
            0,                // maxWriteIops
            100,              // burstPercentage
            0,                // boostTime
            0,                // boostRefillTime
            0,                // boostPercentage
            10_MB,            // maxPostponedWeight
            TDuration::Max(), // maxPostponedTime
            Max<ui32>(),      // maxWriteCostMultiplier
            1                 // defaultPostponedRequestWeight
        );
        TThrottlingPolicy tp(config);

        for (ui32 i = 0; i < 9; ++i) {
            DO_TEST(tp, 0, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        }
        DO_TEST(tp, 38'131, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Read), 1});
        DO_TEST(tp, 0, 48'131, 4_KB, static_cast<ui32>(EOpType::Read));

        DO_TEST(tp, 1'075'954, 48'131, 1_MB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 1'075'954, 48'131, 1_MB, static_cast<ui32>(EOpType::Write));
    }

    Y_UNIT_TEST(SmallMaxBurst)
    {
        const auto config = MakeConfig(
            1_MB,             // maxReadBandwidth
            0,                // maxWriteBandwidth
            10,               // maxReadIops
            0,                // maxWriteIops
            20,               // burstPercentage
            0,                // boostTime
            0,                // boostRefillTime
            0,                // boostPercentage
            10_MB,            // maxPostponedWeight
            TDuration::Max(), // maxPostponedTime
            Max<ui32>(),      // maxWriteCostMultiplier
            1                 // defaultPostponedRequestWeight
        );
        TThrottlingPolicy tp(config);

        DO_TEST(tp, 0, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 7'626, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
    }

    Y_UNIT_TEST(PostponeCountAndReset)
    {
        const auto config = MakeConfig(
            1_MB,             // maxReadBandwidth
            0,                // maxWriteBandwidth
            10,               // maxReadIops
            0,                // maxWriteIops
            20,               // burstPercentage
            0,                // boostTime
            0,                // boostRefillTime
            0,                // boostPercentage
            5_KB,             // maxPostponedWeight
            TDuration::Max(), // maxPostponedTime
            Max<ui32>(),      // maxWriteCostMultiplier
            1                 // defaultPostponedRequestWeight
        );
        TThrottlingPolicy tp(config);

        const auto delay = 7'626;
        DO_TEST_V(tp, 0, 0, 4_KB, static_cast<ui32>(EOpType::Read), 1);
        DO_TEST_V(tp, delay, 0, 4_KB, static_cast<ui32>(EOpType::Write), 1);
        DO_TEST_REJECT_V(tp, 0, 4_KB, static_cast<ui32>(EOpType::Write), 1);

        tp.Reset(config);

        DO_TEST_REJECT_V(tp, 0, 4_KB, static_cast<ui32>(EOpType::Read), 1);
        DO_TEST_REJECT_V(tp, 0, 4_KB, static_cast<ui32>(EOpType::Write), 1);
        DO_TEST_V(tp, 0, 0, 4_KB, static_cast<ui32>(EOpType::Read), 2);
        DO_TEST_V(tp, delay, 0, 4_KB, static_cast<ui32>(EOpType::Write), 2);
        DO_TEST_REJECT_V(tp, 0, 4_KB, static_cast<ui32>(EOpType::Write), 2);
        tp.OnPostponedEvent(TInstant::Zero(), {4_KB, static_cast<ui32>(EOpType::Write), 1});
        DO_TEST_REJECT_V(tp, 0, 4_KB, static_cast<ui32>(EOpType::Write), 2);
        tp.OnPostponedEvent(TInstant::Zero(), {4_KB, static_cast<ui32>(EOpType::Write), 2});
        DO_TEST_V(tp, delay, 0, 4_KB, static_cast<ui32>(EOpType::Write), 2);
    }

    Y_UNIT_TEST(BoostRate)
    {
        const auto config = MakeConfig(
            1_MB,             // maxReadBandwidth
            0,                // maxWriteBandwidth
            10,               // maxReadIops
            0,                // maxWriteIops
            100,              // burstPercentage
            1'000,            // boostTime
            10'000,           // boostRefillTime
            1100,             // boostPercentage
            10_MB,            // maxPostponedWeight
            TDuration::Max(), // maxPostponedTime
            Max<ui32>(),      // maxWriteCostMultiplier
            1                 // defaultPostponedRequestWeight
        );
        TThrottlingPolicy tp(config);

        DO_TEST(tp, 95'628, 10'000, 2_MB, static_cast<ui32>(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(10'000, tp.GetCurrentBoostBudget().MilliSeconds());
        DO_TEST(tp, 0, 105'628, 2_MB, static_cast<ui32>(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(9'043, tp.GetCurrentBoostBudget().MilliSeconds());
        DO_TEST(tp, 186'537, 105'628, 2_MB, static_cast<ui32>(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(9'043, tp.GetCurrentBoostBudget().MilliSeconds());
        DO_TEST(tp, 0, 10'000'000, 5_MB, static_cast<ui32>(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(9'043, tp.GetCurrentBoostBudget().MilliSeconds());
    }

    Y_UNIT_TEST(SeparateReadAndWriteLimits)
    {
        const auto config = MakeConfig(
            1_MB,             // maxReadBandwidth
            2_MB,             // maxWriteBandwidth
            10,               // maxReadIops
            50,               // maxWriteIops
            100,              // burstPercentage
            0,                // boostTime
            0,                // boostRefillTime
            0,                // boostPercentage
            10_MB,            // maxPostponedWeight
            TDuration::Max(), // maxPostponedTime
            Max<ui32>(),      // maxWriteCostMultiplier
            1                 // defaultPostponedRequestWeight
        );
        TThrottlingPolicy tp(config);

        for (ui32 i = 0; i < 9; ++i) {
            DO_TEST(tp, 0, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        }
        DO_TEST(tp, 38'131, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Read), 1});
        DO_TEST(tp, 0, 48'131, 4_KB, static_cast<ui32>(EOpType::Read));

        for (ui32 i = 0; i < 49; ++i) {
            DO_TEST(tp, 0, 10'000'000, 4_KB, static_cast<ui32>(EOpType::Write));
        }
        DO_TEST(tp, 5'900, 10'000'000, 4_KB, static_cast<ui32>(EOpType::Write));
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Write), 1});
        DO_TEST(tp, 0, 10'005'900, 4_KB, static_cast<ui32>(EOpType::Write));

        DO_TEST(tp, 1'075'954, 10'005'900, 1_MB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 513'666, 10'005'900, 1_MB, static_cast<ui32>(EOpType::Write));
    }

    Y_UNIT_TEST(MaxDelay)
    {
        const auto config = MakeConfig(
            1_MB,                  // maxReadBandwidth
            0,                     // maxWriteBandwidth
            10,                    // maxReadIops
            0,                     // maxWriteIops
            100,                   // burstPercentage
            0,                     // boostTime
            0,                     // boostRefillTime
            0,                     // boostPercentage
            1_GB,                  // maxPostponedWeight
            TDuration::Seconds(1), // maxPostponedTime
            Max<ui32>(),           // maxWriteCostMultiplier
            1                      // defaultPostopnedRequestWeight
        );
        TThrottlingPolicy tp(config);

        DO_TEST(tp, 75'955, 10'000, 1_MB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 0, 85'955, 1_MB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 587'977, 85'955, 512_KB, static_cast<ui32>(EOpType::Read));
        UNIT_ASSERT(!tp.SuggestDelay(
            TInstant::MicroSeconds(85'955),
            TDuration::Zero(),
            {1_MB, static_cast<ui32>(EOpType::Read), 1}
        ).Defined());
    }

    Y_UNIT_TEST(MoreThan4g)
    {
        // From SeparateReadAndWriteLimits
        const auto config = MakeConfig(
            10_GB,    // maxReadBandwidth
            20_GB,    // maxWriteBandwidth
            100000,   // maxReadIops
            500000,   // maxWriteIops
            100,      // burstPercentage
            0,        // boostTime
            0,        // boostRefillTime
            0,        // boostPercentage
            1_GB,     // maxPostponedWeight
            TDuration::Seconds(1),   // maxPostponedTime
            Max<ui32>(),             // maxWriteCostMultiplier
            1                        // defaultPostponedRequestWeight
        );
        TThrottlingPolicy tp(config);
        for (ui32 i = 0; i < 10; ++i) {
            DO_TEST(tp, 0, 10'000, 1_GB, static_cast<ui32>(EOpType::Read));
        }
        DO_TEST(tp, 72'995, 10'000, 1_GB, static_cast<ui32>(EOpType::Read));
        tp.OnPostponedEvent({}, {1_GB, static_cast<ui32>(EOpType::Read), 1});
        DO_TEST(tp, 34'864, 48'131, 1_GB, static_cast<ui32>(EOpType::Read));
        for (ui32 i = 0; i < 19; ++i) {
           DO_TEST(tp, 0, 10'000'000, 1_GB, static_cast<ui32>(EOpType::Write));
        }
        DO_TEST(tp, 0, 10'000'000, 1_GB, static_cast<ui32>(EOpType::Write));
        tp.OnPostponedEvent({}, {1_GB, static_cast<ui32>(EOpType::Write), 1});
        DO_TEST(tp, 30'319, 10'010'000, 1_GB, static_cast<ui32>(EOpType::Write));
        DO_TEST(tp, 0, 11'025'900, 1_GB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 0, 11'025'900, 1_GB, static_cast<ui32>(EOpType::Write));
    }
}

}   // namespace NCloud::NFileStore::NStorage
