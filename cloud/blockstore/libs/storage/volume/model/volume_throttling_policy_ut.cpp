#include "volume_throttling_policy.h"

#include "cloud/storage/core/libs/throttling/helpers.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <initializer_list>
#include <utility>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TVolumePerformanceProfile MakeConfig(
    ui64 maxReadBandwidth,
    ui64 maxWriteBandwidth,
    ui32 maxReadIops,
    ui32 maxWriteIops,
    ui32 burstPercentage,
    ui32 boostTime,
    ui32 boostRefillTime,
    ui32 boostPercentage,
    ui64 maxPostponedWeight)
{
    NProto::TVolumePerformanceProfile config;

    config.SetMaxReadBandwidth(maxReadBandwidth);
    config.SetMaxWriteBandwidth(maxWriteBandwidth);
    config.SetMaxReadIops(maxReadIops);
    config.SetMaxWriteIops(maxWriteIops);
    config.SetBurstPercentage(burstPercentage);
    config.SetMaxPostponedWeight(maxPostponedWeight);
    config.SetBoostTime(boostTime);
    config.SetBoostRefillTime(boostRefillTime);
    config.SetBoostPercentage(boostPercentage);

    return config;
}

NProto::TVolumePerformanceProfile MakeSimpleConfig(
    ui64 maxBandwidth,
    ui32 maxIops,
    ui32 burstPercentage,
    ui32 boostTime,
    ui32 boostRefillTime,
    ui32 boostPercentage,
    ui64 maxPostponedWeight)
{
    return MakeConfig(
        maxBandwidth,
        0,
        maxIops,
        0,
        burstPercentage,
        boostTime,
        boostRefillTime,
        boostPercentage,
        maxPostponedWeight
    );
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeThrottlingPolicyTest)
{
    using EOpType = EVolumeThrottlingOpType;

#define DO_TEST_V(tp, expectedDelayMcs, nowMcs, byteCount, opType, cv)      \
    UNIT_ASSERT_VALUES_EQUAL(                                               \
        TDuration::MicroSeconds(expectedDelayMcs),                          \
        tp.SuggestDelay(                                                    \
            TInstant::MicroSeconds(nowMcs),                                 \
            TDuration::Zero(),                                              \
            {byteCount, opType, cv}                                         \
        ).GetOrElse(TDuration::Max())                                       \
    )                                                                       \
// DO_TEST_V

#define DO_TEST(tp, expectedDelayMcs, nowMcs, byteCount, opType)            \
    DO_TEST_V(tp, expectedDelayMcs, nowMcs, byteCount, opType, 1)           \
// DO_TEST

#define DO_TEST_REJECT_V(tp, nowMcs, byteCount, opType, cv)                 \
    UNIT_ASSERT(                                                            \
        !tp.SuggestDelay(                                                   \
            TInstant::MicroSeconds(nowMcs),                                 \
            TDuration::Zero(),                                              \
            {byteCount, opType, cv}                                         \
        ).Defined()                                                         \
    )                                                                       \
// DO_TEST_REJECT_V

#define DO_TEST_REJECT(tp, nowMcs, byteCount, opType)                       \
    DO_TEST_REJECT_V(tp, nowMcs, byteCount, opType, 1)                      \
// DO_TEST

    Y_UNIT_TEST(Params)
    {
        const auto config = MakeConfig(
            100_MB,   // maxReadBandwidth
            200_MB,   // maxWriteBandwidth
            1000,     // maxReadIops
            5000,     // maxWriteIops
            200,      // burstPercentage
            0,        // boostTime
            0,        // boostRefillTime
            0,        // boostPercentage
            1000      // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(1039, tp.C1(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(102, tp.C2(EOpType::Read) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(5535, tp.C1(EOpType::Write));
        UNIT_ASSERT_VALUES_EQUAL(201, tp.C2(EOpType::Write) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(1000, tp.C1(EOpType::Describe));
        UNIT_ASSERT_VALUES_EQUAL(0, tp.C2(EOpType::Describe));
        UNIT_ASSERT_VALUES_EQUAL(0, tp.GetCurrentBoostBudget().MilliSeconds());
    }

    Y_UNIT_TEST(ParamsWithCustomInitialBudget)
    {
        TVolumeThrottlingPolicy tp(
            MakeConfig(100_MB, 200_MB, 1000, 5000, 200, 0, 0, 0, 1000),
            TThrottlerConfig(
                TDuration::Max(),        // maxDelay
                Max<ui32>(),             // maxWriteCostMultiplier
                1,                       // defaultPostponedRequestWeight
                TDuration::Seconds(10),  // initialBoostBudget
                false                    // useDiskSpaceScore
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(1039, tp.C1(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(102, tp.C2(EOpType::Read) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(5535, tp.C1(EOpType::Write));
        UNIT_ASSERT_VALUES_EQUAL(201, tp.C2(EOpType::Write) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(1000, tp.C1(EOpType::Describe));
        UNIT_ASSERT_VALUES_EQUAL(0, tp.C2(EOpType::Describe));
        UNIT_ASSERT_VALUES_EQUAL(10'000, tp.GetCurrentBoostBudget().MilliSeconds());
    }

    Y_UNIT_TEST(InconsistentParams)
    {
        const auto config = MakeConfig(
            4_MB,       // maxReadBandwidth
            20_MB,      // maxWriteBandwidth
            1024,       // maxReadIops
            5 * 1024,   // maxWriteIops
            200,        // burstPercentage
            0,          // boostTime
            0,          // boostRefillTime
            0,          // boostPercentage
            1000        // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(1024, tp.C1(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(4, tp.C2(EOpType::Read) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(5 * 1024, tp.C1(EOpType::Write));
        UNIT_ASSERT_VALUES_EQUAL(19, tp.C2(EOpType::Write) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(1024, tp.C1(EOpType::Describe));
        UNIT_ASSERT_VALUES_EQUAL(0, tp.C2(EOpType::Describe));
        UNIT_ASSERT_VALUES_EQUAL(0, tp.GetCurrentBoostBudget().MilliSeconds());
    }

    Y_UNIT_TEST(InconsistentParamsWithCustomInitialBudget)
    {
        TVolumeThrottlingPolicy tp(
            MakeConfig(4_MB, 20_MB, 1024, 5 * 1024, 200, 0, 0, 0, 1000),
            TThrottlerConfig(
                TDuration::Max(),        // maxDelay
                Max<ui32>(),             // maxWriteCostMultiplier
                1,                       // defaultPostponedRequestWeight
                TDuration::Seconds(10),  // initialBoostBudget
                false                    // useDiskSpaceScore
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(1024, tp.C1(EOpType::Read));
        UNIT_ASSERT_VALUES_EQUAL(4, tp.C2(EOpType::Read) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(5 * 1024, tp.C1(EOpType::Write));
        UNIT_ASSERT_VALUES_EQUAL(19, tp.C2(EOpType::Write) / 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(1024, tp.C1(EOpType::Describe));
        UNIT_ASSERT_VALUES_EQUAL(0, tp.C2(EOpType::Describe));
        UNIT_ASSERT_VALUES_EQUAL(10'000, tp.GetCurrentBoostBudget().MilliSeconds());
    }

    Y_UNIT_TEST(BasicThrottling)
    {
        const auto config = MakeSimpleConfig(
            1_MB,   // maxBandwidth
            10,     // maxIops
            100,    // burstPercentage
            0,      // boostTime
            0,      // boostRefillTime
            0,      // boostPercentage
            8_KB    // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1_KB,                        // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );

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
        const auto config = MakeSimpleConfig(
            1_MB,   // maxBandwidth
            10,     // maxIops
            100,    // burstPercentage
            0,      // boostTime
            0,      // boostRefillTime
            0,      // boostPercentage
            10_MB   // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudge
                false                        // useDiskSpaceScore
            )
        );

        for (ui32 i = 0; i < 9; ++i) {
            DO_TEST(tp, 0, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        }
        DO_TEST(tp, 38'131, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        tp.OnPostponedEvent({}, {4_KB, static_cast<ui32>(EOpType::Read), 1});
        DO_TEST(tp, 0, 48'131, 4_KB, static_cast<ui32>(EOpType::Read));

        DO_TEST(tp, 1'075'954, 48'131, 1_MB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 1'075'954, 48'131, 1_MB, static_cast<ui32>(EOpType::Write));
        DO_TEST(tp, 100'000, 48'131, 1_MB, static_cast<ui32>(EOpType::Describe));
    }

    Y_UNIT_TEST(SmallMaxBurst)
    {
        const auto config = MakeSimpleConfig(
            1_MB,   // maxBandwidth
            10,     // maxIops
            20,     // burstPercentage
            0,      // boostTime
            0,      // boostRefillTime
            0,      // boostPercentage
            10_MB   // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );

        DO_TEST(tp, 0, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 7'626, 10'000, 4_KB, static_cast<ui32>(EOpType::Read));
    }

    Y_UNIT_TEST(ThrottlingWithBackpressure)
    {
        const auto config = MakeSimpleConfig(
            1_MB,   // maxBandwidth
            10,     // maxIops
            100,    // burstPercentage
            0,      // boostTime
            0,      // boostRefillTime
            0,      // boostPercentage
            10_MB   // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );
        tp.OnBackpressureReport({}, {0., 0., 0.}, 0);       // should cause no discount

        TInstant t = TInstant::Seconds(1337);               // initial ts does not matter
        const auto eternity = TDuration::Hours(1);
        for (size_t i = 0; i < 9; ++i) {
            DO_TEST(tp, 0, t.MicroSeconds(), 4_KB, static_cast<ui32>(EOpType::Write));
        }
        DO_TEST(tp, 38'131, t.MicroSeconds(), 4_KB, static_cast<ui32>(EOpType::Write));

#define TEST_FEATURE(featureName)                                              \
        {                                                                      \
            t += eternity;                                                     \
            TBackpressureReport report;                                        \
            report.featureName = 2;                                            \
            tp.OnBackpressureReport({}, report, 0);                            \
            for (ui32 i = 0; i < 9; ++i) {                                     \
                DO_TEST(                                                       \
                    tp,                                                        \
                    0,                                                         \
                    t.MicroSeconds(),                                          \
                    4_KB,                                                      \
                    static_cast<ui32>(EOpType::Read));                         \
            }                                                                  \
            DO_TEST(                                                           \
                tp,                                                            \
                38'131,                                                        \
                t.MicroSeconds(),                                              \
                4_KB,                                                          \
                static_cast<ui32>(EOpType::Read));                             \
            tp.OnPostponedEvent(                                               \
                {},                                                            \
                {4_KB, static_cast<ui32>(EOpType::Read), 1});                  \
                                                                               \
            t += eternity;                                                     \
            for (ui32 i = 0; i < 4; ++i) {                                     \
                DO_TEST(                                                       \
                    tp,                                                        \
                    0,                                                         \
                    t.MicroSeconds(),                                          \
                    4_KB,                                                      \
                    static_cast<ui32>(EOpType::Write));                        \
            }                                                                  \
            DO_TEST(                                                           \
                tp,                                                            \
                38'130,                                                        \
                t.MicroSeconds(),                                              \
                4_KB,                                                          \
                static_cast<ui32>(EOpType::Write));                            \
            tp.OnPostponedEvent(                                               \
                {},                                                            \
                {4_KB, static_cast<ui32>(EOpType::Write), 1});                 \
        }                                                                      \
// TEST_FEATURE

        TEST_FEATURE(FreshIndexScore);
        TEST_FEATURE(CompactionScore);
        //TEST_FEATURE(DiskSpaceScore);
        TEST_FEATURE(CleanupScore);

#undef TEST_FEATURE
    }

    Y_UNIT_TEST(PostponedQueueAndBackpressure)
    {
        // Right now this code just tests that postponed queue max size is not
        // affected by backpressure. In the future it might get affected.
        const auto config = MakeSimpleConfig(
            1_MB,   // maxBandwidth
            10,     // maxIops
            100,    // burstPercentage
            0,      // boostTime
            0,      // boostRefillTime
            0,      // boostPercentage
            10_MB   // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );
        tp.OnBackpressureReport({}, {1000, 100, 10}, 0);    // => WriteCostMultiplier = 1000

        auto now = 10'000;
        auto delay = 1'074'954'000;

        DO_TEST(tp, delay, now, 1_MB, static_cast<ui32>(EOpType::Write));

        for (int i = 0; i < 9; ++i) {
            UNIT_ASSERT(tp.TryPostpone(
                TInstant::MicroSeconds(now), {1_MB, static_cast<ui32>(EOpType::Write)}
            ));
        }
        UNIT_ASSERT(!tp.TryPostpone(
            TInstant::MicroSeconds(now), {1_MB, static_cast<ui32>(EOpType::Write)}
        ));
    }

    Y_UNIT_TEST(PostponeCountAndReset)
    {
        const auto config = MakeSimpleConfig(
            1_MB,   // maxBandwidth
            10,     // maxIops
            20,     // burstPercentage
            0,      // boostTime
            0,      // boostRefillTime
            0,      // boostPercentage
            5_KB    // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );

        const auto delay = 7'626;
        DO_TEST_V(tp, 0, 0, 4_KB, static_cast<ui32>(EOpType::Read), 1);
        DO_TEST_V(tp, delay, 0, 4_KB, static_cast<ui32>(EOpType::Write), 1);
        DO_TEST_REJECT_V(tp, 0, 4_KB, static_cast<ui32>(EOpType::Write), 1);

        tp.Reset(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );
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
        const auto config = MakeSimpleConfig(
            1_MB,     // maxBandwidth
            10,       // maxIops
            100,      // burstPercentage
            1'000,    // boostTime
            10'000,   // boostRefillTime
            1100,     // boostPercentage
            10_MB     // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );

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
            1_MB,   // maxReadBandwidth
            2_MB,   // maxWriteBandwidth
            10,     // maxReadIops
            50,     // maxWriteIops
            100,    // burstPercentage
            0,      // boostTime
            0,      // boostRefillTime
            0,      // boostPercentage
            10_MB   // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );

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
        const auto config = MakeSimpleConfig(
            1_MB,  // maxBandwidth
            10,    // maxIops
            100,   // burstPercentage
            0,     // boostTime
            0,     // boostRefillTime
            0,     // boostPercentage
            1_GB   // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Seconds(1),       // maxDelay
                Max<ui32>(),                 // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );

        DO_TEST(tp, 75'955, 10'000, 1_MB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 0, 85'955, 1_MB, static_cast<ui32>(EOpType::Read));
        DO_TEST(tp, 587'977, 85'955, 512_KB, static_cast<ui32>(EOpType::Read));
        UNIT_ASSERT(!tp.SuggestDelay(
            TInstant::MicroSeconds(85'955),
            TDuration::Zero(),
            {1_MB, static_cast<ui32>(EOpType::Read), 1}
        ).Defined());
    }

    Y_UNIT_TEST(MaxWriteCostMultiplier)
    {
        const auto config = MakeSimpleConfig(
            1_MB,  // maxBandwidth
            10,    // maxIops
            100,   // burstPercentage
            0,     // boostTime
            0,     // boostRefillTime
            0,     // boostPercentage
            1_GB   // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                5,                           // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );
        tp.OnBackpressureReport({}, {1000, 100, 10}, 0);    // => WriteCostMultiplier = 1000
                                                            // but due to MaxWriteCostMultiplier
                                                            // we actually get 5

        DO_TEST(tp, 4'379'770, 10'000, 1_MB, static_cast<ui32>(EOpType::Write));
    }

    Y_UNIT_TEST(CalculateSplittedUsedQuota)
    {
        const ui64 maxBandwidth = 2_MB;
        const ui64 maxIops = 4;

        const auto config = MakeSimpleConfig(
            maxBandwidth,
            maxIops,
            100,   // burstPercentage
            0,     // boostTime
            0,     // boostRefillTime
            0,     // boostPercentage
            1_GB   // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Seconds(1),        // maxDelay
                Max<ui32>(),                  // maxWriteCostMultiplier
                1,                            // defaultPostponedRequestWeight
                CalculateBoostTime(config),   // initialBoostBudget
                false                         // useDiskSpaceScore
                ));

        const auto byteCount = 256_KB;
        const auto requestCount = 2;

        for (auto i = 0; i < requestCount; ++i) {
            DO_TEST(tp, 0, 0, byteCount, static_cast<ui32>(EOpType::Read));
        }

        auto recalculatedMaxIops = CalculateThrottlerC1(maxIops, maxBandwidth);
        auto recalculatedMaxBandwidth =
            CalculateThrottlerC2(maxIops, maxBandwidth);

        auto splittedUsedQuota = tp.TakeSplittedUsedQuota();

        UNIT_ASSERT_DOUBLES_EQUAL(
            splittedUsedQuota.Bandwidth,
            static_cast<double>(requestCount * byteCount) /
                static_cast<double>(recalculatedMaxBandwidth),
            1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(
            splittedUsedQuota.Iops,
            static_cast<double>(requestCount) /
                static_cast<double>(recalculatedMaxIops),
            1e-6);

        splittedUsedQuota = tp.TakeSplittedUsedQuota();

        UNIT_ASSERT_DOUBLES_EQUAL(splittedUsedQuota.Bandwidth, 0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(splittedUsedQuota.Iops, 0, 1e-6);
    }

    Y_UNIT_TEST(CalculateSplittedUsedQuotaCorrectlyWhenThrottlingByBandwidthDisabled)
    {
        const ui64 maxIops = 4;

        const auto config = MakeSimpleConfig(
            0,   // maxBandwidth
            maxIops,
            100,   // burstPercentage
            0,     // boostTime
            0,     // boostRefillTime
            0,     // boostPercentage
            1_GB   // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            config,
            TThrottlerConfig(
                TDuration::Seconds(1),        // maxDelay
                Max<ui32>(),                  // maxWriteCostMultiplier
                1,                            // defaultPostponedRequestWeight
                CalculateBoostTime(config),   // initialBoostBudget
                false                         // useDiskSpaceScore
                ));

        const auto byteCount = 256_KB;
        const auto requestCount = 2;

        for (auto i = 0; i < requestCount; ++i) {
            DO_TEST(tp, 0, 0, byteCount, static_cast<ui32>(EOpType::Read));
        }

        const auto [usedIopsQuota, usedBandwidthQuota] =
            tp.TakeSplittedUsedQuota();

        auto recalculatedMaxIops = CalculateThrottlerC1(maxIops, 0);

        UNIT_ASSERT_DOUBLES_EQUAL(usedBandwidthQuota, 0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(
            usedIopsQuota,
            static_cast<double>(requestCount) /
                static_cast<double>(recalculatedMaxIops),
            1e-6);
    }

#undef DO_TEST

    void DoShouldPreserveBandwidthLimitsWhenApplyingCoefficients(
        EOpType opType,
        TMaybe<double> coefficient,
        std::initializer_list<std::pair<ui64, ui64>> bandwidthLimits)
    {
        // Keep IOPS fixed and disable boost to check bandwidth limits.
        const bool isRead = (opType == EOpType::Read);
        auto config = MakeConfig(10_KB, 0, 1000, 1000, 1, 0, 0, 0, 8_MB);
        const TThrottlerConfig
            throttlerConfig(TDuration::Max(), 1, 1, TDuration::Zero(), false);
        TVolumeThrottlingPolicy policy(config, throttlerConfig);

        // Apply only the selected coefficient, or leave it absent.
        NProto::TVolumeThrottlingRule rule;
        if (coefficient) {
            if (isRead) {
                rule.MutableCoefficients()->SetMaxReadBandwidth(*coefficient);
            } else {
                rule.MutableCoefficients()->SetMaxWriteBandwidth(*coefficient);
            }
        }
        policy.Reset(rule, 1);

        for (const auto& [originalBandwidth, expectedBandwidth]:
             bandwidthLimits)
        {
            const TString context =
                TStringBuilder()
                << "operation: " << static_cast<ui32>(opType)
                << ", bandwidth: " << originalBandwidth << ", coefficient: "
                << (coefficient ? ToString(*coefficient) : "unset");

            // Change the original profile and check the saved coefficient.
            if (isRead) {
                config.SetMaxReadBandwidth(originalBandwidth);
            } else {
                config.SetMaxWriteBandwidth(originalBandwidth);
            }
            policy.Reset(config, throttlerConfig);
            const auto expectedReadBandwidth =
                isRead ? expectedBandwidth : config.GetMaxReadBandwidth();
            const auto expectedWriteBandwidth =
                isRead ? config.GetMaxWriteBandwidth() : expectedBandwidth;
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedReadBandwidth,
                policy.GetConfig().GetMaxReadBandwidth(),
                context);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedWriteBandwidth,
                policy.GetConfig().GetMaxWriteBandwidth(),
                context);

            // Check read/write fallback and the limits used for DescribeBlocks.
            for (const auto requestOpType:
                 {EOpType::Read, EOpType::Write, EOpType::Describe})
            {
                auto effectiveBandwidth =
                    requestOpType == EOpType::Write && expectedWriteBandwidth
                        ? expectedWriteBandwidth
                        : expectedReadBandwidth;
                // Keep DescribeBlocks independent of bandwidth limits.
                if (requestOpType == EOpType::Describe) {
                    effectiveBandwidth = 0;
                }
                const auto expectedC1 =
                    CalculateThrottlerC1(1000, effectiveBandwidth);
                const auto expectedC2 =
                    CalculateThrottlerC2(1000, effectiveBandwidth);
                const TString requestContext =
                    TStringBuilder() << context << ", request operation: "
                                     << static_cast<ui32>(requestOpType);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    expectedC1,
                    policy.C1(requestOpType),
                    requestContext);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    expectedC2,
                    policy.C2(requestOpType),
                    requestContext);

                // Reset the budget for each request. Use the current version
                // so the request reaches the cost calculation.
                const auto getDelay = [&](ui64 byteCount)
                {
                    policy.Reset(rule, 1);
                    const auto delay = policy.SuggestDelay(
                        TInstant::Seconds(1),
                        TDuration::Zero(),
                        {byteCount,
                         static_cast<ui32>(requestOpType),
                         policy.GetVersion()});
                    UNIT_ASSERT_C(delay.Defined(), requestContext);

                    // Account for the 10 ms budget from BurstPercentage == 1
                    // and microsecond rounding of the resulting delay.
                    const auto expectedCost =
                        CostPerIO(expectedC1, expectedC2, byteCount);
                    const auto burstBudget = TDuration::MilliSeconds(10);
                    const auto expectedDelay = expectedCost > burstBudget
                                                   ? expectedCost - burstBudget
                                                   : TDuration::Zero();
                    UNIT_ASSERT_DOUBLES_EQUAL_C(
                        expectedDelay.SecondsFloat(),
                        delay->SecondsFloat(),
                        TDuration::MicroSeconds(2).SecondsFloat(),
                        requestContext);
                    return *delay;
                };
                const auto smallRequestDelay = getDelay(4_KB);
                const auto largeRequestDelay = getDelay(4_MB);

                // Check that only an active bandwidth limit makes larger
                // requests wait longer; otherwise their delays must be equal.
                if (expectedC2) {
                    UNIT_ASSERT_C(
                        largeRequestDelay > smallRequestDelay,
                        requestContext);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        smallRequestDelay,
                        largeRequestDelay,
                        requestContext);
                }
            }

            // Reapply the rule and check that it still scales the original
            // profile rather than the previously calculated limit.
            policy.Reset(rule, 1);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedBandwidth,
                isRead ? policy.GetConfig().GetMaxReadBandwidth()
                       : policy.GetConfig().GetMaxWriteBandwidth(),
                context);
        }
    }

    // Verify that coefficients keep positive read/write bandwidth limits
    // active, preserve intentional zeros and use updated original profiles.
    Y_UNIT_TEST(ShouldPreserveBandwidthLimitsWhenApplyingCoefficients)
    {
        for (const auto opType: {EOpType::Read, EOpType::Write}) {
            // Change the original profile and check that a small positive
            // coefficient keeps the limit at least 1 byte/s after each update.
            DoShouldPreserveBandwidthLimitsWhenApplyingCoefficients(
                opType,
                0.000001,
                {{100, 1}, {5'000'000, 5}, {100, 1}});

            // Check values below, at and above 1 byte/s, and preserve
            // truncation of larger fractions and an original zero value.
            DoShouldPreserveBandwidthLimitsWhenApplyingCoefficients(
                opType,
                0.5,
                {{1, 1}, {2, 1}, {3, 1}, {5, 2}, {0, 0}});

            // Preserve explicit zeros and unchanged limits when the coefficient
            // is absent or one; keep the read fallback for zero write limits.
            DoShouldPreserveBandwidthLimitsWhenApplyingCoefficients(
                opType,
                0.0,
                {{100, 0}});
            DoShouldPreserveBandwidthLimitsWhenApplyingCoefficients(
                opType,
                1.0,
                {{100, 100}});
            DoShouldPreserveBandwidthLimitsWhenApplyingCoefficients(
                opType,
                Nothing(),
                {{100, 100}, {0, 0}});
        }
    }

    void DoShouldPreserveIopsLimitsWhenApplyingCoefficients(
        EOpType opType,
        TMaybe<double> coefficient,
        std::initializer_list<std::pair<ui32, ui32>> iopsLimits)
    {
        // Disable bandwidth and boost to check IOPS limits.
        const bool isRead = (opType == EOpType::Read);
        auto config = MakeConfig(0, 0, 1000, 0, 1, 0, 0, 0, 8_MB);
        const TThrottlerConfig
            throttlerConfig(TDuration::Max(), 1, 1, TDuration::Zero(), false);
        TVolumeThrottlingPolicy policy(config, throttlerConfig);

        // Apply only the selected coefficient, or leave it absent.
        NProto::TVolumeThrottlingRule rule;
        if (coefficient) {
            if (isRead) {
                rule.MutableCoefficients()->SetMaxReadIops(*coefficient);
            } else {
                rule.MutableCoefficients()->SetMaxWriteIops(*coefficient);
            }
        }
        policy.Reset(rule, 1);

        for (const auto& [originalIops, expectedIops]: iopsLimits) {
            const TString context =
                TStringBuilder()
                << "operation: " << static_cast<ui32>(opType)
                << ", IOPS: " << originalIops << ", coefficient: "
                << (coefficient ? ToString(*coefficient) : "unset");

            // Change the original profile and check the saved coefficient.
            if (isRead) {
                config.SetMaxReadIops(originalIops);
            } else {
                config.SetMaxWriteIops(originalIops);
            }
            policy.Reset(config, throttlerConfig);
            const auto expectedReadIops =
                isRead ? expectedIops : config.GetMaxReadIops();
            const auto expectedWriteIops =
                isRead ? config.GetMaxWriteIops() : expectedIops;
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedReadIops,
                policy.GetConfig().GetMaxReadIops(),
                context);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedWriteIops,
                policy.GetConfig().GetMaxWriteIops(),
                context);

            // Check read/write fallback and the limits used for DescribeBlocks.
            for (const auto requestOpType:
                 {EOpType::Read, EOpType::Write, EOpType::Describe})
            {
                const auto effectiveIops =
                    requestOpType == EOpType::Write && expectedWriteIops
                        ? expectedWriteIops
                        : expectedReadIops;
                const auto expectedC1 = effectiveIops;
                const ui64 expectedC2 = 0;
                const TString requestContext =
                    TStringBuilder() << context << ", request operation: "
                                     << static_cast<ui32>(requestOpType);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    expectedC1,
                    policy.C1(requestOpType),
                    requestContext);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    expectedC2,
                    policy.C2(requestOpType),
                    requestContext);

                // Check intentional zeros above without passing zero IOPS to
                // the request-cost calculation, which requires positive IOPS.
                if (!effectiveIops) {
                    continue;
                }

                // Reset the budget for each request. Use the current version
                // so the request reaches the cost calculation.
                const auto getDelay = [&](ui64 byteCount)
                {
                    policy.Reset(rule, 1);
                    const auto delay = policy.SuggestDelay(
                        TInstant::Seconds(1),
                        TDuration::Zero(),
                        {byteCount,
                         static_cast<ui32>(requestOpType),
                         policy.GetVersion()});
                    UNIT_ASSERT_C(delay.Defined(), requestContext);

                    // Account for the 10 ms budget from BurstPercentage == 1
                    // and microsecond rounding of the resulting delay.
                    const auto expectedCost =
                        CostPerIO(expectedC1, expectedC2, byteCount);
                    const auto burstBudget = TDuration::MilliSeconds(10);
                    const auto expectedDelay = expectedCost > burstBudget
                                                   ? expectedCost - burstBudget
                                                   : TDuration::Zero();
                    UNIT_ASSERT_DOUBLES_EQUAL_C(
                        expectedDelay.SecondsFloat(),
                        delay->SecondsFloat(),
                        TDuration::MicroSeconds(2).SecondsFloat(),
                        requestContext);
                    return *delay;
                };
                const auto smallRequestDelay = getDelay(4_KB);
                const auto largeRequestDelay = getDelay(4_MB);

                // Check that only an active bandwidth limit makes larger
                // requests wait longer; otherwise their delays must be equal.
                if (expectedC2) {
                    UNIT_ASSERT_C(
                        largeRequestDelay > smallRequestDelay,
                        requestContext);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        smallRequestDelay,
                        largeRequestDelay,
                        requestContext);
                }
            }

            // Reapply the rule and check that it still scales the original
            // profile rather than the previously calculated limit.
            policy.Reset(rule, 1);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedIops,
                isRead ? policy.GetConfig().GetMaxReadIops()
                       : policy.GetConfig().GetMaxWriteIops(),
                context);
        }
    }

    // Verify that coefficients keep positive read/write IOPS limits active,
    // preserve intentional zeros and use updated original profiles.
    Y_UNIT_TEST(ShouldPreserveIopsLimitsWhenApplyingCoefficients)
    {
        for (const auto opType: {EOpType::Read, EOpType::Write}) {
            // Change the original profile and check that a small positive
            // coefficient keeps the limit at least 1 IOPS after each update.
            DoShouldPreserveIopsLimitsWhenApplyingCoefficients(
                opType,
                0.000001,
                {{100, 1}, {5'000'000, 5}, {100, 1}});

            // Check values below, at and above 1 IOPS, and preserve
            // truncation of larger fractions and an original zero value.
            DoShouldPreserveIopsLimitsWhenApplyingCoefficients(
                opType,
                0.5,
                {{1, 1}, {2, 1}, {3, 1}, {5, 2}, {0, 0}});

            // Preserve explicit zeros and unchanged limits when the coefficient
            // is absent or one; keep the read fallback for zero write limits.
            DoShouldPreserveIopsLimitsWhenApplyingCoefficients(
                opType,
                0.0,
                {{100, 0}});
            DoShouldPreserveIopsLimitsWhenApplyingCoefficients(
                opType,
                1.0,
                {{100, 100}});
            DoShouldPreserveIopsLimitsWhenApplyingCoefficients(
                opType,
                Nothing(),
                {{100, 100}, {0, 0}});
        }
    }

    Y_UNIT_TEST(MergeBackpressureReports)
    {
        const auto config = MakeSimpleConfig(
            1,   // maxBandwidth
            1,   // maxIops
            1,   // burstPercentage
            1,   // boostTime
            1,   // boostRefillTime
            1,   // boostPercentage
            1    // maxPostponedWeight
        );
        TVolumeThrottlingPolicy tp(
            // config doesn't really matter in this test
            config,
            TThrottlerConfig(
                TDuration::Max(),            // maxDelay
                5,                           // maxWriteCostMultiplier
                1,                           // defaultPostponedRequestWeight
                CalculateBoostTime(config),  // initialBoostBudget
                false                        // useDiskSpaceScore
            )
        );

        tp.OnBackpressureReport({}, {2, 1, 1}, 0);
        UNIT_ASSERT_VALUES_EQUAL(2, tp.GetWriteCostMultiplier());
        tp.OnBackpressureReport({}, {1, 3, 1}, 1);
        UNIT_ASSERT_VALUES_EQUAL(3, tp.GetWriteCostMultiplier());
        tp.OnBackpressureReport({}, {1, 1, 1}, 0);
        UNIT_ASSERT_VALUES_EQUAL(3, tp.GetWriteCostMultiplier());
        tp.OnBackpressureReport({}, {1, 1, 2}, 0);
        UNIT_ASSERT_VALUES_EQUAL(3, tp.GetWriteCostMultiplier());
        tp.OnBackpressureReport({}, {1, 1, 1}, 1);
        UNIT_ASSERT_VALUES_EQUAL(1, tp.GetWriteCostMultiplier());
        tp.OnBackpressureReport({}, {1, 1, 1}, 0);
        UNIT_ASSERT_VALUES_EQUAL(1, tp.GetWriteCostMultiplier());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
