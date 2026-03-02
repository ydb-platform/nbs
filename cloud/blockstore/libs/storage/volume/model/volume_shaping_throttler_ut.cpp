#include "volume_shaping_throttler.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/storage/core/libs/throttling/helpers.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

using EOpType = EVolumeThrottlingOpType;
using EMediaKind = NCloud::NProto::EStorageMediaKind;

NProto::TShapingThrottlerQuota MakeQuota(
    ui32 readIops,
    ui64 readBandwidth,
    ui32 writeIops,
    ui64 writeBandwidth,
    ui32 expectedIoParallelism,
    ui32 maxBudgetMs,
    ui32 budgetRefillTimeMs,
    double budgetSpendRate)
{
    NProto::TShapingThrottlerQuota quota;
    if (readIops || readBandwidth) {
        auto* read = quota.MutableRead();
        read->SetIops(readIops);
        read->SetBandwidth(readBandwidth);
    }
    if (writeIops || writeBandwidth) {
        auto* write = quota.MutableWrite();
        write->SetIops(writeIops);
        write->SetBandwidth(writeBandwidth);
    }
    quota.SetExpectedIoParallelism(expectedIoParallelism);
    quota.SetMaxBudget(maxBudgetMs);
    quota.SetBudgetRefillTime(budgetRefillTimeMs);
    quota.SetBudgetSpendRate(budgetSpendRate);
    return quota;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeShapingThrottlerTest)
{
    Y_UNIT_TEST(ShouldDisableThrottlingWithEmptyConfig)
    {
        NProto::TShapingThrottlerConfig config;
        TVolumeShapingThrottler throttler(
            config,
            EMediaKind::STORAGE_MEDIA_SSD,
            0.0);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            throttler.SuggestDelay(
                TInstant::Seconds(1),
                4_KB,
                EOpType::Read,
                TDuration::Zero()));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            throttler.SuggestDelay(
                TInstant::Seconds(2),
                4_KB,
                EOpType::Write,
                TDuration::Zero()));
    }

    Y_UNIT_TEST(ShouldSelectCorrectQuotaByMediaKind)
    {
        auto makeConfig = [](EMediaKind mediaKind, ui32 writeIops) {
            NProto::TShapingThrottlerConfig config;

            auto quota = MakeQuota(
                0, 0,               // read: no profile
                writeIops, 0,       // write: writeIops iops, no bandwidth
                1,                  // expectedIoParallelism
                1000,               // maxBudgetMs
                10000,              // budgetRefillTimeMs
                2.0);               // budgetSpendRate

            switch (mediaKind) {
                case EMediaKind::STORAGE_MEDIA_SSD:
                    *config.MutableSsdQuota() = quota;
                    break;
                case EMediaKind::STORAGE_MEDIA_HDD:
                    *config.MutableHddQuota() = quota;
                    break;
                case EMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED:
                    *config.MutableNonreplQuota() = quota;
                    break;
                case EMediaKind::STORAGE_MEDIA_SSD_MIRROR2:
                    *config.MutableMirror2Quota() = quota;
                    break;
                case EMediaKind::STORAGE_MEDIA_SSD_MIRROR3:
                    *config.MutableMirror3Quota() = quota;
                    break;
                default:
                    break;
            }
            return config;
        };

        const auto mediaKinds = {
            EMediaKind::STORAGE_MEDIA_SSD,
            EMediaKind::STORAGE_MEDIA_HDD,
            EMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED,
            EMediaKind::STORAGE_MEDIA_SSD_MIRROR2,
            EMediaKind::STORAGE_MEDIA_SSD_MIRROR3,
        };

        for (auto mediaKind : mediaKinds) {
            const ui32 writeIops = 1000;
            auto config = makeConfig(mediaKind, writeIops);

            TVolumeShapingThrottler throttler(config, mediaKind, 0.0);
            auto delay = throttler.SuggestDelay(
                TInstant::Seconds(1),
                0,
                EOpType::Write,
                TDuration::Zero());

            // CostPerIO(1000, 0, 0) = ceil(1e6 / 1000) = 1000us
            // cost = parallelism(1) * 1000us = 1000us
            // bucket is full (1s), so delay comes from the bucket logic
            UNIT_ASSERT_VALUES_UNEQUAL(TDuration::Zero(), delay);
        }
    }

    Y_UNIT_TEST(ShouldUseSsdQuotaForSsd)
    {
        NProto::TShapingThrottlerConfig config;
        *config.MutableSsdQuota() = MakeQuota(
            100, 0,     // read: 100 iops
            0, 0,       // no write profile
            1,          // expectedIoParallelism
            1000,       // maxBudgetMs
            10000,      // budgetRefillTimeMs
            2.0);       // budgetSpendRate
        // HDD quota has different values - should not be picked for SSD.
        *config.MutableHddQuota() = MakeQuota(
            200, 0,     // read: 200 iops
            0, 0,       // no write profile
            1,          // expectedIoParallelism
            1000,       // maxBudgetMs
            10000,      // budgetRefillTimeMs
            2.0);       // budgetSpendRate

        TVolumeShapingThrottler ssdThrottler(
            config,
            EMediaKind::STORAGE_MEDIA_SSD,
            0.0);
        TVolumeShapingThrottler hddThrottler(
            config,
            EMediaKind::STORAGE_MEDIA_HDD,
            0.0);

        // CostPerIO(100, 0, 0) = ceil(1e6 / 100) = 10000us
        // CostPerIO(200, 0, 0) = ceil(1e6 / 200) = 5000us
        auto ssdDelay = ssdThrottler.SuggestDelay(
            TInstant::Seconds(1),
            0,
            EOpType::Read,
            TDuration::Zero());
        auto hddDelay = hddThrottler.SuggestDelay(
            TInstant::Seconds(1),
            0,
            EOpType::Read,
            TDuration::Zero());

        UNIT_ASSERT(ssdDelay > hddDelay);
    }

    Y_UNIT_TEST(ShouldUseHddQuotaForHybridMedia)
    {
        NProto::TShapingThrottlerConfig config;
        *config.MutableHddQuota() = MakeQuota(
            100, 0,     // read: 100 iops
            0, 0,       // no write profile
            1,          // expectedIoParallelism
            1000,       // maxBudgetMs
            10000,      // budgetRefillTimeMs
            2.0);       // budgetSpendRate

        TVolumeShapingThrottler hddThrottler(
            config,
            EMediaKind::STORAGE_MEDIA_HDD,
            0.0);
        TVolumeShapingThrottler hybridThrottler(
            config,
            EMediaKind::STORAGE_MEDIA_HYBRID,
            0.0);

        auto hddDelay = hddThrottler.SuggestDelay(
            TInstant::Seconds(1), 0, EOpType::Read, TDuration::Zero());
        auto hybridDelay = hybridThrottler.SuggestDelay(
            TInstant::Seconds(1), 0, EOpType::Read, TDuration::Zero());

        UNIT_ASSERT_VALUES_EQUAL(hddDelay, hybridDelay);
    }

    Y_UNIT_TEST(ShouldReturnZeroDelayForZeroAndDescribeOps)
    {
        NProto::TShapingThrottlerConfig config;
        *config.MutableSsdQuota() = MakeQuota(
            100, 0,     // read: 100 iops
            100, 0,     // write: 100 iops
            1,          // expectedIoParallelism
            1000,       // maxBudgetMs
            10000,      // budgetRefillTimeMs
            2.0);       // budgetSpendRate

        TVolumeShapingThrottler throttler(
            config,
            EMediaKind::STORAGE_MEDIA_SSD,
            0.0);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            throttler.SuggestDelay(
                TInstant::Seconds(1),
                4_KB,
                EOpType::Zero,
                TDuration::Zero()));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            throttler.SuggestDelay(
                TInstant::Seconds(2),
                4_KB,
                EOpType::Describe,
                TDuration::Zero()));
    }

    Y_UNIT_TEST(ShouldReturnZeroDelayIfPerformanceProfileMissing)
    {
        NProto::TShapingThrottlerConfig config;
        // Only set write profile, no read profile.
        *config.MutableSsdQuota() = MakeQuota(
            0, 0,       // no read profile
            100, 0,     // write: 100 iops
            1,          // expectedIoParallelism
            1000,       // maxBudgetMs
            10000,      // budgetRefillTimeMs
            2.0);       // budgetSpendRate

        TVolumeShapingThrottler throttler(
            config,
            EMediaKind::STORAGE_MEDIA_SSD,
            0.0);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            throttler.SuggestDelay(
                TInstant::Seconds(1),
                4_KB,
                EOpType::Read,
                TDuration::Zero()));

        // Write should produce a non-zero delay.
        UNIT_ASSERT_VALUES_UNEQUAL(
            TDuration::Zero(),
            throttler.SuggestDelay(
                TInstant::Seconds(2),
                0,
                EOpType::Write,
                TDuration::Zero()));
    }

    Y_UNIT_TEST(ShouldScaleCostByExpectedIoParallelism)
    {
        auto makeThrottler = [](ui32 parallelism) {
            NProto::TShapingThrottlerConfig config;
            *config.MutableSsdQuota() = MakeQuota(
                1000, 0,        // read: 1000 iops
                0, 0,           // no write profile
                parallelism,    // expectedIoParallelism
                10000,          // maxBudgetMs (large to avoid depletion)
                1000,           // budgetRefillTimeMs
                2.0);           // budgetSpendRate

            return TVolumeShapingThrottler(
                config,
                EMediaKind::STORAGE_MEDIA_SSD,
                0.0);
        };

        auto throttler1 = makeThrottler(1);
        auto throttler2 = makeThrottler(3);

        // CostPerIO(1000, 0, 0) = 1000us
        // parallelism=1: cost=1*1000=1000us
        // parallelism=3: cost=3*1000=3000us
        auto delay1 = throttler1.SuggestDelay(
            TInstant::Seconds(1),
            0,
            EOpType::Read,
            TDuration::Zero());
        auto delay2 = throttler2.SuggestDelay(
            TInstant::Seconds(1),
            0,
            EOpType::Read,
            TDuration::Zero());

        UNIT_ASSERT(delay2 > delay1);
    }

    Y_UNIT_TEST(ShouldAccountForByteCountInCost)
    {
        NProto::TShapingThrottlerConfig config;
        *config.MutableSsdQuota() = MakeQuota(
            1000, 100_MB,   // read: 1000 iops, 100MB/s bandwidth
            0, 0,           // no write profile
            1,              // expectedIoParallelism
            10000,          // maxBudgetMs (large to avoid depletion)
            1000,           // budgetRefillTimeMs
            2.0);           // budgetSpendRate

        TVolumeShapingThrottler throttler(
            config,
            EMediaKind::STORAGE_MEDIA_SSD,
            0.0);

        // Small IO should produce smaller delay than large IO.
        auto delaySmall = throttler.SuggestDelay(
            TInstant::Seconds(1),
            4_KB,
            EOpType::Read,
            TDuration::Zero());
        auto delayLarge = throttler.SuggestDelay(
            TInstant::Seconds(2),
            4_MB,
            EOpType::Read,
            TDuration::Zero());

        UNIT_ASSERT(delayLarge > delaySmall);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
