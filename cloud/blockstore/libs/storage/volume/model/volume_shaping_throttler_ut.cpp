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
            NCloud::NProto::STORAGE_MEDIA_SSD,
            /*spentShapingBudgetShare=*/0.0);

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

    void ShouldSelectCorrectQuotaByMediaKind(
        NCloud::NProto::EStorageMediaKind mediaKind)
    {
        auto makeConfig = [mediaKind]()
        {
            constexpr ui32 WriteIops = 100;
            constexpr ui64 WriteBandwidth = 10_MB;
            NProto::TShapingThrottlerConfig config;

            auto quota = MakeQuota(
                0,                // readIops
                0,                // readBandwidth
                WriteIops,        // writeIops
                WriteBandwidth,   // writeBandwidth
                1,                // expectedIoParallelism
                100,              // maxBudgetMs
                0,                // budgetRefillTimeMs
                2.0);             // budgetSpendRate

            switch (mediaKind) {
                case NCloud::NProto::STORAGE_MEDIA_SSD:
                    *config.MutableSsdQuota() = quota;
                    break;
                case NCloud::NProto::STORAGE_MEDIA_HDD:
                case NCloud::NProto::STORAGE_MEDIA_HYBRID:
                    *config.MutableHddQuota() = quota;
                    break;
                case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED:
                    *config.MutableNonreplQuota() = quota;
                    break;
                case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2:
                    *config.MutableMirror2Quota() = quota;
                    break;
                case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3:
                    *config.MutableMirror3Quota() = quota;
                    break;
                default:
                    break;
            }
            return config;
        };

        auto config = makeConfig();

        TVolumeShapingThrottler throttler(
            config,
            mediaKind,
            /*spentShapingBudgetShare=*/0.0);
        auto delay = throttler.SuggestDelay(
            TInstant::Seconds(1),   // ts
            1_MB,                   // byteCount
            EOpType::Write,
            TDuration::Zero());   // executionTime
        UNIT_ASSERT_VALUES_UNEQUAL(TDuration::Zero(), delay);
    }

    Y_UNIT_TEST(ShouldSelectCorrectQuotaByMediaKind_SSD)
    {
        ShouldSelectCorrectQuotaByMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);
    }

    Y_UNIT_TEST(ShouldSelectCorrectQuotaByMediaKind_HDD)
    {
        ShouldSelectCorrectQuotaByMediaKind(NCloud::NProto::STORAGE_MEDIA_HDD);
    }

    Y_UNIT_TEST(ShouldSelectCorrectQuotaByMediaKind_HYBRID)
    {
        ShouldSelectCorrectQuotaByMediaKind(
            NCloud::NProto::STORAGE_MEDIA_HYBRID);
    }

    Y_UNIT_TEST(ShouldSelectCorrectQuotaByMediaKind_SSD_NONREPLICATED)
    {
        ShouldSelectCorrectQuotaByMediaKind(
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldSelectCorrectQuotaByMediaKind_SSD_MIRROR2)
    {
        ShouldSelectCorrectQuotaByMediaKind(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2);
    }

    Y_UNIT_TEST(ShouldSelectCorrectQuotaByMediaKind_SSD_MIRROR3)
    {
        ShouldSelectCorrectQuotaByMediaKind(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3);
    }

    Y_UNIT_TEST(ShouldUseSsdQuotaForSsd)
    {
        NProto::TShapingThrottlerConfig config;
        *config.MutableSsdQuota() = MakeQuota(
            90,     // readIops
            0,      // readBandwidth
            0,      // writeIops
            0,      // writeBandwidth
            1,      // expectedIoParallelism
            10,     // maxBudgetMs
            0,      // budgetRefillTimeMs (no refill)
            2.0);   // budgetSpendRate
        *config.MutableHddQuota() = MakeQuota(
            50,     // readIops
            0,      // readBandwidth
            0,      // writeIops
            0,      // writeBandwidth
            1,      // expectedIoParallelism
            10,     // maxBudgetMs
            0,      // budgetRefillTimeMs (no refill)
            2.0);   // budgetSpendRate

        TVolumeShapingThrottler ssdThrottler(
            config,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            0.0);
        TVolumeShapingThrottler hddThrottler(
            config,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            0.0);

        const TDuration ssdDelay = ssdThrottler.SuggestDelay(
            TInstant::Seconds(1),   // ts
            0,                      // byteCount
            EOpType::Read,
            TDuration::Zero());
        const TDuration hddDelay = hddThrottler.SuggestDelay(
            TInstant::Seconds(1),   // ts
            0,                      // byteCount
            EOpType::Read,
            TDuration::Zero());

        UNIT_ASSERT_GT(ssdDelay, TDuration::Zero());
        UNIT_ASSERT_GT(hddDelay, TDuration::Zero());
        UNIT_ASSERT_GT(hddDelay, ssdDelay);
    }

    Y_UNIT_TEST(ShouldUseHddQuotaForHybridMedia)
    {
        NProto::TShapingThrottlerConfig config;
        *config.MutableHddQuota() = MakeQuota(
            100,
            0,   // read: 100 iops
            0,
            0,       // no write profile
            1,       // expectedIoParallelism
            1000,    // maxBudgetMs
            10000,   // budgetRefillTimeMs
            2.0);    // budgetSpendRate

        TVolumeShapingThrottler hddThrottler(
            config,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            0.0);
        TVolumeShapingThrottler hybridThrottler(
            config,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            0.0);

        auto hddDelay = hddThrottler.SuggestDelay(
            TInstant::Seconds(1),
            0,
            EOpType::Read,
            TDuration::Zero());
        auto hybridDelay = hybridThrottler.SuggestDelay(
            TInstant::Seconds(1),
            0,
            EOpType::Read,
            TDuration::Zero());

        UNIT_ASSERT_VALUES_EQUAL(hddDelay, hybridDelay);
    }

    Y_UNIT_TEST(ShouldReturnZeroDelayForZeroAndDescribeOps)
    {
        NProto::TShapingThrottlerConfig config;
        *config.MutableSsdQuota() = MakeQuota(
            100,
            0,   // read: 100 iops
            100,
            0,       // write: 100 iops
            1,       // expectedIoParallelism
            1000,    // maxBudgetMs
            10000,   // budgetRefillTimeMs
            2.0);    // budgetSpendRate

        TVolumeShapingThrottler throttler(
            config,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            /*spentShapingBudgetShare=*/0.0);

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
            0,       // readIops
            0,       // readBandwidth
            100,     // writeIops
            10_MB,   // writeBandwidth
            1,       // expectedIoParallelism
            100,     // maxBudgetMs
            0,       // budgetRefillTimeMs (no refill)
            2.0);    // budgetSpendRate

        TVolumeShapingThrottler throttler(
            config,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            /*spentShapingBudgetShare=*/0.0);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            throttler.SuggestDelay(
                TInstant::Seconds(1),
                4_MB,
                EOpType::Read,
                TDuration::Zero()));

        // Write should produce a non-zero delay.
        UNIT_ASSERT_VALUES_UNEQUAL(
            TDuration::Zero(),
            throttler.SuggestDelay(
                TInstant::Seconds(2),
                4_MB,
                EOpType::Write,
                TDuration::Zero()));
    }

    Y_UNIT_TEST(ShouldScaleCostByExpectedIoParallelism)
    {
        auto makeThrottler = [](ui32 parallelism)
        {
            NProto::TShapingThrottlerConfig config;
            *config.MutableSsdQuota() = MakeQuota(
                1000,          // readIops
                10_MB,         // readBandwidth
                0,             // writeIops
                0,             // writeBandwidth
                parallelism,   // expectedIoParallelism
                100,           // maxBudgetMs
                0,             // budgetRefillTimeMs (no refill)
                2.0);          // budgetSpendRate

            return TVolumeShapingThrottler(
                config,
                NCloud::NProto::STORAGE_MEDIA_SSD,
                /*spentShapingBudgetShare=*/0.0);
        };

        auto throttler1 = makeThrottler(1);
        auto throttler2 = makeThrottler(3);

        const TDuration delay1 = throttler1.SuggestDelay(
            TInstant::Seconds(1),   // ts
            4_MB,                   // byteCount
            EOpType::Read,
            TDuration::Zero());
        const TDuration delay2 = throttler2.SuggestDelay(
            TInstant::Seconds(1),   // ts
            4_MB,                   // byteCount
            EOpType::Read,
            TDuration::Zero());

        UNIT_ASSERT_GT(delay2, delay1);
    }

    Y_UNIT_TEST(ShouldAccountForByteCountInCost)
    {
        auto makeThrottler = []()
        {
            NProto::TShapingThrottlerConfig config;
            *config.MutableSsdQuota() = MakeQuota(
                100,     // readIops
                10_MB,   // readBandwidth
                0,       // writeIops
                0,       // writeBandwidth
                1,       // expectedIoParallelism
                20,      // maxBudgetMs
                0,       // budgetRefillTimeMs (no refill)
                2.0);    // budgetSpendRate

            return TVolumeShapingThrottler(
                config,
                NCloud::NProto::STORAGE_MEDIA_SSD,
                /*spentShapingBudgetShare=*/0.0);
        };

        auto throttler1 = makeThrottler();
        auto throttler2 = makeThrottler();

        // Small IO should produce smaller delay than large IO.
        auto delaySmall = throttler1.SuggestDelay(
            TInstant::Seconds(1),
            4_KB,
            EOpType::Read,
            TDuration::Zero());
        auto delayLarge = throttler2.SuggestDelay(
            TInstant::Seconds(2),
            4_MB,
            EOpType::Read,
            TDuration::Zero());

        UNIT_ASSERT(delayLarge > delaySmall);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
