#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <contrib/ydb/core/control/immediate_control_board_impl.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigTest)
{
    Y_UNIT_TEST(ShouldOverrideConfigFields)
    {
        NProto::TStorageServiceConfig globalConfigProto;
        globalConfigProto.SetMaxMigrationBandwidth(100);
        globalConfigProto.SetMaxMigrationIoDepth(4);

        NProto::TStorageServiceConfig patch;
        patch.SetMaxMigrationBandwidth(400);

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        auto config = TStorageConfig::Merge(globalConfig, patch);
        UNIT_ASSERT_UNEQUAL(config, globalConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            patch.GetMaxMigrationBandwidth(),
            config->GetMaxMigrationBandwidth());

        UNIT_ASSERT_VALUES_EQUAL(
            globalConfigProto.GetMaxMigrationIoDepth(),
            config->GetMaxMigrationIoDepth());

        UNIT_ASSERT_VALUES_EQUAL("/Root", config->GetSchemeShardDir());
    }

    Y_UNIT_TEST(ShouldIgnoreEmptyPatch)
    {
        NProto::TStorageServiceConfig globalConfigProto;
        globalConfigProto.SetMaxMigrationBandwidth(100);

        NProto::TStorageServiceConfig patch;

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        auto config = TStorageConfig::Merge(globalConfig, patch);
        UNIT_ASSERT_EQUAL(globalConfig, config);

        UNIT_ASSERT_VALUES_EQUAL(
            globalConfigProto.GetMaxMigrationBandwidth(),
            config->GetMaxMigrationBandwidth());

        UNIT_ASSERT_VALUES_EQUAL("/Root", config->GetSchemeShardDir());
    }

    Y_UNIT_TEST(ShouldOverrideConfigsViaImmediateControlBoard)
    {
        const auto defaultConfig = std::make_shared<TStorageConfig>(
            NProto::TStorageServiceConfig{},
            std::make_shared<NFeatures::TFeaturesConfig>());

        NKikimr::TControlBoard controlBoard;

        const NProto::TStorageServiceConfig globalConfigProto = [] {;
            NProto::TStorageServiceConfig proto;
            proto.SetMaxMigrationBandwidth(100);
            proto.SetMaxMigrationIoDepth(4);
            return proto;
        } ();

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        globalConfig->Register(controlBoard);

        UNIT_ASSERT_VALUES_EQUAL(
            globalConfigProto.GetMaxMigrationBandwidth(),
            globalConfig->GetMaxMigrationBandwidth());

        UNIT_ASSERT_VALUES_EQUAL(
            globalConfigProto.GetMaxMigrationIoDepth(),
            globalConfig->GetMaxMigrationIoDepth());

        UNIT_ASSERT_VALUES_EQUAL(
            defaultConfig->GetExpectedDiskAgentSize(),
            globalConfig->GetExpectedDiskAgentSize());

        UNIT_ASSERT_VALUES_EQUAL(
            defaultConfig->GetSchemeShardDir(),
            globalConfig->GetSchemeShardDir());

        // override MaxMigrationBandwidth via ICB

        const ui32 maxMigrationBandwidthICB = 400;

        {
            TAtomic prevValue = {};
            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                maxMigrationBandwidthICB,
                prevValue));

            UNIT_ASSERT_VALUES_EQUAL(
                globalConfigProto.GetMaxMigrationBandwidth(),
                AtomicGet(prevValue));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationBandwidthICB,
            globalConfig->GetMaxMigrationBandwidth());

        // override MaxMigrationIoDepth via ICB

        const ui32 maxMigrationIoDepthICB = 8;

        {
            TAtomic prevValue = {};
            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationIoDepth",
                maxMigrationIoDepthICB,
                prevValue));

            UNIT_ASSERT_VALUES_EQUAL(
                globalConfigProto.GetMaxMigrationIoDepth(),
                AtomicGet(prevValue));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationIoDepthICB,
            globalConfig->GetMaxMigrationIoDepth());

        // Apply a patch with new MaxMigrationIoDepth & ExpectedDiskAgentSize

        const ui32 maxMigrationIoDepthPatch = 1;
        const ui32 expectedDiskAgentSizePatch = 100;

        NProto::TStorageServiceConfig patch;
        patch.SetMaxMigrationIoDepth(maxMigrationIoDepthPatch);
        patch.SetExpectedDiskAgentSize(expectedDiskAgentSizePatch);

        auto config = TStorageConfig::Merge(globalConfig, patch);
        UNIT_ASSERT_UNEQUAL(globalConfig, config);

        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationBandwidthICB,
            config->GetMaxMigrationBandwidth());

        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationIoDepthPatch,
            config->GetMaxMigrationIoDepth());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedDiskAgentSizePatch,
            config->GetExpectedDiskAgentSize());

        UNIT_ASSERT_VALUES_EQUAL(
            defaultConfig->GetSchemeShardDir(),
            config->GetSchemeShardDir());
    }

    Y_UNIT_TEST(ShouldOverrideConfigsViaImmediateControlBoard2)
    {
        // Check for simple overrides.
        {
            NProto::TStorageServiceConfig overriddenProto = []
            {
                NProto::TStorageServiceConfig proto;
                proto.SetMaxMigrationBandwidth(400);
                proto.SetDefaultTabletVersion(1);
                return proto;
            }();
            const auto overriddenConfig = std::make_shared<TStorageConfig>(
                std::move(overriddenProto),
                std::make_shared<NFeatures::TFeaturesConfig>());

            UNIT_ASSERT_VALUES_EQUAL(
                400,
                overriddenConfig->GetMaxMigrationBandwidth());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                overriddenConfig->GetMaxMigrationIoDepth());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                overriddenConfig->GetDefaultTabletVersion());
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::Minutes(1),
                overriddenConfig
                    ->GetNonReplicatedAgentDisconnectRecoveryInterval());
            UNIT_ASSERT_EQUAL(
                NCloud::NProto::AUTHORIZATION_IGNORE,
                overriddenConfig->GetAuthorizationMode());

            NKikimr::TControlBoard controlBoard;
            overriddenConfig->Register(controlBoard);

            UNIT_ASSERT_VALUES_EQUAL(
                400,
                overriddenConfig->GetMaxMigrationBandwidth());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                overriddenConfig->GetDefaultTabletVersion());

            TAtomic prevValue{};
            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                600,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                600,
                overriddenConfig->GetMaxMigrationBandwidth());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                0,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetMaxMigrationBandwidth());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_DefaultTabletVersion",
                0,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetDefaultTabletVersion());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_AuthorizationMode",
                2,
                prevValue));
            UNIT_ASSERT_EQUAL(
                NCloud::NProto::AUTHORIZATION_REQUIRE,
                overriddenConfig->GetAuthorizationMode());
        }

        // Check with zeroed field in the proto config.
        {
            NProto::TStorageServiceConfig overriddenProto = []
            {
                NProto::TStorageServiceConfig proto;
                proto.SetMaxMigrationBandwidth(0);
                proto.SetAuthorizationMode(
                    NCloud::NProto::AUTHORIZATION_ACCEPT);
                return proto;
            }();
            const auto overriddenConfig = std::make_shared<TStorageConfig>(
                std::move(overriddenProto),
                std::make_shared<NFeatures::TFeaturesConfig>());

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetMaxMigrationBandwidth());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetDefaultTabletVersion());
            UNIT_ASSERT_EQUAL(
                NCloud::NProto::AUTHORIZATION_ACCEPT,
                overriddenConfig->GetAuthorizationMode());

            NKikimr::TControlBoard controlBoard;
            overriddenConfig->Register(controlBoard);

            TAtomic prevValue{};

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                100,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                100,
                overriddenConfig->GetMaxMigrationBandwidth());

            UNIT_ASSERT(controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                0,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetMaxMigrationBandwidth());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_DefaultTabletVersion",
                1,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                overriddenConfig->GetDefaultTabletVersion());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_AuthorizationMode",
                2,
                prevValue));
            UNIT_ASSERT_EQUAL(
                NCloud::NProto::AUTHORIZATION_REQUIRE,
                overriddenConfig->GetAuthorizationMode());
        }

        // Check for RO overrides.
        {
            NProto::TStorageServiceConfig overriddenProto = []
            {
                NProto::TStorageServiceConfig proto;
                proto.SetSchemeShardDir("foo");
                proto.SetServiceVersionInfo("bar");
                return proto;
            }();
            const auto overriddenConfig = std::make_shared<TStorageConfig>(
                std::move(overriddenProto),
                std::make_shared<NFeatures::TFeaturesConfig>());

            UNIT_ASSERT_VALUES_EQUAL(
                "foo",
                overriddenConfig->GetSchemeShardDir());
            UNIT_ASSERT_VALUES_EQUAL(
                "bar",
                overriddenConfig->GetServiceVersionInfo());
            UNIT_ASSERT_VALUES_EQUAL("", overriddenConfig->GetFolderId());
        }
    }

    Y_UNIT_TEST(ShouldOverrideDoublesViaImmediateControlBoard)
    {
        NProto::TStorageServiceConfig overriddenProto = []
        {
            NProto::TStorageServiceConfig proto;
            proto.SetNonReplicatedAgentTimeoutGrowthFactor(2.5);
            return proto;
        }();
        const auto overriddenConfig = std::make_shared<TStorageConfig>(
            std::move(overriddenProto),
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT_VALUES_EQUAL(
            2.5,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());
        UNIT_ASSERT_VALUES_EQUAL(
            50,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());

        NKikimr::TControlBoard controlBoard;
        overriddenConfig->Register(controlBoard);

        UNIT_ASSERT_VALUES_EQUAL(
            2.5,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());
        UNIT_ASSERT_VALUES_EQUAL(
            50,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());

        TAtomic prevValue{};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_NonReplicatedAgentTimeoutGrowthFactor",
            123,
            prevValue));
        UNIT_ASSERT_VALUES_EQUAL(
            123,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());

        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_DiskRegistryInitialAgentRejectionThreshold",
            456,
            prevValue));
        UNIT_ASSERT_VALUES_EQUAL(
            456,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());
    }

    Y_UNIT_TEST(ShouldOverrideNegativeValuesViaImmediateControlBoard)
    {
        NProto::TStorageServiceConfig overriddenProto = []
        {
            NProto::TStorageServiceConfig proto;
            proto.SetNonReplicatedAgentTimeoutGrowthFactor(-2.5);
            return proto;
        }();
        const auto overriddenConfig = std::make_shared<TStorageConfig>(
            std::move(overriddenProto),
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT_VALUES_EQUAL(
            -2.5,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());
        UNIT_ASSERT_VALUES_EQUAL(
            50,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());

        NKikimr::TControlBoard controlBoard;
        overriddenConfig->Register(controlBoard);

        UNIT_ASSERT_VALUES_EQUAL(
            -2.5,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());
        UNIT_ASSERT_VALUES_EQUAL(
            50,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());

        TAtomic prevValue{};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_NonReplicatedAgentTimeoutGrowthFactor",
            -123,
            prevValue));
        UNIT_ASSERT_VALUES_EQUAL(
            -123,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());

        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_DiskRegistryInitialAgentRejectionThreshold",
            -456,
            prevValue));
        UNIT_ASSERT_VALUES_EQUAL(
            -456,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());
    }

    Y_UNIT_TEST(ShouldAdaptNodeRegistrationParams)
    {
        NProto::TServerConfig serverConfig;
        serverConfig.SetNodeRegistrationMaxAttempts(10);
        serverConfig.SetNodeRegistrationErrorTimeout(20);
        serverConfig.SetVhostDiscardEnabled(true);

        NProto::TStorageServiceConfig storageConfigProto = []
        {
            NProto::TStorageServiceConfig proto;
            proto.SetNodeRegistrationMaxAttempts(30);
            proto.SetNodeRegistrationTimeout(40);
            return proto;
        }();

        AdaptNodeRegistrationParams("foobar", serverConfig, storageConfigProto);

        const auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(storageConfigProto),
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT_VALUES_EQUAL(
            30,
            storageConfig->GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(20),
            storageConfig->GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(40),
            storageConfig->GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            storageConfig->GetEnableVhostDiscardForNewVolumes());
        UNIT_ASSERT_VALUES_EQUAL(
            "root@builtin",
            storageConfig->GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("foobar", storageConfig->GetNodeType());
    }

    Y_UNIT_TEST(ShouldAdaptNodeRegistrationParamsWhileZeroOverridden)
    {
        NProto::TServerConfig serverConfig;
        serverConfig.SetNodeRegistrationMaxAttempts(10);

        NProto::TStorageServiceConfig storageConfigProto = []
        {
            NProto::TStorageServiceConfig proto;
            proto.SetNodeRegistrationMaxAttempts(0);
            return proto;
        }();

        AdaptNodeRegistrationParams("", serverConfig, storageConfigProto);

        const auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(storageConfigProto),
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            storageConfig->GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL("", storageConfig->GetNodeType());
    }

    Y_UNIT_TEST(ShouldCalcLinkedDisksBandwidthWithoutConfig)
    {
        using EStorageMediaKind = NCloud::NProto::EStorageMediaKind;
        NProto::TStorageServiceConfig globalConfigProto;
        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());
        auto ssdToSsd = GetLinkedDiskFillBandwidth(
            *globalConfig,
            EStorageMediaKind::STORAGE_MEDIA_SSD,
            EStorageMediaKind::STORAGE_MEDIA_SSD);

        UNIT_ASSERT_VALUES_EQUAL(100, ssdToSsd.Bandwidth);
        UNIT_ASSERT_VALUES_EQUAL(1, ssdToSsd.IoDepth);
    }

    Y_UNIT_TEST(ShouldCalcLinkedDisksBandwidthWithDefault)
    {
        using EStorageMediaKind = NCloud::NProto::EStorageMediaKind;
        NProto::TStorageServiceConfig globalConfigProto;
        {
            NProto::TLinkedDiskFillBandwidth defaultBandwidth;
            defaultBandwidth.SetReadBandwidth(200);
            defaultBandwidth.SetReadIoDepth(2);
            defaultBandwidth.SetWriteBandwidth(300);
            defaultBandwidth.SetWriteIoDepth(3);
            globalConfigProto.MutableLinkedDiskFillBandwidth()->Add(
                std::move(defaultBandwidth));
        }

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        auto ssdToSsd = GetLinkedDiskFillBandwidth(
            *globalConfig,
            EStorageMediaKind::STORAGE_MEDIA_SSD,
            EStorageMediaKind::STORAGE_MEDIA_SSD);
        UNIT_ASSERT_VALUES_EQUAL(200, ssdToSsd.Bandwidth);
        UNIT_ASSERT_VALUES_EQUAL(2, ssdToSsd.IoDepth);

        auto ssdToHdd = GetLinkedDiskFillBandwidth(
            *globalConfig,
            EStorageMediaKind::STORAGE_MEDIA_SSD,
            EStorageMediaKind::STORAGE_MEDIA_HDD);
        UNIT_ASSERT_VALUES_EQUAL(200, ssdToHdd.Bandwidth);
        UNIT_ASSERT_VALUES_EQUAL(2, ssdToHdd.IoDepth);
    }

    Y_UNIT_TEST(ShouldReturnStaticFreshThresholdsWhenDynamicSizingDisabled)
    {
        NProto::TStorageServiceConfig proto;
        const auto config = std::make_shared<TStorageConfig>(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        // FlushThresholdPerTB defaults to 0 → dynamic disabled.
        const auto thresholds = config->GetEffectiveFreshThresholds(1_TB);

        UNIT_ASSERT_VALUES_EQUAL(4_MB, thresholds.FlushThreshold);
        UNIT_ASSERT_VALUES_EQUAL(3200, thresholds.FreshBlobCountFlushThreshold);
        UNIT_ASSERT_VALUES_EQUAL(
            16_MB,
            thresholds.FreshBlobByteCountFlushThreshold);
        UNIT_ASSERT_VALUES_EQUAL(256_MB, thresholds.FreshByteCountHardLimit);
        UNIT_ASSERT_VALUES_EQUAL(
            128_MB,
            thresholds.FreshByteCountLimitForBackpressure);
        UNIT_ASSERT_VALUES_EQUAL(
            40_MB,
            thresholds.FreshByteCountThresholdForBackpressure);
    }

    Y_UNIT_TEST(ShouldReturnCustomStaticFreshThresholdsWhenDynamicSizingDisabled)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetFlushThreshold(8_MB);
        proto.SetFreshBlobCountFlushThreshold(1000);
        proto.SetFreshBlobByteCountFlushThreshold(32_MB);
        proto.SetFreshByteCountHardLimit(512_MB);
        proto.SetFreshByteCountLimitForBackpressure(200_MB);
        proto.SetFreshByteCountThresholdForBackpressure(80_MB);
        // FlushThresholdPerTB stays 0 → dynamic disabled.

        const auto config = std::make_shared<TStorageConfig>(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto thresholds = config->GetEffectiveFreshThresholds(1_TB);

        UNIT_ASSERT_VALUES_EQUAL(8_MB, thresholds.FlushThreshold);
        UNIT_ASSERT_VALUES_EQUAL(1000, thresholds.FreshBlobCountFlushThreshold);
        UNIT_ASSERT_VALUES_EQUAL(
            32_MB,
            thresholds.FreshBlobByteCountFlushThreshold);
        UNIT_ASSERT_VALUES_EQUAL(512_MB, thresholds.FreshByteCountHardLimit);
        UNIT_ASSERT_VALUES_EQUAL(
            200_MB,
            thresholds.FreshByteCountLimitForBackpressure);
        UNIT_ASSERT_VALUES_EQUAL(
            80_MB,
            thresholds.FreshByteCountThresholdForBackpressure);
    }

    Y_UNIT_TEST(ShouldScaleFreshThresholdsLinearlyWithPerPartitionDiskSize)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetFlushThresholdPerTB(64_MB);
        proto.SetFlushThresholdMax(64_MB);
        // FlushThresholdMin defaults to 4_MB.

        const auto config = std::make_shared<TStorageConfig>(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        // 512 GiB per-partition → FlushThreshold = 32 MiB, scale = 8.
        const auto thresholds = config->GetEffectiveFreshThresholds(512_GB);

        UNIT_ASSERT_VALUES_EQUAL(32_MB, thresholds.FlushThreshold);
        UNIT_ASSERT_VALUES_EQUAL(
            3200 * 8,
            thresholds.FreshBlobCountFlushThreshold);
        UNIT_ASSERT_VALUES_EQUAL(
            16_MB * 8,
            thresholds.FreshBlobByteCountFlushThreshold);
        UNIT_ASSERT_VALUES_EQUAL(
            256_MB * 8,
            thresholds.FreshByteCountHardLimit);
        UNIT_ASSERT_VALUES_EQUAL(
            128_MB * 8,
            thresholds.FreshByteCountLimitForBackpressure);
        UNIT_ASSERT_VALUES_EQUAL(
            40_MB * 8,
            thresholds.FreshByteCountThresholdForBackpressure);
    }

    Y_UNIT_TEST(ShouldClampFreshThresholdsByMax)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetFlushThresholdPerTB(64_MB);
        proto.SetFlushThresholdMax(64_MB);

        const auto config = std::make_shared<TStorageConfig>(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        // 1 TiB per-partition → raw = 64 MiB, equals cap, scale = 16.
        {
            const auto t = config->GetEffectiveFreshThresholds(1_TB);
            UNIT_ASSERT_VALUES_EQUAL(64_MB, t.FlushThreshold);
        }

        // 2 TiB per-partition → raw = 128 MiB but capped at 64 MiB.
        {
            const auto t = config->GetEffectiveFreshThresholds(2_TB);
            UNIT_ASSERT_VALUES_EQUAL(64_MB, t.FlushThreshold);
            // Same as 1 TiB: scale = 16 applied to the rest.
        }
    }

    Y_UNIT_TEST(ShouldFloorFreshThresholdsByMin)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetFlushThresholdPerTB(64_MB);
        proto.SetFlushThresholdMax(64_MB);
        // Min defaults to 4 MiB.

        const auto config = std::make_shared<TStorageConfig>(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        // 1 GiB per-partition → raw = 64 KiB → floored to 4 MiB.
        {
            const auto t = config->GetEffectiveFreshThresholds(1_GB);
            UNIT_ASSERT_VALUES_EQUAL(4_MB, t.FlushThreshold);
            // scale = 4 MiB / 4 MiB = 1 → all values equal the static defaults.
            UNIT_ASSERT_VALUES_EQUAL(3200, t.FreshBlobCountFlushThreshold);
            UNIT_ASSERT_VALUES_EQUAL(16_MB, t.FreshBlobByteCountFlushThreshold);
            UNIT_ASSERT_VALUES_EQUAL(256_MB, t.FreshByteCountHardLimit);
            UNIT_ASSERT_VALUES_EQUAL(
                128_MB,
                t.FreshByteCountLimitForBackpressure);
            UNIT_ASSERT_VALUES_EQUAL(
                40_MB,
                t.FreshByteCountThresholdForBackpressure);
        }

        // 0 bytes per-partition → floored to 4 MiB.
        {
            const auto t = config->GetEffectiveFreshThresholds(0);
            UNIT_ASSERT_VALUES_EQUAL(4_MB, t.FlushThreshold);
        }
    }

    Y_UNIT_TEST(ShouldRespectCustomFloorAndCustomStaticThreshold)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetFlushThresholdPerTB(64_MB);
        proto.SetFlushThresholdMax(0); // no cap
        proto.SetFlushThresholdMin(8_MB);
        proto.SetFlushThreshold(4_MB);

        const auto config = std::make_shared<TStorageConfig>(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        // 64 GiB → raw = 4 MiB, floored to 8 MiB.
        {
            const auto t = config->GetEffectiveFreshThresholds(64_GB);
            UNIT_ASSERT_VALUES_EQUAL(8_MB, t.FlushThreshold);
            // scale = 8/4 = 2.
            UNIT_ASSERT_VALUES_EQUAL(6400, t.FreshBlobCountFlushThreshold);
        }

        // 1 TiB → raw = 64 MiB, no cap, scale = 16.
        {
            const auto t = config->GetEffectiveFreshThresholds(1_TB);
            UNIT_ASSERT_VALUES_EQUAL(64_MB, t.FlushThreshold);
        }
    }

    Y_UNIT_TEST(ShouldCalcLinkedDisksBandwidth)
    {
        using EStorageMediaKind = NCloud::NProto::EStorageMediaKind;
        NProto::TStorageServiceConfig globalConfigProto;
        {
            NProto::TLinkedDiskFillBandwidth defaultBandwidth;
            defaultBandwidth.SetReadBandwidth(150);
            defaultBandwidth.SetReadIoDepth(2);
            defaultBandwidth.SetWriteBandwidth(200);
            defaultBandwidth.SetWriteIoDepth(2);
            globalConfigProto.MutableLinkedDiskFillBandwidth()->Add(
                std::move(defaultBandwidth));
        }
        {
            NProto::TLinkedDiskFillBandwidth ssdBandwidth;
            ssdBandwidth.SetMediaKind(EStorageMediaKind::STORAGE_MEDIA_SSD);
            ssdBandwidth.SetReadBandwidth(300);
            ssdBandwidth.SetReadIoDepth(3);
            ssdBandwidth.SetWriteBandwidth(300);
            ssdBandwidth.SetWriteIoDepth(2);
            globalConfigProto.MutableLinkedDiskFillBandwidth()->Add(
                std::move(ssdBandwidth));
        }
        {
            NProto::TLinkedDiskFillBandwidth nrdBandwidth;
            nrdBandwidth.SetMediaKind(
                EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED);
            nrdBandwidth.SetReadBandwidth(500);
            nrdBandwidth.SetReadIoDepth(4);
            nrdBandwidth.SetWriteBandwidth(400);
            nrdBandwidth.SetWriteIoDepth(4);
            globalConfigProto.MutableLinkedDiskFillBandwidth()->Add(
                std::move(nrdBandwidth));
        }

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        {
            auto bandwidth = GetLinkedDiskFillBandwidth(
                *globalConfig,
                EStorageMediaKind::STORAGE_MEDIA_SSD,
                EStorageMediaKind::STORAGE_MEDIA_SSD);
            UNIT_ASSERT_VALUES_EQUAL(300, bandwidth.Bandwidth);
            UNIT_ASSERT_VALUES_EQUAL(2, bandwidth.IoDepth);
        }

        {
            auto bandwidth = GetLinkedDiskFillBandwidth(
                *globalConfig,
                EStorageMediaKind::STORAGE_MEDIA_SSD,
                EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED);
            UNIT_ASSERT_VALUES_EQUAL(300, bandwidth.Bandwidth);
            UNIT_ASSERT_VALUES_EQUAL(3, bandwidth.IoDepth);
        }

        {
            auto bandwidth = GetLinkedDiskFillBandwidth(
                *globalConfig,
                EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED,
                EStorageMediaKind::STORAGE_MEDIA_HDD);
            UNIT_ASSERT_VALUES_EQUAL(200, bandwidth.Bandwidth);
            UNIT_ASSERT_VALUES_EQUAL(2, bandwidth.IoDepth);
        }
        {
            auto bandwidth = GetLinkedDiskFillBandwidth(
                *globalConfig,
                EStorageMediaKind::STORAGE_MEDIA_HDD,
                EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED);
            UNIT_ASSERT_VALUES_EQUAL(150, bandwidth.Bandwidth);
            UNIT_ASSERT_VALUES_EQUAL(2, bandwidth.IoDepth);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
