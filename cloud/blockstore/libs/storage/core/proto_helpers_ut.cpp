#include "proto_helpers.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void InitDev(const TString& uuid, NProto::TDeviceConfig* d)
{
    d->SetDeviceUUID(uuid);
    d->SetDeviceName(uuid + "-n");
    d->SetTransportId(uuid + "-t");
    d->SetBlockSize(DefaultBlockSize);
    d->SetBlocksCount(100);
}

NProto::TPartitionConfig MakePartitionConfig(
    ui64 blocksCount,
    ui32 blockSize,
    NCloud::NProto::EStorageMediaKind mediaKind)
{
    NProto::TPartitionConfig config;
    config.SetBlocksCount(blocksCount);
    config.SetBlockSize(blockSize);
    config.SetStorageMediaKind(mediaKind);
    return config;
}

NProto::TStorageServiceConfig MakeTargetFreshCapacityConfig()
{
    NProto::TStorageServiceConfig config;
    config.SetBytesPerFreshCapacityUnitSSD(32_GB);
    config.SetFlushThresholdSSD(64_MB);
    config.SetFreshByteCountLimitForBackpressureSSD(2_GB);
    config.SetFreshByteCountThresholdForBackpressureSSD(320_MB);
    config.SetFreshBlobCountFlushThresholdSSD(49152);
    config.SetFreshBlobByteCountFlushThresholdSSD(256_MB);
    config.SetFreshByteCountHardLimitSSD(1_GB);
    return config;
}

void AssertFreshCapacityLimits(
    const TFreshCapacityLimits& limits,
    ui64 units,
    ui64 flushThreshold,
    ui64 blobCountFlushThreshold,
    ui64 blobByteCountFlushThreshold,
    ui64 backpressureThreshold,
    ui64 backpressureLimit,
    ui64 hardLimit)
{
    UNIT_ASSERT_VALUES_EQUAL(units, limits.Units);
    UNIT_ASSERT_VALUES_EQUAL(flushThreshold, limits.FlushThreshold);
    UNIT_ASSERT_VALUES_EQUAL(
        blobCountFlushThreshold,
        limits.FreshBlobCountFlushThreshold);
    UNIT_ASSERT_VALUES_EQUAL(
        blobByteCountFlushThreshold,
        limits.FreshBlobByteCountFlushThreshold);
    UNIT_ASSERT_VALUES_EQUAL(
        backpressureThreshold,
        limits.FreshByteCountThresholdForBackpressure);
    UNIT_ASSERT_VALUES_EQUAL(
        backpressureLimit,
        limits.FreshByteCountLimitForBackpressure);
    UNIT_ASSERT_VALUES_EQUAL(hardLimit, limits.FreshByteCountHardLimit);
}

}   // namespace

Y_UNIT_TEST_SUITE(TProtoHelpersTest)
{
    Y_UNIT_TEST(TestFillDeviceInfo)
    {

        google::protobuf::RepeatedPtrField<NProto::TDeviceConfig> devices;
        InitDev("uuid-1-1", devices.Add());
        InitDev("uuid-1-2", devices.Add());
        InitDev("uuid-1-3", devices.Add());

        google::protobuf::RepeatedPtrField<NProto::TReplica> replicas;
        auto* r1 = replicas.Add();
        InitDev("uuid-2-1", r1->AddDevices());
        InitDev("uuid-2-2", r1->AddDevices());
        InitDev("uuid-2-3", r1->AddDevices());

        auto* r2 = replicas.Add();
        InitDev("uuid-3-1", r2->AddDevices());
        InitDev("uuid-3-2", r2->AddDevices());
        InitDev("uuid-3-3", r2->AddDevices());

        google::protobuf::RepeatedPtrField<TString> freshDeviceIds;
        *freshDeviceIds.Add() = "uuid-1-1";
        *freshDeviceIds.Add() = "uuid-2-3";

        google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations;
        auto* m = migrations.Add();
        m->SetSourceDeviceId("uuid-1-2");
        InitDev("uuid-1-2-m", m->MutableTargetDevice());
        m = migrations.Add();
        m->SetSourceDeviceId("uuid-1-3");
        InitDev("uuid-1-3-m", m->MutableTargetDevice());
        m = migrations.Add();
        m->SetSourceDeviceId("uuid-2-1");
        InitDev("uuid-2-1-m", m->MutableTargetDevice());
        m = migrations.Add();
        m->SetSourceDeviceId("uuid-3-2");
        InitDev("uuid-3-2-m", m->MutableTargetDevice());

        NProto::TVolume volume;
        FillDeviceInfo(devices, migrations, replicas, freshDeviceIds, volume);

        UNIT_ASSERT_VALUES_EQUAL(3, volume.DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-1",
            volume.GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-2",
            volume.GetDevices(1).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-3",
            volume.GetDevices(2).GetDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL(2, volume.ReplicasSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-1",
            volume.GetReplicas(0).GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-2",
            volume.GetReplicas(0).GetDevices(1).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-3",
            volume.GetReplicas(0).GetDevices(2).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-1",
            volume.GetReplicas(1).GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-2",
            volume.GetReplicas(1).GetDevices(1).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-3",
            volume.GetReplicas(1).GetDevices(2).GetDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL(2, volume.FreshDeviceIdsSize());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1-1", volume.GetFreshDeviceIds(0));
        UNIT_ASSERT_VALUES_EQUAL("uuid-2-3", volume.GetFreshDeviceIds(1));

        UNIT_ASSERT_VALUES_EQUAL(4, volume.MigrationsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-2",
            volume.GetMigrations(0).GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-2-t",
            volume.GetMigrations(0).GetSourceTransportId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-2-m",
            volume.GetMigrations(0).GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-3",
            volume.GetMigrations(1).GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-3-t",
            volume.GetMigrations(1).GetSourceTransportId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-3-m",
            volume.GetMigrations(1).GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-1",
            volume.GetMigrations(2).GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-1-t",
            volume.GetMigrations(2).GetSourceTransportId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-1-m",
            volume.GetMigrations(2).GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-2",
            volume.GetMigrations(3).GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-2-t",
            volume.GetMigrations(3).GetSourceTransportId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-2-m",
            volume.GetMigrations(3).GetTargetDevice().GetDeviceUUID());
    }

    Y_UNIT_TEST(TestThrottling)
    {
        {
            NProto::TStorageServiceConfig storageServiceConfig;
            TStorageConfig config(storageServiceConfig, nullptr);
            NProto::TPartitionConfig partitionConfig;
            UNIT_ASSERT(!GetThrottlingEnabled(config, partitionConfig));
            UNIT_ASSERT(
                !GetThrottlingEnabledZeroBlocks(config, partitionConfig));
        }

        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetThrottlingEnabled(true);
            TStorageConfig config(storageServiceConfig, nullptr);
            NProto::TPartitionConfig partitionConfig;
            UNIT_ASSERT(!GetThrottlingEnabled(config, partitionConfig));
            UNIT_ASSERT(
                !GetThrottlingEnabledZeroBlocks(config, partitionConfig));
        }

        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetThrottlingEnabled(true);
            TStorageConfig config(storageServiceConfig, nullptr);
            NProto::TPartitionConfig partitionConfig;
            partitionConfig.mutable_performanceprofile()->SetThrottlingEnabled(
                true);
            UNIT_ASSERT(GetThrottlingEnabled(config, partitionConfig));
            UNIT_ASSERT(
                GetThrottlingEnabledZeroBlocks(config, partitionConfig));
        }

        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetThrottlingEnabledSSD(true);
            TStorageConfig config(storageServiceConfig, nullptr);
            NProto::TPartitionConfig partitionConfig;
            partitionConfig.mutable_performanceprofile()->SetThrottlingEnabled(
                true);
            partitionConfig.SetStorageMediaKind(
                ::NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HDD);
            UNIT_ASSERT(!GetThrottlingEnabled(config, partitionConfig));
            UNIT_ASSERT(
                !GetThrottlingEnabledZeroBlocks(config, partitionConfig));
        }

        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetThrottlingEnabledSSD(true);
            TStorageConfig config(storageServiceConfig, nullptr);
            NProto::TPartitionConfig partitionConfig;
            partitionConfig.mutable_performanceprofile()->SetThrottlingEnabled(
                true);
            partitionConfig.SetStorageMediaKind(
                ::NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD);
            UNIT_ASSERT(GetThrottlingEnabled(config, partitionConfig));
            UNIT_ASSERT(
                GetThrottlingEnabledZeroBlocks(config, partitionConfig));
        }

        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetThrottlingEnabled(true);
            storageServiceConfig.SetThrottlingEnabledSSD(true);
            storageServiceConfig.SetDisableZeroBlocksThrottlingForYDBBasedDisks(
                true);
            TStorageConfig config(storageServiceConfig, nullptr);
            NProto::TPartitionConfig partitionConfig;
            partitionConfig.mutable_performanceprofile()->SetThrottlingEnabled(
                true);
            partitionConfig.SetStorageMediaKind(
                ::NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD);
            UNIT_ASSERT(
                !GetThrottlingEnabledZeroBlocks(config, partitionConfig));
        }

        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetThrottlingEnabled(true);
            storageServiceConfig.SetThrottlingEnabledSSD(true);
            storageServiceConfig.SetDisableZeroBlocksThrottlingForYDBBasedDisks(
                true);
            TStorageConfig config(storageServiceConfig, nullptr);
            NProto::TPartitionConfig partitionConfig;
            partitionConfig.mutable_performanceprofile()->SetThrottlingEnabled(
                true);
            partitionConfig.SetStorageMediaKind(
                ::NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_MIRROR3);
            UNIT_ASSERT(
                GetThrottlingEnabledZeroBlocks(config, partitionConfig));
        }
    }

    Y_UNIT_TEST(ShouldDisableFreshCapacityScalingByDefault)
    {
        // Both byte quanta default to zero, so an out-of-the-box config
        // resolves to the legacy defaults for both media kinds. Partition size
        // plays no part - see
        // ShouldPreserveLegacyFreshDefaultsWhenScalingIsEnabled for the
        // size-independence of the default caps.
        const TStorageConfig config(
            NProto::TStorageServiceConfig{},
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto hdd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                2 * 256_GB / DefaultBlockSize,
                DefaultBlockSize,
                NCloud::NProto::STORAGE_MEDIA_HDD));
        UNIT_ASSERT_VALUES_EQUAL(0, hdd.BytesPerFreshCapacityUnit);
        AssertFreshCapacityLimits(
            hdd,
            0,
            4_MB,
            3200,
            16_MB,
            40_MB,
            128_MB,
            256_MB);

        const auto ssd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                16 * 32_GB / DefaultBlockSize,
                DefaultBlockSize,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(0, ssd.BytesPerFreshCapacityUnit);
        AssertFreshCapacityLimits(
            ssd,
            0,
            4_MB,
            3200,
            16_MB,
            40_MB,
            128_MB,
            256_MB);
    }

    Y_UNIT_TEST(ShouldPreserveLegacyFreshDefaultsWhenScalingIsEnabled)
    {
        // Every default cap equals its one-unit legacy base, so turning
        // scaling on without raising a cap changes nothing at any size.
        NProto::TStorageServiceConfig proto;
        proto.SetBytesPerFreshCapacityUnitHDD(256_GB);
        proto.SetBytesPerFreshCapacityUnitSSD(32_GB);
        const TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto hdd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                2 * 256_GB / DefaultBlockSize,
                DefaultBlockSize,
                NCloud::NProto::STORAGE_MEDIA_HDD));
        UNIT_ASSERT_VALUES_EQUAL(256_GB, hdd.BytesPerFreshCapacityUnit);
        AssertFreshCapacityLimits(
            hdd,
            2,
            4_MB,
            3200,
            16_MB,
            40_MB,
            128_MB,
            256_MB);

        const auto ssd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                16 * 32_GB / DefaultBlockSize,
                DefaultBlockSize,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(32_GB, ssd.BytesPerFreshCapacityUnit);
        AssertFreshCapacityLimits(
            ssd,
            16,
            4_MB,
            3200,
            16_MB,
            40_MB,
            128_MB,
            256_MB);
    }

    Y_UNIT_TEST(ShouldScaleTargetSSDFreshCapacityAtUnitBoundaries)
    {
        const TStorageConfig config(
            MakeTargetFreshCapacityConfig(),
            std::make_shared<NFeatures::TFeaturesConfig>());

        struct TTestCase
        {
            ui64 PartitionBytes;
            ui64 Units;
            ui64 FlushThreshold;
            ui64 BlobCountFlushThreshold;
            ui64 BlobByteCountFlushThreshold;
            ui64 BackpressureThreshold;
            ui64 BackpressureLimit;
            ui64 HardLimit;
        };

        const TTestCase testCases[] = {
            {32_GB, 1, 4_MB, 3200, 16_MB, 40_MB, 128_MB, 256_MB},
            {128_GB, 4, 16_MB, 12800, 64_MB, 160_MB, 512_MB, 1_GB},
            {256_GB, 8, 32_MB, 25600, 128_MB, 320_MB, 1_GB, 1_GB},
            {512_GB, 16, 64_MB, 49152, 256_MB, 320_MB, 2_GB, 1_GB},
        };

        for (const auto& testCase: testCases) {
            const auto limits = GetEffectiveFreshCapacityLimits(
                config,
                MakePartitionConfig(
                    testCase.PartitionBytes / DefaultBlockSize,
                    DefaultBlockSize,
                    NCloud::NProto::STORAGE_MEDIA_SSD));
            AssertFreshCapacityLimits(
                limits,
                testCase.Units,
                testCase.FlushThreshold,
                testCase.BlobCountFlushThreshold,
                testCase.BlobByteCountFlushThreshold,
                testCase.BackpressureThreshold,
                testCase.BackpressureLimit,
                testCase.HardLimit);
        }

        const auto aboveBoundary = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                32_GB / DefaultBlockSize + 1,
                DefaultBlockSize,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(2, aboveBoundary.Units);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, aboveBoundary.FlushThreshold);

        // Block size only enters the calculation as a multiplier: 48 GiB made
        // of 64 KiB blocks rounds up to the same two units.
        const auto nonStandardBlockSize = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                48_GB / 64_KB,
                64_KB,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(2, nonStandardBlockSize.Units);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, nonStandardBlockSize.FlushThreshold);
    }

    Y_UNIT_TEST(ShouldGrantAtLeastOneFreshCapacityUnit)
    {
        const TStorageConfig config(
            MakeTargetFreshCapacityConfig(),
            std::make_shared<NFeatures::TFeaturesConfig>());

        // A partition below one 32 GiB quantum - an empty one included - earns
        // a full unit rather than a zeroed set of limits.
        for (const ui64 blocksCount: {ui64(0), ui64(1)}) {
            const auto limits = GetEffectiveFreshCapacityLimits(
                config,
                MakePartitionConfig(
                    blocksCount,
                    DefaultBlockSize,
                    NCloud::NProto::STORAGE_MEDIA_SSD));
            AssertFreshCapacityLimits(
                limits,
                1,
                4_MB,
                3200,
                16_MB,
                40_MB,
                128_MB,
                256_MB);
        }
    }

    Y_UNIT_TEST(ShouldClampFreshCapacityToCapsBelowLegacyBases)
    {
        // A cap below its legacy base is returned as is, however many units the
        // partition earns. A zero cap stays a literal zero.
        NProto::TStorageServiceConfig proto;
        proto.SetBytesPerFreshCapacityUnitSSD(4_KB);
        proto.SetFlushThresholdSSD(1_MB);
        proto.SetFreshBlobCountFlushThresholdSSD(100);
        proto.SetFreshBlobByteCountFlushThresholdSSD(2_MB);
        proto.SetFreshByteCountThresholdForBackpressureSSD(3_MB);
        proto.SetFreshByteCountLimitForBackpressureSSD(4_MB);
        proto.SetFreshByteCountHardLimitSSD(0);
        const TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto limits = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(8, 4_KB, NCloud::NProto::STORAGE_MEDIA_SSD));
        AssertFreshCapacityLimits(limits, 8, 1_MB, 100, 2_MB, 3_MB, 4_MB, 0);
    }

    Y_UNIT_TEST(ShouldKeepHDDAndSSDFreshCapacityIndependent)
    {
        // The same partition earns three HDD units and two SSD units. Both caps
        // are far above the scaled values, so each result is unambiguously its
        // own base times its own unit count.
        NProto::TStorageServiceConfig proto;
        proto.SetBytesPerFreshCapacityUnitHDD(10);
        proto.SetBytesPerFreshCapacityUnitSSD(20);
        proto.SetFlushThreshold(100_MB);
        proto.SetFlushThresholdSSD(100_MB);
        TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto hdd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                3,
                10,
                NCloud::NProto::STORAGE_MEDIA_HDD));
        UNIT_ASSERT_VALUES_EQUAL(3, hdd.Units);
        UNIT_ASSERT_VALUES_EQUAL(3 * 4_MB, hdd.FlushThreshold);

        const auto ssd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                3,
                10,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(2, ssd.Units);
        UNIT_ASSERT_VALUES_EQUAL(2 * 4_MB, ssd.FlushThreshold);
    }

    Y_UNIT_TEST(ShouldSelectAllFreshCapacityCapsByMediaKind)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetBytesPerFreshCapacityUnitHDD(0);
        proto.SetFlushThreshold(11);
        proto.SetFreshByteCountLimitForBackpressure(12);
        proto.SetFreshByteCountThresholdForBackpressure(13);
        proto.SetFreshBlobCountFlushThreshold(14);
        proto.SetFreshBlobByteCountFlushThreshold(15);
        proto.SetFreshByteCountHardLimit(16);

        proto.SetBytesPerFreshCapacityUnitSSD(0);
        proto.SetFlushThresholdSSD(21);
        proto.SetFreshByteCountLimitForBackpressureSSD(22);
        proto.SetFreshByteCountThresholdForBackpressureSSD(23);
        proto.SetFreshBlobCountFlushThresholdSSD(24);
        proto.SetFreshBlobByteCountFlushThresholdSSD(25);
        proto.SetFreshByteCountHardLimitSSD(26);

        TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto hdd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                1,
                1,
                NCloud::NProto::STORAGE_MEDIA_HDD));
        AssertFreshCapacityLimits(hdd, 0, 11, 14, 15, 13, 12, 16);

        const auto ssd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                1,
                1,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        AssertFreshCapacityLimits(ssd, 0, 21, 24, 25, 23, 22, 26);
    }

    Y_UNIT_TEST(ShouldMapOnlySSDToSSDFreshCapacity)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetBytesPerFreshCapacityUnitHDD(0);
        proto.SetBytesPerFreshCapacityUnitSSD(0);
        proto.SetFlushThreshold(111);
        proto.SetFlushThresholdSSD(222);
        TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const NCloud::NProto::EStorageMediaKind hddKinds[] = {
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            static_cast<NCloud::NProto::EStorageMediaKind>(999),
        };
        for (const auto mediaKind: hddKinds) {
            const auto limits = GetEffectiveFreshCapacityLimits(
                config,
                MakePartitionConfig(1, 1, mediaKind));
            UNIT_ASSERT_VALUES_EQUAL(111, limits.FlushThreshold);
        }

        const auto ssd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                1,
                1,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(222, ssd.FlushThreshold);
    }

    Y_UNIT_TEST(ShouldSupportSSDFreshCapacityLimitsAbove4GiB)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetFlushThresholdSSD(8_GB);
        proto.SetFreshByteCountLimitForBackpressureSSD(64_GB);
        proto.SetFreshByteCountThresholdForBackpressureSSD(32_GB);
        proto.SetFreshBlobCountFlushThresholdSSD(8_GB);
        proto.SetFreshBlobByteCountFlushThresholdSSD(16_GB);
        proto.SetFreshByteCountHardLimitSSD(128_GB);
        TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto partitionConfig = MakePartitionConfig(
            1_TB / DefaultBlockSize,
            DefaultBlockSize,
            NCloud::NProto::STORAGE_MEDIA_SSD);

        // Scaling is off by default, so the limits survive unclamped.
        AssertFreshCapacityLimits(
            GetEffectiveFreshCapacityLimits(config, partitionConfig),
            0,
            8_GB,
            8_GB,
            16_GB,
            32_GB,
            64_GB,
            128_GB);

        // With scaling on, a 1TiB partition is 32 units, which is not enough
        // to reach any of these caps.
        proto.SetBytesPerFreshCapacityUnitSSD(32_GB);
        TStorageConfig scaledConfig(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());
        AssertFreshCapacityLimits(
            GetEffectiveFreshCapacityLimits(scaledConfig, partitionConfig),
            32,
            32 * 4_MB,
            32 * 3200,
            32 * 16_MB,
            32 * 40_MB,
            32 * 128_MB,
            32 * 256_MB);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
