#include "proto_helpers.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <limits>

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
    ui32 flushThreshold,
    ui32 blobCountFlushThreshold,
    ui32 blobByteCountFlushThreshold,
    ui32 backpressureThreshold,
    ui32 backpressureLimit,
    ui32 hardLimit)
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

    Y_UNIT_TEST(ShouldPreserveLegacyFreshDefaultsForEveryPartitionSize)
    {
        const TStorageConfig config(
            NProto::TStorageServiceConfig{},
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
            ui32 FlushThreshold;
            ui32 BlobCountFlushThreshold;
            ui32 BlobByteCountFlushThreshold;
            ui32 BackpressureThreshold;
            ui32 BackpressureLimit;
            ui32 HardLimit;
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
    }

    Y_UNIT_TEST(ShouldResolveZeroAndExactFreshCapacityUnitsSafely)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetBytesPerFreshCapacityUnitSSD(0);
        proto.SetFlushThresholdSSD(123);
        proto.SetFreshByteCountLimitForBackpressureSSD(456);
        proto.SetFreshByteCountThresholdForBackpressureSSD(78);
        proto.SetFreshBlobCountFlushThresholdSSD(9);
        proto.SetFreshBlobByteCountFlushThresholdSSD(10);
        proto.SetFreshByteCountHardLimitSSD(0);
        TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto exact = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                std::numeric_limits<ui64>::max(),
                std::numeric_limits<ui32>::max(),
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(0, exact.BytesPerFreshCapacityUnit);
        AssertFreshCapacityLimits(exact, 0, 123, 9, 10, 78, 456, 0);

        proto.SetBytesPerFreshCapacityUnitSSD(32_GB);
        TStorageConfig scaledConfig(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());
        const auto zeroSize = GetEffectiveFreshCapacityLimits(
            scaledConfig,
            MakePartitionConfig(
                0,
                DefaultBlockSize,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(1, zeroSize.Units);

        const auto oneBlock = GetEffectiveFreshCapacityLimits(
            scaledConfig,
            MakePartitionConfig(
                1,
                DefaultBlockSize,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(1, oneBlock.Units);
    }

    Y_UNIT_TEST(ShouldScaleFreshCapacityWithNonStandardBlockSize)
    {
        NProto::TStorageServiceConfig proto =
            MakeTargetFreshCapacityConfig();
        proto.SetBytesPerFreshCapacityUnitSSD(10);
        TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto exact = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                5,
                2,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(1, exact.Units);

        const auto above = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                2,
                6,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(2, above.Units);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, above.FlushThreshold);
    }

    Y_UNIT_TEST(ShouldKeepHDDAndSSDFreshCapacityIndependent)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetBytesPerFreshCapacityUnitHDD(10);
        proto.SetBytesPerFreshCapacityUnitSSD(20);
        proto.SetFlushThreshold(12_MB);
        proto.SetFlushThresholdSSD(16_MB);
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
        UNIT_ASSERT_VALUES_EQUAL(12_MB, hdd.FlushThreshold);

        const auto ssd = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                3,
                10,
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(2, ssd.Units);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, ssd.FlushThreshold);
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

    Y_UNIT_TEST(ShouldBoundFreshCapacityAtCapsAndSaturateSizeArithmetic)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetBytesPerFreshCapacityUnitSSD(2);
        proto.SetFlushThresholdSSD(std::numeric_limits<ui32>::max());
        proto.SetFreshByteCountLimitForBackpressureSSD(1);
        proto.SetFreshByteCountThresholdForBackpressureSSD(0);
        proto.SetFreshBlobCountFlushThresholdSSD(49152);
        proto.SetFreshBlobByteCountFlushThresholdSSD(2);
        proto.SetFreshByteCountHardLimitSSD(3);
        TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto limits = GetEffectiveFreshCapacityLimits(
            config,
            MakePartitionConfig(
                std::numeric_limits<ui64>::max(),
                std::numeric_limits<ui32>::max(),
                NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(
            ui64{1} << 63,
            limits.Units);
        AssertFreshCapacityLimits(
            limits,
            ui64{1} << 63,
            std::numeric_limits<ui32>::max(),
            49152,
            2,
            0,
            1,
            3);
    }

    Y_UNIT_TEST(ShouldSelectMaximumUnflushedFreshBlobAgeByMediaKind)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetMaxUnflushedFreshBlobAgeHDD(123);
        proto.SetMaxUnflushedFreshBlobAgeSSD(456);
        TStorageConfig config(
            proto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(456),
            GetMaxUnflushedFreshBlobAge(
                config,
                NCloud::NProto::STORAGE_MEDIA_SSD));

        const NCloud::NProto::EStorageMediaKind hddKinds[] = {
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            static_cast<NCloud::NProto::EStorageMediaKind>(999),
        };
        for (const auto mediaKind: hddKinds) {
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::MilliSeconds(123),
                GetMaxUnflushedFreshBlobAge(config, mediaKind));
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
