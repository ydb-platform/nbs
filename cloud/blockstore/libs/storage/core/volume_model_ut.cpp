#include "volume_model.h"

#include "config.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>

#include <contrib/ydb/core/protos/blockstore_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <array>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const std::array<NCloud::NProto::EStorageMediaKind, 5> MEDIA_KINDS {
    NCloud::NProto::STORAGE_MEDIA_HDD,
    NCloud::NProto::STORAGE_MEDIA_SSD,
    NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
    NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
    NCloud::NProto::STORAGE_MEDIA_HYBRID
};

const std::array<NCloud::NProto::EStorageMediaKind, 2> HDD_MEDIA_KINDS {
    NCloud::NProto::STORAGE_MEDIA_HDD,
    NCloud::NProto::STORAGE_MEDIA_HYBRID
};

////////////////////////////////////////////////////////////////////////////////

auto CreateConfigWithThrottlingParams(
    const NCloud::NProto::EStorageMediaKind mediaKind,
    const ui32 minChannelCount = 1)
{
    NProto::TStorageServiceConfig config;

    if (mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD) {
        config.SetThrottlingEnabledSSD(true);
    } else {
        config.SetThrottlingEnabled(true);
    }

    config.SetMinChannelCount(minChannelCount);
    config.SetFreshChannelCount(1);

    config.SetThrottlingBurstPercentage(10);
    config.SetThrottlingMaxPostponedWeight(1000);

    config.SetAllocationUnitSSD(32);
    config.SetSSDUnitReadBandwidth(15);
    config.SetSSDUnitWriteBandwidth(25);
    config.SetSSDMaxReadBandwidth(250);
    config.SetSSDMaxWriteBandwidth(450);
    config.SetSSDUnitReadIops(300);
    config.SetSSDUnitWriteIops(1000);
    config.SetSSDMaxReadIops(12000);
    config.SetSSDMaxWriteIops(40000);

    config.SetAllocationUnitHDD(256);
    config.SetHDDUnitReadBandwidth(20);
    config.SetHDDUnitWriteBandwidth(30);
    config.SetHDDMaxReadBandwidth(150);
    config.SetHDDMaxWriteBandwidth(300);
    config.SetHDDUnitReadIops(100);
    config.SetHDDUnitWriteIops(300);
    config.SetHDDMaxReadIops(290);
    config.SetHDDMaxWriteIops(1100);

    config.SetAllocationUnitNonReplicatedSSD(16);
    config.SetNonReplicatedSSDUnitReadBandwidth(40);
    config.SetNonReplicatedSSDUnitWriteBandwidth(40);
    config.SetNonReplicatedSSDMaxReadBandwidth(4095);
    config.SetNonReplicatedSSDMaxWriteBandwidth(4095);
    config.SetNonReplicatedSSDUnitReadIops(8000);
    config.SetNonReplicatedSSDUnitWriteIops(8000);
    config.SetNonReplicatedSSDMaxReadIops(800000);
    config.SetNonReplicatedSSDMaxWriteIops(800000);

    config.SetAllocationUnitNonReplicatedHDD(16);
    config.SetNonReplicatedHDDUnitReadBandwidth(1);
    config.SetNonReplicatedHDDUnitWriteBandwidth(5);
    config.SetNonReplicatedHDDMaxReadBandwidth(200);
    config.SetNonReplicatedHDDMaxWriteBandwidth(200);
    config.SetNonReplicatedHDDUnitReadIops(2);
    config.SetNonReplicatedHDDUnitWriteIops(5);
    config.SetNonReplicatedHDDMaxReadIops(200);
    config.SetNonReplicatedHDDMaxWriteIops(1000);

    config.SetThrottlingBoostTime(1e5);
    config.SetThrottlingBoostRefillTime(1e6);
    config.SetThrottlingSSDBoostUnits(2);
    config.SetThrottlingHDDBoostUnits(3);

    return CreateTestStorageConfig(std::move(config));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeModelTest)
{
    Y_UNIT_TEST(ShouldAllocateAtLeastMinChannelCountChannels)
    {
        auto config = CreateConfigWithThrottlingParams(
            NCloud::NProto::STORAGE_MEDIA_SSD,
            4
        );

        TVolumeParams volumeParams;
        volumeParams.BlockSize = DefaultBlockSize;
        volumeParams.BlocksCountPerPartition = 1;
        volumeParams.PartitionsCount = 1;
        volumeParams.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);
        UNIT_ASSERT_VALUES_EQUAL(8, volumeConfig.ExplicitChannelProfilesSize());
        // limits should depend on the actual size - not on the number of channels
        UNIT_ASSERT_VALUES_EQUAL(
            15_MB,
            volumeConfig.GetPerformanceProfileMaxReadBandwidth()
        );

        volumeParams.BlocksCountPerPartition = 150_GB / DefaultBlockSize;
        volumeConfig.Clear();
        ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);
        UNIT_ASSERT_VALUES_EQUAL(9, volumeConfig.ExplicitChannelProfilesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            75_MB,
            volumeConfig.GetPerformanceProfileMaxReadBandwidth()
        );
    }

    Y_UNIT_TEST(ShouldAllocateAtMost255Channels)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetFreshChannelCount(1);
        auto config = CreateTestStorageConfig(std::move(storageServiceConfig));

        TVolumeParams volumeParams;
        volumeParams.BlockSize = DefaultBlockSize;
        volumeParams.BlocksCountPerPartition = 100500_TB / DefaultBlockSize;
        volumeParams.PartitionsCount = 1;
        volumeParams.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);
        UNIT_ASSERT(volumeConfig.ExplicitChannelProfilesSize() == MaxDataChannelCount);
    }

    Y_UNIT_TEST(ShouldSetValuesFromExplicitPerformanceProfile)
    {
        for (const auto mediaKind: MEDIA_KINDS) {
            auto config = CreateConfigWithThrottlingParams(mediaKind);

            ui32 blockSize = DefaultBlockSize;
            ui64 blocksCount = 1;

            NProto::TVolumePerformanceProfile pp;
            pp.SetMaxReadBandwidth(555'000'000);
            pp.SetMaxWriteBandwidth(222'000'000);
            pp.SetMaxReadIops(1111);
            pp.SetMaxWriteIops(2222);

            pp.SetBurstPercentage(25);
            pp.SetMaxPostponedWeight(1000);
            pp.SetThrottlingEnabled(true);

            pp.SetBoostTime(2e5);
            pp.SetBoostRefillTime(2e6);
            pp.SetBoostPercentage(500);

            TVolumeParams volumeParams;
            volumeParams.BlockSize = blockSize;
            volumeParams.BlocksCountPerPartition = blocksCount;
            volumeParams.PartitionsCount = 1;
            volumeParams.MediaKind = mediaKind;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            ResizeVolume(*config, volumeParams, {}, pp, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(1, volumeConfig.PartitionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                blocksCount,
                volumeConfig.GetPartitions(0).GetBlockCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                pp.GetMaxReadBandwidth(),
                volumeConfig.GetPerformanceProfileMaxReadBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                pp.GetMaxWriteBandwidth(),
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                pp.GetMaxReadIops(),
                volumeConfig.GetPerformanceProfileMaxReadIops()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                pp.GetMaxWriteIops(),
                volumeConfig.GetPerformanceProfileMaxWriteIops()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                pp.GetBurstPercentage(),
                volumeConfig.GetPerformanceProfileBurstPercentage()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                pp.GetMaxPostponedWeight(),
                volumeConfig.GetPerformanceProfileMaxPostponedWeight()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                pp.GetBoostTime(),
                volumeConfig.GetPerformanceProfileBoostTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                pp.GetBoostRefillTime(),
                volumeConfig.GetPerformanceProfileBoostRefillTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                pp.GetBoostPercentage(),
                volumeConfig.GetPerformanceProfileBoostPercentage()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                volumeConfig.GetPerformanceProfileThrottlingEnabled()
            );
        }
    }

    Y_UNIT_TEST(ShouldAutocomputeValuesWithoutExplicitPerformanceProfile)
    {
        ui32 blockSize = DefaultBlockSize;
        ui64 blocksCount = 20 * 1_GB / blockSize;

        for (const auto mediaKind: HDD_MEDIA_KINDS) {
            auto config = CreateConfigWithThrottlingParams(mediaKind);

            TVolumeParams volumeParams;
            volumeParams.BlockSize = blockSize;
            volumeParams.BlocksCountPerPartition = blocksCount;
            volumeParams.PartitionsCount = 1;
            volumeParams.MediaKind = mediaKind;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(1, volumeConfig.PartitionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                blocksCount,
                volumeConfig.GetPartitions(0).GetBlockCount()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetHDDUnitReadBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxReadBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetHDDUnitWriteBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetHDDUnitReadIops(),
                volumeConfig.GetPerformanceProfileMaxReadIops()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetHDDUnitWriteIops(),
                volumeConfig.GetPerformanceProfileMaxWriteIops()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBurstPercentage(),
                volumeConfig.GetPerformanceProfileBurstPercentage()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingMaxPostponedWeight(),
                volumeConfig.GetPerformanceProfileMaxPostponedWeight()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostRefillTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostRefillTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingHDDBoostUnits() * 100,
                volumeConfig.GetPerformanceProfileBoostPercentage()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                volumeConfig.GetPerformanceProfileThrottlingEnabled()
            );
        }

        {
            auto config = CreateConfigWithThrottlingParams(
                NCloud::NProto::STORAGE_MEDIA_SSD
            );

            TVolumeParams volumeParams;
            volumeParams.BlockSize = blockSize;
            volumeParams.BlocksCountPerPartition = blocksCount;
            volumeParams.PartitionsCount = 1;
            volumeParams.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(1, volumeConfig.PartitionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                blocksCount,
                volumeConfig.GetPartitions(0).GetBlockCount()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetSSDUnitReadBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxReadBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetSSDUnitWriteBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetSSDUnitReadIops(),
                volumeConfig.GetPerformanceProfileMaxReadIops()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetSSDUnitWriteIops(),
                volumeConfig.GetPerformanceProfileMaxWriteIops()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBurstPercentage(),
                volumeConfig.GetPerformanceProfileBurstPercentage()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingMaxPostponedWeight(),
                volumeConfig.GetPerformanceProfileMaxPostponedWeight()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostRefillTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostRefillTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingSSDBoostUnits() * 100,
                volumeConfig.GetPerformanceProfileBoostPercentage()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                volumeConfig.GetPerformanceProfileThrottlingEnabled()
            );
        }

        const auto nonreplKinds = {
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
        };
        const auto getParams = [] (
            const NCloud::NProto::EStorageMediaKind mediaKind,
            const TStorageConfigPtr& config) -> std::array<ui32, 8>
        {
            switch (mediaKind) {
                case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: return {
                    config->GetNonReplicatedSSDUnitReadBandwidth(),
                    config->GetNonReplicatedSSDUnitWriteBandwidth(),
                    config->GetNonReplicatedSSDUnitReadIops(),
                    config->GetNonReplicatedSSDUnitWriteIops(),
                    config->GetNonReplicatedSSDMaxReadBandwidth(),
                    config->GetNonReplicatedSSDMaxWriteBandwidth(),
                    config->GetNonReplicatedSSDMaxReadIops(),
                    config->GetNonReplicatedSSDMaxWriteIops(),
                };
                case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: return {
                    config->GetNonReplicatedHDDUnitReadBandwidth(),
                    config->GetNonReplicatedHDDUnitWriteBandwidth(),
                    config->GetNonReplicatedHDDUnitReadIops(),
                    config->GetNonReplicatedHDDUnitWriteIops(),
                    config->GetNonReplicatedHDDMaxReadBandwidth(),
                    config->GetNonReplicatedHDDMaxWriteBandwidth(),
                    config->GetNonReplicatedHDDMaxReadIops(),
                    config->GetNonReplicatedHDDMaxWriteIops(),
                };
                default: UNIT_ASSERT(false);
            }

            return {};
        };
        for (const auto mediaKind: nonreplKinds) {
            auto config = CreateConfigWithThrottlingParams(mediaKind);
            const auto mediaParams = getParams(mediaKind, config);

            TVolumeParams volumeParams;
            volumeParams.BlockSize = blockSize;
            volumeParams.BlocksCountPerPartition = blocksCount;
            volumeParams.PartitionsCount = 1;
            volumeParams.MediaKind = mediaKind;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(1, volumeConfig.PartitionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                blocksCount,
                volumeConfig.GetPartitions(0).GetBlockCount()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NKikimrBlockStore::EPartitionType::NonReplicated),
                static_cast<ui32>(volumeConfig.GetPartitions(0).GetType())
            );

            UNIT_ASSERT_VALUES_EQUAL(
                2 * mediaParams[0] * 1_MB,
                volumeConfig.GetPerformanceProfileMaxReadBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                2 * mediaParams[1] * 1_MB,
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                2 * mediaParams[2],
                volumeConfig.GetPerformanceProfileMaxReadIops()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                2 * mediaParams[3],
                volumeConfig.GetPerformanceProfileMaxWriteIops()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBurstPercentage(),
                volumeConfig.GetPerformanceProfileBurstPercentage()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingMaxPostponedWeight(),
                volumeConfig.GetPerformanceProfileMaxPostponedWeight()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostRefillTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostRefillTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                volumeConfig.GetPerformanceProfileBoostPercentage()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                volumeConfig.GetPerformanceProfileThrottlingEnabled()
            );

            volumeParams.BlocksCountPerPartition = 100_TB / blockSize;
            volumeParams.PartitionsCount = 1;

            volumeConfig.Clear();
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(1, volumeConfig.PartitionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                100_TB / blockSize,
                volumeConfig.GetPartitions(0).GetBlockCount()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NKikimrBlockStore::EPartitionType::NonReplicated),
                static_cast<ui32>(volumeConfig.GetPartitions(0).GetType())
            );

            UNIT_ASSERT_VALUES_EQUAL(
                mediaParams[4] * 1_MB,
                volumeConfig.GetPerformanceProfileMaxReadBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                mediaParams[5] * 1_MB,
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                mediaParams[6],
                volumeConfig.GetPerformanceProfileMaxReadIops()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                mediaParams[7],
                volumeConfig.GetPerformanceProfileMaxWriteIops()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBurstPercentage(),
                volumeConfig.GetPerformanceProfileBurstPercentage()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingMaxPostponedWeight(),
                volumeConfig.GetPerformanceProfileMaxPostponedWeight()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostRefillTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostRefillTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                volumeConfig.GetPerformanceProfileBoostPercentage()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                volumeConfig.GetPerformanceProfileThrottlingEnabled()
            );
        }

        {
            auto config = CreateConfigWithThrottlingParams(
                NCloud::NProto::STORAGE_MEDIA_SSD
            );

            TVolumeParams volumeParams;
            volumeParams.BlockSize = blockSize;
            volumeParams.BlocksCountPerPartition =
                1.9 * config->GetAllocationUnitSSD() * 1_GB / blockSize;
            volumeParams.PartitionsCount = 1;
            const auto nc = 2;
            volumeParams.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(1, volumeConfig.PartitionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                volumeParams.BlocksCountPerPartition,
                volumeConfig.GetPartitions(0).GetBlockCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                nc + 4,
                volumeConfig.ExplicitChannelProfilesSize()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                nc * config->GetSSDUnitReadBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxReadBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                nc * config->GetSSDUnitWriteBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                nc * config->GetSSDUnitReadIops(),
                volumeConfig.GetPerformanceProfileMaxReadIops()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                nc * config->GetSSDUnitWriteIops(),
                volumeConfig.GetPerformanceProfileMaxWriteIops()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBurstPercentage(),
                volumeConfig.GetPerformanceProfileBurstPercentage()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingMaxPostponedWeight(),
                volumeConfig.GetPerformanceProfileMaxPostponedWeight()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetThrottlingBoostRefillTime().MilliSeconds(),
                volumeConfig.GetPerformanceProfileBoostRefillTime()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                ui32(config->GetThrottlingSSDBoostUnits() * 100. / nc),
                volumeConfig.GetPerformanceProfileBoostPercentage()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                volumeConfig.GetPerformanceProfileThrottlingEnabled()
            );
        }
    }

    Y_UNIT_TEST(ShouldNotReduceMaxIopsAndBandwidthUponAutorecalculation)
    {
        for (const auto mediaKind: MEDIA_KINDS) {
            auto config = CreateConfigWithThrottlingParams(mediaKind);

            TVolumeParams volumeParams;
            volumeParams.BlockSize = DefaultBlockSize;
            volumeParams.BlocksCountPerPartition = 1024;
            volumeParams.PartitionsCount = 1;
            volumeParams.MediaKind = mediaKind;
            volumeParams.MaxReadIops = 100500;
            volumeParams.MaxReadBandwidth = 1005006000;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);
            UNIT_ASSERT_VALUES_EQUAL(
                volumeParams.MaxReadIops,
                volumeConfig.GetPerformanceProfileMaxReadIops()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                volumeParams.MaxReadBandwidth,
                volumeConfig.GetPerformanceProfileMaxReadBandwidth()
            );
        }
    }

    Y_UNIT_TEST(ShouldSetWriteParamsFromReadParamsIfWriteParamsAreNotSet)
    {
        for (const auto mediaKind: MEDIA_KINDS) {
            auto config = CreateConfigWithThrottlingParams(mediaKind);

            TVolumeParams volumeParams;
            volumeParams.BlockSize = DefaultBlockSize;
            volumeParams.BlocksCountPerPartition = 1024;
            volumeParams.PartitionsCount = 1;
            volumeParams.MediaKind = mediaKind;
            volumeParams.MaxReadIops = 100500;
            volumeParams.MaxReadBandwidth = 1005006000;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(
                volumeParams.MaxReadBandwidth,
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                volumeParams.MaxReadIops,
                volumeConfig.GetPerformanceProfileMaxWriteIops()
            );
        }
    }

    Y_UNIT_TEST(ShouldNotExceedMaxDiskSettingsUponAutorecalculation)
    {
        for (const auto mediaKind: HDD_MEDIA_KINDS) {
            auto config = CreateConfigWithThrottlingParams(mediaKind);

            TVolumeParams volumeParams;
            volumeParams.BlockSize = DefaultBlockSize;
            volumeParams.BlocksCountPerPartition = Max<ui32>();
            volumeParams.PartitionsCount = 1;
            volumeParams.MediaKind = mediaKind;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetHDDMaxReadBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxReadBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetHDDMaxWriteBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetHDDMaxReadIops(),
                volumeConfig.GetPerformanceProfileMaxReadIops()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetHDDMaxWriteIops(),
                volumeConfig.GetPerformanceProfileMaxWriteIops()
            );
        }

        {
            auto config = CreateConfigWithThrottlingParams(
                NCloud::NProto::STORAGE_MEDIA_SSD
            );

            TVolumeParams volumeParams;
            volumeParams.BlockSize = DefaultBlockSize;
            volumeParams.BlocksCountPerPartition = Max<ui32>();
            volumeParams.PartitionsCount = 1;
            volumeParams.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(
                config->GetSSDMaxReadBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxReadBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetSSDMaxWriteBandwidth() * 1_MB,
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetSSDMaxReadIops(),
                volumeConfig.GetPerformanceProfileMaxReadIops()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                config->GetSSDMaxWriteIops(),
                volumeConfig.GetPerformanceProfileMaxWriteIops()
            );
        }
    }

    Y_UNIT_TEST(ShouldNotSetStorageMediaKindIfAlreadySet)
    {
        for (const auto mediaKind: MEDIA_KINDS) {
            TVolumeParams volumeParams;
            volumeParams.BlockSize = DefaultBlockSize;
            volumeParams.MediaKind = mediaKind;

            NKikimrBlockStore::TVolumeConfig volumeConfig;
            NKikimrBlockStore::TVolumeConfig prevConfig;
            prevConfig.SetStorageMediaKind(mediaKind);
            UNIT_ASSERT(!SetMissingParams(
                volumeParams,
                prevConfig,
                volumeConfig
            ));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                volumeConfig.GetStorageMediaKind()
            );
        }
    }

#define CHECK_CHANNEL(channel, poolKind, dataKind, unit)                       \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            poolKind,                                                          \
            volumeConfig.GetExplicitChannelProfiles(channel).GetPoolKind()     \
        );                                                                     \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            static_cast<ui32>(dataKind),                                       \
            volumeConfig.GetExplicitChannelProfiles(channel).GetDataKind()     \
        );                                                                     \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            unit,                                                              \
            volumeConfig.GetExplicitChannelProfiles(channel).GetSize()         \
        );                                                                     \
// CHECK_CHANNEL

#define CHECK_CHANNEL_HDD(channel, poolKind, dataKind)                         \
        CHECK_CHANNEL(                                                         \
            channel,                                                           \
            poolKind,                                                          \
            dataKind,                                                          \
            config->GetAllocationUnitHDD() * 1_GB                              \
        )                                                                      \
// CHECK_CHANNEL_HDD

    void CheckVolumeChannels(
        const TStorageConfig& config,
        const NKikimrBlockStore::TVolumeConfig& volumeConfig)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            volumeConfig.VolumeExplicitChannelProfilesSize());

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetSSDMergedChannelPoolKind(),
            volumeConfig.GetVolumeExplicitChannelProfiles(0).GetPoolKind());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(EChannelDataKind::System),
            volumeConfig.GetVolumeExplicitChannelProfiles(0).GetDataKind());

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetSSDMergedChannelPoolKind(),
            volumeConfig.GetVolumeExplicitChannelProfiles(1).GetPoolKind());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(EChannelDataKind::Log),
            volumeConfig.GetVolumeExplicitChannelProfiles(1).GetDataKind());

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetSSDMergedChannelPoolKind(),
            volumeConfig.GetVolumeExplicitChannelProfiles(2).GetPoolKind());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(EChannelDataKind::Index),
            volumeConfig.GetVolumeExplicitChannelProfiles(2).GetDataKind());
    }

    void DoTestExplicitChannelAllocationSettings(const TStorageConfig& config)
    {
        TVolumeParams params{
            DefaultBlockSize,
            1024,
            1,
            {
                {config.GetHDDMergedChannelPoolKind(), EChannelDataKind::Merged},
                {config.GetHDDMergedChannelPoolKind(), EChannelDataKind::Merged},
            },
            0,
            0,
            0,
            0,
            NCloud::NProto::STORAGE_MEDIA_HDD
        };
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            6,
            volumeConfig.ExplicitChannelProfilesSize()
        );
        CHECK_CHANNEL(
            0,
            config.GetHDDSystemChannelPoolKind(),
            EChannelDataKind::System,
            128_MB
        );
        CHECK_CHANNEL(
            1,
            config.GetHDDLogChannelPoolKind(),
            EChannelDataKind::Log,
            128_MB
        );
        CHECK_CHANNEL(
            2,
            config.GetHDDIndexChannelPoolKind(),
            EChannelDataKind::Index,
            16_MB
        );
        CHECK_CHANNEL(
            3,
            config.GetHDDMergedChannelPoolKind(),
            EChannelDataKind::Merged,
            config.GetAllocationUnitHDD() * 1_GB
        );
        CHECK_CHANNEL(
            4,
            config.GetHDDMergedChannelPoolKind(),
            EChannelDataKind::Merged,
            config.GetAllocationUnitHDD() * 1_GB
        );
        CHECK_CHANNEL(
            5,
            config.GetHDDFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
        CheckVolumeChannels(config, volumeConfig);

        params.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
        params.DataChannels = {
            {config.GetSSDMergedChannelPoolKind(), EChannelDataKind::Merged},
            {config.GetSSDMergedChannelPoolKind(), EChannelDataKind::Merged},
        };
        volumeConfig.ClearExplicitChannelProfiles();
        volumeConfig.ClearVolumeExplicitChannelProfiles();
        ResizeVolume(config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            6,
            volumeConfig.ExplicitChannelProfilesSize()
        );
        CHECK_CHANNEL(
            0,
            config.GetSSDSystemChannelPoolKind(),
            EChannelDataKind::System,
            128_MB
        );
        CHECK_CHANNEL(
            1,
            config.GetSSDLogChannelPoolKind(),
            EChannelDataKind::Log,
            128_MB
        );
        CHECK_CHANNEL(
            2,
            config.GetSSDIndexChannelPoolKind(),
            EChannelDataKind::Index,
            16_MB
        );
        CHECK_CHANNEL(
            3,
            config.GetSSDMergedChannelPoolKind(),
            EChannelDataKind::Merged,
            config.GetAllocationUnitSSD() * 1_GB
        );
        CHECK_CHANNEL(
            4,
            config.GetSSDMergedChannelPoolKind(),
            EChannelDataKind::Merged,
            config.GetAllocationUnitSSD() * 1_GB
        );
        CHECK_CHANNEL(
            5,
            config.GetSSDFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
        CheckVolumeChannels(config, volumeConfig);

        params.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID;
        params.DataChannels = {
            {config.GetHybridMergedChannelPoolKind(), EChannelDataKind::Merged},
            {config.GetHybridMergedChannelPoolKind(), EChannelDataKind::Merged},
        };
        volumeConfig.ClearExplicitChannelProfiles();
        volumeConfig.ClearVolumeExplicitChannelProfiles();
        ResizeVolume(config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            6,
            volumeConfig.ExplicitChannelProfilesSize()
        );
        CHECK_CHANNEL(
            0,
            config.GetHybridSystemChannelPoolKind(),
            EChannelDataKind::System,
            128_MB
        );
        CHECK_CHANNEL(
            1,
            config.GetHybridLogChannelPoolKind(),
            EChannelDataKind::Log,
            128_MB
        );
        CHECK_CHANNEL(
            2,
            config.GetHybridIndexChannelPoolKind(),
            EChannelDataKind::Index,
            16_MB
        );
        CHECK_CHANNEL(
            3,
            config.GetHybridMergedChannelPoolKind(),
            EChannelDataKind::Merged,
            config.GetAllocationUnitHDD() * 1_GB
        );
        CHECK_CHANNEL(
            4,
            config.GetHybridMergedChannelPoolKind(),
            EChannelDataKind::Merged,
            config.GetAllocationUnitHDD() * 1_GB
        );
        CHECK_CHANNEL(
            5,
            config.GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
        CheckVolumeChannels(config, volumeConfig);

        params.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID;
        params.BlocksCountPerPartition = 100_GB / DefaultBlockSize;
        params.PartitionsCount = 1;
        volumeConfig.ClearExplicitChannelProfiles();
        volumeConfig.ClearVolumeExplicitChannelProfiles();
        ResizeVolume(config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            6,
            volumeConfig.ExplicitChannelProfilesSize()
        );
        CHECK_CHANNEL(
            0,
            config.GetHybridSystemChannelPoolKind(),
            EChannelDataKind::System,
            128_MB
        );
        CHECK_CHANNEL(
            1,
            config.GetHybridLogChannelPoolKind(),
            EChannelDataKind::Log,
            128_MB
        );
        CHECK_CHANNEL(
            2,
            config.GetHybridIndexChannelPoolKind(),
            EChannelDataKind::Index,
            100_GB / 1024
        );
        CHECK_CHANNEL(
            3,
            config.GetHybridMergedChannelPoolKind(),
            EChannelDataKind::Merged,
            config.GetAllocationUnitHDD() * 1_GB
        );
        CHECK_CHANNEL(
            4,
            config.GetHybridMergedChannelPoolKind(),
            EChannelDataKind::Merged,
            config.GetAllocationUnitHDD() * 1_GB
        );
        CHECK_CHANNEL(
            5,
            config.GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
        CheckVolumeChannels(config, volumeConfig);
    }

    Y_UNIT_TEST(ShouldSetChannelSettingsForExplicitChannelAllocation)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetMinChannelCount(1);
        storageServiceConfig.SetFreshChannelCount(1);
        auto config = CreateTestStorageConfig(std::move(storageServiceConfig));

        DoTestExplicitChannelAllocationSettings(*config);
    }

    Y_UNIT_TEST(ShouldGetSSDChannelsFromFeaturesConfig)
    {
        NCloud::NProto::TFeaturesConfig featuresConfig;
        NProto::TStorageServiceConfig storageServiceConfig;

        auto* feature = featuresConfig.AddFeatures();
        feature->SetName("SSDLogChannelPoolKind");
        feature->SetValue("ssdmirror");
        auto* wl = feature->MutableWhitelist();
        wl->AddFolderIds("folder_id");
        wl->AddCloudIds("cloud_id");

        auto config = CreateTestStorageConfig(
            std::move(storageServiceConfig), std::move(featuresConfig));

        NKikimrBlockStore::TVolumeConfig volumeConfig;
        volumeConfig.SetCloudId("cloud_id");
        volumeConfig.SetFolderId("folder_id");
        TVolumeParams params;
        params.BlockSize = DefaultBlockSize;
        params.BlocksCountPerPartition = 1;
        params.PartitionsCount = 1;
        params.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        //System PoolKind of channel should be from StorageConfig
        CHECK_CHANNEL(
            0,
            config->GetSSDSystemChannelPoolKind(),
            EChannelDataKind::System,
            128_MB
        );

        // Log PoolKind should be from FeaturesConfig
        CHECK_CHANNEL(
            1,
            "ssdmirror",
            EChannelDataKind::Log,
            128_MB
        );
    }

    Y_UNIT_TEST(ShouldAddMixedChannels)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocateSeparateMixedChannels(true);
        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed");
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged");
        storageServiceConfig.SetHybridFreshChannelPoolKind("fresh");
        storageServiceConfig.SetMinChannelCount(1);
        storageServiceConfig.SetFreshChannelCount(1);
        storageServiceConfig.SetAllocationUnitHDD(1);
        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 1_GB / DefaultBlockSize;
        TVolumeParams params{
            DefaultBlockSize,
            blocksCount,
            1,
            {
                {"merged", EChannelDataKind::Merged},
                {"merged", EChannelDataKind::Merged},
            },
            0,
            0,
            0,
            0,
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        };
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(7, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(
            6,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );

        volumeConfig.ClearExplicitChannelProfiles();

        params.DataChannels = {
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"mixed", EChannelDataKind::Mixed},
        };
        ResizeVolume(*config, params, {}, {}, volumeConfig);
        UNIT_ASSERT_VALUES_EQUAL(7, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(
            6,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );

        volumeConfig.ClearExplicitChannelProfiles();

        params.DataChannels = {
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"mixed", EChannelDataKind::Mixed},
            {"merged", EChannelDataKind::Merged},
        };
        ResizeVolume(*config, params, {}, {}, volumeConfig);
        UNIT_ASSERT_VALUES_EQUAL(9, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(6, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(7, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL(
            8,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );

        volumeConfig.ClearExplicitChannelProfiles();

        params.DataChannels = {
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"mixed", EChannelDataKind::Mixed},
        };
        params.BlocksCountPerPartition = 5 * blocksCount;
        params.PartitionsCount = 1;
        ResizeVolume(*config, params, {}, {}, volumeConfig);
        UNIT_ASSERT_VALUES_EQUAL(10, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(6, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(7, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(8, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL(
            9,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
    }

    Y_UNIT_TEST(ShouldAddFreshChannel)
    {
        TVolumeParams volumeParams;
        volumeParams.BlockSize = DefaultBlockSize;
        volumeParams.BlocksCountPerPartition = 32_GB / DefaultBlockSize;
        volumeParams.PartitionsCount = 1;
        volumeParams.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
        NKikimrBlockStore::TVolumeConfig volumeConfig;

        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetMinChannelCount(1);
            auto config = std::make_unique<TStorageConfig>(
                std::move(storageServiceConfig),
                std::make_shared<NFeatures::TFeaturesConfig>(
                    NCloud::NProto::TFeaturesConfig())
            );
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(4, volumeConfig.ExplicitChannelProfilesSize());
            CHECK_CHANNEL(
                3,
                config->GetSSDMergedChannelPoolKind(),
                EChannelDataKind::Merged,
                config->GetAllocationUnitSSD() * 1_GB
            );
        }
        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetMinChannelCount(1);
            storageServiceConfig.SetFreshChannelCount(1);
            auto config = std::make_unique<TStorageConfig>(
                std::move(storageServiceConfig),
                std::make_shared<NFeatures::TFeaturesConfig>(
                    NCloud::NProto::TFeaturesConfig())
            );
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(5, volumeConfig.ExplicitChannelProfilesSize());
            CHECK_CHANNEL(
                3,
                config->GetSSDMergedChannelPoolKind(),
                EChannelDataKind::Merged,
                config->GetAllocationUnitSSD() * 1_GB
            );
            CHECK_CHANNEL(
                4,
                config->GetSSDFreshChannelPoolKind(),
                EChannelDataKind::Fresh,
                128_MB
            );
        }
    }

    Y_UNIT_TEST(ShouldKeepFreshChannel)
    {
        TVolumeParams volumeParams;
        volumeParams.BlockSize = DefaultBlockSize;
        volumeParams.BlocksCountPerPartition = 32_GB / DefaultBlockSize;
        volumeParams.PartitionsCount = 1;
        volumeParams.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
        NKikimrBlockStore::TVolumeConfig volumeConfig;

        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetMinChannelCount(1);
            storageServiceConfig.SetFreshChannelCount(1);
            auto config = std::make_unique<TStorageConfig>(
                std::move(storageServiceConfig),
                std::make_shared<NFeatures::TFeaturesConfig>(
                    NCloud::NProto::TFeaturesConfig())
            );
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(5, volumeConfig.ExplicitChannelProfilesSize());
            CHECK_CHANNEL(
                3,
                config->GetSSDMergedChannelPoolKind(),
                EChannelDataKind::Merged,
                config->GetAllocationUnitSSD() * 1_GB
            );
            CHECK_CHANNEL(
                4,
                config->GetSSDFreshChannelPoolKind(),
                EChannelDataKind::Fresh,
                128_MB
            );
        }
        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetMinChannelCount(1);
            storageServiceConfig.SetFreshChannelCount(0);
            auto config = std::make_unique<TStorageConfig>(
                std::move(storageServiceConfig),
                std::make_shared<NFeatures::TFeaturesConfig>(
                    NCloud::NProto::TFeaturesConfig())
            );
            ResizeVolume(*config, volumeParams, {}, {}, volumeConfig);

            UNIT_ASSERT_VALUES_EQUAL(5, volumeConfig.ExplicitChannelProfilesSize());
            CHECK_CHANNEL(
                3,
                config->GetSSDMergedChannelPoolKind(),
                EChannelDataKind::Merged,
                config->GetAllocationUnitSSD() * 1_GB
            );
            CHECK_CHANNEL(
                4,
                config->GetSSDFreshChannelPoolKind(),
                EChannelDataKind::Fresh,
                128_MB
            );
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateMixedChannelCount)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocateSeparateMixedChannels(true);
        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed");
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged");
        storageServiceConfig.SetHybridFreshChannelPoolKind("fresh");
        storageServiceConfig.SetMinChannelCount(1);
        storageServiceConfig.SetFreshChannelCount(1);
        storageServiceConfig.SetAllocationUnitHDD(1);
        storageServiceConfig.SetHDDUnitWriteIops(10);
        storageServiceConfig.SetSSDUnitWriteIops(100);
        storageServiceConfig.SetHDDUnitWriteBandwidth(10);
        storageServiceConfig.SetSSDUnitWriteBandwidth(30);
        storageServiceConfig.SetThrottlingEnabled(true);

        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 4_GB / DefaultBlockSize;
        TVolumeParams params{
            DefaultBlockSize,
            blocksCount,
            1,
            {},
            0,
            0,
            0,
            0,
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        };
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(10, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(6, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(7, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(8, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(
            9,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
    }

    Y_UNIT_TEST(ShouldNotOverflowWhenUseSeparatedMixedChannelCount)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocateSeparateMixedChannels(true);
        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed");
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged");
        storageServiceConfig.SetHybridFreshChannelPoolKind("fresh");
        storageServiceConfig.SetMinChannelCount(1);
        storageServiceConfig.SetFreshChannelCount(1);
        storageServiceConfig.SetAllocationUnitHDD(1);
        storageServiceConfig.SetHDDUnitWriteIops(10);
        storageServiceConfig.SetSSDUnitWriteIops(100);
        storageServiceConfig.SetHDDUnitWriteBandwidth(10);
        storageServiceConfig.SetSSDUnitWriteBandwidth(30);
        storageServiceConfig.SetThrottlingEnabled(true);

        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 400_GB / DefaultBlockSize;
        TVolumeParams params{
            DefaultBlockSize,
            blocksCount,
            1,
            {},
            0,
            0,
            0,
            0,
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        };
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        auto countChannels = [&](EChannelDataKind kind)
        {
            return CountIf(
                volumeConfig.GetExplicitChannelProfiles(),
                [kind](const auto& channel)
                { return channel.GetDataKind() == static_cast<ui32>(kind); });
        };
        UNIT_ASSERT_VALUES_EQUAL(255, volumeConfig.ExplicitChannelProfilesSize());
        UNIT_ASSERT_VALUES_EQUAL(216, countChannels(EChannelDataKind::Merged));
        UNIT_ASSERT_VALUES_EQUAL(35, countChannels(EChannelDataKind::Mixed));
        UNIT_ASSERT_VALUES_EQUAL(1, countChannels(EChannelDataKind::Fresh));
    }

    Y_UNIT_TEST(ShouldNotDecreaseDataChannelCount)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocateSeparateMixedChannels(true);
        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed");
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged");
        storageServiceConfig.SetHybridFreshChannelPoolKind("fresh");
        storageServiceConfig.SetMinChannelCount(1);
        storageServiceConfig.SetFreshChannelCount(1);
        storageServiceConfig.SetAllocationUnitHDD(1);
        storageServiceConfig.SetHDDUnitWriteIops(10);
        storageServiceConfig.SetSSDUnitWriteIops(100);
        storageServiceConfig.SetHDDUnitWriteBandwidth(10);
        storageServiceConfig.SetSSDUnitWriteBandwidth(30);
        storageServiceConfig.SetThrottlingEnabled(true);

        auto config = std::make_unique<TStorageConfig>(
            storageServiceConfig,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 4_GB / DefaultBlockSize;
        TVolumeParams params{
            DefaultBlockSize,
            blocksCount,
            1,
            {},
            0,
            0,
            0,
            0,
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        };
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(10, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(6, "merged", EChannelDataKind::Merged);
        // 2 mixed channels added due to high write iops/bandwidth limits
        CHECK_CHANNEL_HDD(7, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(8, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(9, "fresh", EChannelDataKind::Fresh, 128_MB);

        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed2");
        config = std::make_unique<TStorageConfig>(
            storageServiceConfig,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        volumeConfig.Clear();
        NProto::TVolumePerformanceProfile pp;
        // setting low limits
        pp.SetMaxReadIops(1);
        pp.SetMaxWriteIops(1);
        pp.SetMaxReadBandwidth(1);
        pp.SetMaxWriteBandwidth(1);
        params.DataChannels = {
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"mixed", EChannelDataKind::Mixed},
            {"mixed", EChannelDataKind::Mixed},
            {"fresh", EChannelDataKind::Fresh},
        };
        ResizeVolume(*config, params, {}, pp, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(10, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(6, "merged", EChannelDataKind::Merged);
        // we still have 2 mixed channels - channel count decrease not allowed
        // but pool kind hasn't changed
        CHECK_CHANNEL_HDD(7, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(8, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(9, "fresh", EChannelDataKind::Fresh, 128_MB);

        storageServiceConfig.SetPoolKindChangeAllowed(true);
        config = std::make_unique<TStorageConfig>(
            storageServiceConfig,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        volumeConfig.Clear();
        ResizeVolume(*config, params, {}, pp, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(10, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(6, "merged", EChannelDataKind::Merged);
        // pool kind change was allowed => pool kind was changed to mixed2
        CHECK_CHANNEL_HDD(7, "mixed2", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(8, "mixed2", EChannelDataKind::Mixed);
        CHECK_CHANNEL(9, "fresh", EChannelDataKind::Fresh, 128_MB);

        // testing merged and fresh channels as well
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged2");
        storageServiceConfig.SetHybridFreshChannelPoolKind("fresh2");
        storageServiceConfig.SetPoolKindChangeAllowed(false);
        config = std::make_unique<TStorageConfig>(
            storageServiceConfig,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        volumeConfig.Clear();
        params.DataChannels = {
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"mixed2", EChannelDataKind::Mixed},
            {"mixed2", EChannelDataKind::Mixed},
            {"fresh", EChannelDataKind::Fresh},
        };
        ResizeVolume(*config, params, {}, pp, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(10, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(6, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(7, "mixed2", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(8, "mixed2", EChannelDataKind::Mixed);
        CHECK_CHANNEL(9, "fresh", EChannelDataKind::Fresh, 128_MB);

        storageServiceConfig.SetPoolKindChangeAllowed(true);
        config = std::make_unique<TStorageConfig>(
            storageServiceConfig,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        volumeConfig.Clear();
        ResizeVolume(*config, params, {}, pp, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(10, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged2", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged2", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "merged2", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(6, "merged2", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(7, "mixed2", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(8, "mixed2", EChannelDataKind::Mixed);
        CHECK_CHANNEL(9, "fresh2", EChannelDataKind::Fresh, 128_MB);
    }

    Y_UNIT_TEST(ShouldRespectResizeVolumeRequestFlags)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocateSeparateMixedChannels(true);
        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed");
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged");
        storageServiceConfig.SetMinChannelCount(1);
        storageServiceConfig.SetFreshChannelCount(1);
        storageServiceConfig.SetAllocationUnitHDD(1);
        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 1_GB / DefaultBlockSize;
        TVolumeParams params{
            DefaultBlockSize,
            blocksCount,
            1,
            {},
            0,
            0,
            0,
            0,
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        };
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        NProto::TResizeVolumeRequestFlags flags;
        flags.SetNoSeparateMixedChannelAllocation(true);
        ResizeVolume(*config, params, flags, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(5, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL(
            3,
            "merged",
            EChannelDataKind::Merged,
            config->GetAllocationUnitHDD() * 1_GB
        );
    }

    Y_UNIT_TEST(ShouldResizeMultipartitionVolume)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetBytesPerPartitionSSD(1_TB);
        storageServiceConfig.SetMultipartitionVolumesEnabled(true);
        storageServiceConfig.SetBytesPerStripe(16_MB);
        storageServiceConfig.SetMaxPartitionsPerVolume(2);
        storageServiceConfig.SetAllocationUnitSSD(32);
        storageServiceConfig.SetFreshChannelCount(1);
        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 2_TB / 4_KB;
        TVolumeParams volumeParams;
        volumeParams.BlockSize = 4_KB;
        volumeParams.BlocksCountPerPartition = blocksCount / 2;
        volumeParams.PartitionsCount = 2;
        volumeParams.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;

        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(
            *config,
            volumeParams,
            {},
            {},
            volumeConfig
        );

        UNIT_ASSERT_VALUES_EQUAL(2, volumeConfig.PartitionsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            volumeParams.BlocksCountPerPartition,
            volumeConfig.GetPartitions(0).GetBlockCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            volumeParams.BlocksCountPerPartition,
            volumeConfig.GetPartitions(1).GetBlockCount()
        );

        UNIT_ASSERT_VALUES_EQUAL(36, volumeConfig.ExplicitChannelProfilesSize());

        CHECK_CHANNEL(
            0,
            config->GetSSDSystemChannelPoolKind(),
            EChannelDataKind::System,
            128_MB
        );
        CHECK_CHANNEL(
            1,
            config->GetSSDLogChannelPoolKind(),
            EChannelDataKind::Log,
            128_MB
        );
        CHECK_CHANNEL(
            2,
            config->GetSSDIndexChannelPoolKind(),
            EChannelDataKind::Index,
            1_GB
        );
        for (ui32 i = 3; i < 35; ++i) {
            CHECK_CHANNEL(
                i,
                config->GetSSDMergedChannelPoolKind(),
                EChannelDataKind::Merged,
                config->GetAllocationUnitSSD() * 1_GB
            );
        }
        CHECK_CHANNEL(
            35,
            config->GetSSDFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
    }

    Y_UNIT_TEST(ShouldComputeBlocksCountPerPartition)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            ComputeBlocksCountPerPartition(2048, 32, 2)
        );

        UNIT_ASSERT_VALUES_EQUAL(
            1056,
            ComputeBlocksCountPerPartition(2049, 32, 2)
        );

        UNIT_ASSERT_VALUES_EQUAL(
            7777,
            ComputeBlocksCountPerPartition(7777, 0, 1)
        );

        UNIT_ASSERT_VALUES_EQUAL(
            7777,
            ComputeBlocksCountPerPartition(7777, 32, 1)
        );
    }

    Y_UNIT_TEST(ShouldComputePartitionsInfo)
    {
        {
            NProto::TStorageServiceConfig storageServiceConfig;
            auto config = std::make_unique<TStorageConfig>(
                std::move(storageServiceConfig),
                std::make_shared<NFeatures::TFeaturesConfig>(
                    NCloud::NProto::TFeaturesConfig())
            );

            auto info = ComputePartitionsInfo(
                *config,
                "cloud_id",
                "folder_id",
                "disk_id",
                NCloud::NProto::STORAGE_MEDIA_HYBRID,
                100500,
                33,
                false,
                false
            );

            UNIT_ASSERT_VALUES_EQUAL(1, info.PartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(100500, info.BlocksCountPerPartition);
        }

        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetBytesPerPartition(1_TB);
            storageServiceConfig.SetBytesPerPartitionSSD(512_GB);
            storageServiceConfig.SetMultipartitionVolumesEnabled(true);
            storageServiceConfig.SetBytesPerStripe(16_MB);
            storageServiceConfig.SetMaxPartitionsPerVolume(2);
            auto config = std::make_unique<TStorageConfig>(
                std::move(storageServiceConfig),
                std::make_shared<NFeatures::TFeaturesConfig>(
                    NCloud::NProto::TFeaturesConfig())
            );

            auto blocksCount = 1_TB / 4_KB - 1;
            auto info = ComputePartitionsInfo(
                *config,
                "cloud_id",
                "folder_id",
                "disk_id",
                NCloud::NProto::STORAGE_MEDIA_HYBRID,
                blocksCount,
                4_KB,
                false,
                false
            );

            UNIT_ASSERT_VALUES_EQUAL(1, info.PartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(blocksCount, info.BlocksCountPerPartition);

            blocksCount = 1_TB / 4_KB;
            info = ComputePartitionsInfo(
                *config,
                "cloud_id",
                "folder_id",
                "disk_id",
                NCloud::NProto::STORAGE_MEDIA_HYBRID,
                blocksCount,
                4_KB,
                false,
                false
            );

            UNIT_ASSERT_VALUES_EQUAL(1, info.PartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(blocksCount, info.BlocksCountPerPartition);

            blocksCount = 1_TB / 4_KB;
            info = ComputePartitionsInfo(
                *config,
                "cloud_id",
                "folder_id",
                "disk_id",
                NCloud::NProto::STORAGE_MEDIA_SSD,
                blocksCount,
                4_KB,
                false,
                false
            );

            UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(blocksCount / 2, info.BlocksCountPerPartition);

            blocksCount = 1_TB / 4_KB + 1;
            info = ComputePartitionsInfo(
                *config,
                "cloud_id",
                "folder_id",
                "disk_id",
                NCloud::NProto::STORAGE_MEDIA_HYBRID,
                blocksCount,
                4_KB,
                false,
                false
            );

            UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                1_TB / 4_KB / 2 + 16_MB / 4_KB,
                info.BlocksCountPerPartition
            );

            blocksCount = 4_TB / 4_KB;
            info = ComputePartitionsInfo(
                *config,
                "cloud_id",
                "folder_id",
                "disk_id",
                NCloud::NProto::STORAGE_MEDIA_HYBRID,
                blocksCount,
                4_KB,
                false,
                false
            );

            UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                2_TB / 4_KB,
                info.BlocksCountPerPartition
            );

            blocksCount = 1_TB / 64_KB;
            info = ComputePartitionsInfo(
                *config,
                "cloud_id",
                "folder_id",
                "disk_id",
                NCloud::NProto::STORAGE_MEDIA_HYBRID,
                blocksCount,
                64_KB,
                false,
                false
            );

            UNIT_ASSERT_VALUES_EQUAL(1, info.PartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                blocksCount,
                info.BlocksCountPerPartition
            );

            blocksCount = 1_TB / 64_KB + 1;
            info = ComputePartitionsInfo(
                *config,
                "cloud_id",
                "folder_id",
                "disk_id",
                NCloud::NProto::STORAGE_MEDIA_HYBRID,
                blocksCount,
                64_KB,
                false,
                false
            );

            UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                1_TB / 64_KB / 2 + 16_MB / 64_KB,
                info.BlocksCountPerPartition
            );
        }

        {
            const ui32 bytesPerStripe = 7'777'777;
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetBytesPerPartition(1_TB);
            storageServiceConfig.SetMultipartitionVolumesEnabled(true);
            storageServiceConfig.SetBytesPerStripe(bytesPerStripe);
            storageServiceConfig.SetMaxPartitionsPerVolume(4);
            auto config = std::make_unique<TStorageConfig>(
                std::move(storageServiceConfig),
                std::make_shared<NFeatures::TFeaturesConfig>(
                    NCloud::NProto::TFeaturesConfig())
            );

            auto blocksCount = 4_TB / 4_KB;
            auto info = ComputePartitionsInfo(
                *config,
                "cloud_id",
                "folder_id",
                "disk_id",
                NCloud::NProto::STORAGE_MEDIA_HYBRID,
                blocksCount,
                4_KB,
                false,
                false
            );

            /*
               565425 stripes
               1073742075 blocks needed
               141357 stripes per partition
               268436943 blocks per partition
            */

            UNIT_ASSERT_VALUES_EQUAL(4, info.PartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(268436943, info.BlocksCountPerPartition);
        }
    }

    Y_UNIT_TEST(ShouldComputePartitionsInfoForSystemAndOverlayDisks)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetBytesPerPartition(1_TB);
        storageServiceConfig.SetBytesPerPartitionSSD(512_GB);
        storageServiceConfig.SetMultipartitionVolumesEnabled(true);
        storageServiceConfig.SetBytesPerStripe(16_MB);
        storageServiceConfig.SetMaxPartitionsPerVolume(2);
        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        auto blocksCount = 1_TB / 4_KB;
        auto info = ComputePartitionsInfo(
            *config,
            "cloud_id",
            "folder_id",
            "disk_id",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            blocksCount,
            4_KB,
            false,  // isSystem
            false   // isOverlayDisk
        );

        UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionsCount);
        UNIT_ASSERT_VALUES_EQUAL(blocksCount / 2, info.BlocksCountPerPartition);

        info = ComputePartitionsInfo(
            *config,
            "cloud_id",
            "folder_id",
            "disk_id",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            blocksCount,
            4_KB,
            true,  // isSystem
            false  // isOverlayDisk
        );

        UNIT_ASSERT_VALUES_EQUAL(1, info.PartitionsCount);
        UNIT_ASSERT_VALUES_EQUAL(blocksCount, info.BlocksCountPerPartition);

        info = ComputePartitionsInfo(
            *config,
            "cloud_id",
            "folder_id",
            "disk_id",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            blocksCount,
            4_KB,
            false,  // isSystem
            true    // isOverlayDisk
        );

        UNIT_ASSERT_VALUES_EQUAL(1, info.PartitionsCount);
        UNIT_ASSERT_VALUES_EQUAL(blocksCount, info.BlocksCountPerPartition);
    }

    Y_UNIT_TEST(ShouldNotCapWhenEnoughChannels)
    {
        int wantAddMerged = 50;
        int wantAddMixed = 35;
        int wantAddFresh = 15;

        ComputeChannelCountLimits(
            100,
            &wantAddMerged,
            &wantAddMixed,
            &wantAddFresh);

        UNIT_ASSERT_VALUES_EQUAL(50, wantAddMerged);
        UNIT_ASSERT_VALUES_EQUAL(35, wantAddMixed);
        UNIT_ASSERT_VALUES_EQUAL(15, wantAddFresh);
    }

    Y_UNIT_TEST(ShouldCapAllWhenNoFreeChannels)
    {
        int wantAddMerged = 50;
        int wantAddMixed = 35;
        int wantAddFresh = 15;

        ComputeChannelCountLimits(
            0,
            &wantAddMerged,
            &wantAddMixed,
            &wantAddFresh);

        UNIT_ASSERT_VALUES_EQUAL(0, wantAddMerged);
        UNIT_ASSERT_VALUES_EQUAL(0, wantAddMixed);
        UNIT_ASSERT_VALUES_EQUAL(0, wantAddFresh);
    }

    Y_UNIT_TEST(ShouldCapLessAffectedChannel)
    {
        int wantAddMerged = 10;
        int wantAddMixed = 1;
        int wantAddFresh = 1;

        ComputeChannelCountLimits(
            2,
            &wantAddMerged,
            &wantAddMixed,
            &wantAddFresh);
        UNIT_ASSERT_VALUES_EQUAL(0, wantAddMerged);
        UNIT_ASSERT_VALUES_EQUAL(1, wantAddMixed);
        UNIT_ASSERT_VALUES_EQUAL(1, wantAddFresh);
    }

    Y_UNIT_TEST(ShouldDoFairCapping)
    {
        int wantAddMerged = 10;
        int wantAddMixed = 100;
        int wantAddFresh = 1;

        ComputeChannelCountLimits(
            101,
            &wantAddMerged,
            &wantAddMixed,
            &wantAddFresh);
        UNIT_ASSERT_VALUES_EQUAL(9, wantAddMerged);
        UNIT_ASSERT_VALUES_EQUAL(91, wantAddMixed);
        UNIT_ASSERT_VALUES_EQUAL(1, wantAddFresh);
    }

    Y_UNIT_TEST(ShouldGiveMoreToThoseWhoAskForMore)
    {
        int wantAddMerged = 20;
        int wantAddMixed = 2;
        int wantAddFresh = 1;

        ComputeChannelCountLimits(
            11,
            &wantAddMerged,
            &wantAddMixed,
            &wantAddFresh);

        UNIT_ASSERT_VALUES_EQUAL(9, wantAddMerged);
        UNIT_ASSERT_VALUES_EQUAL(1, wantAddMixed);
        UNIT_ASSERT_VALUES_EQUAL(1, wantAddFresh);
    }

    Y_UNIT_TEST(ShouldGiveAtLeastOneChannel)
    {
        int wantAddMerged = 2000;
        int wantAddMixed = 2;
        int wantAddFresh = 1;

        ComputeChannelCountLimits(
            5,
            &wantAddMerged,
            &wantAddMixed,
            &wantAddFresh);

        UNIT_ASSERT_VALUES_EQUAL(3, wantAddMerged);
        UNIT_ASSERT_VALUES_EQUAL(1, wantAddMixed);
        UNIT_ASSERT_VALUES_EQUAL(1, wantAddFresh);
    }

    Y_UNIT_TEST(ShouldGiveTheChannelWhereItIsMostNeeded)
    {
        int wantAddMerged = 2000;
        int wantAddMixed = 2000;
        int wantAddFresh = 1;

        ComputeChannelCountLimits(
            1,
            &wantAddMerged,
            &wantAddMixed,
            &wantAddFresh);
        UNIT_ASSERT_VALUES_EQUAL(0, wantAddMerged);
        UNIT_ASSERT_VALUES_EQUAL(0, wantAddMixed);
        UNIT_ASSERT_VALUES_EQUAL(1, wantAddFresh);
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateMixedChannelCountWithMixedPercentage20)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocateSeparateMixedChannels(true);
        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed");
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged");
        storageServiceConfig.SetHybridFreshChannelPoolKind("fresh");
        storageServiceConfig.SetMinChannelCount(4);
        storageServiceConfig.SetFreshChannelCount(1);
        storageServiceConfig.SetAllocationUnitHDD(256);
        // in case MixedChannelsPercentageFromMerged == 0 we will receive 2 mixed channels
        storageServiceConfig.SetMixedChannelsPercentageFromMerged(20);

        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 16_GB / DefaultBlockSize;
        TVolumeParams params{
            .BlockSize = DefaultBlockSize,
            .BlocksCountPerPartition = blocksCount,
            .PartitionsCount = 1,
            .MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID
        };
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        volumeConfig.SetPerformanceProfileMaxWriteIops(300);
        volumeConfig.SetPerformanceProfileMaxWriteBandwidth(31457280);
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(9, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(6, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(7, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(
            8,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
    }

    Y_UNIT_TEST(
        ShouldCorrectlyCalculateMixedChannelCountForBigDiskWithMixedPercentage20)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocateSeparateMixedChannels(true);
        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed");
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged");
        storageServiceConfig.SetHybridFreshChannelPoolKind("fresh");
        storageServiceConfig.SetMinChannelCount(4);
        storageServiceConfig.SetFreshChannelCount(1);
        storageServiceConfig.SetAllocationUnitHDD(256);
        storageServiceConfig.SetMixedChannelsPercentageFromMerged(20);

        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 100_TB / DefaultBlockSize;
        TVolumeParams params{
            .BlockSize = DefaultBlockSize,
            .BlocksCountPerPartition = blocksCount,
            .PartitionsCount = 1,
            .MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID
        };
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        volumeConfig.SetPerformanceProfileMaxWriteIops(300);
        volumeConfig.SetPerformanceProfileMaxWriteBandwidth(31457280);
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(255, volumeConfig.ExplicitChannelProfilesSize());
        for (int i = 3; i < 212; ++i) {
            CHECK_CHANNEL_HDD(i, "merged", EChannelDataKind::Merged);
        }
        for (int i = 212; i < 254; ++i) {
            CHECK_CHANNEL_HDD(i, "mixed", EChannelDataKind::Mixed);
        }
        CHECK_CHANNEL(
            254,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
    }

    Y_UNIT_TEST(
        ShouldCorrectlyRecalculateMixedChannelCountForBigDiskWithMixedPercentage20)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocateSeparateMixedChannels(true);
        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed");
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged");
        storageServiceConfig.SetHybridFreshChannelPoolKind("fresh");
        storageServiceConfig.SetMinChannelCount(4);
        storageServiceConfig.SetFreshChannelCount(1);
        storageServiceConfig.SetAllocationUnitHDD(256);
        storageServiceConfig.SetMixedChannelsPercentageFromMerged(20);

        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 100_TB / DefaultBlockSize;
        TVolumeParams params{
            .BlockSize = DefaultBlockSize,
            .BlocksCountPerPartition = blocksCount,
            .PartitionsCount = 1,
            .MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID
        };

        for (int i = 0; i < 100; ++i) {
            params.DataChannels.emplace_back("merged", EChannelDataKind::Merged);
        }
        for (int i = 0; i < 20; ++i) {
            params.DataChannels.emplace_back("mixed", EChannelDataKind::Mixed);
        }

        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(MaxChannelCount, volumeConfig.ExplicitChannelProfilesSize());
        for (int i = 3; i < 103; ++i) {
            CHECK_CHANNEL_HDD(i, "merged", EChannelDataKind::Merged);
        }
        for (ui32 i = 103; i < 123; ++i) {
            CHECK_CHANNEL_HDD(i, "mixed", EChannelDataKind::Mixed);
        }

        for (ui32 i = 123; i < 232; ++i) {
            CHECK_CHANNEL_HDD(i, "merged", EChannelDataKind::Merged);
        }
        for (ui32 i = 232; i < MaxChannelCount - 1; ++i) {
            CHECK_CHANNEL_HDD(i, "mixed", EChannelDataKind::Mixed);
        }
        CHECK_CHANNEL(
            MaxChannelCount - 1,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );

        volumeConfig.ClearExplicitChannelProfiles();
        params.DataChannels.clear();
        for (ui32 i = 0; i < MaxMergedChannelCount; ++i) {
            params.DataChannels.emplace_back("merged", EChannelDataKind::Merged);
        }
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(MaxChannelCount, volumeConfig.ExplicitChannelProfilesSize());
        for (ui32 i = 0; i < MaxMergedChannelCount; ++i) {
            CHECK_CHANNEL_HDD(i + 3, "merged", EChannelDataKind::Merged);
        }
        for (ui32 i = MaxMergedChannelCount; i < MaxDataChannelCount - 1; ++i) {
            CHECK_CHANNEL_HDD(i + 3, "mixed", EChannelDataKind::Mixed);
        }
        CHECK_CHANNEL(
            MaxChannelCount - 1,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
    }

    Y_UNIT_TEST(ShouldAddMixedChannelsWithMixedPercentage50)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocateSeparateMixedChannels(true);
        storageServiceConfig.SetHybridMixedChannelPoolKind("mixed");
        storageServiceConfig.SetHybridMergedChannelPoolKind("merged");
        storageServiceConfig.SetHybridFreshChannelPoolKind("fresh");
        storageServiceConfig.SetMinChannelCount(1);
        storageServiceConfig.SetFreshChannelCount(1);
        storageServiceConfig.SetAllocationUnitHDD(1);
        storageServiceConfig.SetMixedChannelsPercentageFromMerged(50);

        auto config = std::make_unique<TStorageConfig>(
            std::move(storageServiceConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto blocksCount = 1_GB / DefaultBlockSize;
        TVolumeParams params{
            DefaultBlockSize,
            blocksCount,
            1,
            {
                {"merged", EChannelDataKind::Merged},
                {"merged", EChannelDataKind::Merged},
            },
            0,
            0,
            0,
            0,
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        };
        NKikimrBlockStore::TVolumeConfig volumeConfig;
        ResizeVolume(*config, params, {}, {}, volumeConfig);

        UNIT_ASSERT_VALUES_EQUAL(7, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(
            6,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );

        volumeConfig.ClearExplicitChannelProfiles();

        params.DataChannels = {
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"mixed", EChannelDataKind::Mixed},
        };
        ResizeVolume(*config, params, {}, {}, volumeConfig);
        UNIT_ASSERT_VALUES_EQUAL(7, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(
            6,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );

        volumeConfig.ClearExplicitChannelProfiles();

        params.DataChannels = {
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"mixed", EChannelDataKind::Mixed},
            {"merged", EChannelDataKind::Merged},
        };
        ResizeVolume(*config, params, {}, {}, volumeConfig);
        UNIT_ASSERT_VALUES_EQUAL(10, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(6, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(7, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(8, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(
            9,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );

        volumeConfig.ClearExplicitChannelProfiles();

        params.DataChannels = {
            {"merged", EChannelDataKind::Merged},
            {"merged", EChannelDataKind::Merged},
            {"mixed", EChannelDataKind::Mixed},
        };
        params.BlocksCountPerPartition = 5 * blocksCount;
        params.PartitionsCount = 1;
        ResizeVolume(*config, params, {}, {}, volumeConfig);
        UNIT_ASSERT_VALUES_EQUAL(12, volumeConfig.ExplicitChannelProfilesSize());
        CHECK_CHANNEL_HDD(3, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(4, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(5, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(6, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(7, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(8, "merged", EChannelDataKind::Merged);
        CHECK_CHANNEL_HDD(9, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL_HDD(10, "mixed", EChannelDataKind::Mixed);
        CHECK_CHANNEL(
            11,
            config->GetHybridFreshChannelPoolKind(),
            EChannelDataKind::Fresh,
            128_MB
        );
    }

    #undef CHECK_CHANNEL
    #undef CHECK_CHANNEL_HDD
}

}   // namespace NCloud::NBlockStore::NStorage
