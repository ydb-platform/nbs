#include "model.h"

#include <cloud/filestore/config/storage.pb.h>
#include <cloud/filestore/libs/storage/model/channel_data_kind.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>

#include <ydb/core/protos/filestore_config.pb.h>

#include <array>
#include <functional>
#include <random>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void SetupFileStorePerformanceAndChannels(
    bool allocateMixed0Channel,
    const TStorageConfig& config,
    NKikimrFileStore::TConfig& fileStore)
{
    SetupFileStorePerformanceAndChannels(
        allocateMixed0Channel,
        config,
        fileStore,
        {}  // clientPerformanceProfile
    );
}

////////////////////////////////////////////////////////////////////////////////

struct TConfigs
    : public NUnitTest::TBaseFixture
{
    const ui64 DefaultBlockSize = 4_KB;
    const ui64 DefaultBlocksCount = 2_GB / DefaultBlockSize;
    const ui32 DefaultStorageMediaKind =
        static_cast<ui32>(::NCloud::NProto::STORAGE_MEDIA_SSD);

    const ui32 DefaultMinChannelCount = 2;
    const ui32 KikimrChannelsCount = 3;

    NProto::TFileStorePerformanceProfile ClientPerformanceProfile;
    NProto::TStorageConfig StorageConfig;
    NKikimrFileStore::TConfig KikimrConfig;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        ClientPerformanceProfile.SetThrottlingEnabled(true);
        ClientPerformanceProfile.SetMaxReadIops(1);
        ClientPerformanceProfile.SetMaxReadBandwidth(2);
        ClientPerformanceProfile.SetMaxWriteIops(3);
        ClientPerformanceProfile.SetMaxWriteBandwidth(4);
        ClientPerformanceProfile.SetBoostTime(5);
        ClientPerformanceProfile.SetBoostRefillTime(6);
        ClientPerformanceProfile.SetBoostPercentage(7);
        ClientPerformanceProfile.SetBurstPercentage(8);
        ClientPerformanceProfile.SetDefaultPostponedRequestWeight(9);
        ClientPerformanceProfile.SetMaxPostponedWeight(10);
        ClientPerformanceProfile.SetMaxWriteCostMultiplier(11);
        ClientPerformanceProfile.SetMaxPostponedTime(12);
        ClientPerformanceProfile.SetMaxPostponedCount(13);

        // Unit count == 2.
        // Size = 2_GB.
        // Storage type = SSD.
        KikimrConfig.SetBlockSize(DefaultBlockSize);
        KikimrConfig.SetBlocksCount(DefaultBlocksCount);
        KikimrConfig.SetStorageMediaKind(DefaultStorageMediaKind);

        // MixedChannelCount = 3.
        StorageConfig.SetMinChannelCount(DefaultMinChannelCount);
        for (size_t i = 0; i < KikimrChannelsCount; ++i) {
            auto pp = KikimrConfig.AddExplicitChannelProfiles();
            pp->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        }
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TModel)
{
    Y_UNIT_TEST_F(ShouldCorrectlyOverrideStorageMediaKind, TConfigs)
    {
#define DO_TEST(srcKind, overrideKind, targetKind)                             \
    KikimrConfig.SetStorageMediaKind(srcKind);                                 \
    StorageConfig.SetHDDMediaKindOverride(overrideKind);                       \
                                                                               \
    SetupFileStorePerformanceAndChannels(false, StorageConfig, KikimrConfig);  \
                                                                               \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        static_cast<ui32>(targetKind),                                         \
        KikimrConfig.GetStorageMediaKind());                                   \
// DO_TEST

        using namespace ::NCloud::NProto;
        DO_TEST(
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_DEFAULT);
        DO_TEST(
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_DEFAULT);
        DO_TEST(
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_DEFAULT);
        DO_TEST(
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_DEFAULT);
        DO_TEST(
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_DEFAULT);
        DO_TEST(
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_DEFAULT);
        DO_TEST(
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_DEFAULT);
        DO_TEST(
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_DEFAULT);

        DO_TEST(
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD);
        DO_TEST(
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD);
        DO_TEST(
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD);
        DO_TEST(
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD);
        DO_TEST(
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD);
        DO_TEST(
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD);
        DO_TEST(
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD);
        DO_TEST(
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD);

        DO_TEST(
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_HYBRID);
        DO_TEST(
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_HYBRID);
        DO_TEST(
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_HYBRID);
        DO_TEST(
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_HYBRID);
        DO_TEST(
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_HYBRID);
        DO_TEST(
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_HYBRID);
        DO_TEST(
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_HYBRID);
        DO_TEST(
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_HYBRID);

        DO_TEST(
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_HDD);
        DO_TEST(
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD);
        DO_TEST(
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_HYBRID);
        DO_TEST(
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_HDD);
        DO_TEST(
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_HDD);
        DO_TEST(
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_HDD);
        DO_TEST(
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_HDD);
        DO_TEST(
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_HDD);

        DO_TEST(
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD_NONREPLICATED);
        DO_TEST(
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD_NONREPLICATED);
        DO_TEST(
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD_NONREPLICATED);
        DO_TEST(
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD_NONREPLICATED);
        DO_TEST(
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD_NONREPLICATED);
        DO_TEST(
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD_NONREPLICATED);
        DO_TEST(
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD_NONREPLICATED);
        DO_TEST(
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD_NONREPLICATED);

        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD_MIRROR2);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD_MIRROR2);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD_MIRROR2);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD_MIRROR2);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD_MIRROR2);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD_MIRROR2);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD_MIRROR2);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD_MIRROR2);

        DO_TEST(
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD_LOCAL);
        DO_TEST(
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD_LOCAL);
        DO_TEST(
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD_LOCAL);
        DO_TEST(
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD_LOCAL);
        DO_TEST(
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD_LOCAL);
        DO_TEST(
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD_LOCAL);
        DO_TEST(
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD_LOCAL);
        DO_TEST(
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD_LOCAL);

        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_DEFAULT,
            STORAGE_MEDIA_SSD_MIRROR3);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD,
            STORAGE_MEDIA_SSD_MIRROR3);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_HYBRID,
            STORAGE_MEDIA_SSD_MIRROR3);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_HDD,
            STORAGE_MEDIA_SSD_MIRROR3);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD_NONREPLICATED,
            STORAGE_MEDIA_SSD_MIRROR3);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD_MIRROR2,
            STORAGE_MEDIA_SSD_MIRROR3);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD_LOCAL,
            STORAGE_MEDIA_SSD_MIRROR3);
        DO_TEST(
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD_MIRROR3,
            STORAGE_MEDIA_SSD_MIRROR3);

#undef DO_TEST
    }

    Y_UNIT_TEST_F(ShouldSetupNodesCount, TConfigs)
    {
#define DO_TEST(blksCount, blkSize, sizeToNodesRatio, nodesLimit, targetCount) \
    KikimrConfig.SetBlocksCount(blksCount);                                    \
    KikimrConfig.SetBlockSize(blkSize);                                        \
    StorageConfig.SetSizeToNodesRatio(sizeToNodesRatio);                       \
    StorageConfig.SetDefaultNodesLimit(nodesLimit);                            \
                                                                               \
    SetupFileStorePerformanceAndChannels(false, StorageConfig, KikimrConfig);  \
                                                                               \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        static_cast<ui32>(targetCount),                                        \
        KikimrConfig.GetNodesCount());                                         \
// DO_TEST

        // (1024 * 4_KB) / 2048 = 2048 > 1.
        DO_TEST(1024, 4_KB, 2048, 1, 2048);

        // (1024 * 4_KB) / 2048 = 2048 < 4096.
        DO_TEST(1024, 4_KB, 2048, 4096, 4096);

        // Check overflow.
        DO_TEST(
            static_cast<ui64>(2) * std::numeric_limits<ui32>::max(),
            64_KB,
            64_KB,
            64_KB,
            std::numeric_limits<ui32>::max());

#undef DO_TEST
    }

#define CHECK_CHANNEL(id, pKind, dKind, size, rIops, rBand, wIops, wBand)      \
    {                                                                          \
        const auto profile = kikimrConfig.GetExplicitChannelProfiles(id);      \
        UNIT_ASSERT_VALUES_EQUAL(pKind, profile.GetPoolKind());                \
        UNIT_ASSERT_VALUES_EQUAL(dKind, profile.GetDataKind());                \
        UNIT_ASSERT_VALUES_EQUAL(size, profile.GetSize());                     \
        UNIT_ASSERT_VALUES_EQUAL(rIops, profile.GetReadIops());                \
        UNIT_ASSERT_VALUES_EQUAL(rBand, profile.GetReadBandwidth());           \
        UNIT_ASSERT_VALUES_EQUAL(wIops, profile.GetWriteIops());               \
        UNIT_ASSERT_VALUES_EQUAL(wBand, profile.GetWriteBandwidth());          \
    }                                                                          \
// CHECK_CHANNEL

    struct TChannelState final
    {
        ui32 DataType;
        TString PoolType;
        ui64 Size;
        ui32 ReadIops;
        ui64 ReadBandwidth;
        ui32 WriteIops;
        ui64 WriteBandwidth;
    };

    void TestChannels(
        ui32 storageType,
        ui32 allocationUnitHDD,
        ui32 allocationUnitSSD,
        ui32 minChannelsCount,
        bool allocateMixed0,
        ui32 channelsCount,
        TVector<TChannelState> channels,
        NKikimrFileStore::TConfig& kikimrConfig,
        NProto::TStorageConfig& storageConfig)
    {
        kikimrConfig.SetStorageMediaKind(storageType);

        // Disable media type override.
        storageConfig.SetHDDMediaKindOverride(5);
        storageConfig.SetAllocationUnitHDD(allocationUnitHDD);
        storageConfig.SetAllocationUnitSSD(allocationUnitSSD);

        storageConfig.SetMinChannelCount(minChannelsCount);
        for (size_t i = 0; i < channelsCount; ++i) {
            auto pp = kikimrConfig.AddExplicitChannelProfiles();
            pp->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        }

        SetupFileStorePerformanceAndChannels(
            allocateMixed0,
            storageConfig,
            kikimrConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            channels.size(),
            kikimrConfig.GetExplicitChannelProfiles().size());
        for (ui32 i = 0; i < channels.size(); ++i) {
            CHECK_CHANNEL(
                i,
                channels[i].PoolType,
                channels[i].DataType,
                channels[i].Size,
                channels[i].ReadIops,
                channels[i].ReadBandwidth,
                channels[i].WriteIops,
                channels[i].WriteBandwidth);
        }
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHDDMinGreater, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "rot",
                .Size = 16_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            }
        };
        for (size_t i = 3; i < 7; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 100,
                    .ReadBandwidth = 31'457'280,
                    .WriteIops = 300,
                    .WriteBandwidth = 31'457'280,
                });
        }
        TestChannels(
            STORAGE_MEDIA_HDD,
            4,
            2,
            4,
            false,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHDDMinLower, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "rot",
                .Size = 16_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            }
        };
        for (size_t i = 3; i < 6; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 100,
                    .ReadBandwidth = 31'457'280,
                    .WriteIops = 300,
                    .WriteBandwidth = 31'457'280,
                });
        }
        TestChannels(
            STORAGE_MEDIA_HDD,
            4,
            2,
            1,
            false,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHDDEnormousSize, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "rot",
                .Size = 16_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            }
        };
        for (size_t i = 3; i < 19; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 300,
                    .ReadBandwidth = 251'658'240,
                    .WriteIops = 4'800,
                    .WriteBandwidth = 251'658'240,
               });
        }

        KikimrConfig.SetBlocksCount(1'000'000);
        KikimrConfig.SetBlockSize(64_KB);
        TestChannels(
            STORAGE_MEDIA_HDD,
            4,
            2,
            1,
            false,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsSSDMinGreater, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            }
        };
        for (size_t i = 3; i < 7; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "ssd",
                    .Size = 2_GB,
                    .ReadIops = 400,
                    .ReadBandwidth = 15'728'640,
                    .WriteIops = 1'000,
                    .WriteBandwidth = 15'728'640,
                });
        }
        TestChannels(
            STORAGE_MEDIA_SSD,
            4,
            2,
            4,
            false,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsSSDMinLower, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            }
        };
        for (size_t i = 3; i < 6; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "ssd",
                    .Size = 2_GB,
                    .ReadIops = 400,
                    .ReadBandwidth = 15'728'640,
                    .WriteIops = 1'000,
                    .WriteBandwidth = 15'728'640,
                });
        }
        TestChannels(
            STORAGE_MEDIA_SSD,
            4,
            2,
            1,
            false,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsSSDEnormousSize, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 12'000,
                .ReadBandwidth = 471'859'200,
                .WriteIops = 31'000,
                .WriteBandwidth = 471'859'200,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 12'000,
                .ReadBandwidth = 471'859'200,
                .WriteIops = 31'000,
                .WriteBandwidth = 471'859'200,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 12'000,
                .ReadBandwidth = 471'859'200,
                .WriteIops = 31'000,
                .WriteBandwidth = 471'859'200,
            }
        };
        for (size_t i = 3; i < 34; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "ssd",
                    .Size = 2_GB,
                    .ReadIops = 12'000,
                    .ReadBandwidth = 471'859'200,
                    .WriteIops = 31'000,
                    .WriteBandwidth = 471'859'200,
               });
        }

        KikimrConfig.SetBlocksCount(1'000'000);
        KikimrConfig.SetBlockSize(64_KB);
        TestChannels(
            STORAGE_MEDIA_SSD,
            4,
            2,
            1,
            false,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHybridMinGreater, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            }
        };
        for (size_t i = 3; i < 7; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 100,
                    .ReadBandwidth = 31'457'280,
                    .WriteIops = 300,
                    .WriteBandwidth = 31'457'280,
                });
        }
        TestChannels(
            STORAGE_MEDIA_HYBRID,
            4,
            2,
            4,
            false,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHybridMinLower, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            }
        };
        for (size_t i = 3; i < 6; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 100,
                    .ReadBandwidth = 31'457'280,
                    .WriteIops = 300,
                    .WriteBandwidth = 31'457'280,
                });
        }
        TestChannels(
            STORAGE_MEDIA_HYBRID,
            4,
            2,
            1,
            false,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHybridEnormousSize, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            }
        };
        for (size_t i = 3; i < 19; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 300,
                    .ReadBandwidth = 251'658'240,
                    .WriteIops = 4'800,
                    .WriteBandwidth = 251'658'240,
               });
        }

        KikimrConfig.SetBlocksCount(1'000'000);
        KikimrConfig.SetBlockSize(64_KB);
        TestChannels(
            STORAGE_MEDIA_HYBRID,
            4,
            2,
            1,
            false,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHDDMinGreaterDefault, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "rot",
                .Size = 16_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Mixed0),
                .PoolType = "rot",
                .Size = 4_GB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            }
        };
        for (size_t i = 4; i < 8; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 100,
                    .ReadBandwidth = 31'457'280,
                    .WriteIops = 300,
                    .WriteBandwidth = 31'457'280,
                });
        }
        TestChannels(
            STORAGE_MEDIA_HDD,
            4,
            2,
            4,
            true,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHDDMinLowerDefault, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "rot",
                .Size = 16_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Mixed0),
                .PoolType = "rot",
                .Size = 4_GB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            }
        };
        for (size_t i = 4; i < 7; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 100,
                    .ReadBandwidth = 31'457'280,
                    .WriteIops = 300,
                    .WriteBandwidth = 31'457'280,
                });
        }
        TestChannels(
            STORAGE_MEDIA_HDD,
            4,
            2,
            1,
            true,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHDDEnormousSizeDefault, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "rot",
                .Size = 16_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "rot",
                .Size = 128_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Mixed0),
                .PoolType = "rot",
                .Size = 4_GB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            }
        };
        for (size_t i = 4; i < 20; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 300,
                    .ReadBandwidth = 251'658'240,
                    .WriteIops = 4'800,
                    .WriteBandwidth = 251'658'240,
               });
        }

        KikimrConfig.SetBlocksCount(1'000'000);
        KikimrConfig.SetBlockSize(64_KB);
        TestChannels(
            STORAGE_MEDIA_HDD,
            4,
            2,
            1,
            true,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsSSDMinGreaterDefault, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Mixed0),
                .PoolType = "ssd",
                .Size = 2_GB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            }
        };
        for (size_t i = 4; i < 8; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "ssd",
                    .Size = 2_GB,
                    .ReadIops = 400,
                    .ReadBandwidth = 15'728'640,
                    .WriteIops = 1'000,
                    .WriteBandwidth = 15'728'640,
                });
        }
        TestChannels(
            STORAGE_MEDIA_SSD,
            4,
            2,
            4,
            true,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsSSDMinLowerDefault, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Mixed0),
                .PoolType = "ssd",
                .Size = 2_GB,
                .ReadIops = 400,
                .ReadBandwidth = 15'728'640,
                .WriteIops = 1'000,
                .WriteBandwidth = 15'728'640,
            }
        };
        for (size_t i = 3; i < 6; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "ssd",
                    .Size = 2_GB,
                    .ReadIops = 400,
                    .ReadBandwidth = 15'728'640,
                    .WriteIops = 1'000,
                    .WriteBandwidth = 15'728'640,
                });
        }
        TestChannels(
            STORAGE_MEDIA_SSD,
            4,
            2,
            1,
            true,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsSSDEnormousSizeDefault, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 12'000,
                .ReadBandwidth = 471'859'200,
                .WriteIops = 31'000,
                .WriteBandwidth = 471'859'200,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 12'000,
                .ReadBandwidth = 471'859'200,
                .WriteIops = 31'000,
                .WriteBandwidth = 471'859'200,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 12'000,
                .ReadBandwidth = 471'859'200,
                .WriteIops = 31'000,
                .WriteBandwidth = 471'859'200,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Mixed0),
                .PoolType = "ssd",
                .Size = 2_GB,
                .ReadIops = 12'000,
                .ReadBandwidth = 471'859'200,
                .WriteIops = 31'000,
                .WriteBandwidth = 471'859'200,
            }
        };
        for (size_t i = 3; i < 34; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "ssd",
                    .Size = 2_GB,
                    .ReadIops = 12'000,
                    .ReadBandwidth = 471'859'200,
                    .WriteIops = 31'000,
                    .WriteBandwidth = 471'859'200,
               });
        }

        KikimrConfig.SetBlocksCount(1'000'000);
        KikimrConfig.SetBlockSize(64_KB);
        TestChannels(
            STORAGE_MEDIA_SSD,
            4,
            2,
            1,
            true,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHybridMinGreaterDefault, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Mixed0),
                .PoolType = "rot",
                .Size = 4_GB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            }
        };
        for (size_t i = 4; i < 8; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 100,
                    .ReadBandwidth = 31'457'280,
                    .WriteIops = 300,
                    .WriteBandwidth = 31'457'280,
                });
        }
        TestChannels(
            STORAGE_MEDIA_HYBRID,
            4,
            2,
            4,
            true,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHybridMinLowerDefault, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Mixed0),
                .PoolType = "rot",
                .Size = 4_GB,
                .ReadIops = 100,
                .ReadBandwidth = 31'457'280,
                .WriteIops = 300,
                .WriteBandwidth = 31'457'280,
            }
        };
        for (size_t i = 4; i < 7; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 100,
                    .ReadBandwidth = 31'457'280,
                    .WriteIops = 300,
                    .WriteBandwidth = 31'457'280,
                });
        }
        TestChannels(
            STORAGE_MEDIA_HYBRID,
            4,
            2,
            1,
            true,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

    Y_UNIT_TEST_F(ShouldCorrectlySetupChannelsHybridEnormousSizeDefault, TConfigs)
    {
        using namespace ::NCloud::NProto;
        TVector<TChannelState> channels = {
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::System),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Index),
                .PoolType = "ssd",
                .Size = 16_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Fresh),
                .PoolType = "ssd",
                .Size = 128_MB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            },
            TChannelState{
                .DataType = static_cast<ui32>(EChannelDataKind::Mixed0),
                .PoolType = "rot",
                .Size = 4_GB,
                .ReadIops = 300,
                .ReadBandwidth = 251'658'240,
                .WriteIops = 4'800,
                .WriteBandwidth = 251'658'240,
            }
        };
        for (size_t i = 4; i < 20; ++i) {
            channels.push_back(
                TChannelState{
                    .DataType = static_cast<ui32>(EChannelDataKind::Mixed),
                    .PoolType = "rot",
                    .Size = 4_GB,
                    .ReadIops = 300,
                    .ReadBandwidth = 251'658'240,
                    .WriteIops = 4'800,
                    .WriteBandwidth = 251'658'240,
               });
        }

        KikimrConfig.SetBlocksCount(1'000'000);
        KikimrConfig.SetBlockSize(64_KB);
        TestChannels(
            STORAGE_MEDIA_HYBRID,
            4,
            2,
            1,
            true,
            3,
            channels,
            KikimrConfig,
            StorageConfig);
    }

#undef CHECK_CHANNEL

    void SetupPerformanceProfile(
        ui32 storageType,
        ui32 allocationUnits,
        ui32 unitReadIops,
        ui32 unitReadBandwidth,
        ui32 unitWriteIops,
        ui32 unitWriteBandwidth,
        ui32 maxReadIops,
        ui32 maxReadBandwidth,
        ui32 maxWriteIops,
        ui32 maxWriteBandwidth,
        ui32 boostTimeMs,
        ui32 boostRefillTimeMs,
        ui32 unitBoost,
        ui32 burstPercentage,
        ui32 defaultPostponedRequestWeight,
        ui32 maxPostponedWeight,
        ui32 maxWriteCostMultiplier,
        ui32 maxPostponedTimeMs,
        ui32 maxPostponedCount,
        NKikimrFileStore::TConfig& kikimrConfig,
        NProto::TStorageConfig& storageConfig,
        const TMaybe<NProto::TFileStorePerformanceProfile>& performanceProfile)
    {
        std::random_device rd;
        std::array<int, std::mt19937::state_size> state;
        std::generate(std::begin(state), std::end(state), std::ref(rd));
        std::seed_seq sq(std::begin(state), std::end(state));
        std::mt19937 engine(sq);

        kikimrConfig.SetStorageMediaKind(storageType);

        std::uniform_int_distribution<ui32> dist(1, 100'000'000);

        // SSD throttler disabled.
        storageConfig.SetAllocationUnitSSD(dist(engine));
        storageConfig.SetSSDUnitReadIops(dist(engine));
        storageConfig.SetSSDUnitReadBandwidth(dist(engine));
        storageConfig.SetSSDUnitWriteIops(dist(engine));
        storageConfig.SetSSDUnitWriteBandwidth(dist(engine));
        storageConfig.SetSSDMaxReadIops(dist(engine));
        storageConfig.SetSSDMaxReadBandwidth(dist(engine));
        storageConfig.SetSSDMaxWriteIops(dist(engine));
        storageConfig.SetSSDMaxWriteBandwidth(dist(engine));
        storageConfig.SetSSDBoostTime(dist(engine));
        storageConfig.SetSSDBoostRefillTime(dist(engine));
        storageConfig.SetSSDUnitBoost(dist(engine));
        storageConfig.SetSSDBurstPercentage(dist(engine));
        storageConfig.SetSSDDefaultPostponedRequestWeight(dist(engine));
        storageConfig.SetSSDMaxPostponedWeight(dist(engine));
        storageConfig.SetSSDMaxWriteCostMultiplier(dist(engine));
        storageConfig.SetSSDMaxPostponedTime(dist(engine));
        storageConfig.SetSSDMaxPostponedCount(dist(engine));

        // HHD throttler disabled.
        storageConfig.SetAllocationUnitHDD(dist(engine));
        storageConfig.SetHDDUnitReadIops(dist(engine));
        storageConfig.SetHDDUnitReadBandwidth(dist(engine));
        storageConfig.SetHDDUnitWriteIops(dist(engine));
        storageConfig.SetHDDUnitWriteBandwidth(dist(engine));
        storageConfig.SetHDDMaxReadIops(dist(engine));
        storageConfig.SetHDDMaxReadBandwidth(dist(engine));
        storageConfig.SetHDDMaxWriteIops(dist(engine));
        storageConfig.SetHDDMaxWriteBandwidth(dist(engine));
        storageConfig.SetHDDBoostTime(dist(engine));
        storageConfig.SetHDDBoostRefillTime(dist(engine));
        storageConfig.SetHDDUnitBoost(dist(engine));
        storageConfig.SetHDDBurstPercentage(dist(engine));
        storageConfig.SetHDDDefaultPostponedRequestWeight(dist(engine));
        storageConfig.SetHDDMaxPostponedWeight(dist(engine));
        storageConfig.SetHDDMaxWriteCostMultiplier(dist(engine));
        storageConfig.SetHDDMaxPostponedTime(dist(engine));
        storageConfig.SetHDDMaxPostponedCount(dist(engine));

        if (performanceProfile.Defined()) {
            SetupFileStorePerformanceAndChannels(
                false,
                storageConfig,
                kikimrConfig,
                *performanceProfile);
            return;
        }

        switch (storageType) {
            case ::NCloud::NProto::STORAGE_MEDIA_SSD: {
                storageConfig.SetSSDThrottlingEnabled(true);
                storageConfig.SetAllocationUnitSSD(allocationUnits);
                storageConfig.SetSSDUnitReadIops(unitReadIops);
                storageConfig.SetSSDUnitReadBandwidth(unitReadBandwidth);
                storageConfig.SetSSDUnitWriteIops(unitWriteIops);
                storageConfig.SetSSDUnitWriteBandwidth(unitWriteBandwidth);
                storageConfig.SetSSDMaxReadIops(maxReadIops);
                storageConfig.SetSSDMaxReadBandwidth(maxReadBandwidth);
                storageConfig.SetSSDMaxWriteIops(maxWriteIops);
                storageConfig.SetSSDMaxWriteBandwidth(maxWriteBandwidth);
                storageConfig.SetSSDBoostTime(boostTimeMs);
                storageConfig.SetSSDBoostRefillTime(boostRefillTimeMs);
                storageConfig.SetSSDUnitBoost(unitBoost);
                storageConfig.SetSSDBurstPercentage(burstPercentage);
                storageConfig.SetSSDDefaultPostponedRequestWeight(defaultPostponedRequestWeight);
                storageConfig.SetSSDMaxPostponedWeight(maxPostponedWeight);
                storageConfig.SetSSDMaxWriteCostMultiplier(maxWriteCostMultiplier);
                storageConfig.SetSSDMaxPostponedTime(maxPostponedTimeMs);
                storageConfig.SetSSDMaxPostponedCount(maxPostponedCount);
                break;
            }
            default: {
                storageConfig.SetHDDThrottlingEnabled(true);
                storageConfig.SetAllocationUnitHDD(allocationUnits);
                storageConfig.SetHDDUnitReadIops(unitReadIops);
                storageConfig.SetHDDUnitReadBandwidth(unitReadBandwidth);
                storageConfig.SetHDDUnitWriteIops(unitWriteIops);
                storageConfig.SetHDDUnitWriteBandwidth(unitWriteBandwidth);
                storageConfig.SetHDDMaxReadIops(maxReadIops);
                storageConfig.SetHDDMaxReadBandwidth(maxReadBandwidth);
                storageConfig.SetHDDMaxWriteIops(maxWriteIops);
                storageConfig.SetHDDMaxWriteBandwidth(maxWriteBandwidth);
                storageConfig.SetHDDBoostTime(boostTimeMs);
                storageConfig.SetHDDBoostRefillTime(boostRefillTimeMs);
                storageConfig.SetHDDUnitBoost(unitBoost);
                storageConfig.SetHDDBurstPercentage(burstPercentage);
                storageConfig.SetHDDDefaultPostponedRequestWeight(defaultPostponedRequestWeight);
                storageConfig.SetHDDMaxPostponedWeight(maxPostponedWeight);
                storageConfig.SetHDDMaxWriteCostMultiplier(maxWriteCostMultiplier);
                storageConfig.SetHDDMaxPostponedTime(maxPostponedTimeMs);
                storageConfig.SetHDDMaxPostponedCount(maxPostponedCount);
                break;
            }
        }

        SetupFileStorePerformanceAndChannels(
            false,
            storageConfig,
            kikimrConfig);
    }

#define DO_TEST(te, ri, rb, wi, wb, bt, br, bop, bp, drw, mp, mwc, mt, mc, pp) \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetThrottlingEnabled() : te,                        \
        kikimrConfig.GetPerformanceProfileThrottlingEnabled());                \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetMaxReadIops() : ri,                              \
        kikimrConfig.GetPerformanceProfileMaxReadIops());                      \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetMaxReadBandwidth() : rb,                         \
        kikimrConfig.GetPerformanceProfileMaxReadBandwidth());                 \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetMaxWriteIops() : wi,                             \
        kikimrConfig.GetPerformanceProfileMaxWriteIops());                     \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetMaxWriteBandwidth() : wb,                        \
        kikimrConfig.GetPerformanceProfileMaxWriteBandwidth());                \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetBoostTime() : bt,                                \
        kikimrConfig.GetPerformanceProfileBoostTime());                        \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetBoostRefillTime() : br,                          \
        kikimrConfig.GetPerformanceProfileBoostRefillTime());                  \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetBoostPercentage() : bop,                         \
        kikimrConfig.GetPerformanceProfileBoostPercentage());                  \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetBurstPercentage() : bp,                          \
        kikimrConfig.GetPerformanceProfileBurstPercentage());                  \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetDefaultPostponedRequestWeight() : drw,           \
        kikimrConfig.GetPerformanceProfileDefaultPostponedRequestWeight());    \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetMaxPostponedWeight() : mp,                       \
        kikimrConfig.GetPerformanceProfileMaxPostponedWeight());               \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetMaxWriteCostMultiplier() : mwc,                  \
        kikimrConfig.GetPerformanceProfileMaxWriteCostMultiplier());           \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetMaxPostponedTime() : mt,                         \
        kikimrConfig.GetPerformanceProfileMaxPostponedTime());                 \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        pp.Defined() ? pp->GetMaxPostponedCount() : mc,                        \
        kikimrConfig.GetPerformanceProfileMaxPostponedCount());                \
// DO_TEST


    void TestPerformanceProfile(
        ui32 storageType,
        ui32 blocksCount,
        ui32 blockSize,
        NKikimrFileStore::TConfig& kikimrConfig,
        NProto::TStorageConfig& storageConfig,
        const TMaybe<NProto::TFileStorePerformanceProfile>& performanceProfile)
    {
        {
            // Single unit, don't reach limits.
            kikimrConfig.Clear();
            kikimrConfig.SetBlocksCount(blocksCount);
            kikimrConfig.SetBlockSize(blockSize);
            SetupPerformanceProfile(
                storageType,       // storageType
                4,                 // allocationUnits
                200,               // unitReadIops
                2,                 // unitReadBandwidth (MiB)
                400,               // unitWriteIops
                4,                 // unitWriteBandwidth (MiB)
                2'000,             // maxReadIops
                100,               // maxReadBandwidth (MiB)
                4'000,             // maxWriteIops
                200,               // maxWriteBandwidth (MiB)
                30'000,            // boostTime (30s)
                600'000,           // boostRefilleTime (10m)
                2,                 // unitBoost
                10,                // burstPercentage
                1_KB,              // defaultPostponedRequestWeight
                10_MB,             // maxPostponedWeight
                5,                 // maxWriteCostMultiplier
                10'000,            // maxPostponedTime (10s)
                15,                // maxPostponedCount
                kikimrConfig,      // kikimrConfig
                storageConfig,     // storageConfig
                performanceProfile // clientPerformanceProfile
            );
            DO_TEST(
                true,              // ThrottlingEnabled
                200,               // MaxReadIops
                2_MB,              // MaxReadBandwidth
                400,               // MaxWriteIops
                4_MB,              // MaxWriteBandwidth
                30'000,            // BoostTime
                600'000,           // BoostRefillTime
                200,               // BoostPercentage
                10,                // BurstPercentage
                1_KB,              // DefaultPostponedRequestWeight
                10_MB,             // MaxPostponedWeight
                5,                 // MaxWriteCostMultiplier
                10'000,            // MaxPostponedTime
                15,                // MaxPostponedCount
                performanceProfile // ClientPerformanceProfile
            );
        }

        {
            // Single unit, reach limits
            kikimrConfig.Clear();
            kikimrConfig.SetBlocksCount(blocksCount);
            kikimrConfig.SetBlockSize(blockSize);
            SetupPerformanceProfile(
                storageType,       // storageType
                4,                 // allocationUnits
                200,               // unitReadIops
                2,                 // unitReadBandwidth (MiB)
                400,               // unitWriteIops
                4,                 // unitWriteBandwidth (MiB)
                100,               // maxReadIops
                1,                 // maxReadBandwidth (MiB)
                200,               // maxWriteIops
                2,                 // maxWriteBandwidth (MiB)
                30'000,            // boostTime (30s)
                600'000,           // boostRefilleTime (10m)
                2,                 // unitBoost
                10,                // burstPercentage
                1_KB,              // defaultPostponedRequestWeight
                10_MB,             // maxPostponedWeight
                5,                 // maxWriteCostMultiplier
                10'000,            // maxPostponedTime (10s)
                15,                // MaxPostponedCount
                kikimrConfig,      // kikimrConfig
                storageConfig,     // storageConfig
                performanceProfile // clientPerformanceProfile
            );
            DO_TEST(
                true,              // ThrottlingEnabled
                100,               // MaxReadIops
                1_MB,              // MaxReadBandwidth
                200,               // MaxWriteIops
                2_MB,              // MaxWriteBandwidth
                30'000,            // BoostTime
                600'000,           // BoostRefillTime
                200,               // BoostPercentage
                10,                // BurstPercentage
                1_KB,              // DefaultPostponedRequestWeight
                10_MB,             // MaxPostponedWeight
                5,                 // MaxWriteCostMultiplier
                10'000,            // MaxPostponedTime
                15,                // MaxPostponedCount
                performanceProfile // ClientPerformanceProfile
            );
        }

        {
            // Multipler units, don't reach max
            kikimrConfig.Clear();
            kikimrConfig.SetBlocksCount(1024_GB / 4_KB);
            kikimrConfig.SetBlockSize(4_KB);
            SetupPerformanceProfile(
                storageType,       // storageType
                4,                 // allocationUnits
                200,               // unitReadIops
                2,                 // unitReadBandwidth (MiB)
                400,               // unitWriteIops
                4,                 // unitWriteBandwidth (MiB)
                200'000,           // maxReadIops
                1'024,             // maxReadBandwidth (MiB)
                400'000,           // maxWriteIops
                2'048,             // maxWriteBandwidth (MiB)
                30'000,            // boostTime (30s)
                600'000,           // boostRefilleTime (10m)
                2,                 // unitBoost
                10,                // burstPercentage
                1_KB,              // defaultPostponedRequestWeight
                10_MB,             // maxPostponedWeight
                5,                 // maxWriteCostMultiplier
                10'000,            // maxPostponedTime (10s)
                15,                // maxPostponedCount
                kikimrConfig,      // kikimrConfig
                storageConfig,     // storageConfig
                performanceProfile // clientPerformanceProfile
            );
            DO_TEST(
                true,
                51'200,            // MaxReadIops
                512_MB,            // MaxReadBandwidth
                102'400,           // MaxWriteIops
                1024_MB,           // MaxWriteBandwidth
                30'000,            // BoostTime
                600'000,           // BoostRefillTime
                0,                 // BoostPercentage
                10,                // BurstPercentage
                1_KB,              // DefaultPostponedRequestWeight
                10_MB,             // MaxPostponedWeight
                5,                 // MaxWriteCostMultiplier
                10'000,            // MaxPostponedTime
                15,                // MaxPostponedCount
                performanceProfile // ClientPerformanceProfile
            );
        }

        {
            // Multipler units, reach limits
            kikimrConfig.Clear();
            kikimrConfig.SetBlocksCount(1024_GB / 4_KB);
            kikimrConfig.SetBlockSize(4_KB);
            SetupPerformanceProfile(
                storageType,       // storageType
                4,                 // allocationUnits
                200,               // unitReadIops
                2,                 // unitReadBandwidth (MiB)
                400,               // unitWriteIops
                4,                 // unitWriteBandwidth (MiB)
                50'000,            // maxReadIops
                256,               // maxReadBandwidth (MiB)
                100'000,           // maxWriteIops
                512,               // maxWriteBandwidth (MiB)
                30'000,            // boostTime (30s)
                600'000,           // boostRefilleTime (10m)
                2,                 // unitBoost
                10,                // burstPercentage
                1_KB,              // defaultPostponedRequestWeight
                10_MB,             // maxPostponedWeight
                5,                 // maxWriteCostMultiplier
                10'000,            // maxPostponedTime (10s)
                15,                // maxPostponedCount
                kikimrConfig,      // kikimrConfig
                storageConfig,     // storageConfig
                performanceProfile // clientPerformanceProfile
            );
            DO_TEST(
                true,              // ThrottlingEnabled
                50'000,            // MaxReadIops
                256_MB,            // MaxReadBandwidth
                100'000,           // MaxWriteIops
                512_MB,            // MaxWriteBandwidth
                30'000,            // BoostTime
                600'000,           // BoostRefillTime
                0,                 // BoostPercentage
                10,                // BurstPercentage
                1_KB,              // DefaultPostponedRequestWeight
                10_MB,             // MaxPostponedWeight
                5,                 // MaxWriteCostMultiplier
                10'000,            // MaxPostponedTime
                15,                // MaxPostponedCount
                performanceProfile // ClientPerformanceProfile
            );
        }

        // Same, just change allocation units.
        {
            // Single unit, don't reach limits.
            kikimrConfig.Clear();
            kikimrConfig.SetBlocksCount(blocksCount);
            kikimrConfig.SetBlockSize(blockSize);
            SetupPerformanceProfile(
                storageType,       // storageType
                2,                 // allocationUnits
                200,               // unitReadIops
                2,                 // unitReadBandwidth (MiB)
                400,               // unitWriteIops
                4,                 // unitWriteBandwidth (MiB)
                2'000,             // maxReadIops
                100,               // maxReadBandwidth (MiB)
                4'000,             // maxWriteIops
                200,               // maxWriteBandwidth (MiB)
                30'000,            // boostTime (30s)
                600'000,           // boostRefilleTime (10m)
                2,                 // unitBoost
                10,                // burstPercentage
                1_KB,              // defaultPostponedRequestWeight
                10_MB,             // maxPostponedWeight
                5,                 // maxWriteCostMultiplier
                10'000,            // maxPostponedTime (10s)
                15,                // maxPostponedCount
                kikimrConfig,      // kikimrConfig
                storageConfig,     // storageConfig
                performanceProfile // clientPerformanceProfile
            );
            DO_TEST(
                true,              // ThrottlingEnabled
                200,               // MaxReadIops
                2_MB,              // MaxReadBandwidth
                400,               // MaxWriteIops
                4_MB,              // MaxWriteBandwidth
                30'000,            // BoostTime
                600'000,           // BoostRefillTime
                200,               // BoostPercentage
                10,                // BurstPercentage
                1_KB,              // DefaultPostponedRequestWeight
                10_MB,             // MaxPostponedWeight
                5,                 // MaxWriteCostMultiplier
                10'000,            // MaxPostponedTime
                15,                // MaxPostponedCount
                performanceProfile // ClientPerformanceProfile
            );
        }

        {
            // Single unit, reach limits
            kikimrConfig.Clear();
            kikimrConfig.SetBlocksCount(blocksCount);
            kikimrConfig.SetBlockSize(blockSize);
            SetupPerformanceProfile(
                storageType,       // storageType
                2,                 // allocationUnits
                200,               // unitReadIops
                2,                 // unitReadBandwidth (MiB)
                400,               // unitWriteIops
                4,                 // unitWriteBandwidth (MiB)
                100,               // maxReadIops
                1,                 // maxReadBandwidth (MiB)
                200,               // maxWriteIops
                2,                 // maxWriteBandwidth (MiB)
                30'000,            // boostTime (30s)
                600'000,           // boostRefilleTime (10m)
                2,                 // unitBoost
                10,                // burstPercentage
                1_KB,              // defaultPostponedRequestWeight
                10_MB,             // maxPostponedWeight
                5,                 // maxWriteCostMultiplier
                10'000,            // maxPostponedTime (10s)
                15,                // maxPostponedCount
                kikimrConfig,      // kikimrConfig
                storageConfig,     // storageConfig
                performanceProfile // clientPerformanceProfile
            );
            DO_TEST(
                true,              // ThrottlingEnabled
                100,               // MaxReadIops
                1_MB,              // MaxReadBandwidth
                200,               // MaxWriteIops
                2_MB,              // MaxWriteBandwidth
                30'000,            // BoostTime
                600'000,           // BoostRefillTime
                200,               // BoostPercentage
                10,                // BurstPercentage
                1_KB,              // DefaultPostponedRequestWeight
                10_MB,             // MaxPostponedWeight
                5,                 // MaxWriteCostMultiplier
                10'000,            // MaxPostponedTime
                15,                // MaxPostponedCount
                performanceProfile // ClientPerformanceProfile
            );
        }

        {
            // Multipler units, don't reach max
            kikimrConfig.Clear();
            kikimrConfig.SetBlocksCount(1024_GB / 4_KB);
            kikimrConfig.SetBlockSize(4_KB);
            SetupPerformanceProfile(
                storageType,       // storageType
                2,                 // allocationUnits
                200,               // unitReadIops
                2,                 // unitReadBandwidth (MiB)
                400,               // unitWriteIops
                4,                 // unitWriteBandwidth (MiB)
                200'000,           // maxReadIops
                1'024,             // maxReadBandwidth (MiB)
                400'000,           // maxWriteIops
                2'048,             // maxWriteBandwidth (MiB)
                30'000,            // boostTime (30s)
                600'000,           // boostRefilleTime (10m)
                2,                 // unitBoost
                10,                // burstPercentage
                1_KB,              // defaultPostponedRequestWeight
                10_MB,             // maxPostponedWeight
                5,                 // maxWriteCostMultiplier
                10'000,            // maxPostponedTime (10s)
                15,                // maxPostponedCount
                kikimrConfig,      // kikimrConfig
                storageConfig,     // storageConfig
                performanceProfile // clientPerformanceProfile
            );
            DO_TEST(
                true,              // ThrottlingEnabled
                102'400,           // MaxReadIops
                1024_MB,           // MaxReadBandwidth
                204'800,           // MaxWriteIops
                2048_MB,           // MaxWriteBandwidth
                30'000,            // BoostTime
                600'000,           // BoostRefillTime
                0,                 // BoostPercentage
                10,                // BurstPercentage
                1_KB,              // DefaultPostponedRequestWeight
                10_MB,             // MaxPostponedWeight
                5,                 // MaxWriteCostMultiplier
                10'000,            // MaxPostponedTime
                15,                // MaxPostponedCount
                performanceProfile // ClientPerformanceProfile
            );
        }

        {
            // Multipler units, reach limits
            kikimrConfig.Clear();
            kikimrConfig.SetBlocksCount(1024_GB / 4_KB);
            kikimrConfig.SetBlockSize(4_KB);
            SetupPerformanceProfile(
                storageType,       // storageType
                2,                 // allocationUnits
                200,               // unitReadIops
                2,                 // unitReadBandwidth (MiB)
                400,               // unitWriteIops
                4,                 // unitWriteBandwidth (MiB)
                50'000,            // maxReadIops
                256,               // maxReadBandwidth (MiB)
                100'000,           // maxWriteIops
                512,               // maxWriteBandwidth (MiB)
                30'000,            // boostTime (30s)
                600'000,           // boostRefilleTime (10m)
                0,                 // unitBoost
                10,                // burstPercentage
                1_KB,              // defaultPostponedRequestWeight
                10_MB,             // maxPostponedWeight
                5,                 // maxWriteCostMultiplier
                10'000,            // maxPostponedTime (10s)
                15,                // maxPostponedCount
                kikimrConfig,      // kikimrConfig
                storageConfig,     // storageConfig
                performanceProfile // clientPerformanceProfile
            );
            DO_TEST(
                true,              // ThrottlingEnabled
                50'000,            // MaxReadIops
                256_MB,            // MaxReadBandwidth
                100'000,           // MaxWriteIops
                512_MB,            // MaxWriteBandwidth
                30'000,            // BoostTime
                600'000,           // BoostRefillTime
                0,                 // BoostPercentage
                10,                // BurstPercentage
                1_KB,              // DefaultPostponedRequestWeight
                10_MB,             // MaxPostponedWeight
                5,                 // MaxWriteCostMultiplier
                10'000,            // MaxPostponedTime
                15,                // MaxPostponedCount
                performanceProfile // ClientPerformanceProfile
            );
        }
    }

    Y_UNIT_TEST_F(ShouldSetupFileStorePerformanceProfileHDD, TConfigs)
    {
        TestPerformanceProfile(
            ::NCloud::NProto::STORAGE_MEDIA_HDD, // storageType
            DefaultBlocksCount,                  // blocksCount
            DefaultBlockSize,                    // blockSize
            KikimrConfig,                        // kikimrConfig
            StorageConfig,                       // storageConfig
            {}                                   // performanceProfile
        );
    }

    Y_UNIT_TEST_F(ShouldSetupFileStorePerformanceProfileSSD, TConfigs)
    {
        TestPerformanceProfile(
            ::NCloud::NProto::STORAGE_MEDIA_SSD, // storageType
            DefaultBlocksCount,                  // blocksCount
            DefaultBlockSize,                    // blockSize
            KikimrConfig,                        // kikimrConfig
            StorageConfig,                       // storageConfig
            {}                                   // performanceProfile
        );
    }

    Y_UNIT_TEST_F(ShouldSetupFileStorePerformanceProfileHybrid, TConfigs)
    {
        TestPerformanceProfile(
            ::NCloud::NProto::STORAGE_MEDIA_HYBRID, // storageType
            DefaultBlocksCount,                     // blocksCount
            DefaultBlockSize,                       // blockSize
            KikimrConfig,                           // kikimrConfig
            StorageConfig,                          // storageConfig
            {}                                      // performanceProfile
        );
    }

    Y_UNIT_TEST_F(ShouldSetupFileStorePerformanceProfileHDDFromClient, TConfigs)
    {
        TestPerformanceProfile(
            ::NCloud::NProto::STORAGE_MEDIA_HDD, // storageType
            DefaultBlocksCount,                  // blocksCount
            DefaultBlockSize,                    // blockSize
            KikimrConfig,                        // kikimrConfig
            StorageConfig,                       // storageConfig
            ClientPerformanceProfile             // performanceProfile
        );
    }

    Y_UNIT_TEST_F(ShouldSetupFileStorePerformanceProfileSSDFromClient, TConfigs)
    {
        TestPerformanceProfile(
            ::NCloud::NProto::STORAGE_MEDIA_SSD, // storageType
            DefaultBlocksCount,                  // blocksCount
            DefaultBlockSize,                    // blockSize
            KikimrConfig,                        // kikimrConfig
            StorageConfig,                       // storageConfig
            ClientPerformanceProfile             // performanceProfile
        );
    }

    Y_UNIT_TEST_F(ShouldSetupFileStorePerformanceProfileHybridFromClient, TConfigs)
    {
        TestPerformanceProfile(
            ::NCloud::NProto::STORAGE_MEDIA_HYBRID, // storageType
            DefaultBlocksCount,                     // blocksCount
            DefaultBlockSize,                       // blockSize
            KikimrConfig,                           // kikimrConfig
            StorageConfig,                          // storageConfig
            ClientPerformanceProfile                // performanceProfile
        );
    }

#undef DO_TEST

    Y_UNIT_TEST_F(ShouldCreateProperNumberOfShards, TConfigs)
    {
        using namespace ::NCloud::NProto;
        KikimrConfig.SetBlockSize(4_KB);

        // Disable media type override.
        StorageConfig.SetAutomaticShardCreationEnabled(true);
        StorageConfig.SetShardAllocationUnit(4_TB);
        StorageConfig.SetAutomaticallyCreatedShardSize(5_TB);
        StorageConfig.SetSSDMaxWriteIops(Max<ui32>());

        auto OldMaxWriteIops = ClientPerformanceProfile.GetMaxWriteIops();
        ClientPerformanceProfile.ClearMaxWriteIops();

        KikimrConfig.SetBlocksCount(4_TB / 4_KB);
        auto fs = SetupMultiShardFileStorePerformanceAndChannels(
            StorageConfig,
            KikimrConfig,
            ClientPerformanceProfile,
            0);
        UNIT_ASSERT_VALUES_EQUAL(1, fs.ShardConfigs.size());
        // MaxWriteIops per allocation unit is 1000, so (4096 GiB / 32 GiB) *
        // 1000 = 128000
        UNIT_ASSERT_VALUES_EQUAL(
            128000,
            fs.MainFileSystemConfig.GetPerformanceProfileMaxWriteIops());

        ClientPerformanceProfile.SetMaxWriteIops(OldMaxWriteIops);

        KikimrConfig.SetBlocksCount(4_TB / 4_KB + 1);
        fs = SetupMultiShardFileStorePerformanceAndChannels(
            StorageConfig,
            KikimrConfig,
            ClientPerformanceProfile,
            0);
        UNIT_ASSERT_VALUES_EQUAL(2, fs.ShardConfigs.size());

        KikimrConfig.SetBlocksCount(5_TB / 4_KB);
        fs = SetupMultiShardFileStorePerformanceAndChannels(
            StorageConfig,
            KikimrConfig,
            ClientPerformanceProfile,
            0);
        UNIT_ASSERT_VALUES_EQUAL(2, fs.ShardConfigs.size());

        KikimrConfig.SetBlocksCount(16_TB / 4_KB);
        fs = SetupMultiShardFileStorePerformanceAndChannels(
            StorageConfig,
            KikimrConfig,
            ClientPerformanceProfile,
            0);
        UNIT_ASSERT_VALUES_EQUAL(4, fs.ShardConfigs.size());

        KikimrConfig.SetBlocksCount(512_TB / 4_KB);
        fs = SetupMultiShardFileStorePerformanceAndChannels(
            StorageConfig,
            KikimrConfig,
            ClientPerformanceProfile,
            0);
        UNIT_ASSERT_VALUES_EQUAL(128, fs.ShardConfigs.size());

        KikimrConfig.SetBlocksCount(1_PB / 4_KB);
        fs = SetupMultiShardFileStorePerformanceAndChannels(
            StorageConfig,
            KikimrConfig,
            ClientPerformanceProfile,
            0);
        UNIT_ASSERT_VALUES_EQUAL(254, fs.ShardConfigs.size());

        for (const auto& sc: fs.ShardConfigs) {
            UNIT_ASSERT_VALUES_EQUAL(5_TB / 4_KB, sc.GetBlocksCount());
        }

        // shards (but not main tablet) should have 'IsSystem' flag
        UNIT_ASSERT(!fs.MainFileSystemConfig.GetIsSystem());
        for (const auto& sc: fs.ShardConfigs) {
            UNIT_ASSERT(sc.GetIsSystem());
        }
    }

    Y_UNIT_TEST_F(ShouldCreateExplicitNumberOfShards, TConfigs)
    {
        using namespace ::NCloud::NProto;
        KikimrConfig.SetBlockSize(4_KB);
        KikimrConfig.SetBlocksCount(4_TB / 4_KB);

        // Disable media type override.
        StorageConfig.SetAutomaticShardCreationEnabled(true);
        StorageConfig.SetShardAllocationUnit(4_TB);
        StorageConfig.SetAutomaticallyCreatedShardSize(5_TB);

        auto fs = SetupMultiShardFileStorePerformanceAndChannels(
            StorageConfig,
            KikimrConfig,
            ClientPerformanceProfile,
            10);
        UNIT_ASSERT_VALUES_EQUAL(10, fs.ShardConfigs.size());

        for (const auto& sc: fs.ShardConfigs) {
            UNIT_ASSERT_VALUES_EQUAL(5_TB / 4_KB, sc.GetBlocksCount());
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
