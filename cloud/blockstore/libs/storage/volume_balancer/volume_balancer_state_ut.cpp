#include <cloud/blockstore/libs/storage/testlib/test_env.h>

#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer_state.h>

#include <cloud/storage/core/libs/features/features_config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TVolumeUsage = std::pair<ui64, ui64>;

////////////////////////////////////////////////////////////////////////////////

TStorageConfigPtr CreateStorageConfig(
    NProto::EVolumePreemptionType type,
    ui32 cpuLackThreshold,
    NFeatures::TFeaturesConfigPtr featuresConfig)
{
    NProto::TStorageServiceConfig storageConfig;
    storageConfig.SetVolumePreemptionType(type);
    storageConfig.SetCpuLackThreshold(cpuLackThreshold);
    if (!featuresConfig) {
        NProto::TFeaturesConfig config;
        featuresConfig = std::make_shared<NFeatures::TFeaturesConfig>(config);
    }

    return std::make_shared<TStorageConfig>(storageConfig, std::move(featuresConfig));
};

NFeatures::TFeaturesConfigPtr CreateFeatureConfig(
    const TString& featureName,
    const TVector<std::pair<TString, TString>>& list,
    bool blacklist)
{
    NProto::TFeaturesConfig config;
    if (featureName) {
        auto* feature = config.MutableFeatures()->Add();
        feature->SetName(featureName);
        if (blacklist) {
            for (const auto& c: list) {
                *feature->MutableBlacklist()->MutableCloudIds()->Add() = c.first;
                *feature->MutableBlacklist()->MutableFolderIds()->Add() = c.second;
            }
        } else {
            for (const auto& c: list) {
                *feature->MutableWhitelist()->MutableCloudIds()->Add() = c.first;
                *feature->MutableWhitelist()->MutableFolderIds()->Add() = c.second;
            }
        }
    }
    return std::make_shared<NFeatures::TFeaturesConfig>(config);
}

NProto::TVolumeBalancerDiskStats CreateVolumeStats(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId,
    bool isLocal,
    NProto::EPreemptionSource source)
{
    NProto::TVolumeBalancerDiskStats stats;
    stats.SetDiskId(diskId);
    stats.SetCloudId(cloudId);
    stats.SetCloudId(folderId);
    stats.SetIsLocal(isLocal);
    stats.SetPreemptionSource(source);
    stats.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);

    return stats;
}

NProto::TVolumeBalancerDiskStats CreateVolumeStats(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId,
    bool isLocal)
{
    NProto::TVolumeBalancerDiskStats stats;
    stats.SetDiskId(diskId);
    stats.SetCloudId(cloudId);
    stats.SetFolderId(folderId);
    stats.SetIsLocal(isLocal);
    stats.SetPreemptionSource(NProto::SOURCE_BALANCER);
    stats.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);

    return stats;
}

NProto::TVolumeBalancerDiskStats CreateVolumeStats(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId,
    bool isLocal,
    NProto::EStorageMediaKind mediaKind)
{
    NProto::TVolumeBalancerDiskStats stats;
    stats.SetDiskId(diskId);
    stats.SetCloudId(cloudId);
    stats.SetFolderId(folderId);
    stats.SetIsLocal(isLocal);
    stats.SetPreemptionSource(NProto::SOURCE_BALANCER);
    stats.SetStorageMediaKind(mediaKind);

    return stats;
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeBalancerStateTest)
{
    Y_UNIT_TEST(ShouldSelectHaviestVolumeForPreemption)
    {
        TVolumeBalancerState state(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            )
        );
        TInstant now = TInstant::Seconds(0);

        TVector<NProto::TVolumeBalancerDiskStats> vols {
            CreateVolumeStats("vol0", "", "", true),
            CreateVolumeStats("vol1", "", "", true)};

        TVolumeBalancerState::TPerfGuaranteesMap perfMap;
        perfMap["vol0"] = 10;
        perfMap["vol1"] = 1;

        state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

        UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPush());
        UNIT_ASSERT(!state.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldSelectLightestVolumeForPreemption)
    {
        TVolumeBalancerState state(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_LEAST_HEAVY,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            )
        );
        TInstant now = TInstant::Seconds(0);

        TVector<NProto::TVolumeBalancerDiskStats> vols {
            CreateVolumeStats("vol0", "", "", true),
            CreateVolumeStats("vol1", "", "", true)};

        TVolumeBalancerState::TPerfGuaranteesMap perfMap;
        perfMap["vol0"] = 10;
        perfMap["vol1"] = 1;

        state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

        UNIT_ASSERT_VALUES_EQUAL("vol1", state.GetVolumeToPush());
        UNIT_ASSERT(!state.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldNotPreemptVolumeIfLoadIsBelowThreshold)
    {
        TVolumeBalancerState state(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            )
        );
        TInstant now = TInstant::Seconds(0);

        TVector<NProto::TVolumeBalancerDiskStats> vols {
            CreateVolumeStats("vol0", "", "", true),
            CreateVolumeStats("vol1", "", "", true)};

        TVolumeBalancerState::TPerfGuaranteesMap perfMap;
        perfMap["vol0"] = 10;
        perfMap["vol1"] = 1;

        state.UpdateVolumeStats(vols, std::move(perfMap), 40, now);

        UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPush());
        UNIT_ASSERT(!state.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldReturnVolumeIfLoadIsBelowThreshold)
    {
        auto storageConfig = CreateStorageConfig(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            70,
            CreateFeatureConfig("Balancer", {}, true));

        TVolumeBalancerState state(storageConfig);
        TInstant now = TInstant::Seconds(0);

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats("vol0", "", "", true),
                CreateVolumeStats("vol1", "", "", true) };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

            UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats("vol0", "", "", false),
                CreateVolumeStats("vol1", "", "", true) };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 40, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());

            now += storageConfig->GetInitialPullDelay();

            state.UpdateVolumeStats(vols, perfMap, 40 , now);
            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPull());
        }
    }

    Y_UNIT_TEST(ShouldNotSelectVolumeFromBlacklistedCloud)
    {
        auto storageConfig = CreateStorageConfig(
            NProto::PREEMPTION_NONE,
            70,
            CreateFeatureConfig("Balancer", {{"cloudid1", "folderid1"}}, true));

        TVolumeBalancerState state(storageConfig);

        TInstant now = TInstant::Seconds(0);

        TVector<NProto::TVolumeBalancerDiskStats> vols {
            CreateVolumeStats("vol0", "cloudid1", "folderid1", true),
            CreateVolumeStats("vol1", "cloudid2", "folderid2", true) };

        TVolumeBalancerState::TPerfGuaranteesMap perfMap;
        perfMap["vol0"] = 10;
        perfMap["vol1"] = 1;

        now += storageConfig->GetInitialPullDelay();
        state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

        UNIT_ASSERT_VALUES_EQUAL("vol1", state.GetVolumeToPush());
        UNIT_ASSERT(!state.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldReturnVolumeOnlyAfterPullDelay)
    {
        auto storageConfig = CreateStorageConfig(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            70,
            CreateFeatureConfig("Balancer", {}, true));

        TVolumeBalancerState state(storageConfig);
        TInstant now = TInstant::Seconds(0);

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats("vol0", "", "", true),
                CreateVolumeStats("vol1", "", "", true) };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

            UNIT_ASSERT(state.GetVolumeToPush() == "vol0");
            UNIT_ASSERT(!state.GetVolumeToPull());
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats("vol0", "", "", false),
                CreateVolumeStats("vol1", "", "", true) };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 40, now);

            UNIT_ASSERT(!state.GetVolumeToPush());

            now += storageConfig->GetInitialPullDelay();

            state.UpdateVolumeStats(vols, std::move(perfMap), 40, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(state.GetVolumeToPull() == "vol0");
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats("vol0", "", "", true),
                CreateVolumeStats("vol1", "", "", true) };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

            UNIT_ASSERT(state.GetVolumeToPush() == "vol0");
            UNIT_ASSERT(!state.GetVolumeToPull());
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats("vol0", "", "", false),
                CreateVolumeStats("vol1", "", "", true) };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 40, now);

            UNIT_ASSERT(!state.GetVolumeToPush());

            now += storageConfig->GetInitialPullDelay();

            state.UpdateVolumeStats(vols, std::move(perfMap), 40, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(state.GetVolumeToPull() == "vol0");
        }
    }

    Y_UNIT_TEST(ShouldNotReturnManuallyPreemptedVolume)
    {
        TVolumeBalancerState state(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            )
        );
        TInstant now = TInstant::Seconds(0);

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats("vol0", "", "", true),
                CreateVolumeStats("vol1", "", "", true) };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

            UNIT_ASSERT(state.GetVolumeToPush() == "vol0");
            UNIT_ASSERT(!state.GetVolumeToPull());
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats("vol0", "", "", false, NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT),
                CreateVolumeStats("vol1", "", "", true, NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT) };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 30, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());
        }
    }

    Y_UNIT_TEST(ShouldNotMoveNonKikimrDisks)
    {
        TVector<NProto::EStorageMediaKind> kinds {
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD_MIRROR2,
            NProto::STORAGE_MEDIA_SSD_LOCAL,
            NProto::STORAGE_MEDIA_SSD_MIRROR3
        };

        TVolumeBalancerState state(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            )
        );

        for (ui32 i = 0; i < kinds.size(); ++i) {
            TInstant now = TInstant::Seconds(0);

            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats("vol0", "", "", true, kinds[i])
            };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;

            state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
