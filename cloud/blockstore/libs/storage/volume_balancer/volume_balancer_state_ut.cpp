#include <cloud/blockstore/libs/storage/testlib/test_env.h>

#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer_state.h>
#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer_ut_helpers.h>

#include <cloud/blockstore/libs/diagnostics/volume_balancer_switch.h>

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
    bool volumeBalancerEnabled,
    ui32 cpuLackThreshold,
    NFeatures::TFeaturesConfigPtr featuresConfig)
{
    NProto::TStorageServiceConfig storageConfig;
    storageConfig.SetVolumePreemptionType(type);
    storageConfig.SetVolumeBalancerEnabled(volumeBalancerEnabled);
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
            // All but blacklisted
            feature->SetCloudProbability(1);
            feature->SetFolderProbability(1);
        } else {
            for (const auto& c: list) {
                *feature->MutableWhitelist()->MutableCloudIds()->Add() = c.first;
                *feature->MutableWhitelist()->MutableFolderIds()->Add() = c.second;
            }
            // None but whitelisted
            feature->SetCloudProbability(0);
            feature->SetFolderProbability(0);
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

IVolumeBalancerSwitchPtr CreateSwitch(bool enabled)
{
    auto volumeBalancerSwitch = CreateVolumeBalancerSwitch();
    if (enabled) {
        volumeBalancerSwitch->EnableVolumeBalancer();
    }
    return volumeBalancerSwitch;
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeBalancerStateTest)
{
    // Verify that dynamic config updates to InitialPullDelay preserve
    // NextPullAttempt, the time when an existing volume can be selected for
    // moving back to this node. Use the new delay for newly tracked volumes.
    Y_UNIT_TEST(ShouldPreservePullDelaysOnConfigUpdate)
    {
        // Track "existing" with InitialPullDelay=10s and check that it cannot
        // be selected before its NextPullAttempt.
        NProto::TStorageServiceConfig config;
        config.SetVolumeBalancerEnabled(true);
        config.SetInitialPullDelay(TDuration::Seconds(10).MilliSeconds());
        const auto features = CreateFeatureConfig({}, {}, false);
        TVolumeBalancerState state(
            std::make_shared<TStorageConfig>(config, features),
            CreateDiagnosticsConfig(),
            CreateSwitch(true));
        auto now = TInstant::Seconds(0);
        state.UpdateVolumeStats(
            {CreateVolumeStats("existing", {}, {}, false)},
            {},
            0,
            now);
        UNIT_ASSERT(!state.GetVolumeToPull());

        // Increase InitialPullDelay to 60s, add "new", and check that neither
        // volume can be selected yet.
        config.SetInitialPullDelay(TDuration::Seconds(60).MilliSeconds());
        state.UpdateStorageConfig(
            std::make_shared<TStorageConfig>(config, features));
        const TVector<NProto::TVolumeBalancerDiskStats> volumes{
            CreateVolumeStats("existing", {}, {}, false),
            CreateVolumeStats("new", {}, {}, false)};
        now += TDuration::Seconds(1);
        state.UpdateVolumeStats(volumes, {}, 0, now);
        UNIT_ASSERT(!state.GetVolumeToPull());

        // Advance time and check that "existing" becomes selectable exactly
        // at its original NextPullAttempt.
        now = TInstant::Seconds(9);
        state.UpdateVolumeStats(volumes, {}, 0, now);
        UNIT_ASSERT(!state.GetVolumeToPull());
        now = TInstant::Seconds(10);
        state.UpdateVolumeStats(volumes, {}, 0, now);
        UNIT_ASSERT_VALUES_EQUAL("existing", state.GetVolumeToPull());

        // Mark "existing" in progress and check that "new" is not selected
        // before its own NextPullAttempt.
        state.SetVolumeInProgress("existing");
        now = TInstant::Seconds(60);
        state.UpdateVolumeStats(volumes, {}, 0, now);
        UNIT_ASSERT(!state.GetVolumeToPull());

        // Advance to NextPullAttempt for "new" and check that it is selected
        // after the updated InitialPullDelay.
        now = TInstant::Seconds(61);
        state.UpdateVolumeStats(volumes, {}, 0, now);
        UNIT_ASSERT_VALUES_EQUAL("new", state.GetVolumeToPull());
    }

    // Verify that dynamic config updates to InitialPullDelay change the time
    // a volume must stay on this node before BackoffDelayProvider.Reset().
    // Reset restores InitialPullDelay saved when the volume was first seen.
    Y_UNIT_TEST(ShouldUpdateBackoffResetInterval)
    {
        // Use InitialPullDelay=10s and check that "vol" is selected after 10s.
        // Mark it local, leaving a 20s delay in BackoffDelayProvider.
        const auto initialDelay = TDuration::Seconds(10);
        NProto::TStorageServiceConfig config;
        config.SetVolumeBalancerEnabled(true);
        config.SetInitialPullDelay(initialDelay.MilliSeconds());
        const auto features = CreateFeatureConfig({}, {}, false);
        TVolumeBalancerState state(
            std::make_shared<TStorageConfig>(config, features),
            CreateDiagnosticsConfig(),
            CreateSwitch(true));
        const TVector<NProto::TVolumeBalancerDiskStats> remote{
            CreateVolumeStats("vol", {}, {}, false)};
        const TVector<NProto::TVolumeBalancerDiskStats> local{
            CreateVolumeStats("vol", {}, {}, true)};
        auto now = TInstant::Seconds(100);
        state.UpdateVolumeStats(remote, {}, 0, now);
        now += initialDelay;
        state.UpdateVolumeStats(remote, {}, 0, now);
        UNIT_ASSERT_VALUES_EQUAL("vol", state.GetVolumeToPull());
        state.UpdateVolumeStats(local, {}, 0, now);

        // Increase InitialPullDelay to 30s and check that 15s on this node
        // does not reset BackoffDelayProvider's accumulated 20s delay.
        config.SetInitialPullDelay(TDuration::Seconds(30).MilliSeconds());
        state.UpdateStorageConfig(
            std::make_shared<TStorageConfig>(config, features));
        now += TDuration::Seconds(15);
        state.UpdateVolumeStats(local, {}, 0, now);
        state.UpdateVolumeStats(remote, {}, 0, now);

        // Advance time and check that GetVolumeToPull() selects "vol" after
        // 20s, not after the original InitialPullDelay of 10s.
        now += initialDelay;
        state.UpdateVolumeStats(remote, {}, 0, now);
        UNIT_ASSERT(!state.GetVolumeToPull());
        now += initialDelay;
        state.UpdateVolumeStats(remote, {}, 0, now);
        UNIT_ASSERT_VALUES_EQUAL("vol", state.GetVolumeToPull());
        state.UpdateVolumeStats(local, {}, 0, now);

        // Lower InitialPullDelay to 5s and check that 6s on this node allows
        // BackoffDelayProvider.Reset() before the next move away.
        config.SetInitialPullDelay(TDuration::Seconds(5).MilliSeconds());
        state.UpdateStorageConfig(
            std::make_shared<TStorageConfig>(config, features));
        now += TDuration::Seconds(6);
        state.UpdateVolumeStats(local, {}, 0, now);
        state.UpdateVolumeStats(remote, {}, 0, now);

        // Advance time and check that GetVolumeToPull() uses the saved
        // InitialPullDelay=10s after Reset(), despite the current value of 5s.
        now += initialDelay - TDuration::Seconds(1);
        state.UpdateVolumeStats(remote, {}, 0, now);
        UNIT_ASSERT(!state.GetVolumeToPull());
        now += TDuration::Seconds(1);
        state.UpdateVolumeStats(remote, {}, 0, now);
        UNIT_ASSERT_VALUES_EQUAL("vol", state.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldSelectHaviestVolumeForPreemption)
    {
        TVolumeBalancerState state(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                /*volumeBalancerEnabled=*/true,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            ),
            CreateDiagnosticsConfig(),
            CreateSwitch(true)
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
                /*volumeBalancerEnabled=*/true,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            ),
            CreateDiagnosticsConfig(),
            CreateSwitch(true)
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
                /*volumeBalancerEnabled=*/true,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            ),
            CreateDiagnosticsConfig(),
            CreateSwitch(true)
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
            /*volumeBalancerEnabled=*/true,
            70,
            CreateFeatureConfig("Balancer", {}, true));

        TVolumeBalancerState state(
            storageConfig,
            CreateDiagnosticsConfig(),
            CreateSwitch(true));
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
                CreateVolumeStats("vol1", "", "", false) };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 40, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());

            now += storageConfig->GetInitialPullDelay();

            state.UpdateVolumeStats(vols, perfMap, 40 , now);
            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(state.GetVolumeToPull());
        }
    }

    Y_UNIT_TEST(ShouldNotSelectVolumeFromBlacklistedCloud)
    {
        auto storageConfig = CreateStorageConfig(
            NProto::PREEMPTION_NONE,
            /*volumeBalancerEnabled=*/false,
            70,
            CreateFeatureConfig("Balancer", {{"cloudid1", "folderid1"}}, true));

        TVolumeBalancerState state(
            storageConfig,
            CreateDiagnosticsConfig(),
            CreateSwitch(true));

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
            /*volumeBalancerEnabled=*/true,
            70,
            CreateFeatureConfig("Balancer", {}, true));

        TVolumeBalancerState state(
            storageConfig,
            CreateDiagnosticsConfig(),
            CreateSwitch(true));
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
    }

    Y_UNIT_TEST(ShouldResetPullDelayAfterCooldown)
    {
        auto storageConfig = CreateStorageConfig(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            /*volumeBalancerEnabled=*/true,
            70,
            CreateFeatureConfig("Balancer", {}, true));

        TVector nonePushed{
            CreateVolumeStats("vol0", "", "", true),
            CreateVolumeStats("vol1", "", "", true)};

        TVector vol0Pushed{
            CreateVolumeStats("vol0", "", "", false),
            CreateVolumeStats("vol1", "", "", true)};

        TVolumeBalancerState::TPerfGuaranteesMap perfMap{
            {"vol0", 10},
            {"vol1", 1}};

        constexpr ui64 CpuUnderloaded = 40;
        constexpr ui64 CpuOverloaded = 80;

        auto overlappingPullDelay =
            storageConfig->GetInitialPullDelay() + TDuration::Seconds(10);

        TVolumeBalancerState state(
            storageConfig,
            CreateDiagnosticsConfig(),
            CreateSwitch(true));
        TInstant now = TInstant::Seconds(0);

        // Push vol0
        state.UpdateVolumeStats(nonePushed, perfMap, CpuOverloaded, now);

        UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPush().c_str());
        UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPull().c_str());

        // skip initial pull delay & pull back
        state.UpdateVolumeStats(vol0Pushed, perfMap, CpuUnderloaded, now);
        now += overlappingPullDelay;
        state.UpdateVolumeStats(vol0Pushed, perfMap, CpuUnderloaded, now);

        UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPush().c_str());
        UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPull().c_str());

        // Push vol0
        state.UpdateVolumeStats(nonePushed, perfMap, CpuOverloaded, now);

        UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPush().c_str());
        UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPull().c_str());

        // skip 2x initial pull delay & pull back
        state.UpdateVolumeStats(vol0Pushed, perfMap, CpuUnderloaded, now);
        now += overlappingPullDelay * 2;
        state.UpdateVolumeStats(vol0Pushed, perfMap, CpuUnderloaded, now);

        UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPush().c_str());
        UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPull().c_str());

        // Idle so pull delay can be reset
        state.UpdateVolumeStats(nonePushed, perfMap, CpuUnderloaded, now);
        now += overlappingPullDelay;
        state.UpdateVolumeStats(nonePushed, perfMap, CpuUnderloaded, now);

        UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPush().c_str());
        UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPull().c_str());

        // skip initial pull delay & try push
        state.UpdateVolumeStats(nonePushed, perfMap, CpuOverloaded, now);

        UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPush().c_str());
        UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPull().c_str());

        // skip single initial pull delay & pull back
        // we expect success since pull delay is reset to initial
        state.UpdateVolumeStats(vol0Pushed, perfMap, CpuUnderloaded, now);
        now += overlappingPullDelay;
        state.UpdateVolumeStats(vol0Pushed, perfMap, CpuUnderloaded, now);

        UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPush().c_str());
        UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPull().c_str());
    }

    Y_UNIT_TEST(ShouldNotReturnManuallyPreemptedVolume)
    {
        TVolumeBalancerState state(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                /*volumeBalancerEnabled=*/true,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            ),
            CreateDiagnosticsConfig(),
            CreateSwitch(true)
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

    Y_UNIT_TEST(ShouldSelectLightestVolumeWhenEnabledWithPreemptionTypeNone)
    {
        TVolumeBalancerState state(
            CreateStorageConfig(
                NProto::PREEMPTION_NONE,
                /*volumeBalancerEnabled=*/true,
                70,
                /*featuresConfig=*/nullptr),
            CreateDiagnosticsConfig(),
            CreateSwitch(true));
        TInstant now = TInstant::Seconds(0);

        TVector<NProto::TVolumeBalancerDiskStats> vols{
            CreateVolumeStats("vol0", "", "", true),
            CreateVolumeStats("vol1", "", "", true)};

        TVolumeBalancerState::TPerfGuaranteesMap perfMap;
        perfMap["vol0"] = 10;
        perfMap["vol1"] = 1;

        state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

        UNIT_ASSERT_VALUES_EQUAL("vol1", state.GetVolumeToPush());
        UNIT_ASSERT(!state.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldNotPreemptWhenVolumeBalancerSwitchIsDisabled)
    {
        auto storageConfig = CreateStorageConfig(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            /*volumeBalancerEnabled=*/true,
            70,
            /*featuresConfig=*/nullptr);

        TVolumeBalancerState state(
            storageConfig,
            CreateDiagnosticsConfig(),
            CreateSwitch(false));
        TInstant now = TInstant::Seconds(0);

        TVolumeBalancerState::TPerfGuaranteesMap perfMap;
        perfMap["vol0"] = 10;
        perfMap["vol1"] = 1;

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats("vol0", "", "", true),
                CreateVolumeStats("vol1", "", "", true)};

            state.UpdateVolumeStats(vols, perfMap, 80, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats("vol0", "", "", false),
                CreateVolumeStats("vol1", "", "", false)};

            state.UpdateVolumeStats(vols, perfMap, 40, now);
            now += storageConfig->GetInitialPullDelay();
            state.UpdateVolumeStats(vols, std::move(perfMap), 40, now);

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
                /*volumeBalancerEnabled=*/true,
                70,
                CreateFeatureConfig("Balancer", {}, true)
            ),
            CreateDiagnosticsConfig(),
            CreateSwitch(true)
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
