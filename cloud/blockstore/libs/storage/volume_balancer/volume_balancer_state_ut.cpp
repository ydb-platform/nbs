#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer_state.h>
#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer_ut_helpers.h>

#include <cloud/blockstore/libs/diagnostics/volume_balancer_switch.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/features/features_config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultCpuThreshold = 70;
constexpr ui64 CpuOverload = 80;
constexpr ui64 CpuUnderload = 40;
constexpr TDuration InitialPullDelay = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

TStorageConfigPtr CreateStorageConfig(
    NProto::EVolumePreemptionType type,
    bool volumeBalancerEnabled,
    ui32 cpuLackThreshold,
    NFeatures::TFeaturesConfigPtr featuresConfig,
    TDuration initialPullDelay = InitialPullDelay,
    NProto::TStorageServiceConfig storageConfig = {})
{
    storageConfig.SetVolumePreemptionType(type);
    storageConfig.SetVolumeBalancerEnabled(volumeBalancerEnabled);
    storageConfig.SetCpuLackThreshold(cpuLackThreshold);
    if (!featuresConfig) {
        NProto::TFeaturesConfig config;
        featuresConfig = std::make_shared<NFeatures::TFeaturesConfig>(config);
    }
    storageConfig.SetInitialPullDelay(initialPullDelay.MilliSeconds());

    return std::make_shared<TStorageConfig>(
        std::move(storageConfig),
        std::move(featuresConfig));
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

struct TVolumeStatsRecipe
{
    const TString DiskId = "";
    const TString CloudId = "";
    const TString FolderId = "";
    const bool IsLocal = true;
    const NProto::EPreemptionSource Source = NProto::SOURCE_BALANCER;
    const NProto::EStorageMediaKind Kind = NProto::STORAGE_MEDIA_SSD;
    const TVolumeLoadProfile LoadProfile = LoadZero;
    const ui32 SufferCount = 0;
};

NProto::TVolumeBalancerDiskStats CreateVolumeStats(
    const TVolumeStatsRecipe& recipe)
{
    NProto::TVolumeBalancerDiskStats stats;
    stats.SetDiskId(recipe.DiskId);
    stats.SetCloudId(recipe.CloudId);
    stats.SetFolderId(recipe.FolderId);
    stats.SetIsLocal(recipe.IsLocal);
    stats.SetPreemptionSource(recipe.Source);
    stats.SetStorageMediaKind(recipe.Kind);

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

class TVolumeBalancerStateTestEnv
{
public:
    TVolumeBalancerStateTestEnv(
        TStorageConfigPtr storageConfig,
        TDiagnosticsConfigPtr diagnosticsConfig,
        const TVector<TVolumeStatsRecipe>& volumes,
        IVolumeBalancerSwitchPtr volumeBalancerSwitch = CreateSwitch(true));

    void Push(const TString& diskId);
    void Pull(const TString& diskId);

    void Simulate(TDuration elapsed, ui64 cpuLack);
    void AddCounters(TDuration elapsed);

    TString GetVolumeToPush() const;
    TString GetVolumeToPull() const;

private:
    TVolumeBalancerState State;
    TVector<NProto::TVolumeBalancerDiskStats> VolumeStats;
    TVolumeBalancerState::TPerfGuaranteesMap VolumeSufferMap;
    THashMap<TString, TVolumeLoadProfile> LoadProfiles;
    TInstant Now;
};

TVolumeBalancerStateTestEnv::TVolumeBalancerStateTestEnv(
    TStorageConfigPtr storageConfig,
    TDiagnosticsConfigPtr diagnosticsConfig,
    const TVector<TVolumeStatsRecipe>& volumes,
    IVolumeBalancerSwitchPtr volumeBalancerSwitch)
    : State(
          std::move(storageConfig),
          std::move(diagnosticsConfig),
          std::move(volumeBalancerSwitch),
          CreateLoggingService("null"))
    , Now(TInstant::Zero())
{
    for (const auto& recipe: volumes) {
        VolumeStats.push_back(CreateVolumeStats(recipe));
        LoadProfiles[recipe.DiskId] = recipe.LoadProfile;
        VolumeSufferMap[recipe.DiskId] = recipe.SufferCount;
    }
    State.UpdateVolumeStats(VolumeStats, VolumeSufferMap, CpuUnderload, Now);
}

void TVolumeBalancerStateTestEnv::Push(const TString& diskId)
{
    for (auto& v: VolumeStats) {
        if (v.GetDiskId() == diskId) {
            UNIT_ASSERT_C(v.GetIsLocal(), "Attempt to push non-local volume");
            v.SetIsLocal(false);
            return;
        }
    }
    UNIT_FAIL("Unknown volume: " << diskId);
}

void TVolumeBalancerStateTestEnv::Pull(const TString& diskId)
{
    for (auto& v: VolumeStats) {
        if (v.GetDiskId() == diskId) {
            UNIT_ASSERT_C(!v.GetIsLocal(), "Attempt to pull local volume");
            v.SetIsLocal(true);
            return;
        }
    }
    UNIT_FAIL("Unknown volume: " << diskId);
}

void TVolumeBalancerStateTestEnv::Simulate(TDuration elapsed, ui64 cpuLack)
{
    while (elapsed > TDuration::Zero()) {
        auto timeSinceLastStatsUpdate = TDuration::FromValue(
            Now.GetValue() % UpdateCountersInterval.GetValue());
        auto timeBeforeNextStatsUpdate =
            UpdateCountersInterval - timeSinceLastStatsUpdate;
        if (elapsed < timeBeforeNextStatsUpdate) {
            Now += elapsed;
            AddCounters(elapsed);
            break;
        }
        Now += timeBeforeNextStatsUpdate;
        elapsed -= timeBeforeNextStatsUpdate;
        AddCounters(timeBeforeNextStatsUpdate);
        State.UpdateVolumeStats(VolumeStats, VolumeSufferMap, cpuLack, Now);
        // The stats service resets the delta after each statistics request.
        for (auto& v: VolumeStats) {
            v.SetReadBlobCount(0);
            v.SetWriteBlobCount(0);
            v.SetReadBlobBytes(0);
            v.SetWriteBlobBytes(0);
        }
    }
}

void TVolumeBalancerStateTestEnv::AddCounters(const TDuration elapsed)
{
    for (auto& v: VolumeStats) {
        UNIT_ASSERT(LoadProfiles.contains(v.GetDiskId()));
        const auto& [readBlobIops, writeBlobIops, readBlobBW, writeBlobBW] =
            LoadProfiles[v.GetDiskId()];
        v.SetReadBlobCount(
            v.GetReadBlobCount() + readBlobIops * elapsed.SecondsFloat());
        v.SetWriteBlobCount(
            v.GetWriteBlobCount() + writeBlobIops * elapsed.SecondsFloat());
        v.SetReadBlobBytes(
            v.GetReadBlobBytes() + readBlobBW * elapsed.SecondsFloat());
        v.SetWriteBlobBytes(
            v.GetWriteBlobBytes() + writeBlobBW * elapsed.SecondsFloat());
    }
}

TString TVolumeBalancerStateTestEnv::GetVolumeToPush() const
{
    return State.GetVolumeToPush();
}

TString TVolumeBalancerStateTestEnv::GetVolumeToPull() const
{
    return State.GetVolumeToPull();
}

////////////////////////////////////////////////////////////////////////////////

TDuration WholeCyclesOver(const TDuration duration)
{
    return UpdateCountersInterval *
           ceil(
               duration.SecondsFloat() / UpdateCountersInterval.SecondsFloat());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeBalancerStateTest)
{
    static void TestSelectVolumeForPreemption(
        NProto::EVolumePreemptionType preemptionType,
        const TString& expectedDiskId)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                preemptionType,
                /*volumeBalancerEnabled=*/true,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true)),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy, .SufferCount = 1},
                {.DiskId = "vol1", .LoadProfile = LoadLight, .SufferCount = 10},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL(expectedDiskId, testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldSelectHaviestVolumeForPreemption)
    {
        TestSelectVolumeForPreemption(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            "vol1");
    }

    Y_UNIT_TEST(ShouldSelectHeaviestVolumeForPreemptionWithIoCostOrdering)
    {
        TestSelectVolumeForPreemption(
            NProto::PREEMPTION_MOVE_IO_COST_MOST_HEAVY,
            "vol0");
    }

    Y_UNIT_TEST(ShouldSelectLightestVolumeForPreemption)
    {
        TestSelectVolumeForPreemption(
            NProto::PREEMPTION_MOVE_LEAST_HEAVY,
            "vol0");
    }

    Y_UNIT_TEST(ShouldSelectLightestVolumeForPreemptionWithIoCostOrdering)
    {
        TestSelectVolumeForPreemption(
            NProto::PREEMPTION_MOVE_IO_COST_LEAST_HEAVY,
            "vol1");
    }

    static void TestNotPreemptVolumeIfLoadIsBelowThreshold(
        NProto::EVolumePreemptionType preemptionType)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                preemptionType,
                /*volumeBalancerEnabled=*/true,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true)),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy, .SufferCount = 10},
                {.DiskId = "vol1", .LoadProfile = LoadLight, .SufferCount = 1},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldNotPreemptVolumeIfLoadIsBelowThreshold)
    {
        TestNotPreemptVolumeIfLoadIsBelowThreshold(
            NProto::PREEMPTION_MOVE_MOST_HEAVY);
    }

    Y_UNIT_TEST(ShouldNotPreemptVolumeIfLoadIsBelowThresholdWithIoCostOrdering)
    {
        TestNotPreemptVolumeIfLoadIsBelowThreshold(
            NProto::PREEMPTION_MOVE_IO_COST_MOST_HEAVY);
    }

    static void TestReturnVolumeIfLoadIsBelowThreshold(
        NProto::EVolumePreemptionType preemptionType)
    {
        auto storageConfig = CreateStorageConfig(
            preemptionType,
            /*volumeBalancerEnabled=*/true,
            DefaultCpuThreshold,
            CreateFeatureConfig("Balancer", {}, true));

        TVolumeBalancerStateTestEnv testEnv(
            storageConfig,
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy, .SufferCount = 10},
                {.DiskId = "vol1", .LoadProfile = LoadLight, .SufferCount = 1},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        testEnv.Push("vol0");

        // vol0 registered as preempted, PullInterval set
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        // vol0 should be nominated for pull at the first cycle after
        // InitialPullDelay has passed
        testEnv.Simulate(WholeCyclesOver(InitialPullDelay), CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldReturnVolumeIfLoadIsBelowThreshold)
    {
        TestReturnVolumeIfLoadIsBelowThreshold(
            NProto::PREEMPTION_MOVE_MOST_HEAVY);
    }

    Y_UNIT_TEST(ShouldReturnVolumeIfLoadIsBelowThresholdWithIoCostOrdering)
    {
        TestReturnVolumeIfLoadIsBelowThreshold(
            NProto::PREEMPTION_MOVE_IO_COST_MOST_HEAVY);
    }

    Y_UNIT_TEST(ShouldNotSelectVolumeFromBlacklistedCloud)
    {
        auto storageConfig = CreateStorageConfig(
            NProto::PREEMPTION_MOVE_IO_COST_MOST_HEAVY,
            /*volumeBalancerEnabled=*/false,
            DefaultCpuThreshold,
            CreateFeatureConfig("Balancer", {{"cloudid1", "folderid1"}}, true));

        TVolumeBalancerStateTestEnv testEnv(
            storageConfig,
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0",
                 .CloudId = "cloudid1",
                 .FolderId = "folderid1",
                 .LoadProfile = LoadHeavy},
                {.DiskId = "vol1",
                 .CloudId = "cloudid2",
                 .FolderId = "folderid2",
                 .LoadProfile = LoadLight},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL("vol1", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
    }

    static void TestReturnVolumeOnlyAfterPullDelay(
        NProto::EVolumePreemptionType preemptionType)
    {
        auto storageConfig = CreateStorageConfig(
            preemptionType,
            /*volumeBalancerEnabled=*/true,
            DefaultCpuThreshold,
            CreateFeatureConfig("Balancer", {}, true));

        TVolumeBalancerStateTestEnv testEnv(
            storageConfig,
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy, .SufferCount = 10},
                {.DiskId = "vol1", .LoadProfile = LoadLight, .SufferCount = 1},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        testEnv.Push("vol0");

        // vol0 registered as preempted, PullInterval set
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        // no volume should be nominated for pull before InitialPullDelay
        // has passed
        for (int i = 1; i * UpdateCountersInterval < InitialPullDelay; ++i) {
            testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
        }

        // vol0 must be nominated for pull at the first cycle after
        // InitialPullDelay has passed
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldReturnVolumeOnlyAfterPullDelay)
    {
        TestReturnVolumeOnlyAfterPullDelay(NProto::PREEMPTION_MOVE_MOST_HEAVY);
    }

    Y_UNIT_TEST(ShouldReturnVolumeOnlyAfterPullDelayWithIoCostOrdering)
    {
        TestReturnVolumeOnlyAfterPullDelay(
            NProto::PREEMPTION_MOVE_IO_COST_MOST_HEAVY);
    }

    static void TestResetPullDelayAfterCooldown(
        NProto::EVolumePreemptionType preemptionType)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                preemptionType,
                /*volumeBalancerEnabled=*/true,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true)),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy, .SufferCount = 10},
                {.DiskId = "vol1", .LoadProfile = LoadLight, .SufferCount = 1},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        testEnv.Push("vol0");

        // vol0 registered as preempted, PullInterval set
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        // no volume should be nominated for pull before InitialPullDelay
        // has passed
        for (int i = 1; i * UpdateCountersInterval < InitialPullDelay; ++i) {
            testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
        }

        // vol0 must be nominated for pull at the first cycle after
        // InitialPullDelay has passed
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPull());

        testEnv.Pull("vol0");

        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        testEnv.Push("vol0");

        // vol0 registered as preempted, PullInterval set
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        // no volume should be nominated for pull before 2 * InitialPullDelay
        // has passed
        for (int i = 1; i * UpdateCountersInterval < 2 * InitialPullDelay; ++i)
        {
            testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
        }

        // vol0 must be nominated for pull at the first cycle after
        // 2 * InitialPullDelay has passed
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPull());

        testEnv.Pull("vol0");

        // Register the successful pull before starting the cooldown
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        // Idle for InitialPullDelay so pull delay for vol0 is reset
        testEnv.Simulate(WholeCyclesOver(InitialPullDelay), CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        // Push again
        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        testEnv.Push("vol0");

        // vol0 registered as preempted, PullInterval set
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        // no volume should be nominated for pull before InitialPullDelay
        // has passed
        for (int i = 1; i * UpdateCountersInterval < InitialPullDelay; ++i) {
            testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
        }

        // vol0 must be nominated for pull at the first cycle after
        // InitialPullDelay has passed
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldResetPullDelayAfterCooldown)
    {
        TestResetPullDelayAfterCooldown(NProto::PREEMPTION_MOVE_MOST_HEAVY);
    }

    Y_UNIT_TEST(ShouldResetPullDelayAfterCooldownWithIoCostOrdering)
    {
        TestResetPullDelayAfterCooldown(NProto::PREEMPTION_MOVE_IO_COST_MOST_HEAVY);
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
            CreateSwitch(true),
            CreateLoggingService("null")
        );
        TInstant now = TInstant::Seconds(0);

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats({.DiskId = "vol0"}),
                CreateVolumeStats({.DiskId = "vol1"})};

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

            UNIT_ASSERT(state.GetVolumeToPush() == "vol0");
            UNIT_ASSERT(!state.GetVolumeToPull());
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats(
                    {.DiskId = "vol0",
                     .IsLocal = false,
                     .Source = NProto::SOURCE_MANUAL}),
                CreateVolumeStats({.DiskId = "vol1"})};

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;
            perfMap["vol1"] = 1;

            // Register the preempted volume and start its pull delay
            state.UpdateVolumeStats(vols, perfMap, CpuUnderload, now);

            now += InitialPullDelay;
            state.UpdateVolumeStats(vols, perfMap, CpuUnderload, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());

            // Changing only the preemption source makes the volume eligible
            // for pull, since the delay has already elapsed
            vols[0].SetPreemptionSource(NProto::SOURCE_BALANCER);
            state.UpdateVolumeStats(vols, perfMap, CpuUnderload, now);

            UNIT_ASSERT_VALUES_EQUAL("", state.GetVolumeToPush());
            UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPull());
        }
    }

    Y_UNIT_TEST(ShouldSelectLightestVolumeWhenEnabledWithPreemptionTypeNone)
    {
        TVolumeBalancerState state(
            CreateStorageConfig(
                NProto::PREEMPTION_NONE,
                /*volumeBalancerEnabled=*/true,
                DefaultCpuThreshold,
                /*featuresConfig=*/nullptr),
            CreateDiagnosticsConfig(),
            CreateSwitch(true),
            CreateLoggingService("null"));
        TInstant now = TInstant::Seconds(0);

        TVector<NProto::TVolumeBalancerDiskStats> vols{
            CreateVolumeStats({.DiskId = "vol0"}),
            CreateVolumeStats({.DiskId = "vol1"})};

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
            CreateSwitch(false),
            CreateLoggingService("null"));
        TInstant now = TInstant::Seconds(0);

        TVolumeBalancerState::TPerfGuaranteesMap perfMap;
        perfMap["vol0"] = 10;
        perfMap["vol1"] = 1;

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats({.DiskId = "vol0", .IsLocal = true}),
                CreateVolumeStats({.DiskId = "vol1", .IsLocal = true})};

            state.UpdateVolumeStats(vols, perfMap, 80, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats({.DiskId = "vol0", .IsLocal = false}),
                CreateVolumeStats({.DiskId = "vol1", .IsLocal = false})};

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
            CreateSwitch(true),
            CreateLoggingService("null")
        );

        for (ui32 i = 0; i < kinds.size(); ++i) {
            TInstant now = TInstant::Seconds(0);

            TVector<NProto::TVolumeBalancerDiskStats> vols {
                CreateVolumeStats({.DiskId = "vol0", .IsLocal = true, .Kind = kinds[i]})
            };

            TVolumeBalancerState::TPerfGuaranteesMap perfMap;
            perfMap["vol0"] = 10;

            state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());
        }
    }

    Y_UNIT_TEST(ShouldOnlyPullVolumesInWarningState)
    {
        NProto::TStorageServiceConfig config;
        config.SetVolumeBalancerAutoWarningEnabled(true);
        config.SetVolumeBalancerWarningSampleCount(2);
        auto storageConfig = CreateStorageConfig(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            /*volumeBalancerEnabled=*/true,
            70,
            /*featuresConfig=*/nullptr,
            InitialPullDelay,
            std::move(config));

        TVolumeBalancerState state(
            storageConfig,
            CreateDiagnosticsConfig(),
            CreateSwitch(true),
            CreateLoggingService("null"));

        state.SetStatus(NPrivateProto::EBalancerOpStatus::WARNING);
        TInstant now = TInstant::Seconds(0);

        TVolumeBalancerState::TPerfGuaranteesMap perfMap;
        perfMap["vol0"] = 15;
        perfMap["vol1"] = 1;

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats({.DiskId = "vol0"}),
                CreateVolumeStats({.DiskId = "vol1"})};

            state.UpdateVolumeStats(vols, perfMap, 80, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats({.DiskId = "vol0", .IsLocal = false}),
                CreateVolumeStats({.DiskId = "vol1"})};

            state.UpdateVolumeStats(vols, perfMap, 80, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());

            now += storageConfig->GetInitialPullDelay();
            state.UpdateVolumeStats(vols, perfMap, 80, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPull());
        }
    }

    Y_UNIT_TEST(ShouldTurnIntoWarningStateIfPreemptedVolumeSuffers)
    {
        NProto::TStorageServiceConfig config;
        config.SetVolumeBalancerAutoWarningEnabled(true);
        config.SetVolumeBalancerWarningSampleCount(2);
        auto storageConfig = CreateStorageConfig(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            /*volumeBalancerEnabled=*/true,
            70,
            /*featuresConfig=*/nullptr,
            InitialPullDelay,
            std::move(config));

        TVolumeBalancerState state(
            storageConfig,
            CreateDiagnosticsConfig(),
            CreateSwitch(true),
            CreateLoggingService("null"));
        TInstant now = TInstant::Seconds(0);

        TVolumeBalancerState::TPerfGuaranteesMap perfMap;
        perfMap["vol0"] = 15;
        perfMap["vol1"] = 1;

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats({.DiskId = "vol0"}),
                CreateVolumeStats({.DiskId = "vol1"})};

            state.UpdateVolumeStats(vols, perfMap, 80, now);

            UNIT_ASSERT_VALUES_EQUAL("vol0", state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());
            UNIT_ASSERT_VALUES_EQUAL(
                state.GetStatus(),
                NPrivateProto::EBalancerOpStatus::ENABLE);
        }

        {
            TVector<NProto::TVolumeBalancerDiskStats> vols{
                CreateVolumeStats({.DiskId = "vol0", .IsLocal = false}),
                CreateVolumeStats({.DiskId = "vol1", .IsLocal = false})};

            state.UpdateVolumeStats(vols, perfMap, 80, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull());
            UNIT_ASSERT_VALUES_EQUAL(
                state.GetStatus(),
                NPrivateProto::EBalancerOpStatus::ENABLE);

            now += storageConfig->GetInitialPullDelay();
            state.UpdateVolumeStats(vols, std::move(perfMap), 80, now);

            UNIT_ASSERT(!state.GetVolumeToPush());
            UNIT_ASSERT(!state.GetVolumeToPull().empty());
            UNIT_ASSERT_VALUES_EQUAL(
                state.GetStatus(),
                NPrivateProto::EBalancerOpStatus::WARNING);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage

template <>
inline void Out<NCloud::NBlockStore::NPrivateProto::EBalancerOpStatus>(
    IOutputStream& out,
    const NCloud::NBlockStore::NPrivateProto::EBalancerOpStatus status)
{
    out << EBalancerOpStatus_Name(status);
}
