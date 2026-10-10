#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer_state.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/features/features_config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

TStorageConfigPtr CreateStorageConfig(
    NProto::EVolumePreemptionType type,
    ui32 cpuLackThreshold,
    NFeatures::TFeaturesConfigPtr featuresConfig,
    TDuration initialPullDelay)
{
    NProto::TStorageServiceConfig storageConfig;
    storageConfig.SetVolumePreemptionType(type);
    storageConfig.SetCpuLackThreshold(cpuLackThreshold);
    if (!featuresConfig) {
        NProto::TFeaturesConfig config;
        featuresConfig = std::make_shared<NFeatures::TFeaturesConfig>(config);
    }
    storageConfig.SetInitialPullDelay(initialPullDelay.MilliSeconds());

    return std::make_shared<TStorageConfig>(
        storageConfig,
        std::move(featuresConfig));
}

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
                *feature->MutableBlacklist()->MutableCloudIds()->Add() =
                    c.first;
                *feature->MutableBlacklist()->MutableFolderIds()->Add() =
                    c.second;
            }
        } else {
            for (const auto& c: list) {
                *feature->MutableWhitelist()->MutableCloudIds()->Add() =
                    c.first;
                *feature->MutableWhitelist()->MutableFolderIds()->Add() =
                    c.second;
            }
        }
    }
    return std::make_shared<NFeatures::TFeaturesConfig>(config);
}

TDiagnosticsConfigPtr CreateDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig config;
    ParseProtoTextFromString(
        R"(
        SsdPerfSettings {
          Write {
            Iops: 100
            Bandwidth: 409600
          }
          Read {
            Iops: 100
            Bandwidth: 409600
          }
        })",
        config);
    return std::make_shared<TDiagnosticsConfig>(config);
}

////////////////////////////////////////////////////////////////////////////////

struct TVolumeLoadProfile
{
    ui64 ReadBlobIops = 0;
    ui64 WriteBlobIops = 0;
    ui64 ReadBlobBW = 0;
    ui64 WriteBlobBW = 0;
};

constexpr TVolumeLoadProfile LoadHeavy{
    100,
    100 * 4_KB,
    100,
    100 * 4_KB,
};

constexpr TVolumeLoadProfile LoadLight{
    10,
    10 * 4_KB,
    10,
    10 * 4_KB,
};

constexpr TVolumeLoadProfile LoadZero{};

struct TVolumeStatsRecipe
{
    const TString DiskId = "";
    const TString CloudId = "";
    const TString FolderId = "";
    const bool IsLocal = true;
    const NProto::EPreemptionSource Source = NProto::SOURCE_BALANCER;
    const NProto::EStorageMediaKind Kind = NProto::STORAGE_MEDIA_SSD;
    const TVolumeLoadProfile LoadProfile = LoadZero;
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

constexpr ui64 DefaultCpuThreshold = 70;
constexpr ui64 CpuOverload = 80;
constexpr ui64 CpuUnderload = 40;
constexpr TDuration InitialPullDelay = TDuration::Minutes(1);

class TVolumeBalancerStateTestEnv
{
public:
    TVolumeBalancerStateTestEnv(
        TStorageConfigPtr storageConfig,
        TDiagnosticsConfigPtr diagnosticsConfig,
        const TVector<TVolumeStatsRecipe>& volumes);

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
    const TVector<TVolumeStatsRecipe>& volumes)
    : State(std::move(storageConfig), std::move(diagnosticsConfig))
    , Now(TInstant::Zero())
{
    for (const auto& recipe: volumes) {
        VolumeStats.push_back(CreateVolumeStats(recipe));
        LoadProfiles[recipe.DiskId] = recipe.LoadProfile;
        VolumeSufferMap[recipe.DiskId] = 0;
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
    Y_UNIT_TEST(ShouldSelectHaviestVolumeForPreemption)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true),
                InitialPullDelay),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy},
                {.DiskId = "vol1", .LoadProfile = LoadLight},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldSelectLightestVolumeForPreemption)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_LEAST_HEAVY,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true),
                InitialPullDelay),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy},
                {.DiskId = "vol1", .LoadProfile = LoadLight},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL("vol1", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldNotPreemptVolumeIfLoadIsBelowThreshold)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true),
                InitialPullDelay),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy},
                {.DiskId = "vol1", .LoadProfile = LoadLight},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldReturnVolumeIfLoadIsBelowThreshold)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true),
                InitialPullDelay),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy},
                {.DiskId = "vol1", .LoadProfile = LoadLight},
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

    Y_UNIT_TEST(ShouldNotSelectVolumeFromBlacklistedCloud)
    {
        // This test needs to be revised within
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                NProto::PREEMPTION_NONE,
                DefaultCpuThreshold,
                CreateFeatureConfig(
                    "Balancer",
                    {{"cloudid1", "folderid1"}},
                    true),
                InitialPullDelay),
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

    Y_UNIT_TEST(ShouldReturnVolumeOnlyAfterPullDelay)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true),
                InitialPullDelay),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy},
                {.DiskId = "vol1", .LoadProfile = LoadLight},
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

    Y_UNIT_TEST(ShouldResetPullDelayAfterCooldown)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true),
                InitialPullDelay),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy},
                {.DiskId = "vol1", .LoadProfile = LoadLight},
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

        // 2 cycles so Cost can be calculated
        testEnv.Simulate(2 * UpdateCountersInterval, CpuOverload);

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

    Y_UNIT_TEST(ShouldNotReturnManuallyPreemptedVolume)
    {
        TVolumeBalancerStateTestEnv testEnv(
            CreateStorageConfig(
                NProto::PREEMPTION_MOVE_MOST_HEAVY,
                DefaultCpuThreshold,
                CreateFeatureConfig("Balancer", {}, true),
                InitialPullDelay),
            CreateDiagnosticsConfig(),
            {
                {.DiskId = "vol0",
                 .Source = NProto::SOURCE_INITIAL_MOUNT,
                 .LoadProfile = LoadHeavy},
            });

        testEnv.Simulate(UpdateCountersInterval, CpuOverload);

        UNIT_ASSERT_VALUES_EQUAL("vol0", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        testEnv.Push("vol0");

        // vol0 registered as preempted, PullInterval set
        testEnv.Simulate(UpdateCountersInterval, CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());

        // vol0 should not be nominated for pull due to SOURCE_INITIAL_MOUNT
        testEnv.Simulate(WholeCyclesOver(InitialPullDelay), CpuUnderload);

        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
        UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
    }

    Y_UNIT_TEST(ShouldNotMoveNonKikimrDisks)
    {
        const TVector kinds{
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD_MIRROR2,
            NProto::STORAGE_MEDIA_SSD_LOCAL,
            NProto::STORAGE_MEDIA_SSD_MIRROR3};

        for (const auto kind: kinds) {
            TVolumeBalancerStateTestEnv testEnv(
                CreateStorageConfig(
                    NProto::PREEMPTION_MOVE_MOST_HEAVY,
                    DefaultCpuThreshold,
                    CreateFeatureConfig("Balancer", {}, true),
                    InitialPullDelay),
                CreateDiagnosticsConfig(),
                {
                    {.DiskId = "vol0", .Kind = kind, .LoadProfile = LoadHeavy},
                });

            testEnv.Simulate(UpdateCountersInterval, CpuOverload);

            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPush());
            UNIT_ASSERT_VALUES_EQUAL("", testEnv.GetVolumeToPull());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
