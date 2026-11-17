#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/configs_manager/events.h>
#include <cloud/blockstore/libs/configs_manager/public.h>
#include <cloud/blockstore/libs/configs_manager/testlib/dynconfig.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/diagnostics/volume_balancer_switch.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume_balancer.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer.h>
#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer_ut_helpers.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>
#include <cloud/storage/core/libs/features/features_config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeState
{
    TString DiskId;
    bool IsLocal = true;
    NProto::EPreemptionSource Source = NProto::SOURCE_NONE;
    TVolumeLoadProfile LoadProfile = LoadZero;
};

////////////////////////////////////////////////////////////////////////////////

NProto::TVolumeBalancerDiskStats CreateVolumeStats(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId,
    const bool isLocal,
    const TVolumeLoadProfile& counters)
{
    NProto::TVolumeBalancerDiskStats stats;
    stats.SetDiskId(diskId);
    stats.SetCloudId(cloudId);
    stats.SetFolderId(folderId);
    stats.SetIsLocal(isLocal);
    stats.SetPreemptionSource(NProto::SOURCE_BALANCER);
    stats.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);

    stats.SetReadBlobCount(counters.ReadBlobIops);
    stats.SetWriteBlobCount(counters.WriteBlobIops);
    stats.SetReadBlobBytes(counters.ReadBlobBW);
    stats.SetWriteBlobBytes(counters.WriteBlobBW);

    return stats;
}

NProto::TVolumeBalancerDiskStats CreateVolumeStats(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId,
    const bool isLocal,
    const TVolumeLoadProfile& counters,
    const NProto::EPreemptionSource source)
{
    auto stats =
        CreateVolumeStats(diskId, cloudId, folderId, isLocal, counters);
    stats.SetPreemptionSource(source);

    return stats;
}

////////////////////////////////////////////////////////////////////////////////

class TVolumeBalancerConfigBuilder
{
private:
    NProto::TStorageServiceConfig StorageConfig;

public:
    TVolumeBalancerConfigBuilder& WithType(NProto::EVolumePreemptionType mode)
    {
        StorageConfig.SetVolumePreemptionType(mode);
        return *this;
    }

    TVolumeBalancerConfigBuilder& WithInitialPullDelay(TDuration delay)
    {
        StorageConfig.SetInitialPullDelay(delay.MilliSeconds());
        return *this;
    }

    TVolumeBalancerConfigBuilder& WithMaxInProgress(ui64 maxInProgress)
    {
        StorageConfig.SetVolumeBalancerMaxInProgress(maxInProgress);
        return *this;
    }

    TVolumeBalancerConfigBuilder& WithEnabled(bool enabled)
    {
        StorageConfig.SetVolumeBalancerEnabled(enabled);
        return *this;
    }

    NProto::TStorageServiceConfig Build()
    {
        return StorageConfig;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeStatsTestMock final: public IVolumeStats
{
    TVolumePerfStatuses PerfStats;

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) override
    {
        Y_UNUSED(volume);
        Y_UNUSED(clientId);
        Y_UNUSED(instanceId);
        return true;
    }

    void UnmountVolume(const TString& diskId, const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
    }

    IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);

        return nullptr;
    }

    IVolumeInfoPinPtr PinVolumeInfo(
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);

        return nullptr;
    }

    NProto::EStorageMediaKind GetStorageMediaKind(
        const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    }

    ui32 GetBlockSize(const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return 0;
    }

    void TrimVolumes() override
    {}

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }

    void SetPerfStats(TVolumePerfStatuses perfStats)
    {
        PerfStats = std::move(perfStats);
    }

    TVolumePerfStatuses GatherVolumePerfStatuses() override
    {
        return PerfStats;
    }

    TDowntimeHistory GetDowntimeHistory(const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return {};
    }

    bool HasStorageConfigPatch(const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return {};
    }

    void SetServingCellHost(
        const TString& diskId,
        const TString& clientId,
        ui64 connectionId,
        const TString& cellId,
        const TString& fqdn) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
        Y_UNUSED(connectionId);
        Y_UNUSED(cellId);
        Y_UNUSED(fqdn);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStatsFetcherMock: public NCloud::NStorage::IStatsFetcher
{
    TResultOrError<TDuration> Value = TDuration::Zero();

    void SetCpuWaitValue(TResultOrError<TDuration> value)
    {
        Value = std::move(value);
    }

    TResultOrError<TDuration> GetCpuWait() override
    {
        return Value;
    }
};

////////////////////////////////////////////////////////////////////////////////

using EChangeBindingOp =
    TEvService::TEvChangeVolumeBindingRequest::EChangeBindingOp;
using EBalancerStatus = NPrivateProto::EBalancerOpStatus;

class TVolumeBalancerTestEnv
{
private:
    TTestEnv TestEnv;
    TActorId Sender;

public:
    std::shared_ptr<TVolumeStatsTestMock> VolumeStats;
    std::shared_ptr<TStatsFetcherMock> Fetcher;

public:
    TVolumeBalancerTestEnv()
    {
        Sender = TestEnv.GetRuntime().AllocateEdgeActor();
        VolumeStats = std::make_shared<TVolumeStatsTestMock>();
        Fetcher = std::make_shared<TStatsFetcherMock>();
    }

    TActorId GetEdgeActor() const
    {
        return Sender;
    }

    TTestActorRuntime& GetRuntime()
    {
        return TestEnv.GetRuntime();
    }

    TActorId Register(IActorPtr actor)
    {
        auto actorId = TestEnv.GetRuntime().Register(actor.release());
        TestEnv.GetRuntime().EnableScheduleForActor(actorId);

        return actorId;
    }

    void AdjustTime()
    {
        TestEnv.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(15));
        TestEnv.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
    }

    void AdjustTime(TDuration interval)
    {
        TestEnv.GetRuntime().AdvanceCurrentTime(interval);
        TestEnv.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
    }

    void Send(const TActorId& recipient, IEventBasePtr event)
    {
        TestEnv.GetRuntime().Send(
            new IEventHandle(recipient, Sender, event.release()));
    }

    void DispatchEvents()
    {
        TestEnv.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration());
    }

    void DispatchEvents(TDuration timeout)
    {
        TestEnv.GetRuntime().DispatchEvents(TDispatchOptions(), timeout);
    }

    THolder<TEvService::TEvChangeVolumeBindingRequest> GrabBindingRequest()
    {
        return TestEnv.GetRuntime()
            .GrabEdgeEvent<TEvService::TEvChangeVolumeBindingRequest>(
                TDuration());
    }

    THolder<TEvService::TEvGetVolumeStatsRequest> GrabVolumesStatsRequest()
    {
        return TestEnv.GetRuntime()
            .GrabEdgeEvent<TEvService::TEvGetVolumeStatsRequest>(TDuration());
    }

    void SendChangeVolumeBindingResponse(
        TActorId receiver,
        TString diskId,
        NProto::TError error)
    {
        Send(
            receiver,
            std::make_unique<TEvService::TEvChangeVolumeBindingResponse>(
                error,
                diskId));
    }

    void SendVolumesStatsResponse(
        TActorId receiver,
        TVector<NProto::TVolumeBalancerDiskStats> volumes)
    {
        Send(
            receiver,
            std::make_unique<TEvService::TEvGetVolumeStatsResponse>(
                std::move(volumes)));
    }

    void SendConfigureVolumeBalancerRequest(
        TActorId receiver,
        EBalancerStatus status)
    {
        auto request = std::make_unique<
            TEvVolumeBalancer::TEvConfigureVolumeBalancerRequest>();
        request->Record.SetOpStatus(status);
        Send(receiver, std::move(request));
    }

    THolder<TEvVolumeBalancer::TEvConfigureVolumeBalancerResponse>
    GrabConfigureVolumeBalancerResponse()
    {
        return TestEnv.GetRuntime()
            .GrabEdgeEvent<
                TEvVolumeBalancer::TEvConfigureVolumeBalancerResponse>(
                TDuration());
    }
};

NFeatures::TFeaturesConfigPtr CreateFeatureConfig(
    const TString& featureName,
    const TVector<std::pair<TString, TString>>& list,
    bool blacklist = true)
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
            // All but blacklisted
            feature->SetCloudProbability(1);
            feature->SetFolderProbability(1);
        } else {
            for (const auto& c: list) {
                *feature->MutableWhitelist()->MutableCloudIds()->Add() =
                    c.first;
                *feature->MutableWhitelist()->MutableFolderIds()->Add() =
                    c.second;
            }
            // None but whitelisted
            feature->SetCloudProbability(0);
            feature->SetFolderProbability(0);
        }
    }
    return std::make_shared<NFeatures::TFeaturesConfig>(config);
}

IActorPtr CreateVolumeBalancerActor(
    TVolumeBalancerConfigBuilder& config,
    IVolumeStatsPtr volumeStats,
    NCloud::NStorage::IStatsFetcherPtr statsFetcher,
    TActorId serviceActorId,
    NFeatures::TFeaturesConfigPtr featureConfig = nullptr)
{
    NProto::TStorageServiceConfig storageConfig = config.Build();

    auto volumeBalancerSwitch = CreateVolumeBalancerSwitch();
    volumeBalancerSwitch->EnableVolumeBalancer();

    const auto featureConf =
        featureConfig ? featureConfig : CreateFeatureConfig("Balancer", {});

    return CreateVolumeBalancerActor(
        std::make_shared<TStorageConfig>(
            config.Build(),
            featureConf),
        CreateDiagnosticsConfig(),
        std::move(volumeStats),
        std::move(statsFetcher),
        std::move(volumeBalancerSwitch),
        std::move(serviceActorId));
}

TString RunState(
    TVolumeBalancerTestEnv& testEnv,
    TActorId actorId,
    TVector<TVolumeState> volumes,
    TVolumePerfStatuses perfStats,
    TResultOrError<double> cpuWait,
    TMaybe<EChangeBindingOp> expected,
    TDuration runFor,
    TMaybe<bool> expectedGentlePreemption = {})
{
    auto now = testEnv.GetRuntime().GetCurrentTime();
    while (testEnv.GetRuntime().GetCurrentTime() - now < runFor) {
        testEnv.AdjustTime();

        auto ev = testEnv.GrabBindingRequest();
        UNIT_ASSERT(!ev);

        auto statRequestEvent = testEnv.GrabVolumesStatsRequest();
        UNIT_ASSERT_C(statRequestEvent, "Expected a volume statistics request");

        TVector<NProto::TVolumeBalancerDiskStats> stats;
        for (const auto& v: volumes) {
            const auto intervalSeconds = UpdateCountersInterval.Seconds();
            const TVolumeLoadProfile delta{
                .ReadBlobIops = v.LoadProfile.ReadBlobIops * intervalSeconds,
                .WriteBlobIops = v.LoadProfile.WriteBlobIops * intervalSeconds,
                .ReadBlobBW = v.LoadProfile.ReadBlobBW * intervalSeconds,
                .WriteBlobBW = v.LoadProfile.WriteBlobBW * intervalSeconds,
            };
            auto stat = CreateVolumeStats(
                v.DiskId,
                "",
                "",
                v.IsLocal,
                delta,
                v.Source);

            stats.push_back(stat);
        }

        testEnv.VolumeStats->SetPerfStats(std::move(perfStats));
        testEnv.Fetcher->SetCpuWaitValue(
            HasError(cpuWait)
                ? TResultOrError<TDuration>(cpuWait.GetError())
                : TResultOrError<TDuration>(
                      cpuWait.GetResult() * TDuration::Seconds(15)));

        testEnv.SendVolumesStatsResponse(actorId, stats);

        testEnv.DispatchEvents(TDuration::Seconds(1));
    }

    auto ev = testEnv.GrabBindingRequest();
    if (expected) {
        UNIT_ASSERT(ev);
        UNIT_ASSERT(ev->Action == expected);
        if (expectedGentlePreemption.Defined()) {
            UNIT_ASSERT_VALUES_EQUAL(
                *expectedGentlePreemption,
                ev->UseGentlePreemption);
        }
        return ev->DiskId;
    }

    UNIT_ASSERT(!ev);
    return {};
}

auto SetupCriticalEvents(IMonitoringServicePtr monitoring)
{
    auto rootGroup =
        monitoring->GetCounters()->GetSubgroup("counters", "storage");

    auto serverGroup = rootGroup->GetSubgroup("component", "server");
    InitCriticalEventsCounter(serverGroup);

    return serverGroup;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeBalancerTest)
{
    static void TestPushVolumeToHiveControl(
        NProto::EVolumePreemptionType preemptionType,
        const TString& expectedDiskId)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(preemptionType).WithEnabled(true),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0", .LoadProfile = LoadHeavy},
                {.DiskId = "vol1", .LoadProfile = LoadLight},
            },
            {{"vol0", 1}, {"vol1", 10}},
            1,
            EChangeBindingOp::RELEASE_TO_HIVE,
            UpdateCountersInterval);

        UNIT_ASSERT_VALUES_EQUAL(expectedDiskId, diskId);
    }

    Y_UNIT_TEST(ShouldPushMostSufferingVolumeToHiveControl)
    {
        TestPushVolumeToHiveControl(NProto::PREEMPTION_MOVE_MOST_HEAVY, "vol1");
    }

    Y_UNIT_TEST(ShouldPushMostLoadedVolumeToHiveControl)
    {
        TestPushVolumeToHiveControl(
            NProto::PREEMPTION_MOVE_IO_COST_MOST_HEAVY,
            "vol0");
    }

    Y_UNIT_TEST(ShouldPushLeastSufferingVolumeToHiveControl)
    {
        TestPushVolumeToHiveControl(
            NProto::PREEMPTION_MOVE_LEAST_HEAVY,
            "vol0");
    }

    Y_UNIT_TEST(ShouldPushLeastLoadedVolumeToHiveControl)
    {
        TestPushVolumeToHiveControl(
            NProto::PREEMPTION_MOVE_IO_COST_LEAST_HEAVY,
            "vol1");
    }

    Y_UNIT_TEST(ShouldPushVolumesToHiveControlAndReturnBack)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY);
        config.WithInitialPullDelay(TDuration::Seconds(20));
        config.WithEnabled(true);

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1"},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            EChangeBindingOp::RELEASE_TO_HIVE,
            TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL("vol0", diskId);

        testEnv.SendChangeVolumeBindingResponse(
            volumeBindingActorID,
            "vol0",
            {});

        diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0",
                 .IsLocal = false,
                 .Source = NProto::SOURCE_BALANCER},
                {.DiskId = "vol1",
                 .IsLocal = true,
                 .Source = NProto::SOURCE_NONE},
            },
            {{"vol0", 10}, {"vol1", 1}},
            0.1,
            EChangeBindingOp::ACQUIRE_FROM_HIVE,
            TDuration::Seconds(15) + TDuration::Seconds(20));

        UNIT_ASSERT_VALUES_EQUAL("vol0", diskId);
    }

    Y_UNIT_TEST(ShouldNotPullVolumeIfItWasPreemptedByUser)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY);
        config.WithInitialPullDelay(TDuration::Seconds(20));
        config.WithEnabled(true);

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0", .IsLocal = true},
                {.DiskId = "vol1", .IsLocal = true},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            EChangeBindingOp::RELEASE_TO_HIVE,
            TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL("vol0", diskId);

        RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0",
                 .IsLocal = false,
                 .Source = NProto::SOURCE_MANUAL},
                {.DiskId = "vol1",
                 .IsLocal = true,
                 .Source = NProto::SOURCE_NONE},
            },
            {{"vol0", 10}, {"vol1", 1}},
            0.1,
            {},
            TDuration::Seconds(15) + TDuration::Seconds(20));

        UNIT_ASSERT_VALUES_EQUAL("vol0", diskId);
    }

    Y_UNIT_TEST(ShouldNotDoAnythingIfBalancerIsDisabled)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor(),
            CreateFeatureConfig("Balancer", {}, false)));

        testEnv.DispatchEvents();

        RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1"},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            {},
            TDuration::Seconds(15));
    }

    Y_UNIT_TEST(ShouldNotDoAnythingIfBalancerIsDisabledViaPrivateApi)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY);
        config.WithEnabled(true);

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        testEnv.SendConfigureVolumeBalancerRequest(
            volumeBindingActorID,
            EBalancerStatus::DISABLE);
        auto response = testEnv.GrabConfigureVolumeBalancerResponse();

        UNIT_ASSERT_VALUES_EQUAL(
            EBalancerStatus::ENABLE,
            response->Record.GetOpStatus());

        RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1"},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            {},
            TDuration::Seconds(15));
    }

    Y_UNIT_TEST(ShouldNotDoAnythingIfBalancerDisabledWithConfig)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBalancerActorId = testEnv.Register(CreateVolumeBalancerActor(
            config.WithEnabled(false),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor(),
            CreateFeatureConfig("Balancer", {}, false)));

        testEnv.DispatchEvents();

        RunState(
            testEnv,
            volumeBalancerActorId,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1"},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            {},
            TDuration::Seconds(15));
    }

    Y_UNIT_TEST(ShouldSetCpuWaitCounter)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto serverGroup = SetupCriticalEvents(monitoring);

        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY)
            .WithEnabled(true),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {{.DiskId = "vol0"}, {.DiskId = "vol1"}},
            {{"vol0", 10}, {"vol1", 1}},
            .9,
            EChangeBindingOp::RELEASE_TO_HIVE,
            TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL("vol0", diskId);

        auto counters = testEnv.GetRuntime()
                            .GetAppData(0)
                            .Counters->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server");

        auto cpuWaitCounter = counters->GetCounter("CpuWait", false);
        UNIT_ASSERT_VALUES_UNEQUAL(0, cpuWaitCounter->Val());
        UNIT_ASSERT(cpuWaitCounter->Val() <= 90);

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            serverGroup
                ->GetCounter("AppCriticalEvents/CpuWaitCounterReadError", true)
                ->Val());
    }

    Y_UNIT_TEST(ShouldNotDoAnythingIfBalancerIsNotActivated)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            std::make_shared<TStorageConfig>(
                config.Build(),
                CreateFeatureConfig("Balancer", {})),
            CreateDiagnosticsConfig(),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            CreateVolumeBalancerSwitch(),
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1"},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            {},
            TDuration::Seconds(15));
    }

    Y_UNIT_TEST(ShouldSetCountersForPreemptedVolumes)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY)
            .WithEnabled(true),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto root = testEnv.GetRuntime()
                        .GetAppData(0)
                        .Counters->GetSubgroup("counters", "blockstore")
                        ->GetSubgroup("component", "service");

        auto manuallyPreempted = root->GetCounter("ManuallyPreempted", false);
        auto balancerPreempted = root->GetCounter("BalancerPreempted", false);
        auto initiallyPreempted = root->GetCounter("InitiallyPreempted", false);

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0", .Source = NProto::SOURCE_NONE},
                {.DiskId = "vol1", .Source = NProto::SOURCE_MANUAL},
                {.DiskId = "vol2", .Source = NProto::SOURCE_MANUAL},
                {.DiskId = "vol3", .Source = NProto::SOURCE_BALANCER},
                {.DiskId = "vol4", .Source = NProto::SOURCE_BALANCER},
                {.DiskId = "vol5", .Source = NProto::SOURCE_BALANCER},
                {.DiskId = "vol6", .Source = NProto::SOURCE_INITIAL_MOUNT},
                {.DiskId = "vol7", .Source = NProto::SOURCE_INITIAL_MOUNT},
                {.DiskId = "vol8", .Source = NProto::SOURCE_INITIAL_MOUNT},
                {.DiskId = "vol9", .Source = NProto::SOURCE_INITIAL_MOUNT},
            },
            {{"vol0", 10}, {"vol1", 1}},
            .9,
            EChangeBindingOp::RELEASE_TO_HIVE,
            TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL("vol0", diskId);

        UNIT_ASSERT_VALUES_EQUAL(2, manuallyPreempted->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, balancerPreempted->Val());
        UNIT_ASSERT_VALUES_EQUAL(4, initiallyPreempted->Val());
    }

    Y_UNIT_TEST(ShouldSetCpuWaitCounterReadError)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto serverGroup = SetupCriticalEvents(monitoring);

        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY)
            .WithEnabled(true),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1"},
            },
            {{"vol0", 10}, {"vol1", 1}},
            MakeError(E_INVALID_STATE),
            {},
            TDuration::Seconds(15));

        auto counters = testEnv.GetRuntime()
                            .GetAppData(0)
                            .Counters->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server");

        auto cpuWaitCounter = counters->GetCounter("CpuWait", false);
        UNIT_ASSERT_VALUES_EQUAL(0, cpuWaitCounter->Val());

        UNIT_ASSERT_VALUES_UNEQUAL(
            0,
            serverGroup
                ->GetCounter("AppCriticalEvents/CpuWaitCounterReadError", true)
                ->Val());
    }

    Y_UNIT_TEST(ShouldRespectInProgressLimitDisabled)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(NProto::PREEMPTION_MOVE_LEAST_HEAVY)
                .WithEnabled(true)
                .WithMaxInProgress(0),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1"},
                {.DiskId = "vol2"},
            },
            {{"vol0", 10}, {"vol1", 1}, {"vol2", 2}},
            1,
            EChangeBindingOp::RELEASE_TO_HIVE,
            TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL("vol1", diskId);

        auto diskId2 = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1"},
                {.DiskId = "vol2"},
            },
            {{"vol0", 10}, {"vol1", 1}, {"vol2", 2}},
            1,
            EChangeBindingOp::RELEASE_TO_HIVE,
            TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL("vol2", diskId2);
    }

    Y_UNIT_TEST(ShouldRespectInProgressLimitSetToOne)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(NProto::PREEMPTION_MOVE_LEAST_HEAVY)
                .WithEnabled(true)
                .WithMaxInProgress(1),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1"},
                {.DiskId = "vol2"},
            },
            {{"vol0", 10}, {"vol1", 1}, {"vol2", 2}},
            1,
            EChangeBindingOp::RELEASE_TO_HIVE,
            UpdateCountersInterval);

        UNIT_ASSERT_VALUES_EQUAL("vol1", diskId);

        // Expect to not get any preemption requests while first one
        // is in progress
        auto diskId2 = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1",
                 .IsLocal = false,
                 .Source = NProto::SOURCE_BALANCER},
                {.DiskId = "vol2"},
            },
            {{"vol0", 10}, {"vol1", 1}, {"vol2", 2}},
            1,
            {},
            UpdateCountersInterval);

        UNIT_ASSERT(diskId2.empty());

        testEnv.SendChangeVolumeBindingResponse(
            volumeBindingActorID,
            "vol1",
            {});

        auto diskId3 = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0"},
                {.DiskId = "vol1",
                 .IsLocal = false,
                 .Source = NProto::SOURCE_BALANCER},
                {.DiskId = "vol2"},
            },
            {{"vol0", 10}, {"vol1", 1}, {"vol2", 2}},
            1,
            EChangeBindingOp::RELEASE_TO_HIVE,
            UpdateCountersInterval);

        UNIT_ASSERT_VALUES_EQUAL("vol2", diskId3);
    }

    // Verify that dynamic config updates to VolumeBalancerEnabled stop and
    // resume RELEASE_TO_HIVE requests without restarting the actor.
    Y_UNIT_TEST(ShouldToggleBalancerFromDynamicConfig)
    {
        // Start with VolumeBalancerEnabled=true and prepare an update setting
        // it to false; keep the Balancer feature disabled.
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;
        config.WithEnabled(true).WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY);
        const auto actorId = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor(),
            CreateFeatureConfig("Balancer", {}, false)));
        testEnv.DispatchEvents();
        auto provider =
            std::make_shared<NTestDynconfig::TTestBlockstoreConfigProvider>();
        *provider->Config.MutableStorageService() = config.Build();
        provider->Config.MutableStorageService()->SetVolumeBalancerEnabled(
            false);
        provider->Publish();
        const TVector<TVolumeState> volumes{
            {"vol0", true, NProto::SOURCE_NONE}};

        // Apply a dynamic config update with VolumeBalancerEnabled=false and
        // check that no RELEASE_TO_HIVE request is sent.
        testEnv.Send(
            actorId,
            std::make_unique<
                TEvConfigsManager::TEvConfigChanged>(
                provider->Get()));
        RunState(
            testEnv,
            actorId,
            volumes,
            {{"vol0", 10}},
            1,
            {},
            TDuration::Seconds(15));

        // Set VolumeBalancerEnabled=true and check RELEASE_TO_HIVE for vol0,
        // then complete that request.
        provider->Config.MutableStorageService()->SetVolumeBalancerEnabled(
            true);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        UNIT_ASSERT_VALUES_EQUAL(
            "vol0",
            RunState(
                testEnv,
                actorId,
                volumes,
                {{"vol0", 10}},
                1,
                EChangeBindingOp::RELEASE_TO_HIVE,
                TDuration::Seconds(15)));
        testEnv.SendChangeVolumeBindingResponse(actorId, "vol0", {});

        // Set VolumeBalancerEnabled=false and check that RELEASE_TO_HIVE stops.
        provider->Config.MutableStorageService()->SetVolumeBalancerEnabled(
            false);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        RunState(
            testEnv,
            actorId,
            volumes,
            {{"vol0", 10}},
            1,
            {},
            TDuration::Seconds(15));

        // Restore VolumeBalancerEnabled=true and check that RELEASE_TO_HIVE
        // resumes.
        provider->Config.MutableStorageService()->SetVolumeBalancerEnabled(
            true);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        UNIT_ASSERT_VALUES_EQUAL(
            "vol0",
            RunState(
                testEnv,
                actorId,
                volumes,
                {{"vol0", 10}},
                1,
                EChangeBindingOp::RELEASE_TO_HIVE,
                TDuration::Seconds(15)));
    }

    // Verify that dynamic config updates change VolumePreemptionType selection
    // and VolumeBalancerMaxInProgress limits without losing unfinished
    // requests.
    Y_UNIT_TEST(ShouldPreserveInFlightBindingsOnDynamicConfig)
    {
        // Use PREEMPTION_MOVE_MOST_HEAVY with VolumeBalancerMaxInProgress=1 and
        // check RELEASE_TO_HIVE selects vol0 without completing its request.
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;
        config.WithEnabled(true)
            .WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY)
            .WithMaxInProgress(1);
        const auto actorId = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));
        testEnv.DispatchEvents();
        auto provider =
            std::make_shared<NTestDynconfig::TTestBlockstoreConfigProvider>();
        *provider->Config.MutableStorageService() = config.Build();
        TVector<TVolumeState> volumes{
            {"vol0", true, NProto::SOURCE_NONE},
            {"vol1", true, NProto::SOURCE_NONE},
            {"vol2", true, NProto::SOURCE_NONE}};
        const TVolumePerfStatuses perfStats{
            {"vol0", 10},
            {"vol1", 1},
            {"vol2", 2}};
        UNIT_ASSERT_VALUES_EQUAL(
            "vol0",
            RunState(
                testEnv,
                actorId,
                volumes,
                perfStats,
                1,
                EChangeBindingOp::RELEASE_TO_HIVE,
                TDuration::Seconds(15)));

        // Set VolumePreemptionType=PREEMPTION_MOVE_LEAST_HEAVY and check that
        // the unfinished request blocks a new RELEASE_TO_HIVE at the limit.
        provider->Config.MutableStorageService()->SetVolumePreemptionType(
            NProto::PREEMPTION_MOVE_LEAST_HEAVY);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        RunState(
            testEnv,
            actorId,
            volumes,
            perfStats,
            1,
            {},
            TDuration::Seconds(15));

        // Set VolumeBalancerMaxInProgress=2 and check RELEASE_TO_HIVE selects
        // the volume with the lowest SufferCount.
        provider->Config.MutableStorageService()
            ->SetVolumeBalancerMaxInProgress(2);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        UNIT_ASSERT_VALUES_EQUAL(
            "vol1",
            RunState(
                testEnv,
                actorId,
                volumes,
                perfStats,
                1,
                EChangeBindingOp::RELEASE_TO_HIVE,
                TDuration::Seconds(15)));

        // Lower VolumeBalancerMaxInProgress to 1 and check that no new
        // RELEASE_TO_HIVE is sent while two requests are unfinished.
        provider->Config.MutableStorageService()
            ->SetVolumeBalancerMaxInProgress(1);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        RunState(
            testEnv,
            actorId,
            volumes,
            perfStats,
            1,
            {},
            TDuration::Seconds(15));

        // Complete "vol0" and check that no new RELEASE_TO_HIVE is sent:
        // the unfinished request count equals VolumeBalancerMaxInProgress.
        testEnv.SendChangeVolumeBindingResponse(actorId, "vol0", {});
        volumes[0] = {"vol0", false, NProto::SOURCE_BALANCER};
        RunState(
            testEnv,
            actorId,
            volumes,
            perfStats,
            1,
            {},
            TDuration::Seconds(15));

        // Complete the remaining request and check that RELEASE_TO_HIVE selects
        // the only volume still on this node.
        testEnv.SendChangeVolumeBindingResponse(actorId, "vol1", {});
        volumes[1] = {"vol1", false, NProto::SOURCE_BALANCER};
        UNIT_ASSERT_VALUES_EQUAL(
            "vol2",
            RunState(
                testEnv,
                actorId,
                volumes,
                perfStats,
                1,
                EChangeBindingOp::RELEASE_TO_HIVE,
                TDuration::Seconds(15)));
    }

    // Verify that an API DISABLE command survives dynamic config updates to
    // VolumePreemptionType; an API ENABLE command uses the updated value.
    Y_UNIT_TEST(ShouldPreserveManualDisableOnDynamicConfig)
    {
        // Send an API DISABLE command and check that the response reports
        // the previous ENABLE state.
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;
        config.WithEnabled(true).WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY);
        const auto actorId = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));
        testEnv.DispatchEvents();
        testEnv.SendConfigureVolumeBalancerRequest(
            actorId,
            EBalancerStatus::DISABLE);
        UNIT_ASSERT_VALUES_EQUAL(
            EBalancerStatus::ENABLE,
            testEnv.GrabConfigureVolumeBalancerResponse()
                ->Record.GetOpStatus());

        // Set VolumePreemptionType=PREEMPTION_MOVE_LEAST_HEAVY and check that
        // the API DISABLE command still prevents RELEASE_TO_HIVE.
        auto provider =
            std::make_shared<NTestDynconfig::TTestBlockstoreConfigProvider>();
        *provider->Config.MutableStorageService() = config.Build();
        provider->Config.MutableStorageService()->SetVolumePreemptionType(
            NProto::PREEMPTION_MOVE_LEAST_HEAVY);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        const TVector<TVolumeState> volumes{
            {"vol0", true, NProto::SOURCE_NONE},
            {"vol1", true, NProto::SOURCE_NONE}};
        RunState(
            testEnv,
            actorId,
            volumes,
            {{"vol0", 10}, {"vol1", 1}},
            1,
            {},
            TDuration::Seconds(15));

        // Send an API ENABLE command and check that the response reports
        // the previous DISABLE state and RELEASE_TO_HIVE selects the volume
        // with the lowest SufferCount.
        testEnv.SendConfigureVolumeBalancerRequest(
            actorId,
            EBalancerStatus::ENABLE);
        UNIT_ASSERT_VALUES_EQUAL(
            EBalancerStatus::DISABLE,
            testEnv.GrabConfigureVolumeBalancerResponse()
                ->Record.GetOpStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            "vol1",
            RunState(
                testEnv,
                actorId,
                volumes,
                {{"vol0", 10}, {"vol1", 1}},
                1,
                EChangeBindingOp::RELEASE_TO_HIVE,
                TDuration::Seconds(15)));
    }

    // Verify that dynamic config updates change the CpuLackThreshold used for
    // RELEASE_TO_HIVE and the VolumeBalancerGentlePreemptionEnabled in
    // requests.
    Y_UNIT_TEST(ShouldApplyDynamicThresholdAndGentlePreemption)
    {
        // Keep CPU wait below CpuLackThreshold and check that
        // no RELEASE_TO_HIVE request is sent.
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;
        config.WithEnabled(true).WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY);
        const auto actorId = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));
        testEnv.DispatchEvents();
        const TVector<TVolumeState> volumes{
            {"vol0", true, NProto::SOURCE_NONE}};
        RunState(
            testEnv,
            actorId,
            volumes,
            {{"vol0", 10}},
            0.5,
            {},
            TDuration::Seconds(15));

        // Enable VolumeBalancerGentlePreemptionEnabled, lower CpuLackThreshold
        // and check RELEASE_TO_HIVE is sent with UseGentlePreemption=true.
        auto provider =
            std::make_shared<NTestDynconfig::TTestBlockstoreConfigProvider>();
        *provider->Config.MutableStorageService() = config.Build();
        provider->Config.MutableStorageService()->SetCpuLackThreshold(40);
        provider->Config.MutableStorageService()
            ->SetVolumeBalancerGentlePreemptionEnabled(true);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        UNIT_ASSERT_VALUES_EQUAL(
            "vol0",
            RunState(
                testEnv,
                actorId,
                volumes,
                {{"vol0", 10}},
                0.5,
                EChangeBindingOp::RELEASE_TO_HIVE,
                TDuration::Seconds(15),
                true));
        testEnv.SendChangeVolumeBindingResponse(actorId, "vol0", {});

        // Disable VolumeBalancerGentlePreemptionEnabled, raise CpuLackThreshold
        // and check that the same CPU wait no longer causes RELEASE_TO_HIVE.
        provider->Config.MutableStorageService()->SetCpuLackThreshold(70);
        provider->Config.MutableStorageService()
            ->SetVolumeBalancerGentlePreemptionEnabled(false);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        RunState(
            testEnv,
            actorId,
            volumes,
            {{"vol0", 10}},
            0.5,
            {},
            TDuration::Seconds(15));

        // Exceed CpuLackThreshold and check RELEASE_TO_HIVE has
        // UseGentlePreemption=false.
        UNIT_ASSERT_VALUES_EQUAL(
            "vol0",
            RunState(
                testEnv,
                actorId,
                volumes,
                {{"vol0", 10}},
                1,
                EChangeBindingOp::RELEASE_TO_HIVE,
                TDuration::Seconds(15),
                false));
    }

    // Verify that enabling and removing the Balancer feature starts and stops
    // RELEASE_TO_HIVE while VolumeBalancerEnabled remains false.
    Y_UNIT_TEST(ShouldUpdateBalancerFeatureFromDynamicConfig)
    {
        // Disable the Balancer feature with VolumeBalancerEnabled=false and
        // check that no RELEASE_TO_HIVE request is sent.
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;
        config.WithEnabled(false).WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY);
        const auto actorId = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor(),
            CreateFeatureConfig("Balancer", {}, false)));
        testEnv.DispatchEvents();
        const TVector<TVolumeState> volumes{
            {"vol0", true, NProto::SOURCE_NONE}};
        RunState(
            testEnv,
            actorId,
            volumes,
            {{"vol0", 10}},
            1,
            {},
            TDuration::Seconds(15));

        // Enable the Balancer feature while VolumeBalancerEnabled=false and
        // check that RELEASE_TO_HIVE is sent.
        auto provider =
            std::make_shared<NTestDynconfig::TTestBlockstoreConfigProvider>();
        *provider->Config.MutableStorageService() = config.Build();
        auto* feature = provider->Config.MutableFeatures()->AddFeatures();
        feature->SetName("Balancer");
        feature->SetCloudProbability(1);
        feature->SetFolderProbability(1);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        UNIT_ASSERT_VALUES_EQUAL(
            "vol0",
            RunState(
                testEnv,
                actorId,
                volumes,
                {{"vol0", 10}},
                1,
                EChangeBindingOp::RELEASE_TO_HIVE,
                TDuration::Seconds(15)));
        testEnv.SendChangeVolumeBindingResponse(actorId, "vol0", {});

        // Remove the Balancer feature and check that RELEASE_TO_HIVE stops.
        provider->Config.ClearFeatures();
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            testEnv.GetRuntime(),
            actorId,
            provider);
        RunState(
            testEnv,
            actorId,
            volumes,
            {{"vol0", 10}},
            1,
            {},
            TDuration::Seconds(15));
    }

    // Verify that the real actor sends TEvSetConfigSubscriptionRequest to
    // ConfigsManager and TEvRemoveConfigSubscriptionRequest on shutdown;
    // check that CreateVolumeBalancerActorStub sends neither request.
    Y_UNIT_TEST(ShouldSubscribeOnlyRealBalancer)
    {
        // Receive ConfigsManager requests at MakeConfigsManagerServiceId().
        TVolumeBalancerTestEnv testEnv;
        auto& runtime = testEnv.GetRuntime();
        const auto manager = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeConfigsManagerServiceId(), manager);

        // Start both actors and check for one TEvSetConfigSubscriptionRequest
        // from the real actor, with Subscriber empty or identifying that actor.
        TVolumeBalancerConfigBuilder config;
        const auto actorId = testEnv.Register(CreateVolumeBalancerActor(
            config,
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));
        const auto stubId = testEnv.Register(CreateVolumeBalancerActorStub());
        const auto subscription = runtime.GrabEdgeEvent<
            TEvConfigsManager::TEvSetConfigSubscriptionRequest>(
            manager,
            TDuration::Seconds(1));
        UNIT_ASSERT(subscription);
        UNIT_ASSERT_VALUES_EQUAL(actorId, subscription->Sender);
        UNIT_ASSERT_VALUES_EQUAL(
            actorId,
            subscription->Get()->Subscriber ? subscription->Get()->Subscriber
                                            : subscription->Sender);
        UNIT_ASSERT(!runtime.GrabEdgeEvent<
                     TEvConfigsManager::TEvSetConfigSubscriptionRequest>(
            manager,
            TDuration::Seconds(1)));

        // Stop both actors and check for one TEvRemoveConfigSubscriptionRequest
        // from the real actor, with Subscriber empty or identifying that actor.
        testEnv.Send(actorId, std::make_unique<TEvents::TEvPoisonPill>());
        testEnv.Send(stubId, std::make_unique<TEvents::TEvPoisonPill>());
        const auto removal = runtime.GrabEdgeEvent<
            TEvConfigsManager::TEvRemoveConfigSubscriptionRequest>(
            manager,
            TDuration::Seconds(1));
        UNIT_ASSERT(removal);
        UNIT_ASSERT_VALUES_EQUAL(actorId, removal->Sender);
        UNIT_ASSERT_VALUES_EQUAL(
            actorId,
            removal->Get()->Subscriber ? removal->Get()->Subscriber
                                       : removal->Sender);
        UNIT_ASSERT(!runtime.GrabEdgeEvent<
                     TEvConfigsManager::TEvRemoveConfigSubscriptionRequest>(
            manager,
            TDuration::Seconds(1)));
    }
}

}   // namespace NCloud::NBlockStore::NStorage

template <>
inline void Out<NCloud::NBlockStore::NPrivateProto::EBalancerOpStatus>(
    IOutputStream& out,
    const NCloud::NBlockStore::NPrivateProto::EBalancerOpStatus status)
{
    out << NCloud::NBlockStore::NPrivateProto::EBalancerOpStatus(status);
}
