#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/diagnostics/volume_balancer_switch.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume_balancer.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>
#include <cloud/storage/core/libs/features/features_config.h>

#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/protos/nbs/blockstore.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NConsole;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeState
{
    TString DiskId;
    bool IsLocal;
    NProto::EPreemptionSource Source;
};

////////////////////////////////////////////////////////////////////////////////

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
    NProto::EPreemptionSource source)
{
    NProto::TVolumeBalancerDiskStats stats;
    stats.SetDiskId(diskId);
    stats.SetCloudId(cloudId);
    stats.SetFolderId(folderId);
    stats.SetIsLocal(isLocal);
    stats.SetPreemptionSource(source);
    stats.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);

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
};

////////////////////////////////////////////////////////////////////////////////

struct TStatsFetcherMock: public NCloud::NStorage::IStatsFetcher
{
    TResultOrError<TDuration> Value = TDuration::Zero();

    void SetCpuWaitValue(TResultOrError<TDuration> value)
    {
        Value = std::move(value);
    }

    void Start() override
    {}

    void Stop() override
    {}

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
        const TString diskId,
        bool isLocal)
    {
        auto stats = CreateVolumeStats(diskId, "", "", isLocal);

        TVector<NProto::TVolumeBalancerDiskStats> volumes;
        volumes.push_back(std::move(stats));

        Send(
            receiver,
            std::make_unique<TEvService::TEvGetVolumeStatsResponse>(
                std::move(volumes)));
    }

    void SendVolumesStatsResponse(
        TActorId receiver,
        const TString diskId,
        bool isLocal,
        NProto::EPreemptionSource source)
    {
        auto stats = CreateVolumeStats(diskId, "", "", isLocal, source);

        TVector<NProto::TVolumeBalancerDiskStats> volumes;
        volumes.push_back(std::move(stats));

        Send(
            receiver,
            std::make_unique<TEvService::TEvGetVolumeStatsResponse>(
                std::move(volumes)));
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

    THolder<TEvConsole::TEvConfigNotificationResponse>
    GrabConfigNotificationResponse()
    {
        return TestEnv.GetRuntime()
            .GrabEdgeEvent<TEvConsole::TEvConfigNotificationResponse>(
                TDuration());
    }

    THolder<TEvVolumeBalancer::TEvConfigureVolumeBalancerResponse>
    GrabConfigureVolumeBalancerResponse()
    {
        return TestEnv.GetRuntime()
            .GrabEdgeEvent<
                TEvVolumeBalancer::TEvConfigureVolumeBalancerResponse>(
                TDuration());
    }

    void AddVolumeToVolumesStatsResponse(
        TVector<NProto::TVolumeBalancerDiskStats>& volumes,
        const TString diskId,
        bool isLocal)
    {
        auto stats = CreateVolumeStats(diskId, "", "", isLocal);

        volumes.push_back(std::move(stats));
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

IActorPtr CreateVolumeBalancerActor(
    TVolumeBalancerConfigBuilder& config,
    IVolumeStatsPtr volumeStats,
    NCloud::NStorage::IStatsFetcherPtr statsFetcher,
    TActorId serviceActorId)
{
    NProto::TStorageServiceConfig storageConfig = config.Build();

    auto volumeBalancerSwitch = CreateVolumeBalancerSwitch();
    volumeBalancerSwitch->EnableVolumeBalancer();

    return CreateVolumeBalancerActor(
        std::make_shared<TStorageConfig>(
            config.Build(),
            CreateFeatureConfig("Balancer", {})),
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
    TDuration runFor)
{
    auto now = testEnv.GetRuntime().GetCurrentTime();
    while (testEnv.GetRuntime().GetCurrentTime() - now < runFor) {
        testEnv.AdjustTime();

        auto ev = testEnv.GrabBindingRequest();
        UNIT_ASSERT(!ev);

        testEnv.GrabVolumesStatsRequest();

        TVector<NProto::TVolumeBalancerDiskStats> stats;
        for (const auto& v: volumes) {
            auto stat =
                CreateVolumeStats(v.DiskId, "", "", v.IsLocal, v.Source);

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
    Y_UNIT_TEST(ShouldPushMostSufferingVolumeToHiveControl)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            EChangeBindingOp::RELEASE_TO_HIVE,
            TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL("vol0", diskId);
    }

    Y_UNIT_TEST(ShouldPushLeastSufferingVolumeToHiveControl)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBindingActorID = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(NProto::PREEMPTION_MOVE_LEAST_HEAVY),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            EChangeBindingOp::RELEASE_TO_HIVE,
            TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL("vol1", diskId);
    }

    Y_UNIT_TEST(ShouldPushVolumesToHiveControlAndReturnBack)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY);
        config.WithInitialPullDelay(TDuration::Seconds(20));

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
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
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
                {"vol0", false, NProto::EPreemptionSource::SOURCE_BALANCER},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
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
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
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
                {"vol0", false, NProto::EPreemptionSource::SOURCE_MANUAL},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
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
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        RunState(
            testEnv,
            volumeBindingActorID,
            {
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
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
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            {},
            TDuration::Seconds(15));
    }

    Y_UNIT_TEST(ShouldNotDoAnythingIfBalancerIsDisabledViaConfigDispatcher)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;

        auto volumeBalancerActorId = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        // Send config update with VolumeBalancer = false
        auto request =
            std::make_unique<TEvConsole::TEvConfigNotificationRequest>();
        request->Record.MutableConfig()
            ->MutableBlockstoreConfig()
            ->SetVolumePreemptionType(NKikimrConfig::PREEMPTION_NONE);

        testEnv.Send(volumeBalancerActorId, std::move(request));

        auto response = testEnv.GrabConfigNotificationResponse();

        RunState(
            testEnv,
            volumeBalancerActorId,
            {
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
            },
            {{"vol0", 10}, {"vol1", 1}},
            1,
            {},
            TDuration::Seconds(15));
    }

    Y_UNIT_TEST(ShouldNotDoAnythingIfPreemptionTypeNone)
    {
        TVolumeBalancerTestEnv testEnv;
        TVolumeBalancerConfigBuilder config;
        config.WithType(NProto::PREEMPTION_NONE);

        auto volumeBalancerActorId = testEnv.Register(CreateVolumeBalancerActor(
            config.WithType(NProto::PREEMPTION_NONE),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        RunState(
            testEnv,
            volumeBalancerActorId,
            {
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
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
            config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {{.DiskId = "vol0",
              .IsLocal = true,
              .Source = NProto::EPreemptionSource::SOURCE_NONE},
             {.DiskId = "vol1",
              .IsLocal = true,
              .Source = NProto::EPreemptionSource::SOURCE_NONE}},
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
            testEnv.VolumeStats,
            testEnv.Fetcher,
            CreateVolumeBalancerSwitch(),
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        RunState(
            testEnv,
            volumeBindingActorID,
            {
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_NONE},
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
            config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY),
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
                {"vol0", true, NProto::EPreemptionSource::SOURCE_NONE},
                {"vol1", true, NProto::EPreemptionSource::SOURCE_MANUAL},
                {"vol2", true, NProto::EPreemptionSource::SOURCE_MANUAL},
                {"vol3", true, NProto::EPreemptionSource::SOURCE_BALANCER},
                {"vol4", true, NProto::EPreemptionSource::SOURCE_BALANCER},
                {"vol5", true, NProto::EPreemptionSource::SOURCE_BALANCER},
                {"vol6", true, NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT},
                {"vol7", true, NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT},
                {"vol8", true, NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT},
                {"vol9", true, NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT},
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
            config.WithType(NProto::PREEMPTION_MOVE_MOST_HEAVY),
            testEnv.VolumeStats,
            testEnv.Fetcher,
            testEnv.GetEdgeActor()));

        testEnv.DispatchEvents();

        auto diskId = RunState(
            testEnv,
            volumeBindingActorID,
            {
                {.DiskId = "vol0",
                 .IsLocal = true,
                 .Source = NProto::EPreemptionSource::SOURCE_NONE},
                {.DiskId = "vol1",
                 .IsLocal = true,
                 .Source = NProto::EPreemptionSource::SOURCE_NONE},
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
}

}   // namespace NCloud::NBlockStore::NStorage

template <>
inline void Out<NCloud::NBlockStore::NPrivateProto::EBalancerOpStatus>(
    IOutputStream& out,
    const NCloud::NBlockStore::NPrivateProto::EBalancerOpStatus status)
{
    out << NCloud::NBlockStore::NPrivateProto::EBalancerOpStatus(status);
}
