#include <cloud/blockstore/config/diagnostics.pb.h>

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>
#include <cloud/blockstore/libs/diagnostics/config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service.h>

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>

#include <cloud/blockstore/libs/storage/service/service_events_private.h>

#include <cloud/storage/core/config/features.pb.h>

#include <util/generic/size_literals.h>
#include <util/string/printf.h>
#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NYdbStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString DefaultDiskId = "path_to_test_volume";
static const TString DefaultCloudId = "test_cloud";

////////////////////////////////////////////////////////////////////////////////

using TYdbStatsCallback =
    std::function<NThreading::TFuture<NProto::TError>(const TVector<TYdbRow>& stats)>;

class TYdbStatsMock:
    public IYdbVolumesStatsUploader
{
private:
    TYdbStatsCallback Callback;

public:
    TYdbStatsMock(TYdbStatsCallback callback)
        : Callback(std::move(callback))
    {}

    virtual ~TYdbStatsMock() = default;

    NThreading::TFuture<NProto::TError> UploadStats(
        const TVector<TYdbRow>& stats,
        const TVector<TYdbBlobLoadMetricRow>&) override
    {
        return Callback(stats);
    }

    void Start() override
    {
    }

    void Stop() override
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

enum EVolumeTestOptions
{
    VOLUME_HASCHECKPOINT = 1,
    VOLUME_HASCLIENTS = 2
};

////////////////////////////////////////////////////////////////////////////////

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    return std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());
}

////////////////////////////////////////////////////////////////////////////////

NMonitoring::TDynamicCounters::TCounterPtr GetCounterToCheck(
    NMonitoring::TDynamicCounters& counters)
{
    auto volumeCounters = counters.GetSubgroup("counters", "blockstore")
        ->GetSubgroup("component", "service_volume")
        ->GetSubgroup("host", "cluster")
        ->GetSubgroup("volume", DefaultDiskId)
        ->GetSubgroup("cloud", DefaultCloudId);
    return volumeCounters->GetCounter("MixedBytesCount");
}

bool VolumeMetricsExists(NMonitoring::TDynamicCounters& counters)
{
    auto volumeCounters = counters.GetSubgroup("counters", "blockstore")
        ->GetSubgroup("component", "service_volume")
        ->GetSubgroup("host", "cluster");

    return (bool)volumeCounters->FindSubgroup("volume", DefaultDiskId);
}

////////////////////////////////////////////////////////////////////////////////

void UnregisterVolume(TTestActorRuntime& runtime, const TString& diskId)
{
    auto unregisterMsg = std::make_unique<TEvStatsService::TEvUnregisterVolume>(diskId);
    runtime.Send(
        new IEventHandle(
            MakeStorageStatsServiceId(),
            MakeStorageStatsServiceId(),
            unregisterMsg.release(),
            0, // flags
            0),
            0);
}

void RegisterVolume(
    TTestActorRuntime& runtime,
    const TString& diskId,
    NProto::EStorageMediaKind kind,
    bool isSystem)
{
    NProto::TVolume volume;
    volume.SetDiskId(diskId);
    volume.SetCloudId(DefaultCloudId);
    volume.SetStorageMediaKind(kind);
    volume.SetIsSystem(isSystem);
    volume.SetPartitionsCount(1);

    auto registerMsg = std::make_unique<TEvStatsService::TEvRegisterVolume>(
        diskId,
        0,
        std::move(volume));
    runtime.Send(
        new IEventHandle(
            MakeStorageStatsServiceId(),
            MakeStorageStatsServiceId(),
            registerMsg.release(),
            0, // flags
            0),
            0);
}

void RegisterVolume(
    TTestActorRuntime& runtime,
    const TString& diskId)
{
    RegisterVolume(runtime, diskId, NProto::STORAGE_MEDIA_SSD, false);
}

void SendDiskStats(
    TTestActorRuntime& runtime,
    const TString& diskId,
    TPartitionDiskCountersPtr diskCounters,
    TVolumeSelfCountersPtr volumeCounters,
    EVolumeTestOptions volumeOptions,
    ui32 nodeIdx)
{
    auto countersMsg = std::make_unique<TEvStatsService::TEvVolumePartCounters>(
        MakeIntrusive<TCallContext>(),
        diskId,
        std::move(diskCounters),
        0,
        0,
        volumeOptions & EVolumeTestOptions::VOLUME_HASCHECKPOINT,
        NBlobMetrics::TBlobLoadMetrics());

    auto volumeMsg = std::make_unique<TEvStatsService::TEvVolumeSelfCounters>(
        diskId,
        volumeOptions & EVolumeTestOptions::VOLUME_HASCLIENTS,
        false,
        std::move(volumeCounters));

    runtime.Send(
        new IEventHandle(
            MakeStorageStatsServiceId(),
            MakeStorageStatsServiceId(),
            countersMsg.release(),
            0, // flags
            0),
            nodeIdx);

    runtime.Send(
        new IEventHandle(
            MakeStorageStatsServiceId(),
            MakeStorageStatsServiceId(),
            volumeMsg.release(),
            0, // flags
            0),
            nodeIdx);
}

TVector<ui64> BroadcastVolumeCounters(
    TTestActorRuntime& runtime,
    const TVector<ui64>& nodes,
    EVolumeTestOptions volumeOptions
)
{
    TDispatchOptions options;

    for (ui32 i = 0; i < nodes.size(); ++i) {
        auto counters = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        auto volume = CreateVolumeSelfCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->Simple.MixedBytesCount.Set(1);

        SendDiskStats(
            runtime,
            DefaultDiskId,
            std::move(counters),
            std::move(volume),
            volumeOptions,
            0);

        auto updateMsg = std::make_unique<TEvents::TEvWakeup>();
        runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                MakeStorageStatsServiceId(),
                updateMsg.release(),
                0, // flags
                0),
            0);

        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);
    }

    runtime.DispatchEvents(options);

    TVector<ui64> res;
    for (const auto& nodeIdx : nodes) {
        auto counters = runtime.GetAppData(nodeIdx).Counters;
        auto val = GetCounterToCheck(*counters)->Val();
        res.push_back(val);
    }

    return res;
}

void ForceYdbStatsUpdate(
    TTestActorRuntime& runtime,
    const TVector<TString>& volumes,
    ui32 cnt,
    ui32 uploadTriggers)
{
    TDispatchOptions options;

    for (ui32 i = 0; i < volumes.size(); ++i) {
        auto counters = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        auto volume = CreateVolumeSelfCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->Simple.MixedBytesCount.Set(1);

        SendDiskStats(
            runtime,
            volumes[i],
            std::move(counters),
            std::move(volume),
            {},
            0);
    }

    while (uploadTriggers--) {
        auto uploadTrigger = std::make_unique<TEvStatsServicePrivate::TEvUploadDisksStats>();
        runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                MakeStorageStatsServiceId(),
                uploadTrigger.release(),
                0, // flags
                0),
            0);
    }

    if (cnt) {
        options.FinalEvents.clear();
        options.FinalEvents.emplace_back(
            TEvStatsServicePrivate::EvUploadDisksStatsCompleted,
            cnt);

        runtime.DispatchEvents(options);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    TTestActorRuntime& Runtime;

    TTestEnv(
            TTestActorRuntime& runtime,
            NProto::TStorageServiceConfig storageConfig,
            NYdbStats::IYdbVolumesStatsUploaderPtr ydbStatsUpdater)
        : Runtime(runtime)
    {
        SetupLogging();

        auto config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        SetupTabletServices(Runtime);

        auto storageStatsService = CreateStorageStatsService(
            std::move(config),
            CreateTestDiagnosticsConfig(),
            std::move(ydbStatsUpdater),
            CreateStatsAggregatorStub());

        auto storageStatsServiceId = Runtime.Register(
            storageStatsService.release(),
            0);

        Runtime.RegisterService(
            MakeStorageStatsServiceId(),
            storageStatsServiceId,
            0);

        Runtime.EnableScheduleForActor(storageStatsServiceId);
    }

    explicit TTestEnv(TTestActorRuntime& runtime)
        : TTestEnv(runtime, {}, NYdbStats::CreateVolumesStatsUploaderStub())
    {}

    void SetupLogging()
    {
        Runtime.AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        // for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
        //   Runtime.SetLogPriority(i, NLog::PRI_DEBUG);
        // }
        // Runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStatsServiceClient
{
private:
    NKikimr::TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    NActors::TActorId Sender;

public:
    TStatsServiceClient(
            NKikimr::TTestActorRuntime& runtime,
            ui32 nodeIdx = 0)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , Sender(runtime.AllocateEdgeActor(nodeIdx))
    {}

    const NActors::TActorId& GetSender() const
    {
        return Sender;
    }

    template <typename TRequest>
    void SendRequest(
        const NActors::TActorId& recipient,
        std::unique_ptr<TRequest> request)
    {
        auto* ev = new NActors::IEventHandle(
            recipient,
            Sender,
            request.release());

        Runtime.Send(ev, NodeIdx);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    std::unique_ptr<TEvService::TEvUploadClientMetricsRequest> CreateUploadClientMetricsRequest()
    {
        return std::make_unique<TEvService::TEvUploadClientMetricsRequest>();
    }

    std::unique_ptr<TEvStatsService::TEvGetVolumeStatsRequest> CreateGetVolumeStatsRequest()
    {
        return std::make_unique<TEvStatsService::TEvGetVolumeStatsRequest>();
    }

    std::unique_ptr<TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest>
    CreateRegisterTrafficSourceRequest(TString sourceId, ui32 bandwidth)
    {
        return std::make_unique<
            TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest>(
            std::move(sourceId),
            bandwidth);
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(MakeStorageStatsServiceId(), std::move(request));          \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()            \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>();                        \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(MakeStorageStatsServiceId(), std::move(request));          \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_DECLARE_METHOD(UploadClientMetrics, TEvService)
    BLOCKSTORE_DECLARE_METHOD(GetVolumeStats, TEvStatsService)
    BLOCKSTORE_DECLARE_METHOD(RegisterTrafficSource, TEvStatsServicePrivate)

#undef BLOCKSTORE_DECLARE_METHOD
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceVolumeStatsTest)
{
    Y_UNIT_TEST(ShouldNotReportSolomonMetricsIfNotMounted)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(runtime, DefaultDiskId);
        auto counters = BroadcastVolumeCounters(runtime, {0}, {});
        UNIT_ASSERT(counters[0]== 0);
    }

    Y_UNIT_TEST(ShouldReportSolomonMetricsIfVolumeRunsLocallyAndMounted)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(runtime, DefaultDiskId);
        auto counters = BroadcastVolumeCounters(runtime, {0}, EVolumeTestOptions::VOLUME_HASCLIENTS);
        UNIT_ASSERT(counters[0]== 1);
    }

    Y_UNIT_TEST(ShouldStopReportSolomonMetricsIfIsMoved)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(runtime, DefaultDiskId);
        auto c1 = BroadcastVolumeCounters(runtime, {0}, EVolumeTestOptions::VOLUME_HASCLIENTS);
        UNIT_ASSERT(c1[0]== 1);

        UnregisterVolume(runtime, DefaultDiskId);

        auto counters = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        auto volume = CreateVolumeSelfCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->Simple.MixedBytesCount.Set(1);

        SendDiskStats(
            runtime,
            DefaultDiskId,
            std::move(counters),
            std::move(volume),
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            0);

        auto updateMsg = std::make_unique<TEvents::TEvWakeup>();
        runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                MakeStorageStatsServiceId(),
                updateMsg.release(),
                0, // flags
                0),
            0);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);
        runtime.DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(false, VolumeMetricsExists(*runtime.GetAppData(0).Counters));
    }

    Y_UNIT_TEST(ShouldReportSolomonMetricsIfVolumeRunsLocallyAndHasCheckpoint)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(runtime, DefaultDiskId);
        auto counters = BroadcastVolumeCounters(runtime, {0}, EVolumeTestOptions::VOLUME_HASCHECKPOINT);
        UNIT_ASSERT(counters[0] == 1);
    }

    Y_UNIT_TEST(ShouldReportMaximumsForCompactionScore)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TDispatchOptions options;

        RegisterVolume(runtime, "vol0");
        RegisterVolume(runtime, "vol1");

        auto counters1 = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        counters1->Simple.CompactionScore.Set(1);
        SendDiskStats(
            runtime,
            "vol0",
            std::move(counters1),
            CreateVolumeSelfCounters(
                EPublishingPolicy::Repl,
                EHistogramCounterOption::ReportMultipleCounters),
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            0);

        auto counters2 = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        counters2->Simple.CompactionScore.Set(3);
        SendDiskStats(
            runtime,
            "vol1",
            std::move(counters2),
            CreateVolumeSelfCounters(
                EPublishingPolicy::Repl,
                EHistogramCounterOption::ReportMultipleCounters),
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            0);

        auto updateMsg = std::make_unique<TEvents::TEvWakeup>();
        runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                MakeStorageStatsServiceId(),
                updateMsg.release(),
                0, // flags
                0),
            0);

        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);

        runtime.DispatchEvents(options);

        auto counter = runtime.GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "service")
            ->GetCounter("CompactionScore");

        UNIT_ASSERT(*counter == 3);
    }

    void DoTestShouldReportBytesCount(
        EPublishingPolicy policy,
        NProto::EStorageMediaKind mediaKind,
        TString type,
        bool isSystem)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(runtime, DefaultDiskId, mediaKind, isSystem);

        auto counters = CreatePartitionDiskCounters(
            policy,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->Simple.BytesCount.Set(100500);
        SendDiskStats(
            runtime,
            DefaultDiskId,
            std::move(counters),
            CreateVolumeSelfCounters(
                policy,
                EHistogramCounterOption::ReportMultipleCounters),
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            0);
        auto updateMsg = std::make_unique<TEvents::TEvWakeup>();
        runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                MakeStorageStatsServiceId(),
                updateMsg.release(),
                0, // flags
                0),
            0);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);
        runtime.DispatchEvents(options);

        // should report "ssd_system" metrics.
        ui64 actual = *runtime.GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "service")
            ->GetSubgroup("type", type)
            ->GetCounter("BytesCount");
        UNIT_ASSERT_VALUES_EQUAL(100500, actual);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForHDDVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::Repl,
            NProto::STORAGE_MEDIA_HDD,
            "hdd",
            false);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::Repl,
            NProto::STORAGE_MEDIA_SSD,
            "ssd",
            false);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForHDDSystemVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::Repl,
            NProto::STORAGE_MEDIA_HDD,
            "hdd_system",
            true);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDSystemVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::Repl,
            NProto::STORAGE_MEDIA_SSD,
            "ssd_system",
            true);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDNonreplVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::DiskRegistryBased,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            "ssd_nonrepl",
            false);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForHDDNonreplVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::DiskRegistryBased,
            NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
            "hdd_nonrepl",
            false);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDMirror2Volumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::DiskRegistryBased,
            NProto::STORAGE_MEDIA_SSD_MIRROR2,
            "ssd_mirror2",
            false);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDMirror3Volumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::DiskRegistryBased,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            "ssd_mirror3",
            false);
    }

    Y_UNIT_TEST(ShouldReportDiskCountAndPartitionCount)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto crank = [&] () {
            auto updateMsg = std::make_unique<TEvents::TEvWakeup>();
            runtime.Send(
                new IEventHandle(
                    MakeStorageStatsServiceId(),
                    MakeStorageStatsServiceId(),
                    updateMsg.release(),
                    0, // flags
                    0),
                0);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);
            runtime.DispatchEvents(options);
        };

        auto ssd = runtime.GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "service")
            ->GetSubgroup("type", "ssd");

        auto totalCounters = runtime.GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "service");

#define CHECK_STATS(dc, dc15m, dc1h, pc, ltu1, lt1to5, lto5, stu1, st1to5, sto5)\
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            dc,                                                                \
            ssd->GetCounter("TotalDiskCount")->Val());                         \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            dc15m,                                                             \
            ssd->GetCounter("TotalDiskCountLast15Min")->Val());                \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            dc1h,                                                              \
            ssd->GetCounter("TotalDiskCountLastHour")->Val());                 \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            pc,                                                                \
            ssd->GetCounter("TotalPartitionCount")->Val());                    \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            ltu1,                                                              \
            ssd->GetCounter("VolumeLoadTimeUnder1Sec")->Val());                \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            lt1to5,                                                            \
            ssd->GetCounter("VolumeLoadTime1To5Sec")->Val());                  \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            lto5,                                                              \
            ssd->GetCounter("VolumeLoadTimeOver5Sec")->Val());                 \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            stu1,                                                              \
            ssd->GetCounter("VolumeStartTimeUnder1Sec")->Val());               \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            st1to5,                                                            \
            ssd->GetCounter("VolumeStartTime1To5Sec")->Val());                 \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            sto5,                                                              \
            ssd->GetCounter("VolumeStartTimeOver5Sec")->Val());                \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            dc,                                                                \
            totalCounters->GetCounter("TotalDiskCount")->Val());               \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            dc15m,                                                             \
            totalCounters->GetCounter("TotalDiskCountLast15Min")->Val());      \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            dc1h,                                                              \
            totalCounters->GetCounter("TotalDiskCountLastHour")->Val());       \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            pc,                                                                \
            totalCounters->GetCounter("TotalPartitionCount")->Val());          \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            ltu1,                                                              \
            totalCounters->GetCounter("VolumeLoadTimeUnder1Sec")->Val());      \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            lt1to5,                                                            \
            totalCounters->GetCounter("VolumeLoadTime1To5Sec")->Val());        \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            lto5,                                                              \
            totalCounters->GetCounter("VolumeLoadTimeOver5Sec")->Val());       \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            stu1,                                                              \
            totalCounters->GetCounter("VolumeStartTimeUnder1Sec")->Val());     \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            st1to5,                                                            \
            totalCounters->GetCounter("VolumeStartTime1To5Sec")->Val());       \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            sto5,                                                              \
            totalCounters->GetCounter("VolumeStartTimeOver5Sec")->Val());      \
// CHECK_STATS

        auto makeVolumeCounters = [=](ui64 lt, ui64 st)
        {
            auto counters = CreateVolumeSelfCounters(
                EPublishingPolicy::Repl,
                EHistogramCounterOption::ReportMultipleCounters);
            counters->Simple.LastVolumeLoadTime.Set(lt);
            counters->Simple.LastVolumeStartTime.Set(st);
            return counters;
        };

        struct TDiskInfo
        {
            TString DiskId;
            ui64 LoadTime = 0;
            ui64 StartTime = 0;
        };

        TVector<TDiskInfo> disks = {
            {"disk-1", 500'000, 6'000'000},
            {"disk-2", 1'500'000, 2'000'000},
            {"disk-3", 5'500'000, 3'000'000},
        };

        auto sendDiskStats = [&](const TDiskInfo& diskInfo)
        {
            SendDiskStats(
                runtime,
                diskInfo.DiskId,
                CreatePartitionDiskCounters(
                    EPublishingPolicy::Repl,
                    EHistogramCounterOption::ReportMultipleCounters),
                makeVolumeCounters(diskInfo.LoadTime, diskInfo.StartTime),
                EVolumeTestOptions::VOLUME_HASCLIENTS,
                0);
        };

        for (const auto& diskInfo: disks) {
            RegisterVolume(runtime, diskInfo.DiskId);
            sendDiskStats(diskInfo);
        }

        crank();
        CHECK_STATS(3, 3, 3, 3, 1, 1, 1, 0, 2, 1);

        for (const auto& diskId: {"disk-1", "disk-2"}) {
            UnregisterVolume(runtime, diskId);
        }
        sendDiskStats(disks[2]);

        crank();
        CHECK_STATS(1, 3, 3, 1, 0, 0, 1, 0, 1, 0);

        runtime.AdvanceCurrentTime(TDuration::Minutes(14));
        crank();
        CHECK_STATS(1, 3, 3, 1, 0, 0, 0, 0, 0, 0);

        runtime.AdvanceCurrentTime(TDuration::Minutes(2));
        crank();
        CHECK_STATS(1, 1, 3, 1, 0, 0, 0, 0, 0, 0);

        for (const auto& diskId: {"disk-1"}) {
            RegisterVolume(runtime, diskId);
        }
        sendDiskStats(disks[0]);
        sendDiskStats(disks[2]);

        crank();
        // only disk-1 is counted in start/load time metrics since disk-3 is
        // not considered to be a recently-started disk
        CHECK_STATS(2, 2, 3, 2, 1, 0, 0, 0, 0, 1);

        runtime.AdvanceCurrentTime(TDuration::Minutes(45));
        crank();
        CHECK_STATS(2, 2, 2, 2, 0, 0, 0, 0, 0, 0);

        for (const auto& diskId: {"disk-1", "disk-3"}) {
            UnregisterVolume(runtime, diskId);
        }
        sendDiskStats(disks[1]);

        crank();
        CHECK_STATS(0, 2, 2, 0, 0, 0, 0, 0, 0, 0);

        runtime.AdvanceCurrentTime(TDuration::Minutes(61));
        crank();
        CHECK_STATS(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    Y_UNIT_TEST(ShouldReportYdbStatsInBatches)
    {
        auto callback = [] (const TVector<TYdbRow>& stats)
        {
            Y_UNUSED(stats);
            return NThreading::MakeFuture(MakeError(S_OK));
        };

        IYdbVolumesStatsUploaderPtr ydbStats = std::make_shared<TYdbStatsMock>(callback);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetStatsUploadDiskCount(1);
        storageServiceConfig.SetStatsUploadInterval(TDuration::Seconds(300).MilliSeconds());
        storageServiceConfig.SetStatsUploadRetryTimeout(TDuration::Seconds(20).MilliSeconds());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, std::move(storageServiceConfig), std::move(ydbStats));

        RegisterVolume(runtime, "disk1");
        RegisterVolume(runtime, "disk2");
        ForceYdbStatsUpdate(runtime, {"disk1", "disk2"}, 2, 2);
    }

    Y_UNIT_TEST(ShouldRetryStatsUploadInCaseOfFailure)
    {
        ui32 attemptCount = 0;
        auto callback = [&] (const TVector<TYdbRow>& stats)
        {
            UNIT_ASSERT_VALUES_EQUAL(1, stats.size());

            if (++attemptCount == 1) {
                return NThreading::MakeFuture(MakeError(E_REJECTED));
            }
            return NThreading::MakeFuture(MakeError(S_OK));
        };

        IYdbVolumesStatsUploaderPtr ydbStats = std::make_shared<TYdbStatsMock>(callback);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetStatsUploadDiskCount(1);
        storageServiceConfig.SetStatsUploadInterval(TDuration::Seconds(300).MilliSeconds());
        storageServiceConfig.SetStatsUploadRetryTimeout(TDuration::MilliSeconds(1).MilliSeconds());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, std::move(storageServiceConfig), std::move(ydbStats));

        RegisterVolume(runtime, "disk1");
        RegisterVolume(runtime, "disk2");
        ForceYdbStatsUpdate(runtime, {"disk1", "disk2"}, 3, 1);

        UNIT_ASSERT_VALUES_EQUAL(3, attemptCount);
    }

    Y_UNIT_TEST(ShouldForgetTooOldStats)
    {
        bool failUpload = true;
        ui32 callCnt = 0;

        auto callback = [&] (const TVector<TYdbRow>& stats)
        {
            UNIT_ASSERT_VALUES_EQUAL(1, stats.size());

            if (failUpload) {
                return NThreading::MakeFuture(MakeError(E_REJECTED));
            } else {
                ++callCnt;
                return NThreading::MakeFuture(MakeError(S_OK));
            }
        };

        IYdbVolumesStatsUploaderPtr ydbStats = std::make_shared<TYdbStatsMock>(callback);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetStatsUploadDiskCount(1);
        storageServiceConfig.SetStatsUploadInterval(TDuration::Seconds(2).MilliSeconds());
        storageServiceConfig.SetStatsUploadRetryTimeout(TDuration::MilliSeconds(99).MilliSeconds());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, std::move(storageServiceConfig), std::move(ydbStats));

        RegisterVolume(runtime, "disk1");
        RegisterVolume(runtime, "disk2");
        ForceYdbStatsUpdate(runtime, {"disk1", "disk2"}, 2, 0);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvStatsServicePrivate::EvUploadDisksStats);
            runtime.DispatchEvents(options);
        }

        failUpload = false;

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvStatsServicePrivate::EvUploadDisksStatsCompleted, 2);
        runtime.DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(2, callCnt);
    }

    Y_UNIT_TEST(ShouldCorrectlyPrepareYdbStatsRequests)
    {
        TVector<TVector<TString>> batches;
        auto callback = [&] (const TVector<TYdbRow>& stats)
        {
            TVector<TString> batch;
            for (const auto& x: stats) {
                batch.push_back(x.DiskId);
            }

            batches.push_back(std::move(batch));

            return NThreading::MakeFuture(MakeError(S_OK));
        };

        IYdbVolumesStatsUploaderPtr ydbStats = std::make_shared<TYdbStatsMock>(callback);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetStatsUploadDiskCount(2);
        storageServiceConfig.SetStatsUploadInterval(TDuration::Seconds(300).MilliSeconds());
        storageServiceConfig.SetStatsUploadRetryTimeout(TDuration::Seconds(20).MilliSeconds());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, std::move(storageServiceConfig), std::move(ydbStats));

        TVector<TString> diskIds;
        for (ui32 i = 0; i < 5; ++i) {
            auto diskId = Sprintf("disk%u", i);
            diskIds.push_back(diskId);
            RegisterVolume(runtime, diskId);
        }

        ForceYdbStatsUpdate(runtime, diskIds, 1, 3);

        UNIT_ASSERT_VALUES_EQUAL(3, batches.size());
        UNIT_ASSERT_VALUES_EQUAL(2, batches[0].size());
        UNIT_ASSERT_VALUES_EQUAL(2, batches[1].size());
        UNIT_ASSERT_VALUES_EQUAL(1, batches[2].size());

        TVector<TString> observedDiskIds;
        for (const auto& batch: batches) {
            for (const auto& x: batch) {
                observedDiskIds.push_back(x);
            }
        }

        Sort(observedDiskIds);

        UNIT_ASSERT_VALUES_EQUAL(diskIds, observedDiskIds);
    }

    Y_UNIT_TEST(ShouldNotTryToPushStatsIfNothingToReportToYDB)
    {
        TVector<TVector<TString>> batches;
        bool uploadSeen = false;
        auto callback = [&] (const TVector<TYdbRow>& stats)
        {
            Y_UNUSED(stats);
            uploadSeen = true;
            return NThreading::MakeFuture(MakeError(S_OK));
        };

        IYdbVolumesStatsUploaderPtr ydbStats = std::make_shared<TYdbStatsMock>(callback);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetStatsUploadDiskCount(2);
        storageServiceConfig.SetStatsUploadInterval(TDuration::Seconds(300).MilliSeconds());
        storageServiceConfig.SetStatsUploadRetryTimeout(TDuration::Seconds(20).MilliSeconds());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, std::move(storageServiceConfig), std::move(ydbStats));

        ForceYdbStatsUpdate(runtime, {}, 0, 1);

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(false, uploadSeen);
    }

    Y_UNIT_TEST(ShouldAcceptAndReplyToClientMetrics)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        TStatsServiceClient client(runtime);

        client.UploadClientMetrics();
    }

    void DoTestShouldReportReadWriteZeroCountersForMediaKindAndPolicy(
        NProto::EStorageMediaKind mediaKind,
        EPublishingPolicy publishingPolicy)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(runtime, "vol0", mediaKind, true);

        auto counters = CreatePartitionDiskCounters(
            publishingPolicy,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->RequestCounters.ReadBlocks.Count = 42;
        counters->RequestCounters.ReadBlocks.RequestBytes = 100500;
        SendDiskStats(
            runtime,
            "vol0",
            std::move(counters),
            CreateVolumeSelfCounters(
                publishingPolicy,
                EHistogramCounterOption::ReportMultipleCounters),
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            0);
        auto updateMsg = std::make_unique<TEvents::TEvWakeup>();
        runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                MakeStorageStatsServiceId(),
                updateMsg.release(),
                0, // flags
                0),
            0);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);
        runtime.DispatchEvents(options);

        {
            ui64 actual = *runtime.GetAppData(0).Counters
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service_volume")
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", "vol0")
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("request", "ReadBlocks")
                ->GetCounter("Count");
            UNIT_ASSERT_VALUES_EQUAL(42, actual);
        }

        {
            ui64 actual = *runtime.GetAppData(0).Counters
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service_volume")
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", "vol0")
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("request", "ReadBlocks")
                ->GetCounter("RequestBytes");
            UNIT_ASSERT_VALUES_EQUAL(100500, actual);
        }
    }

    Y_UNIT_TEST(ShouldReportReadWriteZeroCountersForSsdNonreplDisks)
    {
        DoTestShouldReportReadWriteZeroCountersForMediaKindAndPolicy(
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            EPublishingPolicy::DiskRegistryBased);
    }

    Y_UNIT_TEST(ShouldReportReadWriteZeroCountersForHddNonreplDisks)
    {
        DoTestShouldReportReadWriteZeroCountersForMediaKindAndPolicy(
            NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
            EPublishingPolicy::DiskRegistryBased);
    }

    Y_UNIT_TEST(ShouldReportReadWriteZeroCountersForMirror2Disks)
    {
        DoTestShouldReportReadWriteZeroCountersForMediaKindAndPolicy(
            NProto::STORAGE_MEDIA_SSD_MIRROR2,
            EPublishingPolicy::DiskRegistryBased);
    }

    Y_UNIT_TEST(ShouldReportReadWriteZeroCountersForMirror3Disks)
    {
        DoTestShouldReportReadWriteZeroCountersForMediaKindAndPolicy(
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            EPublishingPolicy::DiskRegistryBased);
    }

    Y_UNIT_TEST(ShouldRegisterTrafficSources)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetBackgroundOperationsTotalBandwidth(100);

        TTestBasicRuntime runtime;
        TTestEnv env(
            runtime,
            std::move(storageServiceConfig),
            NYdbStats::CreateVolumesStatsUploaderStub());

        TStatsServiceClient client(runtime);

        // Register the first source - the entire bandwidth is given to it.
        auto response = client.RegisterTrafficSource("src1", 200);
        UNIT_ASSERT_VALUES_EQUAL(100, response->LimitedBandwidthMiBs);

        // Register the second source - a part of the bandwidth is given to it, with
        // an honest division of the bandwidth into all.
        response = client.RegisterTrafficSource("src2", 600);
        UNIT_ASSERT_VALUES_EQUAL(75, response->LimitedBandwidthMiBs);

        // Re-register the first source - a part of the bandwidth is given to it
        response = client.RegisterTrafficSource("src1", 200);
        UNIT_ASSERT_VALUES_EQUAL(25, response->LimitedBandwidthMiBs);

        // Re-register only first source
        for (int i = 0; i < 4; i++) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            runtime.DispatchEvents({}, TDuration());
            response = client.RegisterTrafficSource("src1", 200);
        }

        // Now the first source gets all the bandwidth.
        response = client.RegisterTrafficSource("src1", 200);
        UNIT_ASSERT_VALUES_EQUAL(100, response->LimitedBandwidthMiBs);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
