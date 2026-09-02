#include <cloud/blockstore/config/diagnostics.pb.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/service/service_events_private.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/ydbstats/ydbrow.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>

#include <cloud/storage/core/config/features.pb.h>
#include <cloud/storage/core/libs/api/user_stats.h>
#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/string/printf.h>

#include <initializer_list>
#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NYdbStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString DefaultDiskId = "path_to_test_volume";
static const TString DefaultCopiedDiskId = "path_to_test_volume-copy";
static const TString DefaultCloudId = "test_cloud";
static const TString DefaultFolderId = "test_folder";

////////////////////////////////////////////////////////////////////////////////

using TYdbStatsCallback =
    std::function<NThreading::TFuture<NProto::TError>(const TYdbRowData& rows)>;

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
        const TYdbRowData& rows) override
    {
        return Callback(rows);
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
        ->GetSubgroup("cloud", DefaultCloudId)
        ->GetSubgroup("folder", DefaultFolderId)
        ->GetSubgroup(
            "type",
            MediaKindToString(NProto::STORAGE_MEDIA_SSD));
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
    bool isSystem,
    ui64 volumeTabletID = 0)
{
    NProto::TVolume volume;
    volume.SetDiskId(diskId);
    volume.SetCloudId(DefaultCloudId);
    volume.SetFolderId(DefaultFolderId);
    volume.SetStorageMediaKind(kind);
    volume.SetIsSystem(isSystem);
    volume.SetPartitionsCount(1);

    auto registerMsg = std::make_unique<TEvStatsService::TEvRegisterVolume>(
        diskId,
        volumeTabletID,
        std::move(volume),
        runtime.AllocateEdgeActor());
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
    const TString& diskId,
    ui64 volumeTabletId)
{
    RegisterVolume(runtime, diskId, NProto::STORAGE_MEDIA_SSD, false, volumeTabletId);
}

void RegisterVolume(
    TTestActorRuntime& runtime,
    const TString& diskId)
{
    RegisterVolume(runtime, diskId, 0);
}

void PartitionBootExternalCompleted(
    TTestActorRuntime& runtime,
    const TString& diskId,
    const ui64 partitionTabletId,
    TVector<NKikimr::TTabletChannelInfo> channels)
{
    auto partitionBootExternalCompletedMsg =
        std::make_unique<TEvStatsService::TEvPartitionBootExternalCompleted>(
            diskId,
            partitionTabletId,
            std::move(channels));
    runtime.Send(
        new IEventHandle(
            MakeStorageStatsServiceId(),
            MakeStorageStatsServiceId(),
            partitionBootExternalCompletedMsg.release(),
            0,   // flags
            0),
        0);
}

void SendDiskStats(
    TTestActorRuntime& runtime,
    const TString& diskId,
    const bool isLocalMount,
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
        NBlobMetrics::TBlobLoadMetrics(),
        NKikimrTabletBase::TMetrics());

    auto volumeMsg = std::make_unique<TEvStatsService::TEvVolumeSelfCounters>(
        diskId,
        isLocalMount,
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
            false, // isLocalMount
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
            false, // isLocalMount
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
    void DoShouldNotReportSolomonMetricsExceptIsLocalMountIfNotMounted(
        bool copiedDisk)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(
            runtime,
            copiedDisk ? DefaultCopiedDiskId : DefaultDiskId);
        auto counters = BroadcastVolumeCounters(runtime, {0}, {});
        UNIT_ASSERT(counters[0] == 0);

        auto isLocalMountCounter =
            runtime.GetAppData(0)
                .Counters->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service_volume")
                ->GetSubgroup("volume", DefaultDiskId)
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetSubgroup(
                    "type",
                    MediaKindToString(NProto::STORAGE_MEDIA_SSD))
                ->FindCounter("IsLocalMount");
        UNIT_ASSERT(isLocalMountCounter);
        UNIT_ASSERT_VALUES_EQUAL(0, isLocalMountCounter->Val());
    }

    Y_UNIT_TEST(ShouldNotReportSolomonMetricsExceptIsLocalMountIfNotMounted)
    {
        DoShouldNotReportSolomonMetricsExceptIsLocalMountIfNotMounted(false);
    }

    Y_UNIT_TEST(
        ShouldNotReportSolomonMetricsExceptIsLocalMountIfNotMountedForCopiedVolume)
    {
        DoShouldNotReportSolomonMetricsExceptIsLocalMountIfNotMounted(true);
    }

    void DoShouldReportSolomonMetricsIfVolumeRunsLocallyAndMounted(bool copiedDisk)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(runtime, copiedDisk ? DefaultCopiedDiskId : DefaultDiskId);
        auto counters = BroadcastVolumeCounters(runtime, {0}, EVolumeTestOptions::VOLUME_HASCLIENTS);
        UNIT_ASSERT(counters[0]== 1);
    }

    Y_UNIT_TEST(ShouldReportSolomonMetricsIfVolumeRunsLocallyAndMounted)
    {
        DoShouldReportSolomonMetricsIfVolumeRunsLocallyAndMounted(false);
    }

    Y_UNIT_TEST(
        ShouldReportSolomonMetricsIfVolumeRunsLocallyAndMountedForCopiedVolume)
    {
        DoShouldReportSolomonMetricsIfVolumeRunsLocallyAndMounted(false);
    }

    Y_UNIT_TEST(ShouldReportSolomonMetricsIfVolumeRunsLocallyAndHasCheckpoint)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(runtime, DefaultDiskId);
        auto counters = BroadcastVolumeCounters(runtime, {0}, EVolumeTestOptions::VOLUME_HASCHECKPOINT);
        UNIT_ASSERT(counters[0] == 1);
    }

    Y_UNIT_TEST(ShouldReregisterCountersWhenMediaKindChanges)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(
            runtime,
            DefaultDiskId,
            NProto::STORAGE_MEDIA_HYBRID,
            false);

        auto diskCounters = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        diskCounters->Simple.MixedBytesCount.Set(1);
        SendDiskStats(
            runtime,
            DefaultDiskId,
            false,
            std::move(diskCounters),
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
                0,
                0),
            0);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);
        runtime.DispatchEvents(options);

        auto findType = [&] (const TString& type) {
            return runtime.GetAppData(0)
                .Counters->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service_volume")
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", DefaultDiskId)
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->FindSubgroup("type", type);
        };

        UNIT_ASSERT(findType("hdd"));
        UNIT_ASSERT(!findType("hybrid"));

        NProto::TVolume config;
        config.SetDiskId(DefaultDiskId);
        config.SetCloudId(DefaultCloudId);
        config.SetFolderId(DefaultFolderId);
        config.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
        config.SetPartitionsCount(1);

        auto configUpdated =
            std::make_unique<TEvStatsService::TEvVolumeConfigUpdated>(
                DefaultDiskId,
                std::move(config));
        runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                MakeStorageStatsServiceId(),
                configUpdated.release(),
                0,
                0),
            0);
        options.FinalEvents.clear();
        options.FinalEvents.emplace_back(
            TEvStatsService::EvVolumeConfigUpdated);
        runtime.DispatchEvents(options);

        UNIT_ASSERT(!findType("hdd"));
        auto ssdCounters = findType("ssd");
        UNIT_ASSERT(ssdCounters);
        UNIT_ASSERT(ssdCounters->FindCounter("MixedBytesCount"));

        UnregisterVolume(runtime, DefaultDiskId);
        options.FinalEvents.clear();
        options.FinalEvents.emplace_back(TEvStatsService::EvUnregisterVolume);
        runtime.DispatchEvents(options);
        UNIT_ASSERT(!VolumeMetricsExists(*runtime.GetAppData(0).Counters));
    }

    void DoShouldUnregisterVolumeGroup(bool copiedDisk)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(
            runtime,
            copiedDisk ? DefaultCopiedDiskId : DefaultDiskId);
        auto c1 = BroadcastVolumeCounters(
            runtime,
            {0},
            EVolumeTestOptions::VOLUME_HASCLIENTS);
        UNIT_ASSERT(c1[0] == 1);

        UnregisterVolume(
            runtime,
            copiedDisk ? DefaultCopiedDiskId : DefaultDiskId);

        auto counters = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        auto volume = CreateVolumeSelfCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->Simple.MixedBytesCount.Set(1);

        SendDiskStats(
            runtime,
            copiedDisk ? DefaultCopiedDiskId : DefaultDiskId,
            false,   // isLocalMount
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
                0,   // flags
                0),
            0);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);
        runtime.DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(
            false,
            VolumeMetricsExists(*runtime.GetAppData(0).Counters));
        auto subGroupForDefaultDiskId =
            runtime.GetAppData(0)
                .Counters->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service_volume")
                ->FindSubgroup("volume", DefaultDiskId);
        UNIT_ASSERT_EQUAL(nullptr, subGroupForDefaultDiskId);
    }

    Y_UNIT_TEST(ShouldUnregisterVolumeGroup)
    {
        DoShouldUnregisterVolumeGroup(false);
    }

    Y_UNIT_TEST(ShouldUnregisterVolumeGroupForCopiedDisk)
    {
        DoShouldUnregisterVolumeGroup(true);
    }

    Y_UNIT_TEST(ShouldUnregisterVolumeGroupAfterSourceAndCopiedUnregistered)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // Register both source and copied disks.
        RegisterVolume(runtime, DefaultDiskId);
        RegisterVolume(runtime, DefaultCopiedDiskId);
        auto c1 = BroadcastVolumeCounters(
            runtime,
            {0},
            EVolumeTestOptions::VOLUME_HASCLIENTS);
        UNIT_ASSERT(c1[0] == 1);

        // Unregister source disk.
        UnregisterVolume(runtime, DefaultDiskId);

        // The copied disk remains registered under logical name of source disk.
        auto counters = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        auto volume = CreateVolumeSelfCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->Simple.MixedBytesCount.Set(1);
        {
            SendDiskStats(
                runtime,
                DefaultDiskId,
                false,   // isLocalMount
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
                    0,   // flags
                    0),
                0);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);
            runtime.DispatchEvents(options);

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                VolumeMetricsExists(*runtime.GetAppData(0).Counters));
            auto subGroupForDefaultDiskId =
                runtime.GetAppData(0)
                    .Counters->GetSubgroup("counters", "blockstore")
                    ->GetSubgroup("component", "service_volume")
                    ->FindSubgroup("volume", DefaultDiskId);
            UNIT_ASSERT_UNEQUAL(nullptr, subGroupForDefaultDiskId);
        }

        // Unregister copied disk.
        UnregisterVolume(runtime, DefaultCopiedDiskId);

        // Statistics for logical disk id should be unregistered.
        {
            auto updateMsg = std::make_unique<TEvents::TEvWakeup>();
            runtime.Send(
                new IEventHandle(
                    MakeStorageStatsServiceId(),
                    MakeStorageStatsServiceId(),
                    updateMsg.release(),
                    0,   // flags
                    0),
                0);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Wakeup);
            runtime.DispatchEvents(options);

            UNIT_ASSERT_VALUES_EQUAL(
                false,
                VolumeMetricsExists(*runtime.GetAppData(0).Counters));
            auto subGroupForDefaultDiskId =
                runtime.GetAppData(0)
                    .Counters->GetSubgroup("counters", "blockstore")
                    ->GetSubgroup("component", "service_volume")
                    ->FindSubgroup("volume", DefaultDiskId);
            UNIT_ASSERT_EQUAL(nullptr, subGroupForDefaultDiskId);
        }
    }

    void DoShouldReportIsLocalMountCounter(bool copiedDisk)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(
            runtime,
            copiedDisk ? DefaultCopiedDiskId : DefaultDiskId);

        SendDiskStats(
            runtime,
            copiedDisk ? DefaultCopiedDiskId : DefaultDiskId,
            false, // isLocalMount
            CreatePartitionDiskCounters(
                EPublishingPolicy::Repl,
                EHistogramCounterOption::ReportMultipleCounters),
            CreateVolumeSelfCounters(
                EPublishingPolicy::Repl,
                EHistogramCounterOption::ReportMultipleCounters),
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            0);

        {
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

            ui64 actual = *runtime.GetAppData(0).Counters
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service_volume")
                ->GetSubgroup("volume", DefaultDiskId)
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetSubgroup(
                    "type",
                    MediaKindToString(NProto::STORAGE_MEDIA_SSD))
                ->GetCounter("IsLocalMount");
            UNIT_ASSERT_VALUES_EQUAL(0, actual);
        }

        SendDiskStats(
            runtime,
            copiedDisk ? DefaultCopiedDiskId : DefaultDiskId,
            true, // isLocalMount
            CreatePartitionDiskCounters(
                EPublishingPolicy::Repl,
                EHistogramCounterOption::ReportMultipleCounters),
            CreateVolumeSelfCounters(
                EPublishingPolicy::Repl,
                EHistogramCounterOption::ReportMultipleCounters),
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            0);

        {
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

            ui64 actual = *runtime.GetAppData(0).Counters
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service_volume")
                ->GetSubgroup("volume", DefaultDiskId)
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetSubgroup(
                    "type",
                    MediaKindToString(NProto::STORAGE_MEDIA_SSD))
                ->GetCounter("IsLocalMount");
            UNIT_ASSERT_VALUES_EQUAL(1, actual);
        }
    }

    Y_UNIT_TEST(ShouldReportIsLocalMountCounter) {
        DoShouldReportIsLocalMountCounter(false);
    }

    Y_UNIT_TEST(ShouldReportIsLocalMountCounterForCopied)
    {
        DoShouldReportIsLocalMountCounter(true);
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
            false, // isLocalMount
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
            false, // isLocalMount
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
        bool isSystem,
        bool copiedDisk = false)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(
            runtime,
            copiedDisk ? DefaultCopiedDiskId : DefaultDiskId,
            mediaKind,
            isSystem);

        auto counters = CreatePartitionDiskCounters(
            policy,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->Simple.BytesCount.Set(100500);
        SendDiskStats(
            runtime,
            copiedDisk ? DefaultCopiedDiskId : DefaultDiskId,
            false, // isLocalMount
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

        auto type = MediaKindToString(mediaKind);
        if (isSystem) {
            type += "_system";
        }

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
            false,
            false);
        DoTestShouldReportBytesCount(
            EPublishingPolicy::Repl,
            NProto::STORAGE_MEDIA_HDD,
            false,
            true);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::Repl,
            NProto::STORAGE_MEDIA_SSD,
            false,
            false);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForHDDSystemVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::Repl,
            NProto::STORAGE_MEDIA_HDD,
            true);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDSystemVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::Repl,
            NProto::STORAGE_MEDIA_SSD,
            true);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDNonreplVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::DiskRegistryBased,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            false);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForHDDNonreplVolumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::DiskRegistryBased,
            NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
            false);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDMirror2Volumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::DiskRegistryBased,
            NProto::STORAGE_MEDIA_SSD_MIRROR2,
            false);
    }

    Y_UNIT_TEST(ShouldReportBytesCountForSSDMirror3Volumes)
    {
        DoTestShouldReportBytesCount(
            EPublishingPolicy::DiskRegistryBased,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            false);
    }

    void DoShouldReportDiskCountAndPartitionCount(bool copiedDisk)
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

        TString suffix = copiedDisk ? "-copy" : "";
        TVector<TDiskInfo> disks = {
            {"disk-1" + suffix, 500'000, 6'000'000},
            {"disk-2" + suffix, 1'500'000, 2'000'000},
            {"disk-3" + suffix, 5'500'000, 3'000'000},
        };

        auto sendDiskStats = [&](const TDiskInfo& diskInfo)
        {
            SendDiskStats(
                runtime,
                diskInfo.DiskId,
                false, // isLocalMount
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

        UnregisterVolume(runtime, disks[0].DiskId);
        UnregisterVolume(runtime, disks[1].DiskId);

        sendDiskStats(disks[2]);

        crank();
        CHECK_STATS(1, 3, 3, 1, 0, 0, 1, 0, 1, 0);

        runtime.AdvanceCurrentTime(TDuration::Minutes(14));
        crank();
        CHECK_STATS(1, 3, 3, 1, 0, 0, 0, 0, 0, 0);

        runtime.AdvanceCurrentTime(TDuration::Minutes(2));
        crank();
        CHECK_STATS(1, 1, 3, 1, 0, 0, 0, 0, 0, 0);

        RegisterVolume(runtime, disks[0].DiskId);

        sendDiskStats(disks[0]);
        sendDiskStats(disks[2]);

        crank();
        // only disk-1 is counted in start/load time metrics since disk-3 is
        // not considered to be a recently-started disk
        CHECK_STATS(2, 2, 3, 2, 1, 0, 0, 0, 0, 1);

        runtime.AdvanceCurrentTime(TDuration::Minutes(45));
        crank();
        CHECK_STATS(2, 2, 2, 2, 0, 0, 0, 0, 0, 0);

        UnregisterVolume(runtime, disks[0].DiskId);
        UnregisterVolume(runtime, disks[2].DiskId);

        sendDiskStats(disks[1]);

        crank();
        CHECK_STATS(0, 2, 2, 0, 0, 0, 0, 0, 0, 0);

        runtime.AdvanceCurrentTime(TDuration::Minutes(61));
        crank();
        CHECK_STATS(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    Y_UNIT_TEST(ShouldReportDiskCountAndPartitionCount)
    {
        DoShouldReportDiskCountAndPartitionCount(false);
    }

    Y_UNIT_TEST(ShouldReportDiskCountAndPartitionCountForCopiedDisk)
    {
        DoShouldReportDiskCountAndPartitionCount(true);
    }

    Y_UNIT_TEST(ShouldReportYdbStatsInBatches)
    {
        auto callback = [] (const TYdbRowData& rows)
        {
            Y_UNUSED(rows);
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
        RegisterVolume(runtime, "disk2-copy");
        ForceYdbStatsUpdate(runtime, {"disk1", "disk2-copy"}, 2, 2);
    }

    Y_UNIT_TEST(ShouldRetryStatsUploadInCaseOfFailure)
    {
        ui32 attemptCount = 0;
        auto callback = [&] (const TYdbRowData& rows)
        {
            UNIT_ASSERT_VALUES_EQUAL(1, rows.Stats.size());

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
        RegisterVolume(runtime, "disk2-copy");
        ForceYdbStatsUpdate(runtime, {"disk1", "disk2-copy"}, 3, 2);

        UNIT_ASSERT_VALUES_EQUAL(3, attemptCount);
    }

    Y_UNIT_TEST(ShouldForgetTooOldStats)
    {
        bool failUpload = true;
        ui32 callCnt = 0;

        auto callback = [&] (const TYdbRowData& rows)
        {
            UNIT_ASSERT_VALUES_EQUAL(1, rows.Stats.size());

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
        RegisterVolume(runtime, "disk2-copy");
        ForceYdbStatsUpdate(runtime, {"disk1", "disk2-copy"}, 2, 0);

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
        auto callback = [&] (const TYdbRowData& rows)
        {
            TVector<TString> batch;
            for (const auto& x: rows.Stats) {
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

    Y_UNIT_TEST(ShouldCorrectlyPrepareGroupsAndPartitionsRequests)
    {
        THashSet<std::tuple<ui64, ui32, ui32, ui32>> groups;
        THashMap<ui64, std::pair<ui64, TString>> partitions;

        auto callback = [&] (const TYdbRowData& rows)
        {
            for (const auto& x : rows.Groups) {
                groups.insert(std::make_tuple(
                    x.TabletId, x.Channel, x.GroupId, x.Generation));
            }
            for (const auto& x : rows.Partitions) {
                partitions[x.PartitionTabletId] = {x.VolumeTabletId, x.DiskId};
            }

            return NThreading::MakeFuture(MakeError(S_OK));
        };

        IYdbVolumesStatsUploaderPtr ydbStats = std::make_shared<TYdbStatsMock>(callback);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetStatsUploadDiskCount(4);
        storageServiceConfig.SetStatsUploadInterval(TDuration::Seconds(300).MilliSeconds());
        storageServiceConfig.SetStatsUploadRetryTimeout(TDuration::Seconds(20).MilliSeconds());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, std::move(storageServiceConfig), std::move(ydbStats));

        RegisterVolume(runtime, "vol0", 0 /* volumeTabletId */);
        RegisterVolume(runtime, "vol1", 10 /* volumeTabletId */);
        RegisterVolume(runtime, "vol2", 20 /* volumeTabletId */);
        RegisterVolume(runtime, "vol3", 30 /* volumeTabletId */);

        {
            TVector<NKikimr::TTabletChannelInfo> channels9(2);
            channels9[0].Channel = 0;
            channels9[0].History = TVector<NKikimr::TTabletChannelInfo::THistoryEntry>{
                {0 /* fromGeneration */, 0 /* groupId*/},
                {1 /* fromGeneration */, 1 /* groupId*/}};
            channels9[1].Channel = 1;
            channels9[1].History = TVector<NKikimr::TTabletChannelInfo::THistoryEntry>{
                {0 /* fromGeneration */, 2 /* groupId*/},
                {1 /* fromGeneration */, 0 /* groupId*/}};

            TVector<NKikimr::TTabletChannelInfo> channels18(1);
            channels18[0].Channel = 0;
            channels18[0].History = TVector<NKikimr::TTabletChannelInfo::THistoryEntry>{
                {0 /* fromGeneration */, 1 /* groupId*/}};

            TVector<NKikimr::TTabletChannelInfo> channels19(1);
            channels19[0].Channel = 0;
            channels19[0].History = TVector<NKikimr::TTabletChannelInfo::THistoryEntry>{
                {0 /* fromGeneration */, 3 /* groupId*/},
                {2 /* fromGeneration */, 2 /* groupId*/}};

            PartitionBootExternalCompleted(
                runtime,
                "vol1",
                9, // partitionTabletId
                std::move(channels9));
            PartitionBootExternalCompleted(
                runtime,
                "vol2",
                18, // partitionTabletId
                std::move(channels18));
            PartitionBootExternalCompleted(
                runtime,
                "vol2",
                19, // partitionTabletId
                std::move(channels19));
            PartitionBootExternalCompleted(
                runtime,
                "vol3",
                29, // partitionTabletId
                TVector<NKikimr::TTabletChannelInfo>{});
        }

        ForceYdbStatsUpdate(runtime, {"vol0", "vol1", "vol2", "vol3"}, 1, 1);

        UNIT_ASSERT_VALUES_EQUAL(4, partitions.size());
        UNIT_ASSERT(partitions.contains(9));
        UNIT_ASSERT_VALUES_EQUAL(10, partitions[9].first);
        UNIT_ASSERT_VALUES_EQUAL("vol1", partitions[9].second);
        UNIT_ASSERT(partitions.contains(18));
        UNIT_ASSERT_VALUES_EQUAL(20, partitions[18].first);
        UNIT_ASSERT_VALUES_EQUAL("vol2", partitions[18].second);
        UNIT_ASSERT(partitions.contains(19));
        UNIT_ASSERT_VALUES_EQUAL(20, partitions[19].first);
        UNIT_ASSERT_VALUES_EQUAL("vol2", partitions[19].second);
        UNIT_ASSERT(partitions.contains(29));
        UNIT_ASSERT_VALUES_EQUAL(30, partitions[29].first);
        UNIT_ASSERT_VALUES_EQUAL("vol3", partitions[29].second);

        UNIT_ASSERT_VALUES_EQUAL(7, groups.size());
        UNIT_ASSERT(groups.contains(
            std::make_tuple<ui64, ui32, ui32, ui32>(9, 0, 0, 0)));
        UNIT_ASSERT(groups.contains(
            std::make_tuple<ui64, ui32, ui32, ui32>(9, 0, 1, 1)));
        UNIT_ASSERT(groups.contains(
            std::make_tuple<ui64, ui32, ui32, ui32>(9, 1, 2, 0)));
        UNIT_ASSERT(groups.contains(
            std::make_tuple<ui64, ui32, ui32, ui32>(9, 1, 0, 1)));
        UNIT_ASSERT(groups.contains(
            std::make_tuple<ui64, ui32, ui32, ui32>(18, 0, 1, 0)));
        UNIT_ASSERT(groups.contains(
            std::make_tuple<ui64, ui32, ui32, ui32>(19, 0, 3, 0)));
        UNIT_ASSERT(groups.contains(
            std::make_tuple<ui64, ui32, ui32, ui32>(19, 0, 2, 2)));
    }

    Y_UNIT_TEST(ShouldSplitRowsIntoMultipleRequests)
    {
        ui32 uploadTimes = 0;
        THashSet<ui32> groups;

        auto callback = [&](const TYdbRowData& rows)
        {
            Y_UNUSED(rows);

            ++uploadTimes;
            for (const auto& groupRow: rows.Groups) {
                groups.insert(groupRow.GroupId);
            }

            return NThreading::MakeFuture(MakeError(S_OK));
        };

        IYdbVolumesStatsUploaderPtr ydbStats =
            std::make_shared<TYdbStatsMock>(callback);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetStatsUploadDiskCount(1);
        storageServiceConfig.SetStatsUploadMaxRowsPerTx(3);
        storageServiceConfig.SetStatsUploadInterval(
            TDuration::Seconds(300).MilliSeconds());
        storageServiceConfig.SetStatsUploadRetryTimeout(
            TDuration::Seconds(20).MilliSeconds());

        TTestBasicRuntime runtime;
        TTestEnv env(
            runtime,
            std::move(storageServiceConfig),
            std::move(ydbStats));

        RegisterVolume(runtime, "vol", 111 /* volumeTabletId */);

        auto partitionBootExternalCompleted = [&](ui32 channelsCount)
        {
            TVector<NKikimr::TTabletChannelInfo> channels(1);
            channels[0].Channel = 0;

            channels[0].History.reserve(channelsCount);
            for (ui32 i = 0; i < channelsCount; ++i) {
                channels[0].History.emplace_back(
                    i /* fromGeneration */,
                    i /* groupId */);
            }

            PartitionBootExternalCompleted(
                runtime,
                "vol",
                222,   // partitionTabletId
                std::move(channels));
        };

        auto checkUploads =
            [&](ui32 expectedUploadTimes, ui32 expectedGroupsCount)
        {
            UNIT_ASSERT_VALUES_EQUAL(expectedUploadTimes, uploadTimes);
            UNIT_ASSERT_VALUES_EQUAL(expectedGroupsCount, groups.size());
            for (ui32 i = 0; i < expectedGroupsCount; ++i) {
                UNIT_ASSERT(groups.contains(i));
            }

            uploadTimes = 0;
            groups.clear();
        };

        partitionBootExternalCompleted(0);
        ForceYdbStatsUpdate(runtime, {"vol"}, 1, 1);
        checkUploads(1, 0);

        partitionBootExternalCompleted(2);
        ForceYdbStatsUpdate(runtime, {"vol"}, 2, 1);
        checkUploads(2, 2);

        partitionBootExternalCompleted(4);
        ForceYdbStatsUpdate(runtime, {"vol"}, 3, 1);
        checkUploads(3, 4);

        partitionBootExternalCompleted(6);
        ForceYdbStatsUpdate(runtime, {"vol"}, 3, 1);
        checkUploads(3, 6);
    }

    Y_UNIT_TEST(ShouldNotTryToPushStatsIfNothingToReportToYDB)
    {
        TVector<TVector<TString>> batches;
        bool uploadSeen = false;
        auto callback = [&] (const TYdbRowData& rows)
        {
            Y_UNUSED(rows);
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

        RegisterVolume(runtime, "vol0", mediaKind, true /* isSystem */);

        auto counters = CreatePartitionDiskCounters(
            publishingPolicy,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->RequestCounters.ReadBlocks.Count = 42;
        counters->RequestCounters.ReadBlocks.RequestBytes = 100500;
        SendDiskStats(
            runtime,
            "vol0",
            false, // isLocalMount
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
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetSubgroup("type", MediaKindToString(mediaKind))
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
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetSubgroup("type", MediaKindToString(mediaKind))
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

    Y_UNIT_TEST(ShouldReportReadWriteZeroCountersPullScheme)
    {
        bool uploadSeen = false;
        auto callback = [&](const TYdbRowData& rows)
        {
            Y_UNUSED(rows);
            uploadSeen = true;
            return NThreading::MakeFuture(MakeError(S_OK));
        };

        IYdbVolumesStatsUploaderPtr ydbStats =
            std::make_shared<TYdbStatsMock>(callback);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetUsePullSchemeForVolumeStatistics(true);

        TTestBasicRuntime runtime;
        TTestEnv env(
            runtime,
            std::move(storageServiceConfig),
            std::move(ydbStats));

        RegisterVolume(
            runtime,
            "vol0",
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            true /* isSystem */);

        auto counters = CreatePartitionDiskCounters(
            EPublishingPolicy::DiskRegistryBased,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->RequestCounters.ReadBlocks.Count = 42;
        counters->RequestCounters.ReadBlocks.RequestBytes = 100500;

        // Statistics were sent using the push method to check for possible
        // failures.
        SendDiskStats(
            runtime,
            "vol0",
            false,   // isLocalMount
            std::move(counters),
            CreateVolumeSelfCounters(
                EPublishingPolicy::DiskRegistryBased,
                EHistogramCounterOption::ReportMultipleCounters),
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            0);

        counters = CreatePartitionDiskCounters(
            EPublishingPolicy::DiskRegistryBased,
            EHistogramCounterOption::ReportMultipleCounters);
        counters->RequestCounters.ReadBlocks.Count = 42;
        counters->RequestCounters.ReadBlocks.RequestBytes = 100500;

        auto partCounters = TEvStatsService::TEvVolumePartCounters(
            MakeIntrusive<TCallContext>(),
            "vol0",
            std::move(counters),
            0,
            0,
            false,
            NBlobMetrics::TBlobLoadMetrics{},
            NKikimrTabletBase::TMetrics{});

        auto selfCounters = TEvStatsService::TEvVolumeSelfCounters(
            "vol0",
            false,
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            false,
            std::move(CreateVolumeSelfCounters(
                EPublishingPolicy::DiskRegistryBased,
                EHistogramCounterOption::ReportMultipleCounters)));

        auto updateMsg = std::make_unique<TEvents::TEvWakeup>();
        runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                MakeStorageStatsServiceId(),
                updateMsg.release(),
                0,   // flags
                0),
            0);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEventRethrow<
            TEvStatsService::TEvGetServiceStatisticsRequest>(
            handle,
            TDuration::Seconds(5));

        UNIT_ASSERT(handle);

        auto response = std::make_unique<
            TEvStatsService::TEvGetServiceStatisticsResponse>();

        response->PartsCounters.push_back(std::move(partCounters));
        response->VolumeCounters.emplace(std::move(selfCounters));

        runtime.Send(
            new IEventHandle(
                handle->Sender,
                MakeStorageStatsServiceId(),
                response.release(),
                0,   // flags
                0),
            0);

        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            ui64 actual = *runtime.GetAppData(0)
                               .Counters->GetSubgroup("counters", "blockstore")
                               ->GetSubgroup("component", "service_volume")
                               ->GetSubgroup("host", "cluster")
                               ->GetSubgroup("volume", "vol0")
                               ->GetSubgroup("cloud", DefaultCloudId)
                               ->GetSubgroup("folder", DefaultFolderId)
                               ->GetSubgroup(
                                   "type",
                                   MediaKindToString(
                                       NProto::STORAGE_MEDIA_SSD_NONREPLICATED))
                               ->GetSubgroup("request", "ReadBlocks")
                               ->GetCounter("Count");
            UNIT_ASSERT_VALUES_EQUAL(84, actual);
        }

        {
            ui64 actual = *runtime.GetAppData(0)
                               .Counters->GetSubgroup("counters", "blockstore")
                               ->GetSubgroup("component", "service_volume")
                               ->GetSubgroup("host", "cluster")
                               ->GetSubgroup("volume", "vol0")
                               ->GetSubgroup("cloud", DefaultCloudId)
                               ->GetSubgroup("folder", DefaultFolderId)
                               ->GetSubgroup(
                                   "type",
                                   MediaKindToString(
                                       NProto::STORAGE_MEDIA_SSD_NONREPLICATED))
                               ->GetSubgroup("request", "ReadBlocks")
                               ->GetCounter("RequestBytes");
            UNIT_ASSERT_VALUES_EQUAL(201000, actual);
        }
    }

    Y_UNIT_TEST(ShouldNotCrashCrashWhenDiskCountersIsNullptrPullScheme)
    {
        bool uploadSeen = false;
        auto callback = [&](const TYdbRowData& rows)
        {
            Y_UNUSED(rows);
            uploadSeen = true;
            return NThreading::MakeFuture(MakeError(S_OK));
        };

        IYdbVolumesStatsUploaderPtr ydbStats =
            std::make_shared<TYdbStatsMock>(callback);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetUsePullSchemeForVolumeStatistics(true);

        TTestBasicRuntime runtime;
        TTestEnv env(
            runtime,
            std::move(storageServiceConfig),
            std::move(ydbStats));

        RegisterVolume(
            runtime,
            "vol0",
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            true /* isSystem */);

        auto partCounters = TEvStatsService::TEvVolumePartCounters(
            MakeIntrusive<TCallContext>(),
            "vol0",
            nullptr,
            0,
            0,
            false,
            NBlobMetrics::TBlobLoadMetrics{},
            NKikimrTabletBase::TMetrics{});

        auto selfCounters = TEvStatsService::TEvVolumeSelfCounters(
            "vol0",
            false,
            EVolumeTestOptions::VOLUME_HASCLIENTS,
            false,
            std::move(CreateVolumeSelfCounters(
                EPublishingPolicy::DiskRegistryBased,
                EHistogramCounterOption::ReportMultipleCounters)));

        auto updateMsg = std::make_unique<TEvents::TEvWakeup>();
        runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                MakeStorageStatsServiceId(),
                updateMsg.release(),
                0,   // flags
                0),
            0);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEventRethrow<
            TEvStatsService::TEvGetServiceStatisticsRequest>(
            handle,
            TDuration::Seconds(5));

        UNIT_ASSERT(handle);

        auto response = std::make_unique<
            TEvStatsService::TEvGetServiceStatisticsResponse>();

        response->PartsCounters.push_back(std::move(partCounters));
        response->VolumeCounters.emplace(std::move(selfCounters));

        runtime.Send(
            new IEventHandle(
                handle->Sender,
                MakeStorageStatsServiceId(),
                response.release(),
                0,   // flags
                0),
            0);

        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            ui64 actual = *runtime.GetAppData(0)
                               .Counters->GetSubgroup("counters", "blockstore")
                               ->GetSubgroup("component", "service_volume")
                               ->GetSubgroup("host", "cluster")
                               ->GetSubgroup("volume", "vol0")
                               ->GetSubgroup("cloud", DefaultCloudId)
                               ->GetSubgroup("folder", DefaultFolderId)
                               ->GetSubgroup(
                                   "type",
                                   MediaKindToString(
                                       NProto::STORAGE_MEDIA_SSD_NONREPLICATED))
                               ->GetSubgroup("request", "ReadBlocks")
                               ->GetCounter("Count");
            UNIT_ASSERT_VALUES_EQUAL(0, actual);
        }

        {
            ui64 actual = *runtime.GetAppData(0)
                               .Counters->GetSubgroup("counters", "blockstore")
                               ->GetSubgroup("component", "service_volume")
                               ->GetSubgroup("host", "cluster")
                               ->GetSubgroup("volume", "vol0")
                               ->GetSubgroup("cloud", DefaultCloudId)
                               ->GetSubgroup("folder", DefaultFolderId)
                               ->GetSubgroup(
                                   "type",
                                   MediaKindToString(
                                       NProto::STORAGE_MEDIA_SSD_NONREPLICATED))
                               ->GetSubgroup("request", "ReadBlocks")
                               ->GetCounter("RequestBytes");
            UNIT_ASSERT_VALUES_EQUAL(0, actual);
        }
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

    Y_UNIT_TEST(ShouldRegisterVolumeForSecondTime)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        RegisterVolume(runtime, DefaultDiskId);
        auto counters = BroadcastVolumeCounters(
            runtime,
            {0},
            EVolumeTestOptions::VOLUME_HASCLIENTS);
        UNIT_ASSERT(counters[0] == 1);

        RegisterVolume(runtime, DefaultDiskId);
        counters = BroadcastVolumeCounters(
            runtime,
            {0},
            EVolumeTestOptions::VOLUME_HASCLIENTS);
        UNIT_ASSERT(counters[0] == 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

using TCounterGroupPtr = TIntrusivePtr<NMonitoring::TDynamicCounters>;

using TCounterPath = std::initializer_list<std::pair<TString, TString>>;

struct TServiceVolumeLabels {
    TString CloudId = DefaultCloudId;
    TString FolderId = DefaultFolderId;
    TString type = "ssd";
};

// never creates missing groups.
TCounterGroupPtr FindCounterGroup(
    TCounterGroupPtr group,
    TCounterPath path)
{
    for (const auto& [name, value]: path) {
        if (!group) {
            return {};
        }

        group = group->FindSubgroup(name, value);
    }

    return group;
}

void AssertScalarCounter(
    const TCounterGroupPtr& group,
    const TString name,
    ui64 expectedValue,
    bool derivative = false)
{
    UNIT_ASSERT(group);

    auto counter = group->FindCounter(name);
    UNIT_ASSERT_C(counter, name);

    UNIT_ASSERT_VALUES_EQUAL(counter->Val(), expectedValue);
    UNIT_ASSERT_VALUES_EQUAL(counter->ForDerivative(), derivative);
}

// Check both scalar counters and nested request/histogram counters.
void AssertPublishedVolumeCounters(
    const TCounterGroupPtr& group,
    ui64 latestValue,
    ui64 accumulatedValue)
{
    // Expiring partition counter.
    AssertScalarCounter(group, "IORequestsQueued", latestValue);

    // Permanent partition counter.
    AssertScalarCounter(group, "MixedBytesCount", latestValue * 100);

    // Expired and permanent volume-self counters.
    AssertScalarCounter(group, "LongRunningReadBlob", latestValue);
    AssertScalarCounter(group, "MaxUsedQuota", latestValue);

    // Cumulative scalar counter.
    AssertScalarCounter(group, "UsedQuota", accumulatedValue, true);

    auto readCounters = FindCounterGroup(group, {{"request", "ReadBlocks"}});
    AssertScalarCounter(readCounters, "Count", accumulatedValue, true);

    auto histogramGroup = FindCounterGroup(
        readCounters,
        {{"histogram", "ThrottlerDelay"}});

    UNIT_ASSERT(histogramGroup);

    auto histogram = histogramGroup->FindHistogram("ThrottlerDelay");
    UNIT_ASSERT(histogram);

    auto snapshot = histogram->Snapshot();
    ui64 samples = 0;
    for (ui64 i = 0; i < snapshot->Count(); ++i) {
        samples += snapshot->Value(i);
    }

    UNIT_ASSERT_VALUES_EQUAL(samples, accumulatedValue);
}

// Reads the actor's actual user-metric supplier without JSON encoders.
class TServiceVolumeUserMetricsProbe final
    : public NMonitoring::IMetricConsumer
{
private:
    TServiceVolumeLabels ExpectedLabels;
    THashMap<TString, TString> CurrentLabels;
    TString CurrentMetricName;

public:
    THashSet<TString> Names;
    bool HasMaxUsedQuota = false;
    i64 MaxUsedQuota = 0;

    explicit TServiceVolumeUserMetricsProbe(TServiceVolumeLabels expectedLabels)
        : ExpectedLabels(std::move(expectedLabels))
    {}

    void OnStreamBegin() override {}
    void OnStreamEnd() override {}
    void OnCommonTime(TInstant) override {}

    void OnMetricBegin(NMonitoring::EMetricType) override
    {
        CurrentLabels.clear();
        CurrentMetricName.clear();
    }

    void OnMetricEnd() override {}
    void OnLabelsBegin() override {}

    void OnLabel(TStringBuf name, TStringBuf value) override
    {
        CurrentLabels.emplace(TString(name), TString(value));
    }

    void AssertLabel(TStringBuf name, const TString& expected)
    {
        const auto it = CurrentLabels.find(TString(name));
        UNIT_ASSERT_C(
            it != CurrentLabels.end(),
            TStringBuilder() << "Missing user metric label: " << name);
        UNIT_ASSERT_VALUES_EQUAL(it->second, expected);
    }

    void OnLabelsEnd() override
    {
        AssertLabel("service", "compute");
        AssertLabel("project", ExpectedLabels.CloudId);
        AssertLabel("cluster", ExpectedLabels.FolderId);
        AssertLabel("disk", DefaultDiskId);

        CurrentMetricName = CurrentLabels.at("name");

        // A second registration must not produce duplicate metrics
        UNIT_ASSERT_C(
            Names.insert(CurrentMetricName).second,
            CurrentMetricName);
    }

    void OnInt64(TInstant, i64 value) override
    {
        if (CurrentMetricName ==
            "disk.io_quota_utilization_percentage_burst")
        {
            HasMaxUsedQuota = true;
            MaxUsedQuota = value;
        }
    }

    void OnDouble(TInstant, double) override {}
    void OnUint64(TInstant, ui64) override {}

    void OnHistogram(
        TInstant,
        NMonitoring::IHistogramSnapshotPtr) override
    {}

    void OnLogHistogram(
        TInstant,
        NMonitoring::TLogHistogramSnapshotPtr) override
    {}

    void OnSummaryDouble(
        TInstant,
        NMonitoring::ISummaryDoubleSnapshotPtr) override
    {}
};

NProto::TStorageServiceConfig MakeRegistrationTestConfig()
{
    NProto::TStorageServiceConfig config;
    config.SetUsePullSchemeForVolumeStatistics(false);
    return config;
}

struct TServiceVolumeCountersTestEnv
{
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    TActorId Edge;
    NCloud::NStorage::IUserMetricsSupplierPtr UserCounters;
    ui64 LastCookie = 0;

    TServiceVolumeCountersTestEnv()
        : Env(
            Runtime,
            MakeRegistrationTestConfig(),
            NYdbStats::CreateVolumesStatsUploaderStub()),
        Edge(Runtime.AllocateEdgeActor())
    {
        // Register the receiver before dispatching the stats actor's bootstrap event
        Runtime.RegisterService(
            NCloud::NStorage::MakeStorageUserStatsId(),
            Edge);

        auto event = Runtime.GrabEdgeEventRethrow<
            NCloud::NStorage::TEvUserStats::TEvUserStatsProviderCreate>(
                Edge,
                TDuration::Seconds(5));

        UNIT_ASSERT(event);
        UserCounters = event->Get()->Provider;
        UNIT_ASSERT(UserCounters);

        RegisterVolume(Runtime, DefaultDiskId);
    }

    template <typename TEvent>
    void SendAndDispatch(std::unique_ptr<TEvent> event)
    {
        const ui64 cookie = ++LastCookie;
        const auto sender = Edge;
        bool delivered = false;

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [cookie, sender, &delivered](IEventHandle& ev) {
                if (ev.GetTypeRewrite() == TEvent::EventType &&
                    ev.Sender == sender &&
                    ev.Cookie == cookie)
                {
                    delivered = true;
                    return true;
                }
                return false;
            });

        Runtime.Send(
            new IEventHandle(
                MakeStorageStatsServiceId(),
                sender,
                event.release(),
                0,
                cookie),
            0);

        Runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(delivered);
    }

    void Flush()
    {
        SendAndDispatch(std::make_unique<TEvents::TEvWakeup>());
    }

    // "value" is the latest gauge value and an increment for cumulative counters.
    // Pass zero when disabling publication.
    void Publish(EVolumeTestOptions options, ui64 value)
    {
        auto diskCounters = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);

        auto volumeCounters = CreateVolumeSelfCounters(
            EPublishingPolicy::Repl,
            EHistogramCounterOption::ReportMultipleCounters);

        diskCounters->Simple.IORequestsQueued.Set(value);
        diskCounters->Simple.MixedBytesCount.Set(value * 100);
        diskCounters->RequestCounters.ReadBlocks.Count = value;
        diskCounters->RequestCounters.ReadBlocks.Total.Increment(
            100,
            value);

        volumeCounters->Simple.LongRunningReadBlob.Set(value);
        volumeCounters->Simple.MaxUsedQuota.Set(value);
        volumeCounters->Cumulative.UsedQuota.Increment(value);
        volumeCounters->ThrottlerDelayRequestCounters.ReadBlocks.Increment(
            100,
            value);

        SendDiskStats(
            Runtime,
            DefaultDiskId,
            false,
            std::move(diskCounters),
            std::move(volumeCounters),
            options,
            0);

        Flush();
    }

    TCounterGroupPtr FindVolume(
        const TServiceVolumeLabels& labels = {})
    {
        return FindCounterGroup(
            Runtime.GetAppData(0).Counters,
            {
                {"counters", "blockstore"},
                {"component", "service_volume"},
                {"host", "cluster"},
                {"volume", DefaultDiskId},
                {"cloud", labels.CloudId},
                {"folder", labels.FolderId},
                {"type", labels.type},
            });
    }

    void AssertDetached()
    {
        // The IsLocalMount branch without host=cluster is intentionally outside the scope of this check
        auto volume = FindCounterGroup(
            Runtime.GetAppData(0).Counters,
            {
                {"counters", "blockstore"},
                {"component", "service_volume"},
                {"host", "cluster"},
                {"volume", DefaultDiskId},
            });

        UNIT_ASSERT(!volume);
    }

    // This fixture has one volume. Verify that no obsolete label branches
    // or duplicate paths remain at any level below host=cluster.
    TCounterGroupPtr AssertOnlyVolume(
        const TServiceVolumeLabels& labels = {})
    {
        auto group = FindCounterGroup(
            Runtime.GetAppData(0).Counters,
            {
                {"counters", "blockstore"},
                {"component", "service_volume"},
                {"host", "cluster"},
            });

        UNIT_ASSERT(group);

        const TCounterPath path = {
            {"volume", DefaultDiskId},
            {"cloud", labels.CloudId},
            {"folder", labels.FolderId},
            {"type", labels.type},
        };

        for (const auto& [name, value]: path) {
            size_t count = 0;

            group->EnumerateSubgroups(
                [&](const TString& childName, const TString& childValue) {
                    ++count;
                    UNIT_ASSERT_VALUES_EQUAL(name, childName);
                    UNIT_ASSERT_VALUES_EQUAL(value, childValue);
                });

            UNIT_ASSERT_VALUES_EQUAL(1, count);

            group = group->FindSubgroup(name, value);
            UNIT_ASSERT(group);
        }

        return group;
    }

    void UpdateConfig(
        const TServiceVolumeLabels& labels,
        NProto::EStorageMediaKind mediaKind = NProto::STORAGE_MEDIA_SSD)
    {
        NProto::TVolume config;
        config.SetDiskId(DefaultDiskId);
        config.SetCloudId(labels.CloudId);
        config.SetFolderId(labels.FolderId);
        config.SetStorageMediaKind(mediaKind);
        config.SetPartitionsCount(1);

        SendAndDispatch(
            std::make_unique<TEvStatsService::TEvVolumeConfigUpdated>(
                DefaultDiskId,
                std::move(config)));
    }

    void AssertUserCounters(
        bool registered,
        ui64 expectedMaxUsedQuota = 0,
        const TServiceVolumeLabels& labels = {})
    {
        TServiceVolumeUserMetricsProbe probe(labels);
        UserCounters->Accept(Runtime.GetCurrentTime(), &probe);

        if (!registered) {
            UNIT_ASSERT(probe.Names.empty());
            UNIT_ASSERT(!probe.HasMaxUsedQuota);
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL(probe.Names.size(), 4);

        for (const auto* name: {
                "disk.io_quota_utilization_percentage",
                "disk.io_quota_utilization_percentage_burst",
                "disk.read_throttler_delay",
                "disk.write_throttler_delay"})

        {
            UNIT_ASSERT_C(probe.Names.contains(name), name);
        }

        UNIT_ASSERT(probe.HasMaxUsedQuota);
        UNIT_ASSERT_VALUES_EQUAL(probe.MaxUsedQuota, expectedMaxUsedQuota);
    }
};

}  // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceVolumeCountersReregistrationTest)
{
    Y_UNIT_TEST(ShouldRegisterServiceVolumeCountersAfterClientsReturn)
    {
        TServiceVolumeCountersTestEnv env;

        env.Publish(VOLUME_HASCLIENTS, 7);

        auto original = env.AssertOnlyVolume();
        AssertPublishedVolumeCounters(original, 7, 7);

        // Materialize the expiring counter before detaching the group.
        auto originalQueued = original->FindCounter("IORequestsQueued");
        UNIT_ASSERT(originalQueued);

        env.Publish({}, 0);
        env.AssertDetached();

        env.Publish(VOLUME_HASCLIENTS, 11);

        auto restored = env.AssertOnlyVolume();
        AssertPublishedVolumeCounters(restored, 11, 18);

        UNIT_ASSERT(original.Get() == restored.Get());

        auto restoredQueued = restored->FindCounter("IORequestsQueued");
        UNIT_ASSERT(restoredQueued);
        UNIT_ASSERT(originalQueued.Get() == restoredQueued.Get());
    }

    Y_UNIT_TEST(ShouldRegisterServiceVolumeCountersAfterCheckpointAppears)
    {
        TServiceVolumeCountersTestEnv env;

        // No clients and no checkpoint.
        env.Publish({}, 0);
        env.AssertDetached();

        // Publication starts because of a checkpoint alone.
        env.Publish(VOLUME_HASCHECKPOINT, 3);

        auto original = env.AssertOnlyVolume();
        AssertPublishedVolumeCounters(original, 3, 3);

        env.Publish({}, 0);
        env.AssertDetached();

        // Still no clients: the checkpoint restores publication.
        env.Publish(VOLUME_HASCHECKPOINT, 5);

        auto restored = env.AssertOnlyVolume();
        AssertPublishedVolumeCounters(restored, 5, 8);
        UNIT_ASSERT(original.Get() == restored.Get());
    }

    Y_UNIT_TEST(ShouldMoveServiceVolumeCountersAfterVolumeConfigUpdate)
    {
        struct TCase
        {
            TServiceVolumeLabels Labels;
            NProto::EStorageMediaKind MediaKind;
        };

        const TCase cases[] = {
            {
                {"new_cloud", DefaultFolderId, "ssd"},
                NProto::STORAGE_MEDIA_SSD,
            },
            {
                {DefaultCloudId, "new_folder", "ssd"},
                NProto::STORAGE_MEDIA_SSD,
            },
            {
                {"new_cloud", "new_folder", "ssd"},
                NProto::STORAGE_MEDIA_SSD,
            },
            {
                {DefaultCloudId, DefaultFolderId, "hdd"},
                NProto::STORAGE_MEDIA_HDD,
            },
        };

        for (const auto& testCase: cases) {
            TServiceVolumeCountersTestEnv env;

            env.Publish(VOLUME_HASCLIENTS, 7);
            auto original = env.AssertOnlyVolume();
            AssertPublishedVolumeCounters(original, 7, 7);

            env.UpdateConfig(testCase.Labels, testCase.MediaKind);

            UNIT_ASSERT(!env.FindVolume());

            auto moved = env.AssertOnlyVolume(testCase.Labels);
            UNIT_ASSERT(original.Get() == moved.Get());

            // Reattachment preserves already published counters.
            AssertPublishedVolumeCounters(moved, 7, 7);
            env.AssertUserCounters(true, 7, testCase.Labels);

            env.Publish(VOLUME_HASCLIENTS, 11);

            AssertPublishedVolumeCounters(
                env.AssertOnlyVolume(testCase.Labels),
                11,
                18);
            UNIT_ASSERT(!env.FindVolume());

            // Subsequent detach/reattach must use the NEW labels too.
            env.Publish({}, 0);
            env.AssertDetached();
            env.AssertUserCounters(false);

            env.Publish(VOLUME_HASCLIENTS, 13);

            auto restored = env.AssertOnlyVolume(testCase.Labels);
            UNIT_ASSERT(original.Get() == restored.Get());
            AssertPublishedVolumeCounters(restored, 13, 31);

            UNIT_ASSERT(!env.FindVolume());
            env.AssertUserCounters(true, 13, testCase.Labels);
        }
    }

    Y_UNIT_TEST(ShouldRegisterServiceVolumeCountersAcrossMultipleCycles)
    {
        TServiceVolumeCountersTestEnv env;

        TCounterGroupPtr original;
        ui64 accumulated = 0;

        for (const ui64 value: {7, 11, 13}) {
            env.Publish(VOLUME_HASCLIENTS, value);
            accumulated += value;

            auto current = env.AssertOnlyVolume();
            if (!original) {
                original = current;
            }

            UNIT_ASSERT(original.Get() == current.Get());
            AssertPublishedVolumeCounters(current, value, accumulated);

            // true -> true: repeated registration must be harmless
            env.Publish(VOLUME_HASCLIENTS, value);
            accumulated += value;

            auto repeated = env.AssertOnlyVolume();
            UNIT_ASSERT(original.Get() == repeated.Get());
            AssertPublishedVolumeCounters(repeated, value, accumulated);
            env.AssertUserCounters(true, value);

            if (value != 13) {
                env.Publish({}, 0);
                env.AssertDetached();
                env.AssertUserCounters(false);

                // false -> false: repeated unregister must be harmless
                env.Publish({}, 0);
                env.AssertDetached();
                env.AssertUserCounters(false);
            }
        }
    }

    Y_UNIT_TEST(ShouldPreserveCumulativeCountersAfterRegistration)
    {
        TServiceVolumeCountersTestEnv env;

        env.Publish(VOLUME_HASCLIENTS, 10);
        AssertPublishedVolumeCounters(env.AssertOnlyVolume(), 10, 10);

        env.Publish({}, 0);
        env.AssertDetached();

        env.Publish(VOLUME_HASCLIENTS, 3);
        AssertPublishedVolumeCounters(env.AssertOnlyVolume(), 3, 13);

        env.Flush();

        AssertPublishedVolumeCounters(env.AssertOnlyVolume(), 0, 13);
    }

    Y_UNIT_TEST(ShouldRegisterServiceVolumeUserCounters)
    {
        TServiceVolumeCountersTestEnv env;

        env.AssertUserCounters(false);

        env.Publish(VOLUME_HASCLIENTS, 7);
        env.AssertUserCounters(true, 7);

        env.Publish({}, 0);
        env.AssertDetached();
        env.AssertUserCounters(false);

        env.Publish(VOLUME_HASCLIENTS, 11);
        env.AssertUserCounters(true, 11);

        // Repeated publication must neither duplicate user metrics nor leave them bound to stale values.
        env.Publish(VOLUME_HASCLIENTS, 13);
        env.AssertUserCounters(true, 13);

        const TServiceVolumeLabels labels = {
            "new_cloud",
            "new_folder",
            "ssd",
        };

        env.UpdateConfig(labels);

        env.AssertUserCounters(true, 13, labels);

        env.Publish(VOLUME_HASCLIENTS, 17);
        env.AssertUserCounters(true, 17, labels);

        env.Publish({}, 0);
        env.AssertUserCounters(false);

        env.Publish(VOLUME_HASCHECKPOINT, 19);
        env.AssertUserCounters(true, 19, labels);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
