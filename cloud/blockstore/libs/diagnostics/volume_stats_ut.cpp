#include "volume_stats.h"

#include "config.h"

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_consumer.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/size_literals.h>

#include <tuple>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultCloudId = "cloud_id";
const TString DefaultFolderId = "folder_id";

////////////////////////////////////////////////////////////////////////////////

class TLabelKeeper: public NMonitoring::IMetricConsumer
{
private:
    std::vector<std::pair<TString, TString>> Labels;
    THashMap<TString, TString> ValueMap;
    TString CurrentLabel;

public:
    size_t FindLabel(TStringBuf name, TStringBuf value)
    {
        return CountIf(
            Labels,
            [name, value](const auto& labels)
            {
                return labels.first == name.data() &&
                       labels.second == value.data();
            });
    }

    TString GetValue(const TString labelName) const
    {
        auto it = ValueMap.find(labelName);
        return it == ValueMap.end() ? TString() : it->second;
    }

    void OnStreamBegin() override
    {}
    void OnStreamEnd() override
    {}
    void OnCommonTime(TInstant) override
    {}
    void OnMetricBegin(NMonitoring::EMetricType) override
    {}
    void OnMetricEnd() override
    {}

    void OnLabelsBegin() override
    {
        CurrentLabel.clear();
    }

    void OnLabelsEnd() override
    {}

    void OnLabel(TStringBuf name, TStringBuf value) override
    {
        Labels.emplace_back(name.data(), value.data());
        if (!CurrentLabel.empty()) {
            CurrentLabel += ".";
        }
        CurrentLabel += value;
    }

    void OnLabel(ui32, ui32) override
    {}

    void OnDouble(TInstant, double value) override
    {
        ValueMap[CurrentLabel] = ToString(value);
    }

    void OnInt64(TInstant, i64 value) override
    {
        ValueMap[CurrentLabel] = ToString(value);
    }

    void OnUint64(TInstant, ui64 value) override
    {
        ValueMap[CurrentLabel] = ToString(value);
    }

    void OnHistogram(TInstant, NMonitoring::IHistogramSnapshotPtr) override
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

void Mount(
    IVolumeStatsPtr volumeStats,
    const TString& name,
    const TString& client,
    const TString& instance,
    NCloud::NProto::EStorageMediaKind mediaKind)
{
    NProto::TVolume volume;
    volume.SetDiskId(name);
    volume.SetStorageMediaKind(mediaKind);
    volume.SetBlockSize(DefaultBlockSize);
    volume.SetCloudId(DefaultCloudId);
    volume.SetFolderId(DefaultFolderId);

    volumeStats->MountVolume(volume, client, instance);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeStatsTest)
{
    Y_TEST_HOOK_BEFORE_RUN(InitTest)
    {
        // NHPTimer warmup, see issue #2830 for more information
        Y_UNUSED(GetCyclesPerMillisecond());
    }

    Y_UNIT_TEST(ShouldTrackRequestsPerVolume)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto getCounters = [&] (auto volume, auto instance) {
            return counters
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", volume)
                ->GetSubgroup("instance", instance)
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId);
        };

        auto writeData = [](auto volume, auto type){
            auto started = volume->RequestStarted(
                type,
                1024 * 1024);

            volume->RequestCompleted(
                type,
                started,
                TDuration::Zero(),
                1024 * 1024,
                EDiagnosticsErrorKind::Success,
                NCloud::NProto::EF_NONE,
                false,
                0);
        };

        auto readData = [](auto volume, auto type){
            auto started = volume->RequestStarted(
                type,
                1024 * 1024);

            volume->RequestCompleted(
                type,
                started,
                TDuration::Zero(),
                1024 * 1024,
                EDiagnosticsErrorKind::Success,
                NCloud::NProto::EF_NONE,
                false,
                0);
        };

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        Mount(
            volumeStats,
            "test2",
            "client2",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_HDD);
        Mount(
            volumeStats,
            "test2",
            "client3",
            "instance2",
            NCloud::NProto::STORAGE_MEDIA_HDD);

        auto volume1 = volumeStats->GetVolumeInfo("test1", "client1");
        auto volume2 = volumeStats->GetVolumeInfo("test2", "client2");
        auto volume3 = volumeStats->GetVolumeInfo("test2", "client3");

        auto volume1Counters = getCounters("test1", "instance1");
        auto volume1WriteCount = volume1Counters
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("Count");
        auto volume1ReadCount = volume1Counters
            ->GetSubgroup("request", "ReadBlocks")
            ->GetCounter("Count");

        auto volume2Counters = getCounters("test2", "instance1");
        auto volume2WriteCount = volume2Counters
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("Count");

        auto volume3Counters = getCounters("test2", "instance2");
        auto volume3WriteCount = volume3Counters
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("Count");

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 0);

        writeData(volume1, EBlockStoreRequest::WriteBlocks);

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 0);

        writeData(volume2, EBlockStoreRequest::WriteBlocks);
        readData(volume1, EBlockStoreRequest::ReadBlocks);

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 0);

        writeData(volume3, EBlockStoreRequest::WriteBlocks);

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 1);

        writeData(volume1, EBlockStoreRequest::WriteBlocksLocal);
        readData(volume1, EBlockStoreRequest::ReadBlocksLocal);

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 2);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 2);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 1);
    }

    Y_UNIT_TEST(ShouldRegisterAndUnregisterCountersPerVolume)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume");

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        Mount(
            volumeStats,
            "test2",
            "client2",
            "instance2",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        UNIT_ASSERT(counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "test1")
            ->GetSubgroup("instance", "instance1")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "test2")
            ->GetSubgroup("instance", "instance2")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        Timer->AdvanceTime(inactivityTimeout * 0.5);
        volumeStats->TrimVolumes();

        Mount(
            volumeStats,
            "test2",
            "client3",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        UNIT_ASSERT(counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "test1")
            ->GetSubgroup("instance", "instance1")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "test2")
            ->GetSubgroup("instance", "instance2")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "test2")
            ->GetSubgroup("instance", "instance1")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        Timer->AdvanceTime(inactivityTimeout * 0.6);
        volumeStats->TrimVolumes();

        UNIT_ASSERT(!counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "test1")
            ->GetSubgroup("instance", "instance1")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(!counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "test2")
            ->GetSubgroup("instance", "instance2")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "test2")
            ->GetSubgroup("instance", "instance1")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));
    }

    Y_UNIT_TEST(ShouldTrackSilentErrorsPerVolume)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto getCounters = [&] (auto volume, auto instance) {
            auto volumeCounters = counters
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", volume)
                ->GetSubgroup("instance", instance)
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetSubgroup("request", "WriteBlocks");

            return std::make_pair(
                volumeCounters->GetCounter("Errors/Fatal"),
                volumeCounters->GetCounter("Errors/Silent")
            );
        };

        auto shoot = [] (auto volume, auto errorKind) {
            auto started = volume->RequestStarted(
                EBlockStoreRequest::WriteBlocks,
                1024 * 1024);

            volume->RequestCompleted(
                EBlockStoreRequest::WriteBlocks,
                started,
                TDuration::Zero(),
                1024 * 1024,
                errorKind,
                NCloud::NProto::EF_NONE,
                false,
                0);
        };

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        Mount(
            volumeStats,
            "test2",
            "client2",
            "instance2",
            NCloud::NProto::STORAGE_MEDIA_HDD);

        auto volume1 = volumeStats->GetVolumeInfo("test1", "client1");
        auto volume2 = volumeStats->GetVolumeInfo("test2", "client2");

        auto [volume1Errors, volume1Silent] = getCounters("test1", "instance1");
        auto [volume2Errors, volume2Silent] = getCounters("test2", "instance2");

        UNIT_ASSERT_VALUES_EQUAL(0, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Silent->Val());

        shoot(volume1, EDiagnosticsErrorKind::ErrorFatal);

        UNIT_ASSERT_VALUES_EQUAL(1, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Silent->Val());

        shoot(volume2, EDiagnosticsErrorKind::ErrorFatal);

        UNIT_ASSERT_VALUES_EQUAL(1, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Silent->Val());

        shoot(volume1, EDiagnosticsErrorKind::ErrorSilent);

        UNIT_ASSERT_VALUES_EQUAL(1, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Silent->Val());

        shoot(volume2, EDiagnosticsErrorKind::ErrorSilent);

        UNIT_ASSERT_VALUES_EQUAL(1, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume2Silent->Val());
    }

    Y_UNIT_TEST(ShouldTrackHwProblemsPerVolume)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto getCounters = [&] (auto volume, auto instance) {
            auto volumeCounters = counters
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", volume)
                ->GetSubgroup("instance", instance)
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId);

            return std::make_tuple(
                volumeCounters
                    ->GetSubgroup("request", "WriteBlocks")
                    ->GetCounter("Errors/Fatal"),
                volumeCounters->GetCounter("HwProblems")
            );
        };

        auto shoot = [] (auto volume, auto errorKind, ui32 errorFlags) {
            auto started = volume->RequestStarted(
                EBlockStoreRequest::WriteBlocks,
                1024 * 1024);

            volume->RequestCompleted(
                EBlockStoreRequest::WriteBlocks,
                started,
                TDuration::Zero(),
                1024 * 1024,
                errorKind,
                errorFlags,
                false,
                0);
        };

        auto mount = [&volumeStats, &getCounters] (
            const TString& name,
            NCloud::NProto::EStorageMediaKind mediaKind)
        {
            const auto client = name + "Client";
            const auto instance = name + "Instance";

            Mount(
                volumeStats,
                name,
                client,
                instance,
                mediaKind);

            auto stats = volumeStats->GetVolumeInfo(name, client);
            auto [errors, hwProblems] = getCounters(name, instance);

            return std::make_tuple(
                std::move(stats),
                std::move(errors),
                std::move(hwProblems));
        };

        auto [localStats, localErrors, localHwProblems] =
            mount("local", NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL);
        auto [nonreplStats, nonreplErrors, nonreplHwProblems] =
            mount("nonrepl", NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        auto [hddNonreplStats, hddNonreplErrors, hddNonreplHwProblems] =
            mount("hdd_nonrepl", NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED);
        auto [ssdStats, ssdErrors, ssdHwProblems] =
            mount("ssd", NCloud::NProto::STORAGE_MEDIA_SSD);

        UNIT_ASSERT_VALUES_EQUAL(0, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            localStats,
            EDiagnosticsErrorKind::ErrorFatal,
            NCloud::NProto::EF_NONE);

        UNIT_ASSERT_VALUES_EQUAL(1, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            nonreplStats,
            EDiagnosticsErrorKind::ErrorSilent,
            NCloud::NProto::EF_NONE);

        UNIT_ASSERT_VALUES_EQUAL(1, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            hddNonreplStats,
            EDiagnosticsErrorKind::ErrorSilent,
            NCloud::NProto::EF_NONE);

        UNIT_ASSERT_VALUES_EQUAL(1, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            ssdStats,
            EDiagnosticsErrorKind::ErrorFatal,
            NCloud::NProto::EF_NONE);

        UNIT_ASSERT_VALUES_EQUAL(1, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            localStats,
            EDiagnosticsErrorKind::ErrorFatal,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            nonreplStats,
            EDiagnosticsErrorKind::ErrorSilent,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            hddNonreplStats,
            EDiagnosticsErrorKind::ErrorSilent,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            ssdStats,
            EDiagnosticsErrorKind::ErrorFatal,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        // Check if mirror disks are forgotten.
        auto [mirror2Stats, mirror2Errors, mirror2HwProblems] =
            mount("mirror2", NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2);
        auto [mirror3Stats, mirror3Errors, mirror3HwProblems] =
            mount("mirror3", NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3);

        shoot(
            mirror2Stats,
            EDiagnosticsErrorKind::ErrorSilent,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);
        shoot(
            mirror3Stats,
            EDiagnosticsErrorKind::ErrorFatal,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, mirror2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, mirror2HwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, mirror3Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, mirror2HwProblems->Val());
    }

    void DoTestShouldReportSufferMetrics(
        const TVector<TString>& strictSLACloudIds,
        bool reportStrictSLA)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server");

        NProto::TDiagnosticsConfig cfg;
        cfg.MutableSsdPerfSettings()->MutableWrite()->SetIops(4200);
        cfg.MutableSsdPerfSettings()->MutableWrite()->SetBandwidth(342000000);
        cfg.MutableSsdPerfSettings()->MutableRead()->SetIops(4200);
        cfg.MutableSsdPerfSettings()->MutableRead()->SetBandwidth(342000000);
        for (const auto& cloudId: strictSLACloudIds) {
            *cfg.AddCloudIdsWithStrictSLA() = cloudId;
        }
        auto diagConfig = std::make_shared<TDiagnosticsConfig>(std::move(cfg));

        auto volumeStats = CreateVolumeStats(
            monitoring,
            diagConfig,
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        auto volume = volumeStats->GetVolumeInfo("test1", "client1");

        auto requestDuration = TDuration::MilliSeconds(400) +
            diagConfig->GetExpectedIoParallelism() * CostPerIO(
                diagConfig->GetSsdPerfSettings().WriteIops,
                diagConfig->GetSsdPerfSettings().WriteBandwidth,
                1_MB);
        auto durationInCycles = DurationToCyclesSafe(requestDuration);
        auto now = GetCycleCount();

        volume->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            now - Min(now, durationInCycles),
            {},
            1_MB,
            {},
            NCloud::NProto::EF_NONE,
            false,
            0);

        volumeStats->UpdateStats(false);

        auto sufferArray = volumeStats->GatherVolumePerfStatuses();
        UNIT_ASSERT_VALUES_EQUAL(1, sufferArray.size());
        UNIT_ASSERT_VALUES_EQUAL("test1", sufferArray[0].first);
        UNIT_ASSERT_VALUES_EQUAL(1, sufferArray[0].second);

        auto disksSufferCounter = counters->GetCounter("DisksSuffer", false);
        auto ssdDisksSufferCounter = counters->GetSubgroup("type", "ssd")
            ->GetCounter("DisksSuffer", false);
        auto hddDisksSufferCounter = counters->GetSubgroup("type", "hdd")
            ->GetCounter("DisksSuffer", false);
        auto smoothDisksSufferCounter =
            counters->GetCounter("SmoothDisksSuffer", false);
        auto smoothSsdDisksSufferCounter = counters->GetSubgroup("type", "ssd")
            ->GetCounter("SmoothDisksSuffer", false);
        auto smoothHddDisksSufferCounter = counters->GetSubgroup("type", "hdd")
            ->GetCounter("SmoothDisksSuffer", false);
        auto criticalDisksSufferCounter =
            counters->GetCounter("CriticalDisksSuffer", false);
        auto criticalSsdDisksSufferCounter = counters->GetSubgroup("type", "ssd")
            ->GetCounter("CriticalDisksSuffer", false);
        auto criticalHddDisksSufferCounter = counters->GetSubgroup("type", "hdd")
            ->GetCounter("CriticalDisksSuffer", false);

        auto strictSLADisksSufferCounter =
            counters->GetCounter("StrictSLADisksSuffer", false);

        UNIT_ASSERT_VALUES_EQUAL(1, disksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(1, smoothDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, smoothSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, smoothHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(1, criticalDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, criticalSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            reportStrictSLA ? 1 : 0,
            strictSLADisksSufferCounter->Val());

        // a bunch of fast requests
        const auto fastRequestCyclesCount =
            DurationToCyclesSafe(TDuration::MilliSeconds(10));
        for (ui32 i = 0; i < 5; ++i) {
            now = GetCycleCount();
            volume->RequestCompleted(
                EBlockStoreRequest::WriteBlocks,
                now - Min(now, fastRequestCyclesCount),
                {},
                1_MB,
                {},
                NCloud::NProto::EF_NONE,
                false,
                0);
        }

        volumeStats->UpdateStats(false);

        UNIT_ASSERT_VALUES_EQUAL(1, disksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, smoothDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, smoothSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, smoothHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, criticalDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, strictSLADisksSufferCounter->Val());

        // a bunch of slow but not critically slow requests
        const auto slowRequestCyclesCount =
            DurationToCyclesSafe(TDuration::MilliSeconds(110));
        for (ui32 i = 0; i < 20; ++i) {
            now = GetCycleCount();
            volume->RequestCompleted(
                EBlockStoreRequest::WriteBlocks,
                now - Min(now, slowRequestCyclesCount),
                {},
                1_MB,
                {},
                NCloud::NProto::EF_NONE,
                false,
                0);
        }

        volumeStats->UpdateStats(false);

        UNIT_ASSERT_VALUES_EQUAL(1, disksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(1, smoothDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, smoothSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, smoothHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, criticalDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            reportStrictSLA ? 1 : 0,
            strictSLADisksSufferCounter->Val());
    }

    Y_UNIT_TEST(ShouldReportSufferMetrics)
    {
        DoTestShouldReportSufferMetrics({"something"}, false);
    }

    Y_UNIT_TEST(ShouldReportSufferMetricsWithStrictSLAFilter)
    {
        DoTestShouldReportSufferMetrics({DefaultCloudId}, true);
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculatePossiblePostponeTimeForVolume)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto volumeStats = CreateVolumeStats(
            CreateMonitoringServiceStub(),
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        TVector<TString> clients = {"client1", "client2"};
        TVector<TString> volumes = {"test1", "test2"};
        TVector<IVolumeInfoPtr> volumeInfos;

        for (size_t i = 0; i < Min(clients.size(), volumes.size()); ++i) {
            Mount(
                volumeStats,
                volumes[i],
                clients[i],
                "instance" + std::to_string(i),
                NCloud::NProto::STORAGE_MEDIA_SSD);
            volumeInfos.push_back(
                volumeStats->GetVolumeInfo(volumes[i], clients[i]));
        }

        const auto postponeDuration = TDuration::Seconds(1);

        volumeInfos[0]->RequestStarted(
            EBlockStoreRequest::WriteBlocks,
            1024);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[1]->GetPossiblePostponeDuration());

        volumeInfos[0]->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            timer->Now().MicroSeconds(),
            postponeDuration,
            1024,
            {},
            NCloud::NProto::EF_NONE,
            false,
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[1]->GetPossiblePostponeDuration());

        timer->AdvanceTime(config->GetPostponeTimePredictorInterval() / 2);

        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[1]->GetPossiblePostponeDuration());

        volumeInfos[1]->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            timer->Now().MicroSeconds(),
            postponeDuration,
            1024,
            {},
            NCloud::NProto::EF_NONE,
            false,
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[1]->GetPossiblePostponeDuration());

        timer->AdvanceTime(config->GetPostponeTimePredictorInterval() / 2);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[1]->GetPossiblePostponeDuration());

        timer->AdvanceTime(config->GetPostponeTimePredictorInterval() / 2);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[1]->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldTrackDownDisksForCompletedRequests)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        TString client {"client1"};
        TString volume {"test1"};
        IVolumeInfoPtr volumeInfo;

        Mount(
            volumeStats,
            volume,
            client,
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        volumeInfo = volumeStats->GetVolumeInfo(volume, client);

        timer->AdvanceTime(TDuration::Seconds(15));

        volumeInfo->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            timer->Now().MicroSeconds(),
            TDuration(),
            1024,
            {},
            NCloud::NProto::EF_NONE,
            false,
            0);

        volumeStats->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(0, counters->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", "test1")
                ->GetSubgroup("instance", "instance")
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetCounter("HasDowntime")
                ->Val());

        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, counters->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", "test1")
                ->GetSubgroup("instance", "instance")
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetCounter("HasDowntime")
                ->Val());
    }

    Y_UNIT_TEST(ShouldTrackDownDisksForIncompleteRequests)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        TString client {"client1"};
        TString volume {"test1"};
        IVolumeInfoPtr volumeInfo;

        Mount(
            volumeStats,
            volume,
            client,
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        volumeInfo = volumeStats->GetVolumeInfo(volume, client);

        volumeInfo->AddIncompleteStats(
            EBlockStoreRequest::WriteBlocks,
            TRequestTime{
                .TotalTime = TDuration::Seconds(15),
                .ExecutionTime = TDuration::Seconds(15)
            }
        );

        timer->AdvanceTime(TDuration::Seconds(15));

        volumeStats->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(0, counters->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("DownDisks")->Val());

        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, counters->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("DownDisks")->Val());
    }

    Y_UNIT_TEST(ShouldAlterVolume)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume");

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        NProto::TVolume volume;
        volume.SetDiskId("volume-1");
        volume.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);
        volume.SetBlockSize(DefaultBlockSize);

        volume.SetCloudId("cloud-1");
        volume.SetFolderId("folder-1");
        volumeStats->MountVolume(volume, "client-1", "instance-1");

        {
            TLabelKeeper keeper;
            volumeStats->GetUserCounters()->Append(TInstant::Now(), &keeper);

            UNIT_ASSERT_UNEQUAL(keeper.FindLabel("project", "cloud-1"), 0);
            UNIT_ASSERT_UNEQUAL(keeper.FindLabel("cluster", "folder-1"), 0);
            UNIT_ASSERT_EQUAL(keeper.FindLabel("project", "cloud-2"), 0);
            UNIT_ASSERT_EQUAL(keeper.FindLabel("cluster", "folder-2"), 0);
        }

        volume.SetCloudId("cloud-2");
        volume.SetFolderId("folder-2");
        volumeStats->MountVolume(volume, "client-1", "instance-1");

        {
            TLabelKeeper keeper;
            volumeStats->GetUserCounters()->Append(TInstant::Now(), &keeper);

            UNIT_ASSERT_EQUAL(keeper.FindLabel("project", "cloud-1"), 0);
            UNIT_ASSERT_EQUAL(keeper.FindLabel("cluster", "folder-1"), 0);
            UNIT_ASSERT_UNEQUAL(keeper.FindLabel("project", "cloud-2"), 0);
            UNIT_ASSERT_UNEQUAL(keeper.FindLabel("cluster", "folder-2"), 0);
        }
    }

    Y_UNIT_TEST(ShouldMountSeveralClientsOnOneInstance)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume");

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        Mount(
            volumeStats,
            "Disk-1",
            "Client-1",
            "Instance-1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        Mount(
            volumeStats,
            "Disk-1",
            "Client-2",
            "Instance-1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        UNIT_ASSERT(counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "Disk-1")
            ->GetSubgroup("instance", "Instance-1")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        {
            auto client1Info = volumeStats->GetVolumeInfo("Disk-1", "Client-2");
            auto client2Info = volumeStats->GetVolumeInfo("Disk-1", "Client-2");
            UNIT_ASSERT_EQUAL(client1Info.get(), client2Info.get());
            UNIT_ASSERT(client1Info);
        }

        Timer->AdvanceTime(inactivityTimeout * 0.5);

        Mount(
            volumeStats,
            "Disk-1",
            "Client-2",
            "Instance-1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        Timer->AdvanceTime(inactivityTimeout * 0.6);
        volumeStats->TrimVolumes();

        UNIT_ASSERT(counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "Disk-1")
            ->GetSubgroup("instance", "Instance-1")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        Timer->AdvanceTime(inactivityTimeout * 1.1);
        volumeStats->TrimVolumes();

        UNIT_ASSERT(!counters
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "Disk-1")
            ->GetSubgroup("instance", "Instance-1")
            ->GetSubgroup("cloud", DefaultCloudId)
            ->FindSubgroup("folder", DefaultFolderId));

        {
            auto client1Info = volumeStats->GetVolumeInfo("Disk-1", "Client-2");
            auto client2Info = volumeStats->GetVolumeInfo("Disk-1", "Client-2");
            UNIT_ASSERT_EQUAL(client1Info.get(), client2Info.get());
            UNIT_ASSERT(!client1Info);
        }
    }

    Y_UNIT_TEST(ShouldSkipReportingZeroBlocksMetricsForYDBBasedDisks)
    {
        auto monitoring = CreateMonitoringServiceStub();
        NProto::TDiagnosticsConfig diagnostics;
        diagnostics.SetSkipReportingZeroBlocksMetricsForYDBBasedDisks(true);

        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            std::make_shared<TDiagnosticsConfig>(diagnostics),
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto sendRequest = [](auto volume, auto type)
        {
            auto started = volume->RequestStarted(type, 1024 * 1024);

            volume->RequestCompleted(
                type,
                started,
                TDuration::Zero(),
                1024 * 1024,
                EDiagnosticsErrorKind::Success,
                NCloud::NProto::EF_NONE,
                false,
                0);
        };

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        Mount(
            volumeStats,
            "test2",
            "client2",
            "instance2",
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3);

        auto volume1 = volumeStats->GetVolumeInfo("test1", "client1");
        auto volume2 = volumeStats->GetVolumeInfo("test2", "client2");

        sendRequest(volume1, EBlockStoreRequest::WriteBlocks);
        sendRequest(volume1, EBlockStoreRequest::ZeroBlocks);
        sendRequest(volume2, EBlockStoreRequest::WriteBlocks);
        sendRequest(volume2, EBlockStoreRequest::ZeroBlocks);

        TLabelKeeper keeper;
        volumeStats->GetUserCounters()->Append(TInstant::Now(), &keeper);

        UNIT_ASSERT_EQUAL(
            keeper.GetValue(
                "compute.cloud_id.folder_id.test1.instance1.disk.write_ops"),
            "1");
        UNIT_ASSERT_EQUAL(
            keeper.GetValue(
                "compute.cloud_id.folder_id.test2.instance2.disk.write_ops"),
            "2");
    }
}

}   // namespace NCloud::NBlockStore
