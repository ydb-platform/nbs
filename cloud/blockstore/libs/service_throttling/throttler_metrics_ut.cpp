#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats_test.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_logger.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>
#include <cloud/blockstore/libs/throttling/throttler_policy.h>
#include <cloud/blockstore/libs/throttling/throttler_tracker.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerPolicy final: public IThrottlerPolicy
{
public:
    TDuration PostponeTimeout;

    TDuration SuggestDelay(
        TInstant now,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        size_t byteCount) override
    {
        Y_UNUSED(now);
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(byteCount);

        return PostponeTimeout;
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const override
    {
        Y_UNUSED(ts);

        return PostponeTimeout.GetValue() / 1e6;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestCountingPolicy
{
    int PostponedCount[static_cast<int>(EBlockStoreRequest::MAX)] = {};
    int PostponedServerCount[static_cast<int>(EBlockStoreRequest::MAX)] = {};
    int AdvancedCount[static_cast<int>(EBlockStoreRequest::MAX)] = {};
    int AdvancedServerCount[static_cast<int>(EBlockStoreRequest::MAX)] = {};

    void RequestPostponed(EBlockStoreRequest requestType)
    {
        UNIT_ASSERT(IsReadWriteRequest(requestType));
        ++PostponedCount[static_cast<int>(requestType)];
    }

    void RequestPostponedServer(EBlockStoreRequest requestType)
    {
        UNIT_ASSERT(IsReadWriteRequest(requestType));
        ++PostponedServerCount[static_cast<int>(requestType)];
    }

    void RequestAdvanced(EBlockStoreRequest requestType)
    {
        UNIT_ASSERT(IsReadWriteRequest(requestType));
        ++AdvancedCount[static_cast<int>(requestType)];
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType)
    {
        UNIT_ASSERT(IsReadWriteRequest(requestType));
        ++AdvancedServerCount[static_cast<int>(requestType)];
    }
};

struct TVolumeProcessingPolicy
{
    std::shared_ptr<TTestVolumeInfo<TRequestCountingPolicy>> VolumeInfo =
        std::make_shared<TTestVolumeInfo<TRequestCountingPolicy>>();

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId)
    {
        Y_UNUSED(clientId);
        Y_UNUSED(instanceId);

        VolumeInfo->Volume = volume;
        return true;
    }

    void UnmountVolume(const TString& diskId, const TString& clientId)
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId)
    {
        Y_UNUSED(diskId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
    }

    IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const
    {
        Y_UNUSED(clientId);

        return diskId == VolumeInfo->Volume.GetDiskId() ? VolumeInfo : nullptr;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceThrotterMetricsTest)
{
    Y_UNIT_TEST(ShouldRegisterCountersOnlyAfterFirstNonZeroQuotaValue)
    {
        const TString clientId = "test_client";
        auto volumeStats =
            std::make_shared<TTestVolumeStats<TVolumeProcessingPolicy>>();

        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        scheduler->Start();

        auto policy = std::make_shared<TThrottlerPolicy>();

        auto requestStats = CreateRequestStatsStub();
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        auto metrics = CreateThrottlerMetrics(timer, totalCounters, "server");

        auto throttler = CreateThrottler(
            CreateThrottlerLoggerStub(),
            metrics,
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto client = std::make_shared<TTestService>();

#define SET_HANDLER(name)                                      \
    client->name##Handler =                                    \
        [&](std::shared_ptr<NProto::T##name##Request> request) \
    {                                                          \
        Y_UNUSED(request);                                     \
        return MakeFuture(NProto::T##name##Response());        \
    };                                                         \
    // SET_HANDLER

        SET_HANDLER(UnmountVolume);

#undef SET_HANDLER

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            NProto::TVolume volume;
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(100);

            NProto::TMountVolumeResponse r;
            r.MutableVolume()->SetDiskId(request->GetDiskId());

            return MakeFuture(std::move(r));
        };

        const TString usedQuota = "UsedQuota";
        const TString maxUsedQuota = "MaxUsedQuota";

        auto getDiskGroupFunction = [&](const TString& diskId)
        {
            return totalCounters->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->FindSubgroup("volume", diskId);
        };

        auto getClientGroupFunction =
            [&](const TString& diskId, const TString& clientId)
        {
            auto diskGroup = getDiskGroupFunction(diskId);
            UNIT_ASSERT_C(
                diskGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                                 << " should be initialized");
            return diskGroup->FindSubgroup("instance", clientId);
        };

        auto getCounterFunction = [&](const TString& diskId,
                                      const TString& clientId,
                                      const TString& sensor)
        {
            auto clientGroup = getClientGroupFunction(diskId, clientId);
            UNIT_ASSERT_C(
                clientGroup,
                TStringBuilder()
                    << "Subgroup volume:" << diskId << ", instance:" << clientId
                    << " should be initialized");
            return clientGroup->FindCounter(sensor);
        };

        const TString volumeId = "test_volume";
        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->MutableHeaders()->SetClientId(clientId);
        mountRequest->SetDiskId(volumeId);

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        UNIT_ASSERT_C(
            totalCounters->GetSubgroup("component", "server")
                ->FindCounter(usedQuota),
            "UsedQuota should be initialized");
        UNIT_ASSERT_C(
            totalCounters->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota),
            "MaxUsedQuota should be initialized");

        metrics->UpdateUsedQuota(0);
        metrics->UpdateMaxUsedQuota();

        UNIT_ASSERT_VALUES_EQUAL(
            totalCounters->GetSubgroup("component", "server")
                ->FindCounter(usedQuota)
                ->Val(),
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            totalCounters->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota)
                ->Val(),
            0);

        metrics->UpdateUsedQuota(12);
        metrics->UpdateMaxUsedQuota();

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto usedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, usedQuota);
            auto maxUsedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        auto unmountRequest = std::make_shared<NProto::TUnmountVolumeRequest>();
        unmountRequest->MutableHeaders()->SetClientId(clientId);
        unmountRequest->SetDiskId(volumeId);

        throttler->UnmountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            unmountRequest);

        {
            // Delete performs on next read
            totalCounters->GetSubgroup("component", "server")->ReadSnapshot();
            getClientGroupFunction(volumeId, clientId)->ReadSnapshot();

            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, usedQuota);
            auto maxUsedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, maxUsedQuota);

            UNIT_ASSERT_C(
                !usedQuotaCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaCounter,
                "MaxUsedQuota counters should not be initialized");
            UNIT_ASSERT_C(
                !usedQuotaVolumeCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should not be initialized");
        }
    }

    Y_UNIT_TEST(ShouldTrimCountersAfterTimeout)
    {
        const TString clientId = "test_client";
        auto volumeStats =
            std::make_shared<TTestVolumeStats<TVolumeProcessingPolicy>>();

        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = std::make_shared<TTestScheduler>();
        scheduler->Start();

        auto policy = std::make_shared<TThrottlerPolicy>();

        auto requestStats = CreateRequestStatsStub();
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        auto metrics = CreateThrottlerMetrics(timer, totalCounters, "server");

        auto throttler = CreateThrottler(
            CreateThrottlerLoggerStub(),
            metrics,
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            NProto::TVolume volume;
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(100);

            NProto::TMountVolumeResponse r;
            r.MutableVolume()->SetDiskId(request->GetDiskId());

            return MakeFuture(std::move(r));
        };

        const TString usedQuota = "UsedQuota";
        const TString maxUsedQuota = "MaxUsedQuota";

        auto getDiskGroupFunction = [&](const TString& diskId)
        {
            return totalCounters->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->FindSubgroup("volume", diskId);
        };

        auto getClientGroupFunction =
            [&](const TString& diskId, const TString& clientId)
        {
            auto diskGroup = getDiskGroupFunction(diskId);
            UNIT_ASSERT_C(
                diskGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                                 << " should be initialized");
            return diskGroup->FindSubgroup("instance", clientId);
        };

        auto getCounterFunction = [&](const TString& diskId,
                                      const TString& clientId,
                                      const TString& sensor)
        {
            auto clientGroup = getClientGroupFunction(diskId, clientId);
            UNIT_ASSERT_C(
                clientGroup,
                TStringBuilder()
                    << "Subgroup volume:" << diskId << ", instance:" << clientId
                    << " should be initialized");
            return clientGroup->FindCounter(sensor);
        };

        const TString volumeId = "test_volume";
        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->MutableHeaders()->SetClientId(clientId);
        mountRequest->SetDiskId(volumeId);

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

        metrics->UpdateUsedQuota(12);
        metrics->UpdateMaxUsedQuota();
        metrics->Trim(timer->Now());

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, usedQuota);
            auto maxUsedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);
        metrics->Trim(timer->Now());

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, usedQuota);
            auto maxUsedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);
        metrics->Trim(timer->Now());

        {
            // Delete performs on next read
            totalCounters->GetSubgroup("component", "server")->ReadSnapshot();
            getClientGroupFunction(volumeId, clientId)->ReadSnapshot();

            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, usedQuota);
            auto maxUsedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, maxUsedQuota);

            UNIT_ASSERT_C(
                !usedQuotaCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaCounter,
                "MaxUsedQuota counters should not be initialized");
            UNIT_ASSERT_C(
                !usedQuotaVolumeCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should not be initialized");
        }
    }

    Y_UNIT_TEST(ShouldTrimCountersAfterTimeoutWithZeroQuota)
    {
        const TString clientId = "test_client";
        auto volumeStats =
            std::make_shared<TTestVolumeStats<TVolumeProcessingPolicy>>();

        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = std::make_shared<TTestScheduler>();
        scheduler->Start();

        auto policy = std::make_shared<TThrottlerPolicy>();

        auto requestStats = CreateRequestStatsStub();
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        auto metrics = CreateThrottlerMetrics(timer, totalCounters, "server");

        auto throttler = CreateThrottler(
            CreateThrottlerLoggerStub(),
            metrics,
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto client = std::make_shared<TTestService>();
        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            NProto::TVolume volume;
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(100);

            NProto::TMountVolumeResponse r;
            r.MutableVolume()->SetDiskId(request->GetDiskId());

            return MakeFuture(std::move(r));
        };

        const TString usedQuota = "UsedQuota";
        const TString maxUsedQuota = "MaxUsedQuota";

        auto getDiskGroupFunction = [&](const TString& diskId)
        {
            return totalCounters->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->FindSubgroup("volume", diskId);
        };

        auto getClientGroupFunction =
            [&](const TString& diskId, const TString& clientId)
        {
            auto diskGroup = getDiskGroupFunction(diskId);
            UNIT_ASSERT_C(
                diskGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                                 << " should be initialized");
            return diskGroup->FindSubgroup("instance", clientId);
        };

        auto getCounterFunction = [&](const TString& diskId,
                                      const TString& clientId,
                                      const TString& sensor)
        {
            auto clientGroup = getClientGroupFunction(diskId, clientId);
            UNIT_ASSERT_C(
                clientGroup,
                TStringBuilder()
                    << "Subgroup volume:" << diskId << ", instance:" << clientId
                    << " should be initialized");
            return clientGroup->FindCounter(sensor);
        };

        const TString volumeId = "test_volume";
        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->MutableHeaders()->SetClientId(clientId);
        mountRequest->SetDiskId(volumeId);

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

        metrics->UpdateUsedQuota(12);
        metrics->UpdateMaxUsedQuota();
        metrics->Trim(timer->Now());

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, usedQuota);
            auto maxUsedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

        metrics->UpdateUsedQuota(0);
        metrics->UpdateMaxUsedQuota();
        metrics->Trim(timer->Now());

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, usedQuota);
            auto maxUsedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(0, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);
        metrics->Trim(timer->Now());

        {
            // Delete performs on next read
            totalCounters->GetSubgroup("component", "server")->ReadSnapshot();
            getClientGroupFunction(volumeId, clientId)->ReadSnapshot();

            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, usedQuota);
            auto maxUsedQuotaVolumeCounter =
                getCounterFunction(volumeId, clientId, maxUsedQuota);

            UNIT_ASSERT_C(
                !usedQuotaCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaCounter,
                "MaxUsedQuota counters should not be initialized");
            UNIT_ASSERT_C(
                !usedQuotaVolumeCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should not be initialized");
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyMultipleMountMultipleUnmount)
    {
        auto volumeStats =
            std::make_shared<TTestVolumeStats<TVolumeProcessingPolicy>>();

        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = std::make_shared<TTestScheduler>();
        scheduler->Start();

        auto policy = std::make_shared<TThrottlerPolicy>();

        auto requestStats = CreateRequestStatsStub();
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        auto metrics = CreateThrottlerMetrics(timer, totalCounters, "server");

        auto throttler = CreateThrottler(
            CreateThrottlerLoggerStub(),
            metrics,
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto client = std::make_shared<TTestService>();

#define SET_HANDLER(name)                                      \
    client->name##Handler =                                    \
        [&](std::shared_ptr<NProto::T##name##Request> request) \
    {                                                          \
        Y_UNUSED(request);                                     \
        return MakeFuture(NProto::T##name##Response());        \
    };                                                         \
    // SET_HANDLER

        SET_HANDLER(UnmountVolume);

#undef SET_HANDLER

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            NProto::TVolume volume;
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(100);

            NProto::TMountVolumeResponse r;
            r.MutableVolume()->SetDiskId(request->GetDiskId());

            return MakeFuture(std::move(r));
        };

        const TVector<std::pair<TString, TString>> diskIds = {
            {"first_test_disk_id", "first_client"},    // common first_client
            {"second_test_disk_id", "first_client"},   // common first_client
            {"second_test_disk_id",
             "second_client"},   // common second_test_disk_id and second_client
            {"third_test_disk_id",
             "second_client"},   // common third_test_disk_id
            {"third_test_disk_id",
             "third_client"},   // common third_test_disk_id
            {"fourth_test_disk_id", "fourh_client"}   // single disk and client
        };

        const TString usedQuota = "UsedQuota";
        const TString maxUsedQuota = "MaxUsedQuota";
        auto mountVolumeFunction =
            [&](const TString& diskId, const TString& clientId)
        {
            auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
            mountRequest->MutableHeaders()->SetClientId(clientId);
            mountRequest->SetDiskId(diskId);

            throttler->MountVolume(
                client,
                MakeIntrusive<TCallContext>(),
                mountRequest);

            timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

            metrics->UpdateUsedQuota(static_cast<ui64>(
                policy->CalculateCurrentSpentBudgetShare(timer->Now()) * 100.));
            metrics->UpdateMaxUsedQuota();
        };

        auto getDiskGroupFunction = [&](const TString& diskId)
        {
            return totalCounters->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->FindSubgroup("volume", diskId);
        };

        auto getClientGroupFunction =
            [&](const TString& diskId, const TString& clientId)
        {
            auto diskGroup = getDiskGroupFunction(diskId);
            UNIT_ASSERT_C(
                diskGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                                 << " should be initialized");
            return diskGroup->FindSubgroup("instance", clientId);
        };

        auto getCounterFunction = [&](const TString& diskId,
                                      const TString& clientId,
                                      const TString& sensor)
        {
            auto clientGroup = getClientGroupFunction(diskId, clientId);
            UNIT_ASSERT_C(
                clientGroup,
                TStringBuilder()
                    << "Subgroup volume:" << diskId << ", instance:" << clientId
                    << " should be initialized");
            return clientGroup->FindCounter(sensor);
        };

        auto unmountVolumeFunction =
            [&](const TString& diskId, const TString& clientId)
        {
            auto unmountRequest =
                std::make_shared<NProto::TUnmountVolumeRequest>();
            unmountRequest->MutableHeaders()->SetClientId(clientId);
            unmountRequest->SetDiskId(diskId);

            throttler->UnmountVolume(
                client,
                MakeIntrusive<TCallContext>(),
                unmountRequest);

            timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

            metrics->UpdateUsedQuota(static_cast<ui64>(
                policy->CalculateCurrentSpentBudgetShare(timer->Now()) * 100.));
            metrics->UpdateMaxUsedQuota();

            // Delete performs on next read
            totalCounters->GetSubgroup("component", "server")->ReadSnapshot();
            if (diskId != "not_disk" && clientId != "not_instance") {
                getClientGroupFunction(diskId, clientId)->ReadSnapshot();
            }
        };

        policy->PostponeTimeout = TDuration::MilliSeconds(500);
        mountVolumeFunction(diskIds[0].first, diskIds[0].second);

        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[1].first),
            TStringBuilder() << "Subgroup volume:" << diskIds[1].first
                             << " should not be initialized");
        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[3].first),
            TStringBuilder() << "Subgroup volume:" << diskIds[3].first
                             << " should not be initialized");
        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[5].first),
            TStringBuilder() << "Subgroup volume:" << diskIds[5].first
                             << " should not be initialized");

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(50, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter0->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(400);
        mountVolumeFunction(diskIds[1].first, diskIds[1].second);

        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[3].first),
            TStringBuilder() << "Subgroup volume:" << diskIds[3].first
                             << " should not be initialized");
        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[5].first),
            TStringBuilder() << "Subgroup volume:" << diskIds[5].first
                             << " should not be initialized");

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(40, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(40, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(40, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter1->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(300);
        mountVolumeFunction(diskIds[2].first, diskIds[2].second);

        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[3].first),
            TStringBuilder() << "Subgroup volume:" << diskIds[3].first
                             << " should not be initialized");
        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[5].first),
            TStringBuilder() << "Subgroup volume:" << diskIds[5].first
                             << " should not be initialized");

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(30, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(30, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(30, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(30, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter2->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(200);
        mountVolumeFunction(diskIds[3].first, diskIds[3].second);

        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[5].first),
            TStringBuilder() << "Subgroup volume:" << diskIds[5].first
                             << " should not be initialized");

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter3 = getCounterFunction(
                diskIds[3].first,
                diskIds[3].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter3 = getCounterFunction(
                diskIds[3].first,
                diskIds[3].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(20, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(20, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(20, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(20, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(20, usedQuotaVolumeCounter3->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter3->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(100);
        mountVolumeFunction(diskIds[4].first, diskIds[4].second);

        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[5].first),
            TStringBuilder() << "Subgroup volume:" << diskIds[5].first
                             << " should not be initialized");

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter3 = getCounterFunction(
                diskIds[3].first,
                diskIds[3].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter3 = getCounterFunction(
                diskIds[3].first,
                diskIds[3].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter4 = getCounterFunction(
                diskIds[4].first,
                diskIds[4].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter4 = getCounterFunction(
                diskIds[4].first,
                diskIds[4].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaVolumeCounter3->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter3->Val());
            UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaVolumeCounter4->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter4->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(10);
        mountVolumeFunction(diskIds[5].first, diskIds[5].second);

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter3 = getCounterFunction(
                diskIds[3].first,
                diskIds[3].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter3 = getCounterFunction(
                diskIds[3].first,
                diskIds[3].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter4 = getCounterFunction(
                diskIds[4].first,
                diskIds[4].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter4 = getCounterFunction(
                diskIds[4].first,
                diskIds[4].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter5 = getCounterFunction(
                diskIds[5].first,
                diskIds[5].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter5 = getCounterFunction(
                diskIds[5].first,
                diskIds[5].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(1, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, usedQuotaVolumeCounter3->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter3->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, usedQuotaVolumeCounter4->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter4->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, usedQuotaVolumeCounter5->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter5->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(1'000);
        unmountVolumeFunction(diskIds[4].first, diskIds[4].second);

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter3 = getCounterFunction(
                diskIds[3].first,
                diskIds[3].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter3 = getCounterFunction(
                diskIds[3].first,
                diskIds[3].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter5 = getCounterFunction(
                diskIds[5].first,
                diskIds[5].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter5 = getCounterFunction(
                diskIds[5].first,
                diskIds[5].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(100, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, usedQuotaVolumeCounter3->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter3->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, usedQuotaVolumeCounter5->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter5->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(900);
        unmountVolumeFunction(diskIds[3].first, diskIds[3].second);

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0].first,
                diskIds[0].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter5 = getCounterFunction(
                diskIds[5].first,
                diskIds[5].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter5 = getCounterFunction(
                diskIds[5].first,
                diskIds[5].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(90, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(90, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(90, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(90, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(90, usedQuotaVolumeCounter5->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter5->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(800);
        unmountVolumeFunction(diskIds[0].first, diskIds[0].second);

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter5 = getCounterFunction(
                diskIds[5].first,
                diskIds[5].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter5 = getCounterFunction(
                diskIds[5].first,
                diskIds[5].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(80, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(80, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(80, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(80, usedQuotaVolumeCounter5->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter5->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(700);
        unmountVolumeFunction(diskIds[5].first, diskIds[5].second);

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1].first,
                diskIds[1].second,
                maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(70, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(70, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(70, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter2->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(600);
        unmountVolumeFunction(diskIds[1].first, diskIds[1].second);

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(60, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(60, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter2->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(500);
        unmountVolumeFunction(diskIds[2].first, "not_instance");

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(50, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter2->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(400);
        unmountVolumeFunction("not_disk", diskIds[2].second);

        {
            auto usedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter =
                totalCounters->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                usedQuota);
            auto maxUsedQuotaVolumeCounter2 = getCounterFunction(
                diskIds[2].first,
                diskIds[2].second,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(40, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(40, usedQuotaVolumeCounter2->Val());
            UNIT_ASSERT_VALUES_EQUAL(100, maxUsedQuotaVolumeCounter2->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(300);
        unmountVolumeFunction(diskIds[2].first, diskIds[2].second);

        {
            UNIT_ASSERT_C(
                !totalCounters->GetSubgroup("component", "server")
                     ->FindCounter(usedQuota),
                "UsedQuota should not be initialized");
            UNIT_ASSERT_C(
                !totalCounters->GetSubgroup("component", "server")
                     ->FindCounter(maxUsedQuota),
                "MaxUsedQuota should not be initialized");
        }
    }
}

}   // namespace NCloud::NBlockStore
