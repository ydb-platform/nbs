#include "metric.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/start_endpoint_test.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

void CheckStartEndpointMetrics(
    const TStartEndpointMode& modes,
    ui32 errorCode,
    bool setModes = true)
{
    auto monitoring = CreateMonitoringServiceStub();
    auto counters = monitoring->GetCounters()
                        ->GetSubgroup("counters", "blockstore")
                        ->GetSubgroup("component", "client");
    auto serverStats = CreateClientStats(
        std::make_shared<TClientAppConfig>(),
        monitoring,
        CreateClientRequestStats(
            counters,
            std::make_shared<TTestTimer>(),
            EHistogramCounterOption::ReportMultipleCounters),
        CreateVolumeStatsStub(),
        "instance");

    auto service = std::make_shared<TTestService>();
    auto promise = NewPromise<NProto::TStartEndpointResponse>();
    service->StartEndpointHandler = [promise](auto)
    {
        return promise.GetFuture();
    };
    auto client = CreateMetricClient(
        service,
        CreateLoggingService("console"),
        serverStats);

    auto callContext = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<NProto::TStartEndpointRequest>();
    if (setModes) {
        request->SetVolumeAccessMode(modes.AccessMode);
        request->SetVolumeMountMode(modes.MountMode);
    }
    auto future = client->StartEndpoint(callContext, request);
    UNIT_ASSERT(!future.HasValue());

    auto selectedCounters =
        counters->GetSubgroup("mount_mode", modes.MountLabel)
            ->GetSubgroup("access_mode", modes.AccessLabel)
            ->GetSubgroup("request", "StartEndpoint");
    auto totalCounters = counters->GetSubgroup("request", "StartEndpoint");
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        selectedCounters->GetCounter("InProgress")->Val());
    UNIT_ASSERT_VALUES_EQUAL(1, totalCounters->GetCounter("InProgress")->Val());

    const auto elapsed = TDuration::MilliSeconds(100);
    callContext->SetRequestStartedCycles(
        GetCycleCount() - DurationToCyclesSafe(elapsed));
    size_t collected = 0;
    const auto count = client->CollectRequests(
        [&](TCallContext& context,
            const TMetricRequest& metricRequest,
            TRequestTime requestTime)
        {
            ++collected;
            UNIT_ASSERT_VALUES_EQUAL(callContext.Get(), &context);
            UNIT_ASSERT(!metricRequest.VolumeInfo);
            UNIT_ASSERT_EQUAL(
                NProto::STORAGE_MEDIA_HDD,
                metricRequest.MediaKind);
            UNIT_ASSERT_EQUAL(
                EBlockStoreRequest::StartEndpoint,
                metricRequest.RequestType);
            UNIT_ASSERT_EQUAL(modes.AccessMode, metricRequest.AccessMode);
            UNIT_ASSERT_EQUAL(modes.MountMode, metricRequest.MountMode);
            UNIT_ASSERT_GE(requestTime.TotalTime, elapsed);
            UNIT_ASSERT_GE(requestTime.ExecutionTime, elapsed);
            serverStats->AddIncompleteRequest(
                context,
                metricRequest,
                requestTime);
        });
    UNIT_ASSERT_VALUES_EQUAL(1, count);
    UNIT_ASSERT_VALUES_EQUAL(1, collected);
    serverStats->UpdateStats(false);
    UNIT_ASSERT_GE(
        selectedCounters->GetCounter("MaxTime")->Val(),
        static_cast<i64>(elapsed.MicroSeconds()));
    UNIT_ASSERT_VALUES_EQUAL(
        totalCounters->GetCounter("MaxTime")->Val(),
        selectedCounters->GetCounter("MaxTime")->Val());
    UNIT_ASSERT_VALUES_EQUAL(
        0,
        selectedCounters->GetCounter("Count", true)->Val());

    if (errorCode == E_CANCELLED) {
        client->Stop();
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            errorCode,
            future.GetValue().GetError().GetCode());

        // The original service may finish after cancellation. It must not
        // count the request a second time or restore it in CollectRequests.
        promise.SetValue(NProto::TStartEndpointResponse());
    } else {
        NProto::TStartEndpointResponse response;
        response.MutableError()->SetCode(errorCode);
        promise.SetValue(response);
    }
    UNIT_ASSERT(future.HasValue());
    UNIT_ASSERT_VALUES_EQUAL(errorCode, future.GetValue().GetError().GetCode());
    UNIT_ASSERT_VALUES_EQUAL(
        0,
        client->CollectRequests(CreateIncompleteRequestsCollectorStub()));

    serverStats->UpdateStats(true);
    const ui64 expectedCount = errorCode == S_OK ? 1 : 0;
    const ui64 expectedErrors = errorCode == S_OK ? 0 : 1;
    for (const auto& mount: {"local", "remote"}) {
        for (const auto& access: {"read_write", "read_only"}) {
            auto group = counters->GetSubgroup("mount_mode", mount)
                             ->GetSubgroup("access_mode", access)
                             ->GetSubgroup("request", "StartEndpoint");
            const bool selected = TString(modes.MountLabel) == mount &&
                                  TString(modes.AccessLabel) == access;
            UNIT_ASSERT_VALUES_EQUAL(0, group->GetCounter("InProgress")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                selected ? expectedCount : 0,
                group->GetCounter("Count", true)->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                selected ? expectedErrors : 0,
                group->GetCounter("Errors", true)->Val());
            if (!selected) {
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    group->GetCounter("MaxTime")->Val());
            }
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(0, totalCounters->GetCounter("InProgress")->Val());
    UNIT_ASSERT_VALUES_EQUAL(
        expectedCount,
        totalCounters->GetCounter("Count", true)->Val());
    UNIT_ASSERT_VALUES_EQUAL(
        expectedErrors,
        totalCounters->GetCounter("Errors", true)->Val());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMetricClientTest)
{
    Y_UNIT_TEST(ShouldTrackStartEndpointModesWhileRunningAndAfterCompletion)
    {
        for (const auto& modes: AllStartEndpointModes) {
            CheckStartEndpointMetrics(modes, S_OK);
        }
    }

    Y_UNIT_TEST(ShouldTrackDefaultStartEndpointModes)
    {
        CheckStartEndpointMetrics(StartEndpointModes[0], S_OK, false);
    }

    Y_UNIT_TEST(ShouldTrackFailedStartEndpointModes)
    {
        for (const auto& modes: AllStartEndpointModes) {
            CheckStartEndpointMetrics(modes, E_FAIL);
        }
    }

    Y_UNIT_TEST(ShouldTrackCancelledStartEndpointModesOnlyOnce)
    {
        for (const auto& modes: AllStartEndpointModes) {
            CheckStartEndpointMetrics(modes, E_CANCELLED);
        }
    }
}

}   // namespace NCloud::NBlockStore::NClient
