#include "request_counters.h"

#include "monitoring.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/histogram_types.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/size_literals.h>
#include <util/string/cast.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    size_t RequestBytes = 0;
    TDuration RequestTime;
    TDuration PostponedTime;
    TDuration BackoffTime;
    TDuration ShapingTime;
    bool Aligned = false;
    ui64 RequestCompletionTime = 0;
};

////////////////////////////////////////////////////////////////////////////////

void AddRequestStats(
    TRequestCounters& requestCounters,
    TRequestCounters::TRequestType requestType,
    std::initializer_list<TRequest> requests)
{
    for (const auto& request: requests) {
        auto requestStarted = requestCounters.RequestStarted(
            requestType,
            request.RequestBytes);

        auto realRequestStarted = requestStarted -
            DurationToCyclesSafe(request.RequestTime) -
            request.RequestCompletionTime;

        ui64 responseSent = request.RequestCompletionTime ?
            realRequestStarted + DurationToCyclesSafe(request.RequestTime) : 0;

        requestCounters.RequestCompleted(
            requestType,
            realRequestStarted,
            request.PostponedTime,
            request.BackoffTime,
            request.ShapingTime,
            request.RequestBytes,
            EDiagnosticsErrorKind::Success,
            NCloud::NProto::EF_NONE,
            request.Aligned,
            ECalcMaxTime::ENABLE,
            responseSent);
    }
}

void AddIncompleteStats(
    TRequestCounters& requestCounters,
    TRequestCounters::TRequestType requestType,
    std::initializer_list<TDuration> requests)
{
    for (auto executionTime: requests) {
        auto totalTime = executionTime;
        requestCounters.AddIncompleteStats(
            requestType,
            executionTime,
            totalTime,
            ECalcMaxTime::ENABLE);
    }
}

////////////////////////////////////////////////////////////////////////////////

const ui32 WriteRequestType = 0;
const ui32 ReadRequestType = 1;

const TString RequestNames[] {
    "WriteBlocks",
    "ReadBlocks",
};

auto RequestType2Name(TRequestCounters::TRequestType t) {
    UNIT_ASSERT(t == 0 || t == 1);
    return RequestNames[t];
}

auto IsReadWriteRequest(TRequestCounters::TRequestType t)
{
    UNIT_ASSERT(t == 0 || t == 1);
    return true;
}

auto IsStartEndpointRequest(TRequestCounters::TRequestType t)
{
    Y_UNUSED(t);
    return false;
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestCountersOptions
{
    TRequestCounters::EOption Options = {};
    EHistogramCounterOptions HistogramCounterOptions =
        EHistogramCounterOption::ReportMultipleCounters;
    TVector<TSizeInterval> ExecutionTimeSizeClasses;
    TIoDepthClock IoDepthClock;
};

auto MakeRequestCounters(TRequestCountersOptions options = {})
{
    return TRequestCounters(
        CreateWallClockTimer(),
        2,
        RequestType2Name,
        IsReadWriteRequest,
        IsStartEndpointRequest,
        options.Options,
        options.HistogramCounterOptions,
        options.ExecutionTimeSizeClasses,
        std::move(options.IoDepthClock));
}

auto MakeRequestCountersPtr(TRequestCountersOptions options = {})
{
    return std::make_shared<TRequestCounters>(
        CreateWallClockTimer(),
        2,
        RequestType2Name,
        IsReadWriteRequest,
        IsStartEndpointRequest,
        options.Options,
        options.HistogramCounterOptions,
        options.ExecutionTimeSizeClasses,
        std::move(options.IoDepthClock));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestCountersTest)
{
    Y_TEST_HOOK_BEFORE_RUN(InitTest)
    {
        // NHPTimer warmup, see issue #2830 for more information
        Y_UNUSED(GetCyclesPerMillisecond());
    }

    Y_UNIT_TEST(ShouldAverageIoDepthOverPublicationInterval)
    {
        ui64 nowNs = 0;
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoDepth,
             .IoDepthClock = [&]
             {
                 return nowNs;
             }});
        counters.Register(*monitoring->GetCounters());

        auto read =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");
        auto average = read->GetCounter("IoDepthAverageMilli");
        auto valid = read->GetCounter("IoDepthAverageValid");

        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 0);

        const auto started = counters.RequestStarted(ReadRequestType, 4096);

        nowNs = 5'000'000'000ULL;
        counters.UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 0);

        nowNs = 10'000'000'000ULL;
        counters.RequestCompleted(
            ReadRequestType,
            started,
            TDuration::Zero(),
            TDuration::Zero(),
            TDuration::Zero(),
            4096,
            EDiagnosticsErrorKind::Success,
            NCloud::NProto::EF_NONE,
            false,
            ECalcMaxTime::ENABLE,
            0);

        nowNs = 12'000'000'000ULL;
        counters.UpdateStats(false);

        nowNs = 20'000'000'000ULL;
        counters.UpdateStats(true);

        // One request was active for 10 seconds out of 20.
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 500);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            read->GetCounter("IoDepthTimeUs", true)->Val(),
            10'000'000);

        auto write =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");
        UNIT_ASSERT_VALUES_EQUAL(
            write->GetCounter("IoDepthAverageMilli")->Val(),
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            write->GetCounter("IoDepthAverageValid")->Val(),
            1);

        nowNs = 21'000'000'000ULL;
        counters.UpdateStats(false);

        // Intermediate updates preserve the published result.
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 500);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 1);

        nowNs = 30'000'000'000ULL;
        counters.UpdateStats(true);

        // No requests were active during the next interval.
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 1);
    }

    Y_UNIT_TEST(ShouldInvalidateIoDepthAverageOnClockRollback)
    {
        ui64 nowNs = 0;
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoDepth,
             .IoDepthClock = [&]
             {
                 return nowNs;
             }});
        counters.Register(*monitoring->GetCounters());

        auto read =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");
        auto average = read->GetCounter("IoDepthAverageMilli");
        auto valid = read->GetCounter("IoDepthAverageValid");

        counters.RequestStarted(ReadRequestType, 4096);

        nowNs = 15'000'000'000ULL;
        counters.UpdateStats(true);

        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 1000);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 1);

        nowNs = 10'000'000'000ULL;
        counters.UpdateStats(false);

        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 0);

        nowNs = 30'000'000'000ULL;
        counters.UpdateStats(true);

        // Continuity remains invalid for this source generation.
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 0);
    }

    Y_UNIT_TEST(ShouldKeepIoDepthAverageWindowOnEqualTimestamps)
    {
        ui64 nowNs = 0;
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoDepth,
             .IoDepthClock = [&]
             {
                 return nowNs;
             }});
        counters.Register(*monitoring->GetCounters());

        auto read =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");
        auto average = read->GetCounter("IoDepthAverageMilli");
        auto valid = read->GetCounter("IoDepthAverageValid");
        counters.RequestStarted(ReadRequestType, 4096);

        counters.UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 0);

        nowNs = 1'000'000'000;
        counters.UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 1000);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 1);

        counters.UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 0);

        nowNs = 3'000'000'000;
        counters.UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 1000);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 1);
    }

    Y_UNIT_TEST(ShouldRestartIoDepthAverageWindowOnRegistration)
    {
        ui64 nowNs = 0;
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoDepth,
             .IoDepthClock = [&]
             {
                 return nowNs;
             }});
        counters.Register(*monitoring->GetCounters());

        auto read =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");
        auto average = read->GetCounter("IoDepthAverageMilli");
        auto valid = read->GetCounter("IoDepthAverageValid");
        const auto started = counters.RequestStarted(ReadRequestType, 4096);

        nowNs = 5'000'000'000;
        counters.UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 1000);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 1);
        const auto before = counters.GetIoDepthSnapshot();

        nowNs = 10'000'000'000ULL;
        counters.Register(*monitoring->GetCounters());
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 0);
        const auto after = counters.GetIoDepthSnapshot();
        UNIT_ASSERT(before->Generation == after->Generation);
        UNIT_ASSERT(after->Continuous);
        UNIT_ASSERT_VALUES_EQUAL(after->Lanes[ReadRequestType].Current, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            after->Lanes[ReadRequestType].IntegralUs,
            10'000'000);

        nowNs = 15'000'000'000ULL;
        counters.RequestCompleted(
            ReadRequestType,
            started,
            TDuration::Zero(),
            TDuration::Zero(),
            TDuration::Zero(),
            4096,
            EDiagnosticsErrorKind::Success,
            NCloud::NProto::EF_NONE,
            false,
            ECalcMaxTime::ENABLE,
            0);

        nowNs = 20'000'000'000ULL;
        counters.UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(average->Val(), 500);
        UNIT_ASSERT_VALUES_EQUAL(valid->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            read->GetCounter("IoDepthTimeUs", true)->Val(),
            15'000'000);
    }

    Y_UNIT_TEST(ShouldAccumulateIoDepthWithoutCompletedRequests)
    {
        ui64 nowNs = 0;
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoDepth,
             .IoDepthClock = [&]
             {
                 return nowNs;
             }});
        counters.Register(*monitoring->GetCounters());

        auto read =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");
        auto write =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");
        const auto started = counters.RequestStarted(ReadRequestType, 4096);

        nowNs = 5'000'000'000;
        counters.UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(read->GetCounter("IoDepthCurrent")->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            read->GetCounter("IoDepthTimeUs", true)->Val(),
            5'000'000);
        UNIT_ASSERT_VALUES_EQUAL(read->GetCounter("Count", true)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(write->GetCounter("IoDepthCurrent")->Val(), 0);

        nowNs = 10'000'000'000ULL;
        counters.UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(
            read->GetCounter("IoDepthAverageMilli")->Val(),
            1000);
        UNIT_ASSERT_VALUES_EQUAL(
            read->GetCounter("IoDepthAverageValid")->Val(),
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            read->GetCounter("IoDepthTimeUs", true)->Val(),
            10'000'000);

        counters.RequestCompleted(
            ReadRequestType,
            started,
            TDuration::Zero(),
            TDuration::Zero(),
            TDuration::Zero(),
            4096,
            EDiagnosticsErrorKind::ErrorFatal,
            NCloud::NProto::EF_NONE,
            false,
            ECalcMaxTime::ENABLE,
            0);
        nowNs = 12'000'000'000ULL;
        counters.UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(read->GetCounter("IoDepthCurrent")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            read->GetCounter("IoDepthTimeUs", true)->Val(),
            10'000'000);
        UNIT_ASSERT_VALUES_EQUAL(read->GetCounter("Errors", true)->Val(), 1);
        UNIT_ASSERT(counters.GetIoDepthSnapshot()->Continuous);
    }

    Y_UNIT_TEST(ShouldNotCreateIoDepthCountersWhenDisabled)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters();
        counters.Register(*monitoring->GetCounters());
        auto read =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");

        UNIT_ASSERT(!counters.GetIoDepthSnapshot());
        UNIT_ASSERT(!read->FindCounter("IoDepthCurrent"));
        UNIT_ASSERT(!read->FindCounter("IoDepthTimeUs"));
        UNIT_ASSERT(!read->FindCounter("IoDepthAverageMilli"));
        UNIT_ASSERT(!read->FindCounter("IoDepthAverageValid"));
    }

    Y_UNIT_TEST(ShouldKeepIoDepthEpochAcrossCounterRegistration)
    {
        ui64 nowNs = 0;
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoDepth,
             .IoDepthClock = [&]
             {
                 return nowNs;
             }});
        counters.Register(*monitoring->GetCounters());
        counters.RequestStarted(ReadRequestType, 4096);
        const auto first = counters.GetIoDepthSnapshot();

        nowNs = 1'000'000'000;
        auto rebound = monitoring->GetCounters()->GetSubgroup("labels", "new");
        counters.Register(*rebound);
        counters.UpdateStats(false);
        const auto second = counters.GetIoDepthSnapshot();

        UNIT_ASSERT(first->Generation == second->Generation);
        UNIT_ASSERT(second->Continuous);
        UNIT_ASSERT_VALUES_EQUAL(second->Lanes[ReadRequestType].Current, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            rebound->GetSubgroup("request", "ReadBlocks")
                ->GetCounter("IoDepthTimeUs", true)
                ->Val(),
            1'000'000);
    }

    Y_UNIT_TEST(ShouldTrackIoDepthOncePerSubscriber)
    {
        ui64 nowNs = 0;
        auto monitoring = CreateMonitoringServiceStub();
        TRequestCountersOptions options{
            .Options = TRequestCounters::EOption::ReportIoDepth,
            .IoDepthClock = [&]
            {
                return nowNs;
            }};
        auto counters = MakeRequestCountersPtr(options);
        auto subscriber = MakeRequestCountersPtr(options);
        counters->Register(*monitoring->GetCounters());
        subscriber->Register(
            *monitoring->GetCounters()->GetSubgroup("source", "subscriber"));
        counters->Subscribe(subscriber);

        const auto started = counters->RequestStarted(ReadRequestType, 4096);
        nowNs = 1'000'000'000;
        UNIT_ASSERT_VALUES_EQUAL(
            counters->GetIoDepthSnapshot()->Lanes[ReadRequestType].Current,
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            subscriber->GetIoDepthSnapshot()->Lanes[ReadRequestType].Current,
            1);
        counters->RequestCompleted(
            ReadRequestType,
            started,
            TDuration::Zero(),
            TDuration::Zero(),
            TDuration::Zero(),
            4096,
            EDiagnosticsErrorKind::Success,
            NCloud::NProto::EF_NONE,
            false,
            ECalcMaxTime::ENABLE,
            0);
        for (const auto& source: {counters, subscriber}) {
            const auto snapshot = source->GetIoDepthSnapshot();
            UNIT_ASSERT(snapshot->Continuous);
            UNIT_ASSERT_VALUES_EQUAL(
                snapshot->Lanes[ReadRequestType].Current,
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                snapshot->Lanes[ReadRequestType].IntegralUs,
                1'000'000);
        }
    }

    Y_UNIT_TEST(ShouldKeepCommonIoDepthIndependentOfExternalCompletionBatches)
    {
        ui64 nowNs = 0;
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoDepth,
             .IoDepthClock = [&]
             {
                 return nowNs;
             }});
        counters.Register(*monitoring->GetCounters());
        const auto firstStarted =
            counters.RequestStarted(ReadRequestType, 4096);
        const auto first = counters.GetIoDepthSnapshot();

        nowNs = 1'000'000'000;
        counters.BatchCompleted(ReadRequestType, 0, 0, 0, {}, {});
        counters.BatchCompleted(ReadRequestType, 1, 4096, 0, {}, {});
        const auto afterExternal = counters.GetIoDepthSnapshot();
        UNIT_ASSERT(afterExternal->Continuous);
        UNIT_ASSERT(first->Generation == afterExternal->Generation);
        UNIT_ASSERT_VALUES_EQUAL(
            afterExternal->Lanes[ReadRequestType].Current,
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            afterExternal->Lanes[ReadRequestType].IntegralUs,
            1'000'000);

        auto finish = [&](ui64 started)
        {
            counters.RequestCompleted(
                ReadRequestType,
                started,
                {},
                {},
                {},
                4096,
                EDiagnosticsErrorKind::Success,
                NCloud::NProto::EF_NONE,
                false,
                ECalcMaxTime::ENABLE,
                0);
        };
        nowNs = 2'000'000'000;
        finish(firstStarted);
        nowNs = 3'000'000'000;
        const auto fallbackStarted =
            counters.RequestStarted(ReadRequestType, 4096);
        nowNs = 4'000'000'000;
        const auto fallback = counters.GetIoDepthSnapshot();
        UNIT_ASSERT(fallback->Continuous);
        UNIT_ASSERT(first->Generation == fallback->Generation);
        UNIT_ASSERT_VALUES_EQUAL(fallback->Lanes[ReadRequestType].Current, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            fallback->Lanes[ReadRequestType].IntegralUs,
            3'000'000);
        nowNs = 5'000'000'000;
        finish(fallbackStarted);
        const auto final = counters.GetIoDepthSnapshot();
        UNIT_ASSERT(final->Continuous);
        UNIT_ASSERT_VALUES_EQUAL(final->Lanes[ReadRequestType].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            final->Lanes[ReadRequestType].IntegralUs,
            4'000'000);
    }

    Y_UNIT_TEST(ShouldTrackRequestsInProgress)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto inProgress = counters->GetCounter("InProgress");
        auto inProgressBytes = counters->GetCounter("InProgressBytes");

        UNIT_ASSERT_EQUAL(inProgress->Val(), 0);
        UNIT_ASSERT_EQUAL(inProgressBytes->Val(), 0);

        auto started = requestCounters.RequestStarted(
            WriteRequestType,
            1_MB);

        UNIT_ASSERT_EQUAL(inProgress->Val(), 1);
        UNIT_ASSERT_EQUAL(inProgressBytes->Val(), 1_MB);

        requestCounters.RequestCompleted(
            WriteRequestType,
            started,
            TDuration::Zero(),   // postponedTime
            TDuration::Zero(),   // backoffTime
            TDuration::Zero(),   // shapingTime
            1_MB,
            EDiagnosticsErrorKind::Success,
            NCloud::NProto::EF_NONE,
            false,
            ECalcMaxTime::ENABLE,
            0);

        UNIT_ASSERT_EQUAL(inProgress->Val(), 0);
        UNIT_ASSERT_EQUAL(inProgressBytes->Val(), 0);
    }

    Y_UNIT_TEST(ShouldTrackIncompleteRequests)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counter = monitoring->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("MaxTime");
        UNIT_ASSERT_EQUAL(counter->Val(), 0);

        AddIncompleteStats(requestCounters, WriteRequestType, {
            TDuration::MilliSeconds(100),
            TDuration::MilliSeconds(150),
            TDuration::MilliSeconds(50),
            TDuration::MilliSeconds(200),
        });

        requestCounters.UpdateStats();
        UNIT_ASSERT_EQUAL(counter->Val(), 200'000);

        AddIncompleteStats(requestCounters, WriteRequestType, {
            TDuration::MilliSeconds(30),
            TDuration::MilliSeconds(170),
            TDuration::MilliSeconds(150),
            TDuration::MilliSeconds(90),
        });

        requestCounters.UpdateStats();
        UNIT_ASSERT_EQUAL(counter->Val(), 200'000);

        AddIncompleteStats(requestCounters, WriteRequestType, {
            TDuration::MilliSeconds(130),
            TDuration::MilliSeconds(70),
            TDuration::MilliSeconds(250),
            TDuration::MilliSeconds(190),
        });

        requestCounters.UpdateStats();
        UNIT_ASSERT_EQUAL(counter->Val(), 250'000);
    }

    Y_UNIT_TEST(ShouldTrackPostponedRequests)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto postponedQueueSize = counters->GetCounter("PostponedQueueSize");
        auto maxPostponedQueueSize = counters->GetCounter("MaxPostponedQueueSize");

        UNIT_ASSERT_EQUAL(postponedQueueSize->Val(), 0);
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 0);

        requestCounters.RequestPostponed(WriteRequestType);
        UNIT_ASSERT_EQUAL(postponedQueueSize->Val(), 1);
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 0);

        requestCounters.RequestPostponed(WriteRequestType);
        UNIT_ASSERT_EQUAL(postponedQueueSize->Val(), 2);
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 0);

        requestCounters.RequestAdvanced(WriteRequestType);
        UNIT_ASSERT_EQUAL(postponedQueueSize->Val(), 1);
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 0);

        requestCounters.UpdateStats();
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 2);
    }

    Y_UNIT_TEST(ShouldTrackFastPathHits)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto fastPathHits = counters->GetCounter("FastPathHits");

        UNIT_ASSERT_EQUAL(fastPathHits->Val(), 0);

        requestCounters.RequestFastPathHit(WriteRequestType);
        requestCounters.RequestFastPathHit(WriteRequestType);
        requestCounters.RequestFastPathHit(WriteRequestType);

        UNIT_ASSERT_EQUAL(fastPathHits->Val(), 3);
    }

    Y_UNIT_TEST(ShouldFillTimePercentiles)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto readBlocks = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "ReadBlocks");

        AddRequestStats(
            requestCounters,
            ReadRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(401),
                 .PostponedTime = TDuration::MilliSeconds(50),
                 .BackoffTime = TDuration::MilliSeconds(200),
                 .ShapingTime = TDuration::MilliSeconds(100)},
            });

        requestCounters.UpdateStats(true);

        {
            auto percentiles = writeBlocks->GetSubgroup("percentiles", "Time")
                                   ->GetSubgroup("units", "usec");

            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }

        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "ExecutionTime")
                    ->GetSubgroup("units", "usec");

            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }

        {
            auto percentiles = readBlocks->GetSubgroup("percentiles", "Time")
                                   ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(500000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(350000, p50->Val());
        }

        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "ExecutionTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(100000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(75000, p50->Val());
        }
    }


    Y_UNIT_TEST(ShouldFillTimePercentilesWithRequestCompletionTime)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto readBlocks = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "ReadBlocks");

        AddRequestStats(
            requestCounters,
            ReadRequestType,
            {{.RequestBytes = 1_MB,
              .RequestTime = TDuration::MilliSeconds(106),
              .PostponedTime = TDuration::MilliSeconds(50),
              .Aligned = false,
              .RequestCompletionTime =
                  DurationToCyclesSafe(TDuration::MilliSeconds(45))}});

        requestCounters.UpdateStats(true);

        {
            auto percentiles = writeBlocks->GetSubgroup("percentiles", "Time")
                                   ->GetSubgroup("units", "usec");

            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }

        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "ExecutionTime")
                    ->GetSubgroup("units", "usec");

            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }

        {
            auto percentiles = readBlocks->GetSubgroup("percentiles", "Time")
                                   ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(200000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(150000, p50->Val());
        }

        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "ExecutionTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(100000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(75000, p50->Val());
        }

        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "RequestCompletionTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(50000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(35000, p50->Val());
        }
    }

    Y_UNIT_TEST(ShouldFillBackoffTimeHistorgram)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        auto readBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");

        AddRequestStats(
            requestCounters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(200),
                 .PostponedTime = TDuration::MilliSeconds(100),
                 .BackoffTime = TDuration::MilliSeconds(50)},
            });

        requestCounters.UpdateStats(true);

        // Check the percentiles for BackoffTime
        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "BackoffTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(50000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(35000, p50->Val());
        }

        // Check the percentiles for ThrottlerDelay
        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "ThrottlerDelay")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(100000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(75000, p50->Val());
        }

        // Check the histogram for BackoffTime
        {
            auto histGroup =
                writeBlocks->GetSubgroup("histogram", "BackoffTime")
                    ->GetSubgroup("units", "usec");

            TMap<TString, uint64_t> expectedValues;
            for (const auto& name: TRequestUsTimeBuckets::MakeNames()) {
                expectedValues[name] = 0;
            }
            expectedValues["50000"] = 1;

            for (const auto& [name, value]: expectedValues) {
                auto counter = histGroup->FindCounter(name);
                UNIT_ASSERT_C(
                    counter,
                    "Counter " + name.Quote() + " not found");
                UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
            }
        }

        // Check the histogram for ThrottlerDelay
        {
            auto histGroup =
                writeBlocks->GetSubgroup("histogram", "ThrottlerDelay")
                    ->GetSubgroup("units", "usec");

            TMap<TString, uint64_t> expectedValues;
            for (const auto& name: TRequestUsTimeBuckets::MakeNames()) {
                expectedValues[name] = 0;
            }
            expectedValues["100000"] = 1;

            for (const auto& [name, value]: expectedValues) {
                auto counter = histGroup->FindCounter(name);
                UNIT_ASSERT_C(
                    counter,
                    "Counter " + name.Quote() + " not found");
                UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
            }
        }

        // Percentiles for BackoffTime for read blocks should be empty
        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "BackoffTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }
    }

    Y_UNIT_TEST(ShouldFillShapingTimeHistogramAndPercentiles)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        auto readBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");

        AddRequestStats(
            requestCounters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(200),
                 .PostponedTime = TDuration::MilliSeconds(50),
                 .BackoffTime = TDuration::MilliSeconds(30),
                 .ShapingTime = TDuration::MilliSeconds(100)},
            });

        requestCounters.UpdateStats(true);

        // Check the percentiles for ShapingTime
        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "ShapingTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(100000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(75000, p50->Val());
        }

        // Check the histogram for ShapingTime
        {
            auto histGroup =
                writeBlocks->GetSubgroup("histogram", "ShapingTime")
                    ->GetSubgroup("units", "usec");

            TMap<TString, uint64_t> expectedValues;
            for (const auto& name: TRequestUsTimeBuckets::MakeNames()) {
                expectedValues[name] = 0;
            }
            expectedValues["100000"] = 1;

            for (const auto& [name, value]: expectedValues) {
                auto counter = histGroup->FindCounter(name);
                UNIT_ASSERT_C(
                    counter,
                    "Counter " + name.Quote() + " not found");
                UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
            }
        }

        // Percentiles for ShapingTime for read blocks should be empty
        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "ShapingTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }
    }

    Y_UNIT_TEST(ShouldFillSizePercentiles)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        AddRequestStats(
            requestCounters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100),
                 .BackoffTime = TDuration::Zero(),
                 .ShapingTime = TDuration::Zero()},
                {.RequestBytes = 2_MB,
                 .RequestTime = TDuration::MilliSeconds(100),
                 .BackoffTime = TDuration::Zero(),
                 .ShapingTime = TDuration::Zero()},
                {.RequestBytes = 3_MB,
                 .RequestTime = TDuration::MilliSeconds(100),
                 .BackoffTime = TDuration::Zero(),
                 .ShapingTime = TDuration::Zero()},
            });

        requestCounters.UpdateStats(true);

        auto percentiles = counters->GetSubgroup("percentiles", "Size")
                               ->GetSubgroup("units", "KB");
        auto p100 = percentiles->GetCounter("100");
        auto p50 = percentiles->GetCounter("50");

        UNIT_ASSERT_VALUES_EQUAL(4*1024, p100->Val());
        UNIT_ASSERT_VALUES_EQUAL(1.5*1024, p50->Val());
    }

    Y_UNIT_TEST(ShouldCountSilentErrors)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto shoot = [&] (auto errorKind) {
            auto requestStarted = requestCounters.RequestStarted(
                WriteRequestType,
                1_MB);

            requestCounters.RequestCompleted(
                WriteRequestType,
                requestStarted -
                    DurationToCyclesSafe(TDuration::MilliSeconds(201)),
                TDuration::MilliSeconds(100),   // postponedTime
                TDuration::Zero(),              // backoffTime
                TDuration::Zero(),              // shapingTime
                1_MB,
                errorKind,
                NCloud::NProto::EF_NONE,
                false,
                ECalcMaxTime::ENABLE,
                0);
        };

        shoot(EDiagnosticsErrorKind::ErrorAborted);
        shoot(EDiagnosticsErrorKind::ErrorFatal);
        shoot(EDiagnosticsErrorKind::ErrorRetriable);
        shoot(EDiagnosticsErrorKind::ErrorThrottling);
        shoot(EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint);
        shoot(EDiagnosticsErrorKind::ErrorSession);
        shoot(EDiagnosticsErrorKind::ErrorSilent);

        requestCounters.UpdateStats(true);

        auto errors = counters->GetCounter("Errors");
        UNIT_ASSERT_VALUES_EQUAL(6, errors->Val());

        auto abort = counters->GetCounter("Errors/Aborted");
        UNIT_ASSERT_VALUES_EQUAL(1, abort->Val());

        auto fatal = counters->GetCounter("Errors/Fatal");
        UNIT_ASSERT_VALUES_EQUAL(1, fatal->Val());

        auto retriable = counters->GetCounter("Errors/Retriable");
        UNIT_ASSERT_VALUES_EQUAL(1, retriable->Val());

        auto throttling = counters->GetCounter("Errors/Throttling");
        UNIT_ASSERT_VALUES_EQUAL(1, throttling->Val());

        auto rejectedByCheckpoint = counters->GetCounter("Errors/CheckpointReject");
        UNIT_ASSERT_VALUES_EQUAL(1, rejectedByCheckpoint->Val());

        auto session = counters->GetCounter("Errors/Session");
        UNIT_ASSERT_VALUES_EQUAL(1, session->Val());

        auto silent = counters->GetCounter("Errors/Silent");
        UNIT_ASSERT_VALUES_EQUAL(1, silent->Val());
    }

    Y_UNIT_TEST(ShouldCountHwProblems)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::AddSpecialCounters,
             .ExecutionTimeSizeClasses = {}});
        requestCounters.Register(*monitoring->GetCounters());

        const auto requestType = ReadRequestType;

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", RequestType2Name(requestType));

        auto shoot = [&] (auto errorKind, ui32 errorFlags) {
            auto requestStarted = requestCounters.RequestStarted(
                requestType,
                1_MB);

            requestCounters.RequestCompleted(
                requestType,
                requestStarted -
                    DurationToCyclesSafe(TDuration::MilliSeconds(201)),
                TDuration::MilliSeconds(100),   // postponedTime
                TDuration::Zero(),              // backoffTime
                TDuration::Zero(),              // shapingTime
                1_MB,
                errorKind,
                errorFlags,
                false,
                ECalcMaxTime::ENABLE,
                0);
        };

        shoot(EDiagnosticsErrorKind::ErrorFatal,
            NCloud::NProto::EF_NONE);
        shoot(EDiagnosticsErrorKind::ErrorRetriable,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);
        shoot(EDiagnosticsErrorKind::ErrorThrottling,
            NCloud::NProto::EF_NONE);
        shoot(EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint,
            NCloud::NProto::EF_NONE);
        shoot(EDiagnosticsErrorKind::ErrorSession,
            NCloud::NProto::EF_NONE);
        shoot(EDiagnosticsErrorKind::ErrorSilent,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);

        requestCounters.UpdateStats(true);

        auto errors = counters->GetCounter("Errors");
        UNIT_ASSERT_VALUES_EQUAL(5, errors->Val());

        auto fatal = counters->GetCounter("Errors/Fatal");
        UNIT_ASSERT_VALUES_EQUAL(1, fatal->Val());

        auto retriable = counters->GetCounter("Errors/Retriable");
        UNIT_ASSERT_VALUES_EQUAL(1, retriable->Val());

        auto throttling = counters->GetCounter("Errors/Throttling");
        UNIT_ASSERT_VALUES_EQUAL(1, throttling->Val());

        auto checkpointReject = counters->GetCounter("Errors/CheckpointReject");
        UNIT_ASSERT_VALUES_EQUAL(1, checkpointReject->Val());

        auto session = counters->GetCounter("Errors/Session");
        UNIT_ASSERT_VALUES_EQUAL(1, session->Val());

        auto silent = counters->GetCounter("Errors/Silent");
        UNIT_ASSERT_VALUES_EQUAL(1, silent->Val());

        auto hwProblems =
            monitoring->GetCounters()->GetCounter("HwProblems");
        UNIT_ASSERT_VALUES_EQUAL(2, hwProblems->Val());
    }

    Y_UNIT_TEST(ShouldNotUpdateSubscribers)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr();
        counters->Register(*monitoring->GetCounters());

        auto subscriber = MakeRequestCountersPtr();
        subscriber->Register(*monitoring->GetCounters()->GetSubgroup("subscribers", "s"));
        counters->Subscribe(subscriber);

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 2_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 3_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            });

        counters->UpdateStats();

        {
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("MaxTime");

            UNIT_ASSERT(maxTime->Val() > 0);
        }

        {
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("subscribers", "s")
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("maxTime");

            UNIT_ASSERT_EQUAL(0, maxTime->Val());
        }
    }

    Y_UNIT_TEST(ShouldNotifySubscribers)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr();
        counters->Register(*monitoring->GetCounters());

        auto outerSubscriber = MakeRequestCountersPtr();
        outerSubscriber->Register(*monitoring->GetCounters()->GetSubgroup("subscribers", "outer"));
        counters->Subscribe(outerSubscriber);

        auto innerSubscriber = MakeRequestCountersPtr();
        innerSubscriber->Register(*monitoring->GetCounters()->GetSubgroup("subscribers", "inner"));
        outerSubscriber->Subscribe(innerSubscriber);

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 2_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 3_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            });

        {
            counters->UpdateStats();
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("MaxTime");

            UNIT_ASSERT(maxTime->Val() > 0);
        }

        {
            outerSubscriber->UpdateStats();
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("subscribers", "outer")
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("MaxTime");

            UNIT_ASSERT(maxTime->Val() > 0);
        }

        {
            innerSubscriber->UpdateStats();
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("subscribers", "inner")
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("MaxTime");

            UNIT_ASSERT(maxTime->Val() > 0);
        }
    }

    Y_UNIT_TEST(ShouldTrackSizeClasses)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .ExecutionTimeSizeClasses = {}});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Minutes(1)},
                {.RequestBytes = 1_MB, .RequestTime = TDuration::Minutes(1)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::Minutes(1),
                 .Aligned = true},
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::Minutes(1),
                 .Aligned = true},
            });

        counters->UpdateStats();
        {
            auto time = monitoring
                ->GetCounters()
                ->GetSubgroup("request", "WriteBlocks")
                ->GetSubgroup("sizeclass", "Unaligned")
                ->GetSubgroup("histogram", "Time")
                ->GetSubgroup("units", "usec")
                ->GetCounter("Inf");

            UNIT_ASSERT_VALUES_EQUAL(time->Val(), 2);
        }
    }

    void ShouldReportCompoundTimeHistogramWithMultipleCounters(
        EHistogramCounterOptions options)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .HistogramCounterOptions =
                 options | EHistogramCounterOption::ReportMultipleCounters});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(8)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(20)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(30)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(37)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(50)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(100)},
            });

        counters->UpdateStats();
        const auto timeHist = monitoring->GetCounters()
                                  ->GetSubgroup("request", "WriteBlocks")
                                  ->GetSubgroup("histogram", "Time");

        const auto usGroup = timeHist->GetSubgroup("units", "usec");
        const auto msGroup = timeHist;

        const auto validateCounters =
            [](TIntrusivePtr<NMonitoring::TDynamicCounters> group,
               TVector<TString> bucketNames,
               TStringBuf suffix,
               bool shouldExist)
        {
            TMap<TString, uint64_t> expectedHistogramValues;
            for (const auto& bucketName: bucketNames) {
                expectedHistogramValues[bucketName] = 0;
            }
            expectedHistogramValues[TString("10000") + suffix] = 1;
            expectedHistogramValues[TString("35000") + suffix] = 2;
            expectedHistogramValues["Inf"] = 3;

            for (const auto& [name, value]: expectedHistogramValues) {
                const auto counter = group->FindCounter(name);
                if (shouldExist) {
                    UNIT_ASSERT_C(
                        counter,
                        "Counter " + name.Quote() + " not found");
                    UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
                } else {
                    UNIT_ASSERT_C(
                        !counter,
                        "Counter " + name.Quote() + " should not exist");
                }
            }
        };

        validateCounters(
            usGroup,
            TRequestUsTimeBuckets::MakeNames(),
            "000",
            !(options & EHistogramCounterOption::UseMsUnitsForTimeHistogram));
        validateCounters(
            msGroup,
            TRequestMsTimeBuckets::MakeNames(),
            "ms",
            options & EHistogramCounterOption::UseMsUnitsForTimeHistogram);
    }

    Y_UNIT_TEST(ShouldReportCompoundTimeHistogram_UseMsUnitsForTimeHistogram)
    {
        ShouldReportCompoundTimeHistogramWithMultipleCounters(
            EHistogramCounterOption::UseMsUnitsForTimeHistogram);
    }

    Y_UNIT_TEST(ShouldReportCompoundTimeHistogram_UseUsUnitsForTimeHistogram)
    {
        ShouldReportCompoundTimeHistogramWithMultipleCounters(
            EHistogramCounterOptions());
    }

    void ShouldReportCompoundTimeHistogramWithSingleCounter(
        EHistogramCounterOptions options)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .HistogramCounterOptions =
                 options | EHistogramCounterOption::ReportSingleCounter});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(8)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(20)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(30)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(37)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(50)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(100)},
            });

        const TMap<size_t, uint64_t> expectedHistogramValues = {
            {22, 1},   // 10000ms
            {23, 2},   // 35000ms
            {24, 3},   // Inf
        };

        counters->UpdateStats();

        const auto timeHist = monitoring->GetCounters()
                                  ->GetSubgroup("request", "WriteBlocks")
                                  ->GetSubgroup("histogram", "Time");

        const auto usGroup = timeHist->GetSubgroup("units", "usec");
        const auto msGroup = timeHist;

        const auto validateCounters =
            [expectedHistogramValues](
                TIntrusivePtr<NMonitoring::TDynamicCounters> group,
                bool shouldExist)
        {
            const auto histogram = group->FindHistogram("Time");
            if (!shouldExist) {
                UNIT_ASSERT(!histogram);
                return;
            }
            UNIT_ASSERT(histogram);
            const auto snapshot = histogram->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(
                snapshot->Count(),
                TRequestMsTimeBuckets::Buckets.size());
            for (size_t bucketId = 0; bucketId < snapshot->Count(); bucketId++)
            {
                auto expectedValue = expectedHistogramValues.contains(bucketId)
                                         ? expectedHistogramValues.at(bucketId)
                                         : 0;
                UNIT_ASSERT_VALUES_EQUAL(
                    snapshot->Value(bucketId),
                    expectedValue);
            }
        };

        validateCounters(
            usGroup,
            !(options & EHistogramCounterOption::UseMsUnitsForTimeHistogram));
        validateCounters(
            msGroup,
            options & EHistogramCounterOption::UseMsUnitsForTimeHistogram);
    }

    Y_UNIT_TEST(
        ShouldReportCompoundTimeHistogramWithSingleCounter_UseMsUnitsForTimeHistogram)
    {
        ShouldReportCompoundTimeHistogramWithSingleCounter(
            EHistogramCounterOption::UseMsUnitsForTimeHistogram);
    }

    Y_UNIT_TEST(
        ShouldReportCompoundTimeHistogramWithSingleCounter_UseUsUnitsForTimeHistogram)
    {
        ShouldReportCompoundTimeHistogramWithSingleCounter(
            EHistogramCounterOptions());
    }

    Y_UNIT_TEST(ShouldReportHistogramAsMultipleSensors)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .HistogramCounterOptions =
                 EHistogramCounterOption::ReportMultipleCounters,
             .ExecutionTimeSizeClasses = {}});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(8)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(20)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(30)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(37)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(50)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(100)},
            });

        TMap<TString, uint64_t> expectedHistogramValues;
        for (const auto& bucketName : TRequestUsTimeBuckets::MakeNames()) {
            expectedHistogramValues[bucketName] = 0;
        }
        expectedHistogramValues["10000000"] = 1;
        expectedHistogramValues["35000000"] = 2;
        expectedHistogramValues["Inf"] = 3;

        counters->UpdateStats();
        const auto group = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetSubgroup("histogram", "Time")
            ->GetSubgroup("units", "usec");

        for (const auto& [name, value]: expectedHistogramValues) {
            const auto counter = group->FindCounter(name);
            UNIT_ASSERT_C(counter, "Counter " + name.Quote() + " not found");
            UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
        }
    }

    Y_UNIT_TEST(ShouldReportHistogramAsSingleSensor)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .HistogramCounterOptions =
                 EHistogramCounterOption::ReportSingleCounter,
             .ExecutionTimeSizeClasses = {}});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(8)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(20)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(30)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(37)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(50)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(100)},
            });

        const TMap<size_t, uint64_t> expectedHistogramValues = {
            { 22, 1 }, // 10000ms
            { 23, 2 }, // 35000ms
            { 24, 3 }, // Inf
        };

        counters->UpdateStats();

        const auto histogram = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetSubgroup("histogram", "Time")
            ->GetSubgroup("units", "usec")
            ->FindHistogram("Time");
        UNIT_ASSERT(histogram);

        const auto snapshot = histogram->Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot->Count(), TRequestMsTimeBuckets::Buckets.size());
        for (size_t bucketId = 0; bucketId < snapshot->Count(); bucketId++) {
            auto expectedValue = expectedHistogramValues.contains(bucketId) ?
                expectedHistogramValues.at(bucketId) : 0;
            UNIT_ASSERT_VALUES_EQUAL(snapshot->Value(bucketId), expectedValue);
        }
    }

    Y_UNIT_TEST(ShouldNotReportHistogramIfOptionIsNotSet)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .ExecutionTimeSizeClasses = {}});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(800)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(1500)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(2000)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(8000)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(36000)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(100000)},
            });

        auto counter = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetSubgroup("histogram", "Time")
            ->GetSubgroup("units", "usec")
            ->FindCounter("1ms");

        UNIT_ASSERT(!counter);

        auto histogram = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetSubgroup("histogram", "Time")
            ->GetSubgroup("units", "usec")
            ->FindHistogram("Time");

        UNIT_ASSERT(!histogram);
    }

    Y_UNIT_TEST(ShouldReportStatsForLargeRequests)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr();
        counters->Register(*monitoring->GetCounters());
        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 8_GB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            });

        counters->UpdateStats();
        auto requestBytes = monitoring->GetCounters()
                                ->GetSubgroup("request", "WriteBlocks")
                                ->GetCounter("RequestBytes");

        UNIT_ASSERT_EQUAL_C(8_GB, requestBytes->Val(), requestBytes->Val());
    }

    Y_UNIT_TEST(ShouldNotRegisterOrAccountThrottlingMetrics)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters(
            {.Options =
                 TRequestCounters::EOption::ThrottlingHistogramsDisabled});
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        AddRequestStats(
            requestCounters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(200),
                 .PostponedTime = TDuration::MilliSeconds(50),
                 .BackoffTime = TDuration::MilliSeconds(30),
                 .ShapingTime = TDuration::MilliSeconds(40)},
            });

        requestCounters.UpdateStats(true);

        // Time percentile must still be reported
        {
            auto p100 = writeBlocks->GetSubgroup("percentiles", "Time")
                            ->GetSubgroup("units", "usec")
                            ->GetCounter("100");
            UNIT_ASSERT_VALUES_UNEQUAL(0, p100->Val());
        }

        for (const TString histogram:
             {"ExecutionTime", "ThrottlerDelay", "BackoffTime", "ShapingTime"})
        {
            auto percentilesGroup =
                writeBlocks->FindSubgroup("percentiles", histogram);
            UNIT_ASSERT_C(
                !percentilesGroup,
                "Percentiles for " << histogram << " should not be registered");
        }
    }

    Y_UNIT_TEST(ShouldNotRegisterDisaggregatedCounters)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto fsCounters = monitoring->GetCounters()->GetSubgroup("type", "fs");
        auto totalCounters = monitoring->GetCounters()->GetSubgroup("type", "total");

        auto aggregated = MakeRequestCountersPtr({});
        aggregated->Register(*totalCounters);

        auto perFs = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::DisaggregatedCountersDisabled});
        perFs.Register(*fsCounters);
        perFs.Subscribe(aggregated);

        AddRequestStats(
            perFs,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            });

        perFs.UpdateStats(true);
        aggregated->UpdateStats(true);

        // Per-fs monitoring group must have no request subgroups at all
        UNIT_ASSERT_C(
            !fsCounters->FindSubgroup("request", "WriteBlocks"),
            "Per-fs request subgroup should not be registered");

        // Aggregated must still receive the stats via subscription
        auto count = totalCounters
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("Count", true);
        UNIT_ASSERT_VALUES_EQUAL(1, count->Val());
    }

    Y_UNIT_TEST(ShouldFillTimePercentilesForDifferentSizeClassesSeparately)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .ExecutionTimeSizeClasses = {{4_KB, 512_KB}, {1_MB, 4_MB}}});
        requestCounters->Register(*monitoring->GetCounters());

        auto writeBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        auto addRequestStats = [&](ui64 size)
        {
            AddRequestStats(
                *requestCounters,
                WriteRequestType,
                {
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(11),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(23),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(33),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(40),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(50),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(100),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                });
        };

        // 1 size class
        addRequestStats(4_KB);
        // 2 size class
        addRequestStats(1_MB);

        // no size class
        addRequestStats(512_KB);
        addRequestStats(4_MB);

        TMap<TString, uint64_t> expectedHistogramValues;
        for (const auto& bucketName: TRequestUsTimeBuckets::MakeNames()) {
            expectedHistogramValues[bucketName] = 0;
        }
        expectedHistogramValues["10000000"] = 1;
        expectedHistogramValues["35000000"] = 2;
        expectedHistogramValues["Inf"] = 3;

        requestCounters->UpdateStats();

        auto checkSizeClass = [&](ui64 start, ui64 end)
        {
            const auto group = monitoring->GetCounters()
                                   ->GetSubgroup("request", "WriteBlocks")
                                   ->GetSubgroup(
                                       "sizeclass",
                                       ToString(TSizeInterval{start, end}))
                                   ->GetSubgroup("histogram", "ExecutionTime")
                                   ->GetSubgroup("units", "usec");

            for (const auto& [name, value]: expectedHistogramValues) {
                const auto counter = group->FindCounter(name);
                UNIT_ASSERT_C(
                    counter,
                    "Counter " + name.Quote() + " not found");
                UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
            }
        };

        checkSizeClass(4_KB, 512_KB);

        checkSizeClass(1_MB, 4_MB);
    }
}

}   // namespace NCloud
