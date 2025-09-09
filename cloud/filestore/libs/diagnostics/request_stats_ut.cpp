#include "request_stats.h"

#include "config.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using namespace NMonitoring;

using namespace NCloud::NStorage::NUserStats;

namespace
{

////////////////////////////////////////////////////////////////////////////////

const TString METRIC_COMPONENT = "test";
const TString METRIC_FS_COMPONENT = METRIC_COMPONENT + "_fs";
const TString CLOUD = "test_cloud";
const TString FOLDER = "test_folder";
const TString FS = "test_filesystem";
const TString CLIENT = "test_client";

template <typename TCountersType>
auto GetFsCounters(
    const TCountersType& counters,
    const TString& fs,
    const TString& client,
    const TString& cloud,
    const TString& folder)
{
    auto fsCounters = counters->FindSubgroup("filesystem", fs);
    UNIT_ASSERT(fsCounters);
    fsCounters = fsCounters->FindSubgroup("client", client);
    UNIT_ASSERT(fsCounters);
    fsCounters = fsCounters->FindSubgroup("cloud", cloud);
    UNIT_ASSERT(fsCounters);
    fsCounters = fsCounters->FindSubgroup("folder", folder);
    return fsCounters;
}

struct TBootstrap
{
    TDynamicCountersPtr Counters;
    ITimerPtr Timer;
    IRequestStatsRegistryPtr Registry;

    TBootstrap()
        : Counters{MakeIntrusive<TDynamicCounters>()}
        , Timer{CreateWallClockTimer()}
        , Registry{CreateRequestStatsRegistry(
            METRIC_COMPONENT,
            std::make_shared<TDiagnosticsConfig>(),
            Counters,
            Timer,
            CreateUserCounterSupplierStub())}
    {}
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestStatRegistryTest)
{
    Y_UNIT_TEST(ShouldCreateRemoveStats)
    {
        TBootstrap bootstrap;

        auto componentCounters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_COMPONENT);
        UNIT_ASSERT(componentCounters);
        UNIT_ASSERT(componentCounters->FindSubgroup("type", "ssd"));
        UNIT_ASSERT(componentCounters->FindSubgroup("type", "hdd"));

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(counters);
        counters = counters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(counters);

        auto stats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER);
        {
            auto fsCounters = counters->FindSubgroup("filesystem", FS);
            UNIT_ASSERT(fsCounters);

            auto clientCounters = fsCounters->FindSubgroup("client", CLIENT);
            UNIT_ASSERT(clientCounters);
        }

        bootstrap.Registry->Unregister(FS, CLIENT);
        {
            auto fsCounters = counters->FindSubgroup("filesystem", FS);
            UNIT_ASSERT(fsCounters);
            UNIT_ASSERT(!fsCounters->FindSubgroup("client", CLIENT));
        }
    }

    Y_UNIT_TEST(ShouldReportTotalAndMediaKindStatsViaComponent)
    {
        TBootstrap bootstrap;

        auto componentCounters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_COMPONENT);
        auto ssdCounters = componentCounters->FindSubgroup("type", "ssd");


        UNIT_ASSERT(
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER));
        bootstrap.Registry->SetFileSystemMediaKind(FS, CLIENT, NProto::STORAGE_MEDIA_SSD);

        auto stats = bootstrap.Registry->GetRequestStats();

        auto context = MakeIntrusive<TCallContext>(FS, ui64(1));
        context->RequestType = EFileStoreRequest::CreateHandle;
        stats->RequestStarted(*context);
        stats->RequestCompleted(*context, {});

        auto counters = componentCounters->FindSubgroup("request", "CreateHandle");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Count")->GetAtomic());

        counters = ssdCounters->FindSubgroup("request", "CreateHandle");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Count")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldUnregisterMaxPredictedPostponeTimeViaFs)
    {
        const TString COUNTER = "MaxPredictedPostponeTime";

        TBootstrap bootstrap;

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(counters);
        counters = counters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(counters);

        auto stats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(stats);
        auto fsCounters = GetFsCounters(counters, FS, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(fsCounters);

        {
            auto predictedCounter = fsCounters->FindCounter(COUNTER);
            UNIT_ASSERT(predictedCounter);
        }

        stats.reset();
        bootstrap.Registry->Unregister(FS, CLIENT);

        {
            auto predictedCounter = fsCounters->FindCounter(COUNTER);
            UNIT_ASSERT(!predictedCounter);
        }
    }

    Y_UNIT_TEST(ShouldReportReportMaxPredictedPostponeTimeForDataRequestsViaFs)
    {
        const TString COUNTER = "MaxPredictedPostponeTime";


        TBootstrap bootstrap;

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(counters);
        counters = counters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(counters);

        auto stats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(stats);
        auto fsCounters = GetFsCounters(counters, FS, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(fsCounters);

        auto predictedCounter = fsCounters->FindCounter(COUNTER);
        UNIT_ASSERT(predictedCounter);

        {
            auto context = MakeIntrusive<TCallContext>(FS, ui64(1));
            context->RequestType = EFileStoreRequest::ReadData;
            stats->RequestStarted(*context);
            stats->UpdateStats(false);

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(0, predictedCounter->GetAtomic());

            const auto inflightTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(10'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                inflightTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                inflightTime.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(50));

            stats->RequestCompleted(*context, {});
            stats->UpdateStats(false);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(60'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                60,
                totalTime.TotalTime.MilliSeconds());
        }

        {
            auto context = MakeIntrusive<TCallContext>(FS, ui64(2));
            context->RequestType = EFileStoreRequest::WriteData;
            stats->RequestStarted(*context);
            stats->UpdateStats(true);

            UNIT_ASSERT_VALUES_EQUAL(
                50'000,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(50'000, predictedCounter->GetAtomic());

            const auto inflightTimeFirst = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(25'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                inflightTimeFirst.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTimeFirst.TotalTime.MilliSeconds());

            const auto inflightTimeSecond = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(65'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                15,
                inflightTimeSecond.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                65,
                inflightTimeSecond.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(20));

            stats->RequestCompleted(*context, {});
            stats->UpdateStats(true);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(75'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                55,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                75,
                totalTime.TotalTime.MilliSeconds());
        }

        {
            auto context = MakeIntrusive<TCallContext>(FS, ui64(3));
            context->RequestType = EFileStoreRequest::ReadData;
            stats->RequestStarted(*context);
            stats->UpdateStats(true);

            UNIT_ASSERT_VALUES_EQUAL(
                35'000,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(50'000, predictedCounter->GetAtomic());

            const auto inflightTimeFirst = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(25'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                inflightTimeFirst.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTimeFirst.TotalTime.MilliSeconds());

            const auto inflightTimeSecond = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(65'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                30,
                inflightTimeSecond.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                65,
                inflightTimeSecond.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(0));

            stats->RequestCompleted(*context, {});
            stats->UpdateStats(true);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(80'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                80,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                80,
                totalTime.TotalTime.MilliSeconds());
        }

        {
            auto context = MakeIntrusive<TCallContext>(FS, ui64(4));
            context->RequestType = EFileStoreRequest::CreateHandle;  // Index op
            stats->RequestStarted(*context);
            stats->UpdateStats(false);

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(50'000, predictedCounter->GetAtomic());

            const auto inflightTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(10'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                inflightTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                inflightTime.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(50));

            stats->RequestCompleted(*context, {});
            stats->UpdateStats(false);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(60'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                60,
                totalTime.TotalTime.MilliSeconds());
        }

        {
            auto context = MakeIntrusive<TCallContext>(FS, ui64(5));
            context->RequestType = EFileStoreRequest::WriteData;
            stats->RequestStarted(*context);
            stats->UpdateStats(true);

            UNIT_ASSERT_VALUES_EQUAL(
                35'000,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(50'000, predictedCounter->GetAtomic());

            const auto inflightTimeFirst = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(25'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                inflightTimeFirst.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTimeFirst.TotalTime.MilliSeconds());

            const auto inflightTimeSecond = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(65'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                30,
                inflightTimeSecond.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                65,
                inflightTimeSecond.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(0));

            stats->RequestCompleted(*context, {});
            stats->UpdateStats(true);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(80'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                80,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                80,
                totalTime.TotalTime.MilliSeconds());
        }
    }

    Y_UNIT_TEST(ShouldReportReportMaxPredictedPostponeTimeForDataRequestsViaFsExclusively)
    {
        const TString COUNTER = "MaxPredictedPostponeTime";
        const TString FS_1 = "test_1";
        const TString FS_2 = "test_2";

        TBootstrap bootstrap;

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(counters);
        counters = counters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(counters);

        auto statsFirst =
            bootstrap.Registry->GetFileSystemStats(FS_1, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(statsFirst);
        auto statsSecond =
            bootstrap.Registry->GetFileSystemStats(FS_2, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(statsSecond);

        auto fsCountersFirst =
            GetFsCounters(counters, FS_1, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(fsCountersFirst);
        auto fsCountersSecond =
            GetFsCounters(counters, FS_2, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(fsCountersSecond);

        auto predictedCounterFirst = fsCountersFirst->FindCounter(COUNTER);
        UNIT_ASSERT(predictedCounterFirst);
        auto predictedCounterSecond = fsCountersSecond->FindCounter(COUNTER);
        UNIT_ASSERT(predictedCounterSecond);

        {
            auto context = MakeIntrusive<TCallContext>(FS_1, ui64(1));
            context->RequestType = EFileStoreRequest::ReadData;
            statsFirst->RequestStarted(*context);
            statsFirst->UpdateStats(false);

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(0, predictedCounterFirst->GetAtomic());

            const auto inflightTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(10'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                inflightTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                inflightTime.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(50));

            statsFirst->RequestCompleted(*context, {});
            statsFirst->UpdateStats(false);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(60'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                60,
                totalTime.TotalTime.MilliSeconds());
        }

        {
            auto context = MakeIntrusive<TCallContext>(FS_1, ui64(2));
            context->RequestType = EFileStoreRequest::WriteData;
            statsFirst->RequestStarted(*context);
            statsFirst->UpdateStats(true);

            UNIT_ASSERT_VALUES_EQUAL(
                50'000,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(50'000, predictedCounterFirst->GetAtomic());

            const auto inflightTimeFirst = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(25'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                inflightTimeFirst.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTimeFirst.TotalTime.MilliSeconds());

            const auto inflightTimeSecond = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(65'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                15,
                inflightTimeSecond.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                65,
                inflightTimeSecond.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(20));

            statsFirst->RequestCompleted(*context, {});
            statsFirst->UpdateStats(true);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(75'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                55,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                75,
                totalTime.TotalTime.MilliSeconds());
        }

        {
            auto context = MakeIntrusive<TCallContext>(FS_2, ui64(3));
            context->RequestType = EFileStoreRequest::ReadData;
            statsSecond->RequestStarted(*context);
            statsSecond->UpdateStats(true);

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(0, predictedCounterSecond->GetAtomic());

            const auto inflightTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(25'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTime.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(25));

            statsSecond->RequestCompleted(*context, {});
            statsSecond->UpdateStats(true);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(35'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                35,
                totalTime.TotalTime.MilliSeconds());
        }
    }

    Y_UNIT_TEST(ShouldNotReportReportMaxPredictedPostponeTimeForNotDataRequestsViaFs)
    {
        const TString COUNTER = "MaxPredictedPostponeTime";


        TBootstrap bootstrap;

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(counters);
        counters = counters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(counters);

        auto stats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(stats);

        auto fsCounters = GetFsCounters(counters, FS, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(fsCounters);

        auto predictedCounter = fsCounters->FindCounter(COUNTER);
        UNIT_ASSERT(predictedCounter);

        {
            auto context = MakeIntrusive<TCallContext>(FS, ui64(1));
            context->RequestType = EFileStoreRequest::CreateHandle;  // Index op
            stats->RequestStarted(*context);
            stats->UpdateStats(false);

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(0, predictedCounter->GetAtomic());

            const auto inflightTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(10'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                inflightTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                inflightTime.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(50));

            stats->RequestCompleted(*context, {});
            stats->UpdateStats(false);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(60'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                60,
                totalTime.TotalTime.MilliSeconds());
        }

        {
            auto context = MakeIntrusive<TCallContext>(FS, ui64(2));
            context->RequestType = EFileStoreRequest::DestroyHandle;  // Index op
            stats->RequestStarted(*context);
            stats->UpdateStats(true);

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(0, predictedCounter->GetAtomic());

            const auto inflightTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(25'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTime.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(20));

            stats->RequestCompleted(*context, {});
            stats->UpdateStats(true);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(75'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                55,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                75,
                totalTime.TotalTime.MilliSeconds());
        }

        {
            auto context = MakeIntrusive<TCallContext>(FS, ui64(3));
            context->RequestType = EFileStoreRequest::CreateNode;  // Control op
            stats->RequestStarted(*context);
            stats->UpdateStats(true);

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                context->GetPossiblePostponeDuration().MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(0, predictedCounter->GetAtomic());

            const auto inflightTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(25'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                inflightTime.TotalTime.MilliSeconds());

            context->AddTime(
                EProcessingStage::Postponed,
                TDuration::MilliSeconds(0));

            stats->RequestCompleted(*context, {});
            stats->UpdateStats(true);

            const auto totalTime = context->CalcRequestTime(
                context->GetRequestStartedCycles()
                    + DurationToCyclesSafe(TDuration::MicroSeconds(80'500)));
            UNIT_ASSERT_VALUES_EQUAL(
                80,
                totalTime.ExecutionTime.MilliSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                80,
                totalTime.TotalTime.MilliSeconds());
        }
    }

    Y_UNIT_TEST(ShouldReportTotalAndMediaKindStatsViaFs)
    {
        TBootstrap bootstrap;

        auto componentCounters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_COMPONENT);
        auto ssdCounters = componentCounters->FindSubgroup("type", "ssd");


        auto stats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER);
        bootstrap.Registry->SetFileSystemMediaKind(FS, CLIENT, NProto::STORAGE_MEDIA_SSD);

        auto context = MakeIntrusive<TCallContext>(FS, ui64(1));
        context->RequestType = EFileStoreRequest::CreateHandle;
        stats->RequestStarted(*context);
        stats->RequestCompleted(*context, {});

        auto counters = componentCounters->FindSubgroup("request", "CreateHandle");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Count")->GetAtomic());

        counters = ssdCounters->FindSubgroup("request", "CreateHandle");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Count")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldNotUnregisterFsStatsIfOnMultipleRegistrations)
    {
        TBootstrap bootstrap;

        auto fsComponentCounters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(fsComponentCounters);
        fsComponentCounters = fsComponentCounters
            ->FindSubgroup("host", "cluster");
        UNIT_ASSERT(fsComponentCounters);


        auto firstStats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER);
        auto secondStats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER);

        {
            auto counters = fsComponentCounters->FindSubgroup("filesystem", FS);
            UNIT_ASSERT(counters);

            counters = counters->FindSubgroup("client", CLIENT);
            UNIT_ASSERT(counters);
        }

        bootstrap.Registry->Unregister(FS, CLIENT);

        {
            auto counters = fsComponentCounters->FindSubgroup("filesystem", FS);
            UNIT_ASSERT(counters);

            counters = counters->FindSubgroup("client", CLIENT);
            UNIT_ASSERT(counters);
        }

        bootstrap.Registry->Unregister(FS, CLIENT);

        {
            auto counters = fsComponentCounters->FindSubgroup("filesystem", FS);
            UNIT_ASSERT(counters);

            counters = counters->FindSubgroup("client", CLIENT);
            UNIT_ASSERT(!counters);
        }
    }

    Y_UNIT_TEST(ShouldNotReportZeroCounters)
    {


        TBootstrap bootstrap;

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(counters);
        counters = counters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(counters);

        auto stats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER);
        UNIT_ASSERT(stats);
        auto fsCounters = GetFsCounters(counters, FS, CLIENT, CLOUD, FOLDER);

        // non lazy-init request
        auto readData = fsCounters->FindSubgroup("request", "ReadData");
        UNIT_ASSERT(readData);

        // lazy-init request
        auto createHandle = fsCounters->FindSubgroup("request", "CreateHandle");
        UNIT_ASSERT(createHandle);

        // these ones should always be present
        auto readDataCount = readData->FindCounter("Count");
        auto readDataErrors = readData->FindCounter("Errors");

        // non lazy-init counters
        auto createHandleCount = createHandle->FindCounter("Count");
        auto createHandleErrorsFatal =
            createHandle->FindCounter("Errors/Fatal");
        // lazy-init counter
        auto createHandleErrors = createHandle->FindCounter("Errors");

        UNIT_ASSERT(readDataCount);
        UNIT_ASSERT_VALUES_EQUAL(0, readDataCount->Val());
        UNIT_ASSERT(readDataErrors);
        UNIT_ASSERT_VALUES_EQUAL(0, readDataErrors->Val());
        UNIT_ASSERT(createHandleCount);
        UNIT_ASSERT_VALUES_EQUAL(0, createHandleCount->Val());
        UNIT_ASSERT(createHandleErrorsFatal);
        UNIT_ASSERT_VALUES_EQUAL(0, createHandleErrorsFatal->Val());
        UNIT_ASSERT(!createHandleErrors);

        {
            auto context = MakeIntrusive<TCallContext>(FS);
            context->RequestType = EFileStoreRequest::CreateHandle;
            stats->RequestStarted(*context);
            stats->RequestCompleted(*context, MakeError(S_OK));

            // we got a request - now all counters for this request should be
            // initialized
            createHandleErrors = createHandle->FindCounter("Errors");
            UNIT_ASSERT(createHandleErrors);
            UNIT_ASSERT_VALUES_EQUAL(0, createHandleErrors->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, createHandleCount->Val());
        }

        {
            auto context = MakeIntrusive<TCallContext>(FS);
            context->RequestType = EFileStoreRequest::CreateHandle;
            stats->RequestStarted(*context);
            stats->RequestCompleted(*context, MakeError(E_REJECTED));

            // and now we successfully registered an error
            UNIT_ASSERT_VALUES_EQUAL(1, createHandleErrors->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, createHandleCount->Val());
        }
    }

    Y_UNIT_TEST(ShouldReportCloudAndFolder)
    {
        TBootstrap bootstrap;

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(counters);
        counters = counters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(counters);


        auto stats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, CLOUD, FOLDER);

        {
            auto fsCounters = GetFsCounters(counters, FS, CLIENT, CLOUD, FOLDER);
            UNIT_ASSERT(fsCounters);
        }
    }

    Y_UNIT_TEST(ShouldMoveFilestatsToNewCloudAndFolder)
    {
        TBootstrap bootstrap;

        auto rootCounters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(rootCounters);
        rootCounters = rootCounters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(rootCounters);


        const TString oldCloud = "";
        const TString oldFolder = "";
        const TString newCloud = CLOUD;
        const TString newFolder = FOLDER;
        auto stats =
            bootstrap.Registry->GetFileSystemStats(FS, CLIENT, "", "");

        // emulating first CreateSession request in vfs loop (it is performed
        // with empty cloud and folder)
        auto context = MakeIntrusive<TCallContext>(FS, ui64(1));
        context->RequestType = EFileStoreRequest::CreateSession;
        stats->RequestStarted(*context);
        stats->RequestCompleted(*context, {});

        {
            auto fsCounters =
                GetFsCounters(rootCounters, FS, CLIENT, oldCloud, oldCloud);
            UNIT_ASSERT(fsCounters);

            auto maxPredictedPostponeTimeCounter =
                fsCounters->FindCounter("MaxPredictedPostponeTime");
            UNIT_ASSERT(maxPredictedPostponeTimeCounter);
            UNIT_ASSERT_EQUAL(maxPredictedPostponeTimeCounter->Val(), 0);

            auto requestsCounters =
                fsCounters->GetSubgroup("request", "CreateSession");
            UNIT_ASSERT(requestsCounters);

            auto createSessionCounter = requestsCounters->GetCounter("Count");
            UNIT_ASSERT(createSessionCounter);
            UNIT_ASSERT_EQUAL(createSessionCounter->Val(), 1);
        }

        // emulating assigning new cloud and folder (after session is created)
        // counters should migrate to new cloud and folder subgroup
        stats = bootstrap.Registry
                    ->GetFileSystemStats(FS, CLIENT, newCloud, newFolder);

        {
            auto fsCounters = rootCounters->FindSubgroup("filesystem", FS);
            UNIT_ASSERT(fsCounters);

            fsCounters = fsCounters->FindSubgroup("client", CLIENT);
            UNIT_ASSERT(fsCounters);

            auto emptyCounters = fsCounters->FindSubgroup("cloud", oldCloud);
            UNIT_ASSERT(!emptyCounters);

            fsCounters = fsCounters->FindSubgroup("cloud", newCloud);
            UNIT_ASSERT(fsCounters);

            fsCounters = fsCounters->FindSubgroup("folder", newFolder);
            UNIT_ASSERT(fsCounters);

            auto maxPredictedPostponeTimeCounter =
                fsCounters->FindCounter("MaxPredictedPostponeTime");
            UNIT_ASSERT(maxPredictedPostponeTimeCounter);
            UNIT_ASSERT_EQUAL(maxPredictedPostponeTimeCounter->Val(), 0);

            auto requestsCounters =
                fsCounters->GetSubgroup("request", "CreateSession");
            UNIT_ASSERT(requestsCounters);

            auto createSessionCounter = requestsCounters->GetCounter("Count");
            UNIT_ASSERT(createSessionCounter);
            UNIT_ASSERT_EQUAL(createSessionCounter->Val(), 1);
        }
    }

    Y_UNIT_TEST(ShouldNotCrashWhenUpdatingStatsAfterReset)
    {
        struct TIncompleteRequestProvider final
            : public IIncompleteRequestProvider
        {
            void Accept(IIncompleteRequestCollector& collector) override
            {
                Y_UNUSED(collector);
            }
        };

        const auto provider = std::make_shared<TIncompleteRequestProvider>();
        TBootstrap bootstrap;
        bootstrap.Registry->GetRequestStats()
            ->RegisterIncompleteRequestProvider(provider);
        bootstrap.Registry->GetRequestStats()->UpdateStats(false);
        bootstrap.Registry->GetRequestStats()->Reset();
        bootstrap.Registry->GetRequestStats()->UpdateStats(false);
    }
}

} // namespace NCloud::NFileStore::NStorage
