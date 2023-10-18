#include "request_stats.h"

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
            nullptr,
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

        auto fsComponentCounters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_FS_COMPONENT);
        UNIT_ASSERT(fsComponentCounters);
        fsComponentCounters = fsComponentCounters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(fsComponentCounters);

        const TString fs = "test";
        const TString client = "client";
        auto stats = bootstrap.Registry->GetFileSystemStats(
            fs,
            client);
        {
            auto fsCounters = fsComponentCounters
                ->FindSubgroup("filesystem", fs);
            UNIT_ASSERT(fsCounters);

            auto clientCounters = fsCounters->FindSubgroup("client", client);
            UNIT_ASSERT(clientCounters);
        }

        bootstrap.Registry->Unregister(fs, client);
        {
            auto fsCounters = fsComponentCounters
                ->FindSubgroup("filesystem", fs);
            UNIT_ASSERT(fsCounters);
            UNIT_ASSERT(!fsCounters->FindSubgroup("client", client));
        }
    }

    Y_UNIT_TEST(ShouldReportTotalAndMediaKindStatsViaComponent)
    {
        TBootstrap bootstrap;

        auto componentCounters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_COMPONENT);
        auto ssdCounters = componentCounters->FindSubgroup("type", "ssd");

        const TString fs = "test";
        const TString client = "client";
        UNIT_ASSERT(bootstrap.Registry->GetFileSystemStats(fs, client));
        bootstrap.Registry->SetFileSystemMediaKind(fs, client, NProto::STORAGE_MEDIA_SSD);

        auto stats = bootstrap.Registry->GetRequestStats();

        auto context = MakeIntrusive<TCallContext>(fs, ui64(1));
        context->RequestType = EFileStoreRequest::CreateHandle;
        stats->RequestStarted(*context);
        stats->RequestCompleted(*context);

        auto counters = componentCounters->FindSubgroup("request", "CreateHandle");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Count")->GetAtomic());

        counters = ssdCounters->FindSubgroup("request", "CreateHandle");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Count")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldReportTotalAndMediaKindStatsViaFs)
    {
        TBootstrap bootstrap;

        auto componentCounters = bootstrap.Counters
            ->FindSubgroup("component", METRIC_COMPONENT);
        auto ssdCounters = componentCounters->FindSubgroup("type", "ssd");

        const TString fs = "test";
        const TString client = "client";
        auto stats = bootstrap.Registry->GetFileSystemStats(fs, client);
        bootstrap.Registry->SetFileSystemMediaKind(fs, client, NProto::STORAGE_MEDIA_SSD);

        auto context = MakeIntrusive<TCallContext>(fs, ui64(1));
        context->RequestType = EFileStoreRequest::CreateHandle;
        stats->RequestStarted(*context);
        stats->RequestCompleted(*context);

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

        const TString fs = "test";
        const TString client = "client";
        auto firstStats = bootstrap.Registry->GetFileSystemStats(fs, client);
        auto secondStats = bootstrap.Registry->GetFileSystemStats(fs, client);

        {
            auto counters = fsComponentCounters->FindSubgroup("filesystem", fs);
            UNIT_ASSERT(counters);

            counters = counters->FindSubgroup("client", client);
            UNIT_ASSERT(counters);
        }

        bootstrap.Registry->Unregister(fs, client);

        {
            auto counters = fsComponentCounters->FindSubgroup("filesystem", fs);
            UNIT_ASSERT(counters);

            counters = counters->FindSubgroup("client", client);
            UNIT_ASSERT(counters);
        }

        bootstrap.Registry->Unregister(fs, client);

        {
            auto counters = fsComponentCounters->FindSubgroup("filesystem", fs);
            UNIT_ASSERT(counters);

            counters = counters->FindSubgroup("client", client);
            UNIT_ASSERT(!counters);
        }
    }
}

} // namespace NCloud::NFileStore::NStorage
