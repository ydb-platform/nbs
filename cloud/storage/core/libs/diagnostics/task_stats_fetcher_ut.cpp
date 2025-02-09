#include "stats_fetcher.h"

#include "critical_events.h"

#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString ComponentName = "STORAGE_STATS";

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TaskStatsFetcherTest)
{
    Y_UNIT_TEST(ShouldGetCpuWait)
    {
        auto fetcher = CreateTaskStatsFetcher(
            ComponentName,
            CreateLoggingService("console"),
            getpid());
        fetcher->Start();
        auto [cpuWait, error] = fetcher->GetCpuWait();
        UNIT_ASSERT_C(!HasError(error), error);
    }
}

}   // namespace NCloud::NStorage
