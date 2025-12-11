#include "service_ut.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceStatsTest)
{
    Y_UNIT_TEST(ShouldReportSelfPingMaxUsSensor)
    {
        TTestEnv env;
        auto nodeIdx = SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        TServiceClient service{runtime, nodeIdx};

        auto counter = runtime.GetAppData(nodeIdx)
                           .Counters->GetSubgroup("counters", "blockstore")
                           ->GetSubgroup("component", "service")
                           ->GetCounter("SelfPingMaxUs", false);

        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(TEvServicePrivate::EvSelfPing, 4);
            runtime.DispatchEvents(opts);
        }

        runtime.AdvanceCurrentTime(TDuration::Seconds(15));
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_UNEQUAL(0, counter->Val());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
