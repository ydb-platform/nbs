
#include "tablet.h"

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Stats)
{
    Y_UNIT_TEST(ShouldProperlyReportDataMetrics)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetThrottlingEnabled(false);

        TTestEnv env({}, storageConfig);
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");
        const auto nodeIdx = env.CreateNode("nfs");
        const auto tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        const auto nodeId =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const auto handle = CreateHandle(tablet, nodeId);
        const int blockCount = 1024;
        const auto sz = DefaultBlockSize * blockCount;

        ui64 reportsCount = 0;

        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case NKikimr::TEvLocal::EvTabletMetrics: {
                        ++reportsCount;
                    }
                }

                return false;
            });

        tablet.WriteData(handle, 0, sz, 'a');

        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(20));
        {
            NActors::TDispatchOptions options;
            options.FinalEvents.emplace_back(NKikimr::TEvLocal::EvTabletMetrics);
            env.GetRuntime().DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_UNEQUAL(0, reportsCount);
    }
}
}   // namespace NCloud::NFileStore::NStorage
