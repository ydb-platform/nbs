
#include "tablet.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <ydb/library/actors/core/mon.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Monitoring)
{

    Y_UNIT_TEST(ShouldHandleHttpInfo)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.WaitReady();

        auto response = tablet.GetRemoteHttpInfo();
        // check that it was served by user part of the tablet
        UNIT_ASSERT(response->Html.Contains("Filesystem Id:"));
    }
}

}   // namespace NCloud::NFileStore::NStorage
