#include "tablet_proxy.h"

#include <cloud/filestore/libs/storage/testlib/ss_proxy_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_proxy_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletProxyTest)
{
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        ssProxy.CreateFileStore("test", 1000);

        TIndexTabletProxyClient tabletProxy(env.GetRuntime(), nodeIdx);
        tabletProxy.WaitReady("test");
    }
}

}   // namespace NCloud::NFileStore::NStorage
