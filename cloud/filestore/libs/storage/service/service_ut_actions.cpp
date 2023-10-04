#include "service.h"

#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

namespace NCloud::NFileStore::NStorage {

using namespace NKikimr;
using namespace std::string_literals;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceActionsTest)
{
    Y_UNIT_TEST(ShouldFail)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        auto response = service.AssertExecuteActionFailed("NonExistingAction", "{}");

        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response->GetStatus());
    }

}

}   // namespace NCloud::NFileStore::NStorage
