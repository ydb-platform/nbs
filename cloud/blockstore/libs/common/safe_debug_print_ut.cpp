#include "safe_debug_print.h"

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(SafeDebugPrintTest)
{
    Y_UNIT_TEST(ShouldPrintAsAs)
    {
        NBlockStore::NProto::TCreateVolumeRequest request;
        request.SetDiskId("disk-1");
        request.SetBlocksCount(1000);

        UNIT_ASSERT_VALUES_EQUAL(
            request.ShortDebugString(),
            SafeDebugPrint(request));
    }

    Y_UNIT_TEST(ShouldMaskAuthToken)
    {
        NBlockStore::NProto::TCreateVolumeRequest request;
        request.MutableHeaders()->MutableInternal()->SetAuthToken("secret");
        request.SetDiskId("disk-1");

        UNIT_ASSERT_VALUES_EQUAL(
            "Headers { Internal { AuthToken: \"xxx\" } } DiskId: \"disk-1\"",
            SafeDebugPrint(request));
    }
}

}   // namespace NCloud::NBlockStore
