#include "device_path.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDevicePathTest)
{
    Y_UNIT_TEST(TestParseValidDevicePath)
    {
        DevicePath devPath("rdma");
        auto error = devPath.Parse("rdma://a.b.c.d:10020/11223344");
        UNIT_ASSERT_C(!HasError(error), error);

        UNIT_ASSERT_EQUAL(devPath.Protocol, "rdma");
        UNIT_ASSERT_EQUAL(devPath.Host, "a.b.c.d");
        UNIT_ASSERT_EQUAL(devPath.Port, 10020);
        UNIT_ASSERT_EQUAL(devPath.Uuid, "11223344");
    }

    Y_UNIT_TEST(TestParseInvalidProtocol)
    {
        DevicePath devPath("unknown");
        auto error = devPath.Parse("rdma://a.b.c.d:10020/11223344");
        UNIT_ASSERT(HasError(error));
    }

    Y_UNIT_TEST(TestParseNoUuid)
    {
        DevicePath devPath("rdma");
        auto error = devPath.Parse("rdma://a.b.c.d:10020");
        UNIT_ASSERT(HasError(error));
    }

    Y_UNIT_TEST(TestSerialize)
    {
        auto expectedPath = "rdma://a.b.c.d:10020/11223344";
        DevicePath devPath("rdma", "a.b.c.d", 10020, "11223344");
        UNIT_ASSERT_EQUAL(devPath.Serialize(), expectedPath);

        DevicePath devPath2("rdma");
        auto error = devPath2.Parse(expectedPath);
        UNIT_ASSERT_C(!HasError(error), error);
        UNIT_ASSERT_EQUAL(expectedPath, devPath2.Serialize());
    }
}

}   // namespace NCloud::NBlockStore
