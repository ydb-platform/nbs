#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalNVMeConfigTest)
{
    Y_UNIT_TEST(ShouldStoreConfig)
    {
        NProto::TLocalNVMeConfig proto;
        proto.SetDevicesSourceUri("file:///etc/service/devices.txt");

        TLocalNVMeConfigPtr config = std::make_shared<TLocalNVMeConfig>(proto);
        UNIT_ASSERT_VALUES_EQUAL(
            proto.GetDevicesSourceUri(),
            config->GetDevicesSourceUri());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
