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

        {
            TLocalNVMeConfigPtr config =
                std::make_shared<TLocalNVMeConfig>(proto);
            UNIT_ASSERT_VALUES_EQUAL(
                proto.GetDevicesSourceUri(),
                config->GetDevicesSourceUri());

            UNIT_ASSERT(!config->GetLockdownConfig());
        }

        const TVector<ui8> allowedAdminOpcodes{1, 2, 3};
        const TVector<ui8> allowedSetFeatureIds{10, 20, 30};

        {
            auto& lockdownProto = *proto.MutableLockdownConfig();
            lockdownProto.SetBlockLockdownCommand(true);
            lockdownProto.MutableAllowedAdminOpcodes()->Assign(
                allowedAdminOpcodes.begin(),
                allowedAdminOpcodes.end());
            lockdownProto.MutableAllowedSetFeatureIds()->Assign(
                allowedSetFeatureIds.begin(),
                allowedSetFeatureIds.end());
        }

        {
            TLocalNVMeConfigPtr config =
                std::make_shared<TLocalNVMeConfig>(proto);
            UNIT_ASSERT_VALUES_EQUAL(
                proto.GetDevicesSourceUri(),
                config->GetDevicesSourceUri());

            auto lockdown = config->GetLockdownConfig();

            UNIT_ASSERT(lockdown);

            UNIT_ASSERT(lockdown->GetBlockLockdownCommand());
            UNIT_ASSERT_EQUAL(
                allowedAdminOpcodes,
                lockdown->GetAllowedAdminOpcodes());

            UNIT_ASSERT_EQUAL(
                allowedSetFeatureIds,
                lockdown->GetAllowedSetFeatureIds());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
