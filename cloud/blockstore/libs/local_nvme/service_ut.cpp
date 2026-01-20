#include "service.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/folder/tempdir.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalNVMeServiceTest)
{
    Y_UNIT_TEST(ShouldStub)
    {
        auto service = CreateLocalNVMeServiceStub();
        service->Start();

        {
            auto [list, error] = service->ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, list.size());
        }

        {
            auto future = service->AcquireNVMeDevice("foo");
            const auto& error = future.GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        }

        {
            auto future = service->ReleaseNVMeDevice("foo");
            const auto& error = future.GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        }

        service->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
