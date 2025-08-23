#include "notify.h"

#include "config.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

#include <library/cpp/testing/unittest/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

namespace NCloud::NBlockStore::NNotify {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(30);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNotifyTest)
{
    Y_UNIT_TEST(ShouldNull)
    {
        auto logging = CreateLoggingService("console");

        auto service = CreateNullService(logging);
        service->Start();

        auto r = service->Notify({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Event = TDiskError{ .DiskId = "nrd0" },
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }

    Y_UNIT_TEST(ShouldStub)
    {
        auto service = CreateServiceStub();
        service->Start();

        auto r = service->Notify({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Event = TDiskError{ .DiskId = "nrd0" },
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NNotify
