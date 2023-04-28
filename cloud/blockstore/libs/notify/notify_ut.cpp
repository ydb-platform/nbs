#include "notify.h"

#include "config.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

namespace NCloud::NBlockStore::NNotify {

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr TDuration WaitTimeout = TDuration::Seconds(30);

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNotifyTest)
{
    Y_UNIT_TEST(ShouldNull)
    {
        auto logging = CreateLoggingService("console");

        auto service = CreateNullService(logging);
        service->Start();

        auto r = service->NotifyDiskError({
            .DiskId = "nrd0",
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder"
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }

    Y_UNIT_TEST(ShouldStub)
    {
        auto service = CreateServiceStub();
        service->Start();

        auto r = service->NotifyDiskError({
            .DiskId = "nrd0",
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder"
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }

    Y_UNIT_TEST(ShouldNotifyDiskError)
    {
        auto config = [&] {
            const TString port = getenv("NOTIFY_SERVICE_MOCK_PORT");

            NProto::TNotifyConfig proto;
            proto.SetEndpoint("https://localhost:" + port + "/notify/v1/send");
            proto.SetCaCertFilename(
                JoinFsPaths(getenv("TEST_CERT_FILES_DIR"), "server.crt"));

            return std::make_shared<TNotifyConfig>(std::move(proto));
        }();

        auto service = CreateService(config);
        service->Start();

        auto r = service->NotifyDiskError({
            .DiskId = "nrd0",
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder"
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NNotify
