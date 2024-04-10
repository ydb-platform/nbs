#include <cloud/storage/core/libs/iam/iface/client.h>
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

auto MakeConfig()
{
    const TString port = getenv("NOTIFY_SERVICE_MOCK_PORT");

    NProto::TNotifyConfig proto;
    proto.SetEndpoint("https://localhost:" + port + "/notify/v1/send");
    proto.SetCaCertFilename(
        JoinFsPaths(getenv("TEST_CERT_FILES_DIR"), "server.crt"));

    return std::make_shared<TNotifyConfig>(std::move(proto));
}

auto MakeConfigV2()
{
    const TString port = getenv("NOTIFY_SERVICE_MOCK_PORT");

    NProto::TNotifyConfig proto;
    proto.SetEndpoint("https://localhost:" + port + "/notify/v2/send");
    proto.SetCaCertFilename(
        JoinFsPaths(getenv("TEST_CERT_FILES_DIR"), "server.crt"));
    proto.SetVersion(2);

    return std::make_shared<TNotifyConfig>(std::move(proto));
}

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

    Y_UNIT_TEST(ShouldNotifyDiskError)
    {
        auto service = CreateService(MakeConfig(), nullptr);
        service->Start();

        auto r = service->Notify({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Timestamp = TInstant::ParseIso8601("2023-01-01T00:00:01Z"),
            .Event = TDiskError{ .DiskId = "nrd0" },
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }

    Y_UNIT_TEST(ShouldNotifyDiskBackOnline)
    {
        auto service = CreateService(MakeConfig(), nullptr);
        service->Start();

        auto r = service->Notify({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Timestamp = TInstant::ParseIso8601("2023-01-01T00:00:01Z"),
            .Event = TDiskBackOnline{ .DiskId = "nrd0" },
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }

    Y_UNIT_TEST(ShouldNotifyAboutLotsOfDiskErrors)
    {
        auto service = CreateService(MakeConfig(), nullptr);
        service->Start();

        TVector<NThreading::TFuture<NProto::TError>> futures;
        for (ui32 i = 0; i < 20; ++i) {
            futures.push_back(service->Notify({
                .CloudId = "yc-nbs",
                .FolderId = "yc-nbs.folder",
                .Timestamp = TInstant::ParseIso8601("2023-01-01T00:00:01Z"),
                .Event = TDiskError{ .DiskId = "nrd0" },
            }));
        }

        for (auto& f: futures) {
            auto r = f.GetValue(WaitTimeout);
            UNIT_ASSERT_C(!HasError(r), r);
        }

        service->Stop();
    }
}

Y_UNIT_TEST_SUITE(TNotifyTestV2)
{
    Y_UNIT_TEST(ShouldNotifyDiskErrorV2)
    {
        auto service = CreateService(MakeConfigV2(), NCloud::NIamClient::CreateIamTokenClientStub());
        service->Start();

        auto r = service->Notify({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Timestamp = TInstant::ParseIso8601("2024-04-01T00:00:01Z"),
            .Event = TDiskError{ .DiskId = "ShouldNotifyDiskErrorV2" },
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }

    Y_UNIT_TEST(ShouldNotifyDiskBackOnlineV2)
    {
        auto service = CreateService(MakeConfigV2(), NCloud::NIamClient::CreateIamTokenClientStub());
        service->Start();

        auto r = service->Notify({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Timestamp = TInstant::ParseIso8601("2024-04-01T00:00:01Z"),
            .Event = TDiskBackOnline{ .DiskId = "nrd0" },
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }

    Y_UNIT_TEST(ShouldNotifyAboutLotsOfDiskErrorsV2)
    {
        auto service = CreateService(MakeConfigV2(), NCloud::NIamClient::CreateIamTokenClientStub());
        service->Start();

        TVector<NThreading::TFuture<NProto::TError>> futures;
        for (ui32 i = 0; i < 20; ++i) {
            futures.push_back(service->Notify({
                .CloudId = "yc-nbs",
                .FolderId = "yc-nbs.folder",
                .Timestamp = TInstant::ParseIso8601("2024-04-01T00:00:01Z"),
                .Event = TDiskError{ .DiskId = "nrd0" },
            }));
        }

        for (auto& f: futures) {
            auto r = f.GetValue(WaitTimeout);
            UNIT_ASSERT_C(!HasError(r), r);
        }

        service->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NNotify
