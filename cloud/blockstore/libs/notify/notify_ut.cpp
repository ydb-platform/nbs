#include "notify.h"

#include "config.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

#include <library/cpp/testing/unittest/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

namespace NCloud::NBlockStore::NNotify {

namespace {

////////////////////////////////////////////////////////////////////////////////
const int v1 = 1;
const int v2 = 2;

static constexpr TDuration WaitTimeout = TDuration::Seconds(30);

////////////////////////////////////////////////////////////////////////////////

//Same class as TIamTokenClientForStub, but returns nonempty token.
class TIamTokenClientForTests final: public NIamClient::IIamTokenClient
{
public:
    TResultOrError<NIamClient::TTokenInfo> GetToken() override
    {
        return NIamClient::TTokenInfo{
            "XXXXXXXXXXXXXXXXXXXXXXXXXX",
            TInstant::Zero()};
    }

    NThreading::TFuture<TResultOrError<NIamClient::TTokenInfo>>
    GetTokenAsync() override
    {
        return NThreading::MakeFuture(TResultOrError(
            NIamClient::TTokenInfo{"XXXXXXXXXXXXXXXXXXXXXXXXXX", TInstant::Zero()}));
    }

    void Start() override
    {}

    void Stop() override
    {}
};

using TIamTokenClientForTestsPtr = std::shared_ptr<TIamTokenClientForTests>;

TIamTokenClientForTestsPtr CreateIamTokenClientForTest()
{
    return std::make_shared<TIamTokenClientForTests>();
}

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

auto CreateNotifyService(int version)
{
    return CreateService(
        version == 2 ? MakeConfigV2() : MakeConfig(),
        CreateIamTokenClientForTest());
}

void ShouldNotifyDiskErrorImpl(int version)
{
    auto service = CreateNotifyService(version);

    service->Start();

    auto r =
        service->Notify({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Timestamp = TInstant::ParseIso8601("2024-04-01T00:00:01Z"),
            .Event =
                TDiskError{
                    .DiskId = "ShouldNotifyDiskErrorV" + ToString(version)},
        }).GetValue(WaitTimeout);

    UNIT_ASSERT_C(!HasError(r), r);

    service->Stop();
}

void ShouldNotifyDiskBackOnlineImpl(int version)
{
    auto service = CreateNotifyService(version);
    service->Start();

    auto r =
        service->Notify({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Timestamp = TInstant::ParseIso8601("2023-01-01T00:00:01Z"),
            .Event = TDiskBackOnline{.DiskId = "nrd0"},
        }).GetValue(WaitTimeout);

    UNIT_ASSERT_C(!HasError(r), r);

    service->Stop();
}

void ShouldNotifyAboutLotsOfDiskErrorsImpl(int version)
{
    auto service = CreateNotifyService(version);
    service->Start();

    TVector<NThreading::TFuture<NProto::TError>> futures;
    for (ui32 i = 0; i < 20; ++i) {
        futures.push_back(service->Notify({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Timestamp = TInstant::ParseIso8601("2023-01-01T00:00:01Z"),
            .Event = TDiskError{.DiskId = "nrd0"},
        }));
    }

    for (auto& f: futures) {
        auto r = f.GetValue(WaitTimeout);
        UNIT_ASSERT_C(!HasError(r), r);
    }

    service->Stop();
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
            .Event = TDiskError{.DiskId = "nrd0"},
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
            .Event = TDiskError{.DiskId = "nrd0"},
        }).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), r);

        service->Stop();
    }

    Y_UNIT_TEST(ShouldNotifyDiskError)
    {
        ShouldNotifyDiskErrorImpl(v1);
    }

    Y_UNIT_TEST(ShouldNotifyDiskBackOnline)
    {
        ShouldNotifyDiskBackOnlineImpl(v1);
    }

    Y_UNIT_TEST(ShouldNotifyAboutLotsOfDiskErrors)
    {
        ShouldNotifyAboutLotsOfDiskErrorsImpl(v1);
    }
}

Y_UNIT_TEST_SUITE(TNotifyTestV2)
{
    Y_UNIT_TEST(ShouldNotifyDiskErrorV2)
    {
        ShouldNotifyDiskErrorImpl(v2);
    }

    Y_UNIT_TEST(ShouldNotifyDiskBackOnlineV2)
    {
        ShouldNotifyDiskBackOnlineImpl(v2);
    }

    Y_UNIT_TEST(ShouldNotifyAboutLotsOfDiskErrorsV2)
    {
        ShouldNotifyAboutLotsOfDiskErrorsImpl(v2);
    }
}

}   // namespace NCloud::NBlockStore::NNotify
