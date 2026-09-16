#include <cloud/blockstore/libs/cells/iface/forward_service.h>

#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTarget: public TTestService
{
    ui32 Mounts = 0;

    TTarget()
    {
        MountVolumeHandler =
            [this] (std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            Y_UNUSED(request);
            ++Mounts;
            return MakeFuture(NProto::TMountVolumeResponse());
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFakeMonRequest: NMonitoring::IMonHttpRequest
{
    TStringStream Out;
    TCgiParameters Params;
    THttpHeaders Headers;

    IOutputStream& Output() override
    {
        return Out;
    }

    HTTP_METHOD GetMethod() const override
    {
        return HTTP_METHOD_GET;
    }
    TStringBuf GetPath() const override { return {}; }
    TStringBuf GetPathInfo() const override { return {}; }
    TStringBuf GetUri() const override { return {}; }
    const TCgiParameters& GetParams() const override { return Params; }
    const TCgiParameters& GetPostParams() const override { return Params; }
    TStringBuf GetPostContent() const override { return {}; }
    const THttpHeaders& GetHeaders() const override { return Headers; }
    TStringBuf GetHeader(TStringBuf) const override { return {}; }
    TStringBuf GetCookie(TStringBuf) const override { return {}; }
    TString GetRemoteAddr() const override { return {}; }
    TString GetServiceTitle() const override { return {}; }
    NMonitoring::IMonPage* GetPage() const override { return nullptr; }
    NMonitoring::IMonHttpRequest* MakeChild(
        NMonitoring::IMonPage*,
        const TString&) const override
    {
        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    std::shared_ptr<TTarget> Authorized = std::make_shared<TTarget>();
    std::shared_ptr<TTarget> Trusted = std::make_shared<TTarget>();
    IMonitoringServicePtr Monitoring = CreateMonitoringServiceStub();
    IBlockStorePtr Service;

    TEnv()
    {
        Service = CreateCellForwardService(
            Authorized,
            Trusted,
            Monitoring,
            CreateLoggingService("console"),
            CreateWallClockTimer());
    }

    void Mount(
        NCloud::NProto::ERequestSource source,
        const TString& cellId,
        const TString& diskId = {})
    {
        auto request = std::make_shared<NProto::TMountVolumeRequest>();
        auto& internal = *request->MutableHeaders()->MutableInternal();
        internal.SetRequestSource(source);
        if (cellId) {
            request->MutableHeaders()->SetCellId(cellId);
        }
        if (diskId) {
            request->SetDiskId(diskId);
        }
        Service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(request));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCellForwardServiceTest)
{
    Y_UNIT_TEST(ShouldForwardTrustedInterCellMountToTrusted)
    {
        TEnv env;
        env.Mount(NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL, "cell-1");
        UNIT_ASSERT_VALUES_EQUAL(1, env.Trusted->Mounts);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Authorized->Mounts);
    }

    Y_UNIT_TEST(ShouldAuthorizeWhenCellIdMissing)
    {
        TEnv env;
        env.Mount(NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL, "");
        UNIT_ASSERT_VALUES_EQUAL(0, env.Trusted->Mounts);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Authorized->Mounts);
    }

    Y_UNIT_TEST(ShouldAuthorizeWhenSourceIsNotTrusted)
    {
        TEnv env;
        env.Mount(NCloud::NProto::SOURCE_INSECURE_CONTROL_CHANNEL, "cell-1");
        UNIT_ASSERT_VALUES_EQUAL(0, env.Trusted->Mounts);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Authorized->Mounts);
    }

    Y_UNIT_TEST(ShouldRecordInboundActivityOnTrustedMount)
    {
        TEnv env;
        env.Mount(
            NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL, "cell-7", "disk-42");

        auto blockstore = env.Monitoring->GetMonPage("blockstore");
        UNIT_ASSERT(blockstore);
        auto* cells = static_cast<NMonitoring::TIndexMonPage&>(*blockstore)
                          .FindPage("Cells");
        UNIT_ASSERT(cells);
        auto* page = static_cast<NMonitoring::TIndexMonPage&>(*cells)
                         .FindPage("Inbound");
        UNIT_ASSERT(page);

        TFakeMonRequest request;
        page->Output(request);
        UNIT_ASSERT_STRING_CONTAINS(request.Out.Str(), "cell-7");
        UNIT_ASSERT_STRING_CONTAINS(request.Out.Str(), "disk-42");
    }
}

}   // namespace NCloud::NBlockStore::NCells
