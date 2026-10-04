#include <cloud/blockstore/libs/cells/iface/forward_service.h>

#include <cloud/blockstore/libs/cells/iface/inbound_activity.h>

#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

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

struct TEnv
{
    std::shared_ptr<TTarget> Authorized = std::make_shared<TTarget>();
    std::shared_ptr<TTarget> Trusted = std::make_shared<TTarget>();
    std::shared_ptr<TCellInboundActivity> Activity =
        std::make_shared<TCellInboundActivity>();
    ITimerPtr Timer = CreateWallClockTimer();
    IBlockStorePtr Service;

    TEnv()
    {
        Service = CreateCellForwardService(
            Authorized,
            Trusted,
            Activity,
            CreateLoggingService("console"),
            Timer);
    }

    void Mount(
        NCloud::NProto::ERequestSource source,
        const TString& cellId,
        const TString& diskId = {})
    {
        auto request = std::make_shared<NProto::TMountVolumeRequest>();
        auto& internal = *request->MutableHeaders()->MutableInternal();
        internal.SetRequestSource(source);
        internal.SetPeer("peer-1");
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

        auto rows = env.Activity->Snapshot(env.Timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(1, rows.size());
        UNIT_ASSERT_VALUES_EQUAL("peer-1", rows[0].Peer);
        UNIT_ASSERT_VALUES_EQUAL("disk-42", rows[0].DiskId);
        UNIT_ASSERT_VALUES_EQUAL(1, rows[0].Mounts);
    }
}

}   // namespace NCloud::NBlockStore::NCells
