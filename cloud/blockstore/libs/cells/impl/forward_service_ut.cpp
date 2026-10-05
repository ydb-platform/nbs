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
        UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            Y_UNUSED(request);
            return MakeFuture(NProto::TUnmountVolumeResponse());
        };
        DescribeVolumeHandler =
            [] (std::shared_ptr<NProto::TDescribeVolumeRequest> request)
        {
            Y_UNUSED(request);
            return MakeFuture(NProto::TDescribeVolumeResponse());
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
    }

    Y_UNIT_TEST(ShouldRecordOnlyMountsAsInboundActivity)
    {
        TEnv env;

        auto headers = [] (auto& request)
        {
            auto& h = *request->MutableHeaders();
            h.SetCellId("cell-7");
            h.MutableInternal()->SetRequestSource(
                NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL);
            h.MutableInternal()->SetPeer("peer-1");
            request->SetDiskId("disk-42");
        };

        auto unmount = std::make_shared<NProto::TUnmountVolumeRequest>();
        headers(unmount);
        env.Service->UnmountVolume(MakeIntrusive<TCallContext>(), unmount);

        auto describe = std::make_shared<NProto::TDescribeVolumeRequest>();
        headers(describe);
        env.Service->DescribeVolume(MakeIntrusive<TCallContext>(), describe);

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            env.Activity->Snapshot(env.Timer->Now()).size());

        env.Mount(
            NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL, "cell-7", "disk-42");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Activity->Snapshot(env.Timer->Now()).size());
    }
}

}   // namespace NCloud::NBlockStore::NCells
