#include <cloud/blockstore/libs/cells/iface/forward_service.h>

#include <cloud/blockstore/libs/cells/iface/inbound_activity.h>
#include <cloud/blockstore/libs/common/constants.h>

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
    ui32 Actions = 0;

    TTarget()
    {
        ExecuteActionHandler =
            [this](std::shared_ptr<NProto::TExecuteActionRequest> request)
        {
            Y_UNUSED(request);
            ++Actions;
            return MakeFuture(NProto::TExecuteActionResponse());
        };
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

    void ExecuteAction(
        const TString& action,
        NCloud::NProto::ERequestSource source,
        const TString& cellId)
    {
        auto request = std::make_shared<NProto::TExecuteActionRequest>();
        request->SetAction(action);
        auto* headers = request->MutableHeaders();
        headers->SetCellId(cellId);
        headers->MutableInternal()->SetRequestSource(source);
        Service->ExecuteAction(MakeIntrusive<TCallContext>(), std::move(request));
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

    Y_UNIT_TEST(ShouldTrustOnlyUpdateLinkOnFollowerAction)
    {
        TEnv env;
        for (const TString action:
             {"updatelinkonfollower", "UpdateLinkOnFollower"})
        {
            env.ExecuteAction(
                action,
                NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL,
                "cell-1");
        }
        UNIT_ASSERT_VALUES_EQUAL(2, env.Trusted->Actions);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Authorized->Actions);
        UNIT_ASSERT(env.Activity->Snapshot(env.Timer->Now()).empty());
    }

    Y_UNIT_TEST(ShouldAuthorizeLinkActionWithoutCellId)
    {
        TEnv env;
        env.ExecuteAction(
            "updatelinkonfollower",
            NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL,
            "");
        UNIT_ASSERT_VALUES_EQUAL(0, env.Trusted->Actions);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Authorized->Actions);
    }

    Y_UNIT_TEST(ShouldAuthorizeLinkActionFromUntrustedSource)
    {
        TEnv env;
        for (const auto source:
             {NCloud::NProto::SOURCE_INSECURE_CONTROL_CHANNEL,
              NCloud::NProto::SOURCE_TCP_DATA_CHANNEL})
        {
            env.ExecuteAction("updatelinkonfollower", source, "cell-1");
        }
        UNIT_ASSERT_VALUES_EQUAL(0, env.Trusted->Actions);
        UNIT_ASSERT_VALUES_EQUAL(2, env.Authorized->Actions);
    }

    Y_UNIT_TEST(ShouldAuthorizeOtherActionsEvenFromTrustedCell)
    {
        TEnv env;
        for (const TString action:
             {"getstorageconfig", "updatelinkonfollower-extra", ""})
        {
            env.ExecuteAction(
                action,
                NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL,
                "cell-1");
        }
        UNIT_ASSERT_VALUES_EQUAL(0, env.Trusted->Actions);
        UNIT_ASSERT_VALUES_EQUAL(3, env.Authorized->Actions);
    }

    Y_UNIT_TEST(ShouldMarkCopyWriteOnlyForTrustedCellIngress)
    {
        TEnv env;
        bool trusted = false;
        env.Trusted->WriteBlocksHandler = [&](auto request)
        {
            trusted = true;
            UNIT_ASSERT(request->GetHeaders().GetInternal().GetIsCellCopyRequest());
            return MakeFuture(NProto::TWriteBlocksResponse());
        };
        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        auto* headers = request->MutableHeaders();
        headers->SetCellId("cell-b");
        headers->SetClientId(TString(CopyVolumeClientId));
        headers->SetExactDiskIdMatch(true);
        headers->SetCopyLinkUUID("copy-link");
        headers->MutableInternal()->SetRequestSource(
            NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL);
        env.Service->WriteBlocks(MakeIntrusive<TCallContext>(), request).GetValue();
        UNIT_ASSERT(trusted);
    }

    Y_UNIT_TEST(ShouldNotTrustCopyMarkerFromInsecureChannel)
    {
        TEnv env;
        bool authorized = false;
        env.Authorized->ZeroBlocksHandler = [&](auto request)
        {
            authorized = true;
            UNIT_ASSERT(!request->GetHeaders().GetInternal().GetIsCellCopyRequest());
            return MakeFuture(NProto::TZeroBlocksResponse());
        };
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        auto* headers = request->MutableHeaders();
        headers->SetCellId("cell-b");
        headers->SetClientId(TString(CopyVolumeClientId));
        headers->SetExactDiskIdMatch(true);
        headers->SetCopyLinkUUID("copy-link");
        headers->MutableInternal()->SetIsCellCopyRequest(true);
        headers->MutableInternal()->SetRequestSource(
            NCloud::NProto::SOURCE_INSECURE_CONTROL_CHANNEL);
        env.Service->ZeroBlocks(MakeIntrusive<TCallContext>(), request).GetValue();
        UNIT_ASSERT(authorized);
    }
}

}   // namespace NCloud::NBlockStore::NCells
