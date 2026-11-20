#include "create_volume_link_actor.h"
#include "propagate_to_follower.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv: TTestActorRuntimeBase
{
    TActorId Edge;
    TCallContextPtr CallContext = MakeIntrusive<TCallContext>(ui64{123});
    TLeaderFollowerLink Link{
        .LinkUUID = "link-uuid",
        .LeaderDiskId = "disk",
        .LeaderCellId = "cell-a",
        .FollowerDiskId = "disk",
        .FollowerCellId = "cell-b"};
    ui32 LocalDescribeCount = 0;

    TEnv()
    {
        InitNodes();
        AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);
        // The default runtime filter drops scheduled events. Retry tests
        // inject wakeups explicitly so they do not depend on timer dispatch.
        SetDispatchTimeout(TDuration::Seconds(5));
        Edge = AllocateEdgeActor();
        RegisterService(MakeSSProxyServiceId(), Edge);
        RegisterService(MakeVolumeProxyServiceId(), Edge);
        RegisterService(MakeCellProxyServiceId(), Edge);
        SetEventFilter([&](auto&, TAutoPtr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() == TEvSSProxy::EvDescribeVolumeRequest) {
                ++LocalDescribeCount;
            }
            return false;
        });
    }

    template <typename TEvent>
    typename TEvent::TPtr Grab()
    {
        return GrabEdgeEventRethrow<TEvent>(Edge);
    }

    template <typename TEvent>
    void Reply(const IEventHandle& request, std::unique_ptr<TEvent> response)
    {
        Send(new IEventHandle(
            request.Sender,
            Edge,
            response.release(),
            0,
            request.Cookie));
    }

    void DescribeSource()
    {
        auto request = Grab<TEvSSProxy::TEvDescribeVolumeRequest>();
        UNIT_ASSERT_VALUES_EQUAL(Link.LeaderDiskId, request->Get()->DiskId);
        UNIT_ASSERT(request->Get()->ExactDiskIdMatch);
        NKikimrSchemeOp::TPathDescription description;
        auto* volume = description.MutableBlockStoreVolumeDescription();
        volume->SetTokenVersion(11);
        auto* config = volume->MutableVolumeConfig();
        config->SetDiskId(Link.LeaderDiskId);
        config->SetBlockSize(4096);
        config->MutablePartitions()->Add()->SetBlockCount(128);
        Reply(
            *request,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                "source",
                std::move(description)));
    }

    void DescribeRemoteFollower(ui64 blocksCount)
    {
        auto request = Grab<TEvCellProxy::TEvDescribeVolumeRequest>();
        UNIT_ASSERT_VALUES_EQUAL("cell-b", request->Get()->CellId);
        UNIT_ASSERT_VALUES_EQUAL(Link.FollowerDiskId, request->Get()->DiskId);
        NProto::TVolume volume;
        volume.SetDiskId(Link.FollowerDiskId);
        volume.SetBlockSize(4096);
        volume.SetBlocksCount(blocksCount);
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_HDD);
        volume.SetTokenVersion(22);
        Reply(
            *request,
            std::make_unique<TEvCellProxy::TEvDescribeVolumeResponse>(
                std::move(volume)));
    }

    TActorId Propagate(TPropagateLinkToFollowerActor::EReason reason)
    {
        return Register(new TPropagateLinkToFollowerActor(
            "test",
            CreateRequestInfo(Edge, 42, CallContext),
            Link,
            reason));
    }

    TEvCellProxy::TEvExecuteActionRequest::TPtr GrabAction(
        NProto::ELinkAction action)
    {
        auto request = Grab<TEvCellProxy::TEvExecuteActionRequest>();
        UNIT_ASSERT(request->Get()->CallContext == CallContext);
        UNIT_ASSERT_VALUES_EQUAL("cell-b", request->Get()->CellId);
        UNIT_ASSERT_VALUES_EQUAL(
            "updatelinkonfollower",
            request->Get()->Record.GetAction());
        NProto::TUpdateLinkOnFollowerRequest payload;
        const auto status = google::protobuf::util::JsonStringToMessage(
            request->Get()->Record.GetInput(),
            &payload);
        UNIT_ASSERT_C(status.ok(), status.ToString());
        UNIT_ASSERT_VALUES_EQUAL(Link.LinkUUID, payload.GetLinkUUID());
        UNIT_ASSERT_VALUES_EQUAL(Link.FollowerDiskId, payload.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            Link.FollowerCellId,
            payload.GetFollowerCellId());
        UNIT_ASSERT_VALUES_EQUAL(Link.LeaderDiskId, payload.GetLeaderDiskId());
        UNIT_ASSERT_VALUES_EQUAL(Link.LeaderCellId, payload.GetLeaderCellId());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(action),
            static_cast<int>(payload.GetAction()));
        return request;
    }

    template <typename TEvent>
    void CheckResult(const NProto::TError& error)
    {
        auto response = Grab<TEvent>();
        UNIT_ASSERT_VALUES_EQUAL(42, response->Cookie);
        UNIT_ASSERT_VALUES_EQUAL(
            error.SerializeAsString(),
            response->Get()->GetError().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(Link.LinkUUID, response->Get()->Link.LinkUUID);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLinkActorsTest)
{
    Y_UNIT_TEST(ShouldDescribeRemoteFollowerWithSameDiskIdAsLocalShadow)
    {
        TEnv env;
        env.Register(new TCreateVolumeLinkActor("test", env.Edge, env.Link));
        env.DescribeSource();
        env.DescribeRemoteFollower(256);
        auto update =
            env.Grab<TEvVolumePrivate::TEvUpdateFollowerStateRequest>();
        UNIT_ASSERT_VALUES_EQUAL(1, env.LocalDescribeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::STORAGE_MEDIA_HDD),
            static_cast<int>(update->Get()->Follower.MediaKind));
        UNIT_ASSERT_EQUAL(
            TFollowerDiskInfo::EState::Created,
            update->Get()->Follower.State);
    }

    Y_UNIT_TEST(ShouldValidateRemoteFollowerSize)
    {
        TEnv env;
        env.Register(new TCreateVolumeLinkActor("test", env.Edge, env.Link));
        env.DescribeSource();
        env.DescribeRemoteFollower(64);
        auto result = env.Grab<TEvVolumePrivate::TEvCreateLinkFinished>();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result->Get()->GetStatus());
    }

    Y_UNIT_TEST(ShouldPreserveRemoteDescribeError)
    {
        TEnv env;
        env.Register(new TCreateVolumeLinkActor("test", env.Edge, env.Link));
        auto request = env.Grab<TEvCellProxy::TEvDescribeVolumeRequest>();
        auto error = MakeError(E_INVALID_STATE, "remote describe failed");
        error.SetFlags(4);
        env.Reply(
            *request,
            std::make_unique<TEvCellProxy::TEvDescribeVolumeResponse>(error));
        auto result = env.Grab<TEvVolumePrivate::TEvCreateLinkFinished>();
        UNIT_ASSERT_VALUES_EQUAL(
            error.SerializeAsString(),
            result->Get()->GetError().SerializeAsString());
    }

    Y_UNIT_TEST(ShouldKeepSameCellDescribeLocal)
    {
        TEnv env;
        env.Link.FollowerCellId = env.Link.LeaderCellId;
        env.Link.FollowerDiskId = "follower";
        env.Register(new TCreateVolumeLinkActor("test", env.Edge, env.Link));
        env.DescribeSource();
        auto request = env.Grab<TEvSSProxy::TEvDescribeVolumeRequest>();
        UNIT_ASSERT_VALUES_EQUAL("follower", request->Get()->DiskId);
        NKikimrSchemeOp::TPathDescription description;
        auto* config = description.MutableBlockStoreVolumeDescription()
                           ->MutableVolumeConfig();
        config->SetDiskId("follower");
        config->SetBlockSize(4096);
        config->MutablePartitions()->Add()->SetBlockCount(256);
        env.Reply(
            *request,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                "follower",
                std::move(description)));
        env.Grab<TEvVolumePrivate::TEvUpdateFollowerStateRequest>();
        UNIT_ASSERT_VALUES_EQUAL(2, env.LocalDescribeCount);
    }

    Y_UNIT_TEST(ShouldPropagateEveryRemoteActionAndPreserveStatus)
    {
        using EReason = TPropagateLinkToFollowerActor::EReason;
        for (const auto code: {S_OK, S_ALREADY, S_FALSE, E_INVALID_STATE}) {
            auto error = MakeError(code, "remote result");
            error.SetFlags(4);
            {
                TEnv env;
                env.Propagate(EReason::Creation);
                auto request = env.GrabAction(NProto::LINK_ACTION_CREATE);
                env.Reply(
                    *request,
                    std::make_unique<TEvCellProxy::TEvExecuteActionResponse>(
                        error));
                env.CheckResult<TEvVolumePrivate::TEvLinkOnFollowerCreated>(
                    error);
            }
            {
                TEnv env;
                env.Propagate(EReason::Destruction);
                auto request = env.GrabAction(NProto::LINK_ACTION_DESTROY);
                env.Reply(
                    *request,
                    std::make_unique<TEvCellProxy::TEvExecuteActionResponse>(
                        error));
                env.CheckResult<TEvVolumePrivate::TEvLinkOnFollowerDestroyed>(
                    error);
            }
            {
                TEnv env;
                env.Propagate(EReason::DataTransferred);
                auto request = env.GrabAction(NProto::LINK_ACTION_COMPLETED);
                env.Reply(
                    *request,
                    std::make_unique<TEvCellProxy::TEvExecuteActionResponse>(
                        error));
                env.CheckResult<
                    TEvVolumePrivate::TEvLinkOnFollowerDataTransferred>(error);
            }
        }
    }

    Y_UNIT_TEST(ShouldRetryRemoteActionWithTheSameUUID)
    {
        TEnv env;
        const auto actor =
            env.Propagate(TPropagateLinkToFollowerActor::EReason::Destruction);
        auto first = env.GrabAction(NProto::LINK_ACTION_DESTROY);
        const auto input = first->Get()->Record.GetInput();
        env.Reply(
            *first,
            std::make_unique<TEvCellProxy::TEvExecuteActionResponse>(
                MakeError(E_REJECTED, "retry")));
        env.Send(new IEventHandle(actor, env.Edge, new TEvents::TEvWakeup()));
        auto second = env.GrabAction(NProto::LINK_ACTION_DESTROY);
        UNIT_ASSERT_VALUES_EQUAL(input, second->Get()->Record.GetInput());
        // An attempt without a response must also retain the pinned identity.
        env.Send(new IEventHandle(actor, env.Edge, new TEvents::TEvWakeup()));
        auto third = env.GrabAction(NProto::LINK_ACTION_DESTROY);
        UNIT_ASSERT_VALUES_EQUAL(input, third->Get()->Record.GetInput());
        const auto error = MakeError(S_ALREADY, "already removed");
        env.Reply(
            *third,
            std::make_unique<TEvCellProxy::TEvExecuteActionResponse>(error));
        env.CheckResult<TEvVolumePrivate::TEvLinkOnFollowerDestroyed>(error);
    }

    Y_UNIT_TEST(ShouldKeepSameCellPropagationOnVolumeProxy)
    {
        TEnv env;
        env.Link.FollowerCellId = env.Link.LeaderCellId;
        env.Propagate(TPropagateLinkToFollowerActor::EReason::Destruction);
        auto request = env.Grab<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        UNIT_ASSERT_VALUES_EQUAL(
            env.Link.LinkUUID,
            request->Get()->Record.GetLinkUUID());
        UNIT_ASSERT(request->Get()->Record.GetHeaders().GetExactDiskIdMatch());
        const auto error = MakeError(S_FALSE, "local result");
        env.Reply(
            *request,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                error));
        env.CheckResult<TEvVolumePrivate::TEvLinkOnFollowerDestroyed>(error);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
