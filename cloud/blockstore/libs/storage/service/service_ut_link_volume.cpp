#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/blockstore/private/api/protos/checkpoints.pb.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <util/generic/algorithm.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TGetLinkStatusResponse GetLinkStatus(
    TServiceClient& service,
    const TString& leaderDiskId,
    const TString& followerDiskId,
    const TString& followerCellId)
{
    NProto::TGetLinkStatusRequest request;
    request.SetLeaderDiskId(leaderDiskId);
    request.SetFollowerDiskId(followerDiskId);
    request.SetFollowerCellId(followerCellId);
    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);
    auto response = service.ExecuteAction("GetLinkStatus", buf);
    NProto::TGetLinkStatusResponse proto;
    UNIT_ASSERT_C(
        google::protobuf::util::JsonStringToMessage(
            response->Record.GetOutput(),
            &proto)
            .ok(),
        response->Record.GetOutput());
    return proto;
}

enum class ETagRemovalRecovery
{
    None,
    Retry,
    Reboot,
    LostRequest,
    LostResponse,
    UnchangedTags,
};

void CheckSourceDiskIdTagRemoval(
    ETagRemovalRecovery recovery,
    TString sourceDiskIdTag = MakeTag("source-disk-id", "vol-1"))
{
    TTestEnv env(1, 1, 4);
    const ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");
    auto& runtime = env.GetRuntime();
    TServiceClient service(runtime, nodeIdx);
    service.CreateVolume("vol-1", DefaultBlocksCount);

    auto create = service.CreateCreateVolumeRequest("vol-1-copy");
    create->Record.SetTagsStr(JoinSeq(
        ",",
        TVector{sourceDiskIdTag, MakeTag("keep", "custom")}));
    service.SendRequest(MakeStorageServiceId(), std::move(create));
    auto created = service.RecvCreateVolumeResponse();
    UNIT_ASSERT_C(!HasError(created->GetError()), created->GetErrorReason());

    auto makeLinkRequest = [](NProto::ELinkAction action)
    {
        auto request =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        request->Record.SetDiskId("vol-1-copy");
        request->Record.SetLeaderDiskId("vol-1");
        request->Record.SetLeaderCellId("cell-a");
        request->Record.SetFollowerCellId("cell-a");
        request->Record.SetLinkUUID("test-link");
        request->Record.SetAction(action);
        return request;
    };

    service.SendRequest(
        MakeVolumeProxyServiceId(),
        makeLinkRequest(NProto::LINK_ACTION_CREATE));
    auto linked =
        service.RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>();
    UNIT_ASSERT_C(!HasError(linked->GetError()), linked->GetErrorReason());

    ui64 followerTabletId = 0;
    ui32 removalAttempts = 0;
    TVector<TString> firstRequestedTags;
    TActorId cleanupSender;
    bool cleanupSucceeded = false;
    bool prematureCompletion = false;
    ui32 completionResponses = 0;
    ui32 sourceDestructionSchedules = 0;
    bool responseWasLost = false;
    std::unique_ptr<IEventHandle> lostResponse;
    TTestActorRuntimeBase::TScheduledEventFilter previousScheduledFilter;
    previousScheduledFilter = runtime.SetScheduledEventFilter(
        [&](TTestActorRuntimeBase& callbackRuntime,
            TAutoPtr<IEventHandle>& event,
            TDuration delay,
            TInstant& deadline)
        {
            if (event->GetTypeRewrite() == TEvService::EvDestroyVolumeRequest) {
                ++sourceDestructionSchedules;
            }
            return previousScheduledFilter(
                callbackRuntime,
                event,
                delay,
                deadline);
        });
    runtime.SetEventFilter(
        [&](TTestActorRuntimeBase& callbackRuntime,
            TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvSSProxy::EvDescribeVolumeResponse) {
                const auto* msg =
                    event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                const auto& description =
                    msg->PathDescription.GetBlockStoreVolumeDescription();
                if (description.GetVolumeConfig().GetDiskId() == "vol-1-copy") {
                    followerTabletId = description.GetVolumeTabletId();
                }
            }
            if (event->GetTypeRewrite() == TEvService::EvRemoveTagsRequest) {
                cleanupSender = event->Sender;
                if (++removalAttempts == 1) {
                    firstRequestedTags =
                        event->Get<TEvService::TEvRemoveTagsRequest>()->Tags;
                    if (recovery == ETagRemovalRecovery::Retry ||
                        recovery == ETagRemovalRecovery::UnchangedTags)
                    {
                        callbackRuntime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                new TEvService::TEvRemoveTagsResponse(
                                    recovery == ETagRemovalRecovery::Retry
                                        ? MakeError(E_REJECTED, "try again")
                                        : MakeError(S_OK)), 0, event->Cookie),
                            nodeIdx,
                            true);
                        return true;
                    }
                    if (recovery == ETagRemovalRecovery::Reboot ||
                        recovery == ETagRemovalRecovery::LostRequest)
                    {
                        return true;
                    }
                }
            }
            if (event->GetTypeRewrite() == TEvService::EvRemoveTagsResponse &&
                event->Recipient == cleanupSender)
            {
                cleanupSucceeded =
                    cleanupSucceeded ||
                    ((recovery != ETagRemovalRecovery::UnchangedTags ||
                      removalAttempts > 1) &&
                     !HasError(event->Get<TEvService::TEvRemoveTagsResponse>()
                                   ->GetError()));
                if (recovery == ETagRemovalRecovery::LostResponse &&
                    !responseWasLost)
                {
                    responseWasLost = true;
                    lostResponse.reset(event.Release());
                    return true;
                }
            }
            if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerResponse &&
                event->Sender == cleanupSender &&
                !HasError(
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerResponse>()
                        ->GetError()))
            {
                prematureCompletion = prematureCompletion || !cleanupSucceeded;
            }
            if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerResponse &&
                event->Recipient == service.GetSender() &&
                !HasError(
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerResponse>()
                        ->GetError()))
            {
                ++completionResponses;
                UNIT_ASSERT_C(
                    cleanupSucceeded,
                    "Leadership transfer was acknowledged before tag removal");
            }
            return false;
        });

    const auto before = service.DescribeVolume("vol-1-copy");
    UNIT_ASSERT_VALUES_EQUAL(
        ParseTags(sourceDiskIdTag).at("source-disk-id"),
        before->Record.GetVolume().GetTags().at("source-disk-id"));
    UNIT_ASSERT_VALUES_EQUAL(0, removalAttempts);
    UNIT_ASSERT(followerTabletId);

    service.SendRequest(
        MakeVolumeProxyServiceId(),
        makeLinkRequest(NProto::LINK_ACTION_COMPLETED));
    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return removalAttempts > 0;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(removalAttempts > 0);
        UNIT_ASSERT_C(
            IsIn(firstRequestedTags, sourceDiskIdTag),
            sourceDiskIdTag);
    }
    if (recovery == ETagRemovalRecovery::Reboot) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return removalAttempts == 1;
        };
        runtime.DispatchEvents(options);
        UNIT_ASSERT_VALUES_EQUAL(1, removalAttempts);
        NKikimr::RebootTablet(
            runtime,
            followerTabletId,
            service.GetSender(),
            nodeIdx);
        auto interrupted =
            service.RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>();
        UNIT_ASSERT(HasError(interrupted->GetError()));

        // Replicated volumes start partitions lazily after reboot.
        auto waitReady = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
        waitReady->Record.SetDiskId("vol-1-copy");
        waitReady->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        service.SendRequest(MakeVolumeProxyServiceId(), std::move(waitReady));
        auto ready = service.RecvResponse<TEvVolume::TEvWaitReadyResponse>();
        UNIT_ASSERT_C(!HasError(ready->GetError()), ready->GetErrorReason());

        options.CustomFinalCondition = [&]
        {
            return cleanupSucceeded;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT_C(
            cleanupSucceeded,
            "Tag removal was not resumed after reboot without a link request");
        UNIT_ASSERT_VALUES_EQUAL(2, removalAttempts);
        const auto recovered = service.DescribeVolume("vol-1-copy");
        UNIT_ASSERT(!recovered->Record.GetVolume().GetTags().contains(
            "source-disk-id"));

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            makeLinkRequest(NProto::LINK_ACTION_COMPLETED));
    }

    if (recovery == ETagRemovalRecovery::LostResponse) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return lostResponse != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(lostResponse);
        UNIT_ASSERT_VALUES_EQUAL(0, completionResponses);
    }

    auto completed =
        service.RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>();
    UNIT_ASSERT_C(
        !HasError(completed->GetError()),
        completed->GetErrorReason());
    UNIT_ASSERT_C(
        !prematureCompletion,
        "The follower acknowledged leadership before observing tag removal");
    const auto after = service.DescribeVolume("vol-1-copy");
    const auto& tags = after->Record.GetVolume().GetTags();
    UNIT_ASSERT(!tags.contains("source-disk-id"));
    UNIT_ASSERT_VALUES_EQUAL("custom", tags.at("keep"));
    UNIT_ASSERT_VALUES_EQUAL(
        recovery == ETagRemovalRecovery::None ||
                recovery == ETagRemovalRecovery::LostResponse
            ? 1
            : 2,
        removalAttempts);

    if (recovery == ETagRemovalRecovery::LostResponse) {
        UNIT_ASSERT_VALUES_EQUAL(1, completionResponses);
        UNIT_ASSERT_VALUES_EQUAL(1, sourceDestructionSchedules);
        runtime.Send(lostResponse.release(), nodeIdx);
        runtime.Send(
            new IEventHandle(
                cleanupSender,
                MakeStorageServiceId(),
                new TEvService::TEvRemoveTagsResponse(
                    MakeError(E_REJECTED, "late error"))),
            nodeIdx);
        runtime.Send(
            new IEventHandle(
                cleanupSender,
                cleanupSender,
                new TEvVolumePrivate::TEvRetryRemoveSourceDiskIdTag()),
            nodeIdx);
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            makeLinkRequest(NProto::LINK_ACTION_COMPLETED));
        auto repeated =
            service.RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>();
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, repeated->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(1, removalAttempts);
        UNIT_ASSERT_VALUES_EQUAL(2, completionResponses);
        UNIT_ASSERT_VALUES_EQUAL(1, sourceDestructionSchedules);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceLinkVolumeTest)
{
    Y_UNIT_TEST(ShouldRemoveSourceDiskIdTagBeforeLeadershipTransfer)
    {
        CheckSourceDiskIdTagRemoval(ETagRemovalRecovery::None);
    }

    Y_UNIT_TEST(ShouldRemoveBareSourceDiskIdTagBeforeLeadershipTransfer)
    {
        CheckSourceDiskIdTagRemoval(
            ETagRemovalRecovery::None,
            "source-disk-id");
    }

    Y_UNIT_TEST(ShouldWaitForSourceDiskIdTagToDisappear)
    {
        CheckSourceDiskIdTagRemoval(ETagRemovalRecovery::UnchangedTags);
    }

    Y_UNIT_TEST(ShouldRetrySourceDiskIdTagRemoval)
    {
        CheckSourceDiskIdTagRemoval(ETagRemovalRecovery::Retry);
    }

    Y_UNIT_TEST(ShouldRetrySourceDiskIdTagRemovalAfterLostRequest)
    {
        CheckSourceDiskIdTagRemoval(ETagRemovalRecovery::LostRequest);
    }

    Y_UNIT_TEST(ShouldFinishSourceDiskIdTagRemovalAfterLostResponse)
    {
        CheckSourceDiskIdTagRemoval(ETagRemovalRecovery::LostResponse);
    }

    Y_UNIT_TEST(ShouldResumeSourceDiskIdTagRemovalAfterReboot)
    {
        CheckSourceDiskIdTagRemoval(ETagRemovalRecovery::Reboot);
    }

    Y_UNIT_TEST(ShouldFailOnInvalidArgumentVolume)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount * 2);

        auto checkCreationError = [&](const TString& leaderDiskId,
                                      const TString& followerDiskId,
                                      const NProto::TError& error)
        {
            const auto status =
                GetLinkStatus(service, leaderDiskId, followerDiskId, {});
            UNIT_ASSERT_EQUAL_C(
                NProto::LINK_STATUS_ERROR,
                status.GetStatus(), status.ShortDebugString());
            UNIT_ASSERT_C(status.GetLinkUUID(), status.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(error), status.GetErrorMessage());
        };

        {
            service.SendCreateVolumeLinkRequest("vol-1", "vol-1");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }
        {
            service.SendCreateVolumeLinkRequest("vol-1", "unknown");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(
                IsNotFoundSchemeShardError(response->GetError()),
                FormatError(response->GetError()));
            checkCreationError("vol-1", "unknown", response->GetError());
        }
        {
            service.SendCreateVolumeLinkRequest("unknown", "vol-1");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(
                IsNotFoundSchemeShardError(response->GetError()),
                FormatError(response->GetError()));
        }
        {
            service.SendCreateVolumeLinkRequest("vol-2", "vol-1");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            checkCreationError("vol-2", "vol-1", response->GetError());
        }
    }

    Y_UNIT_TEST(ShouldLinkVolume)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount * 2);

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        auto response = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
    }

    Y_UNIT_TEST(ShouldPreserveSourceLinkWhenTargetAlreadyOwnsDisk)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);
        service.CreateVolumeLink("vol-1", "vol-2");

        const auto before = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT(before.GetLinkUUID());
        auto completed =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        completed->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        completed->Record.SetDiskId("vol-2");
        completed->Record.SetLeaderDiskId("vol-1");
        completed->Record.SetLeaderCellId(before.GetLeaderCellId());
        completed->Record.SetFollowerCellId(before.GetFollowerCellId());
        completed->Record.SetLinkUUID(before.GetLinkUUID());
        completed->Record.SetAction(NProto::LINK_ACTION_COMPLETED);
        service.SendRequest(MakeVolumeProxyServiceId(), std::move(completed));
        auto transferred =
            service.RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>();
        UNIT_ASSERT_C(
            !HasError(transferred->GetError()),
            transferred->GetErrorReason());

        ui32 sourceUnlinkRequests = 0;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& callbackRuntime,
                TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(callbackRuntime);

                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUnlinkLeaderVolumeFromFollowerRequest)
                {
                    ++sourceUnlinkRequests;
                }
                return false;
            });

        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
        const auto rejected = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, rejected->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, sourceUnlinkRequests);
        const auto after = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT_VALUES_EQUAL(before.GetLinkUUID(), after.GetLinkUUID());
        UNIT_ASSERT(after.GetStatus() != NProto::LINK_STATUS_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldUnlinkVolume)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        service.CreateVolumeLink("vol-1", "vol-2");
        {
            service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
            auto response = service.RecvDestroyVolumeLinkResponse();
            UNIT_ASSERT_EQUAL_C(
                S_OK,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }
        {
            service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
            auto response = service.RecvDestroyVolumeLinkResponse();
            UNIT_ASSERT_EQUAL_C(
                S_ALREADY,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldNotRestoreCreationErrorAfterUnlink)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnv(env);
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);

        TActorId creatingActor;
        bool holdDescription = true;
        std::unique_ptr<IEventHandle> description;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeRequest &&
                    !creatingActor &&
                    event->Get<TEvSSProxy::TEvDescribeVolumeRequest>()
                            ->DiskId == "missing")
                {
                    creatingActor = event->Sender;
                }
                if (holdDescription &&
                    event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeResponse &&
                    event->Recipient == creatingActor)
                {
                    description.reset(event.Release());
                    return true;
                }
                return false;
            });

        service.SendCreateVolumeLinkRequest("vol-1", "missing");
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return description != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(description);

        service.SendDestroyVolumeLinkRequest("vol-1", "missing");
        const auto destroyed = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_C(
            !HasError(destroyed->GetError()), destroyed->GetErrorReason());

        holdDescription = false;
        runtime.SendAsync(description.release(), nodeIdx);
        const auto cancelled = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT(HasError(cancelled->GetError()));
        const auto afterCancel = GetLinkStatus(service, "vol-1", "missing", {});
        UNIT_ASSERT_EQUAL_C(
            NProto::LINK_STATUS_NOT_FOUND,
            afterCancel.GetStatus(), afterCancel.ShortDebugString());

        // A new attempt must still retain its own diagnostic error.
        service.SendCreateVolumeLinkRequest("vol-1", "missing");
        const auto retried = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT(HasError(retried->GetError()));
        const auto afterRetry = GetLinkStatus(service, "vol-1", "missing", {});
        UNIT_ASSERT_EQUAL_C(
            NProto::LINK_STATUS_ERROR,
            afterRetry.GetStatus(), afterRetry.ShortDebugString());
        UNIT_ASSERT_C(afterRetry.GetLinkUUID(), afterRetry.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            FormatError(retried->GetError()), afterRetry.GetErrorMessage());
    }

    Y_UNIT_TEST(ShouldUnlinkVolumeWhenFollowerDestroyed)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        service.CreateVolumeLink("vol-1", "vol-2");
        service.DestroyVolume("vol-2");

        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
        auto response = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
    }

    Y_UNIT_TEST(ShouldUnlinkVolumeWhenLeaderNotExists)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        size_t followerNotificationCount = 0;
        auto listenUnlinkFollower =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (ev->GetTypeRewrite() ==
                TEvVolume::EvUpdateLinkOnFollowerRequest)
            {
                ++followerNotificationCount;

                const auto* msg =
                    ev->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                UNIT_ASSERT_VALUES_EQUAL(
                    "vol-1",
                    msg->Record.GetLeaderDiskId());
                UNIT_ASSERT_EQUAL(
                    NProto::ELinkAction::LINK_ACTION_DESTROY,
                    msg->Record.GetAction());
            }
            return false;
        };

        runtime.SetEventFilter(listenUnlinkFollower);

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
        auto response = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_EQUAL_C(
            S_ALREADY,
            response->GetError().GetCode(),
            FormatError(response->GetError()));

        UNIT_ASSERT_VALUES_EQUAL_C(
            2,
            followerNotificationCount,
            "Follower notification count must be 2 (one for volume proxy and "
            "one for volume)");
    }

    Y_UNIT_TEST(ShouldGetLinkStatus)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        auto getLinkStatus = [&]() -> NProto::TGetLinkStatusResponse
        {
            NProto::TGetLinkStatusRequest request;
            request.SetLeaderDiskId("vol-1");
            request.SetFollowerDiskId("vol-2");
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto response = service.ExecuteAction("GetLinkStatus", buf);
            NProto::TGetLinkStatusResponse proto;
            UNIT_ASSERT_VALUES_EQUAL_C(
                true,
                google::protobuf::util::JsonStringToMessage(
                    response->Record.GetOutput(),
                    &proto)
                    .ok(),
                response->Record.GetOutput());
            return proto;
        };

        //  Create volumes
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        // Link not created yet.
        auto linkStatus = getLinkStatus();
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_NOT_FOUND,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());

        //  Create link
        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        auto response = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));

        // Link in preparing state.
        linkStatus = getLinkStatus();
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_PREPARING,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());
    }

    Y_UNIT_TEST(ShouldReportVolumeOperationRestrictionWhileLinkIsActive)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        auto isOperationRestricted = [&](const TString& diskId)
        {
            auto request = service.CreateStatVolumeRequest(diskId);
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            request->Record.SetNoPartition(true);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvStatVolumeResponse();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
            return response->Record.GetIsVolumeOperationRestricted();
        };

        UNIT_ASSERT(!isOperationRestricted("vol-1"));
        UNIT_ASSERT(!isOperationRestricted("vol-2"));

        service.CreateVolumeLink("vol-1", "vol-2");

        UNIT_ASSERT(isOperationRestricted("vol-1"));
        UNIT_ASSERT(isOperationRestricted("vol-2"));
    }

    Y_UNIT_TEST(ShouldRejectAlterAndResizeVolumeWhileLinkIsActive)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);
        service.CreateVolumeLink("vol-1", "vol-2");

        for (const auto& diskId: {TString("vol-1"), TString("vol-2")}) {
            service
                .SendAlterVolumeRequest(diskId, "project", "folder", "cloud");
            auto response = service.RecvAlterVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                response->GetStatus(),
                response->GetErrorReason());

            service.SendResizeVolumeRequest(
                diskId,
                DefaultBlocksCount * 2);
            auto resizeResponse = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                resizeResponse->GetStatus(),
                resizeResponse->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldRejectCheckpointWhileLinkIsActive)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);
        service.CreateVolumeLink("vol-1", "vol-2");

        for (const auto& diskId: {TString("vol-1"), TString("vol-2")}) {
            service.SendCreateCheckpointRequest(diskId, "checkpoint");
            auto response = service.RecvCreateCheckpointResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldAllowMultipleActiveCheckpoints)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);

        service.CreateCheckpoint("vol-1", "checkpoint-1");
        service.CreateCheckpoint("vol-1", "checkpoint-2");
    }

    Y_UNIT_TEST(ShouldRejectExclusiveOperationsWhileCheckpointDataIsPresent)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        service.CreateCheckpoint("vol-1", "leader-checkpoint");

        service.SendAlterVolumeRequest(
            "vol-1",
            "project",
            "folder",
            "cloud");
        auto alterResponse = service.RecvAlterVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TRY_AGAIN,
            alterResponse->GetStatus(),
            alterResponse->GetErrorReason());

        service.SendResizeVolumeRequest("vol-1", DefaultBlocksCount * 2);
        auto resizeResponse = service.RecvResizeVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TRY_AGAIN,
            resizeResponse->GetStatus(),
            resizeResponse->GetErrorReason());

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        auto response = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TRY_AGAIN,
            response->GetStatus(),
            response->GetErrorReason());
        service.DeleteCheckpoint("vol-1", "leader-checkpoint");

        service.CreateCheckpoint("vol-2", "follower-checkpoint");

        bool linkRejectedByFollower = false;
        TTestActorRuntimeBase::TEventFilter previousFilter;
        previousFilter = runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
            {
                if (ev->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerResponse)
                {
                    const auto* msg = ev->Get<
                        TEvVolume::TEvUpdateLinkOnFollowerResponse>();
                    if (msg->GetError().GetCode() == E_REJECTED) {
                        UNIT_ASSERT_VALUES_EQUAL(
                            "CreateVolumeLink is not allowed while another "
                            "exclusive volume operation is in progress on "
                            "the follower volume",
                            msg->GetError().GetMessage());
                        linkRejectedByFollower = true;
                    }
                }
                return previousFilter(runtime, ev);
            });

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return linkRejectedByFollower; };
        runtime.DispatchEvents(options);

        service.DeleteCheckpoint("vol-2", "follower-checkpoint");

        response = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());

        runtime.SetEventFilter(previousFilter);
    }

    Y_UNIT_TEST(ShouldAllowExclusiveOperationsAfterCheckpointDataDeletion)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount * 2);

        for (const auto& diskId: {TString("vol-1"), TString("vol-2")}) {
            service.CreateCheckpoint(diskId, "checkpoint");

            NPrivateProto::TDeleteCheckpointDataRequest request;
            request.SetDiskId(diskId);
            request.SetCheckpointId("checkpoint");

            TString input;
            google::protobuf::util::MessageToJsonString(request, &input);
            auto response =
                service.ExecuteAction("deletecheckpointdata", input);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        service.SendAlterVolumeRequest("vol-1", "project", "folder", "cloud");
        auto alterResponse = service.RecvAlterVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            alterResponse->GetStatus(),
            alterResponse->GetErrorReason());

        service.SendResizeVolumeRequest("vol-1", DefaultBlocksCount * 2);
        auto resizeResponse = service.RecvResizeVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            resizeResponse->GetStatus(),
            resizeResponse->GetErrorReason());

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        auto linkResponse = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            linkResponse->GetStatus(),
            linkResponse->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldRejectExclusiveOperationsWhileFillIsInProgress)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        auto request = service.CreateCreateVolumeRequest(
            "vol-1",
            DefaultBlocksCount);
        request->Record.SetFillGeneration(1);
        service.SendRequest(MakeStorageServiceId(), std::move(request));
        auto createResponse = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createResponse->GetStatus(),
            createResponse->GetErrorReason());
        service.CreateVolume("vol-2", DefaultBlocksCount);

        service.SendAlterVolumeRequest(
            "vol-1",
            "project",
            "folder",
            "cloud");
        auto alterResponse = service.RecvAlterVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TRY_AGAIN,
            alterResponse->GetStatus(),
            alterResponse->GetErrorReason());

        service.SendResizeVolumeRequest("vol-1", DefaultBlocksCount * 2);
        auto resizeResponse = service.RecvResizeVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TRY_AGAIN,
            resizeResponse->GetStatus(),
            resizeResponse->GetErrorReason());

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        auto linkResponse = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TRY_AGAIN,
            linkResponse->GetStatus(),
            linkResponse->GetErrorReason());

        service.SendCreateCheckpointRequest("vol-1", "checkpoint");
        auto checkpointResponse = service.RecvCreateCheckpointResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            checkpointResponse->GetStatus(),
            checkpointResponse->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldAllowExclusiveOperationsAfterFillIsFinished)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        auto request =
            service.CreateCreateVolumeRequest("vol-1", DefaultBlocksCount);
        request->Record.SetFillGeneration(1);
        service.SendRequest(MakeStorageServiceId(), std::move(request));
        auto createResponse = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createResponse->GetStatus(),
            createResponse->GetErrorReason());
        service.CreateVolume("vol-2", DefaultBlocksCount * 2);

        auto isOperationRestricted = [&]
        {
            auto request = service.CreateStatVolumeRequest("vol-1");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            request->Record.SetNoPartition(true);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvStatVolumeResponse();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
            return response->Record.GetIsVolumeOperationRestricted();
        };

        UNIT_ASSERT(isOperationRestricted());

        const auto volumeConfig = GetVolumeConfig(service, "vol-1");
        NPrivateProto::TFinishFillDiskRequest finishRequest;
        finishRequest.SetDiskId("vol-1");
        finishRequest.SetConfigVersion(volumeConfig.GetVersion());
        finishRequest.SetFillGeneration(1);

        TString input;
        google::protobuf::util::MessageToJsonString(finishRequest, &input);
        auto finishResponse = service.ExecuteAction("finishfilldisk", input);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            finishResponse->GetStatus(),
            finishResponse->GetErrorReason());

        UNIT_ASSERT(!isOperationRestricted());

        service.SendAlterVolumeRequest("vol-1", "project", "folder", "cloud");
        auto alterResponse = service.RecvAlterVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            alterResponse->GetStatus(),
            alterResponse->GetErrorReason());

        service.SendResizeVolumeRequest("vol-1", DefaultBlocksCount * 2);
        auto resizeResponse = service.RecvResizeVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            resizeResponse->GetStatus(),
            resizeResponse->GetErrorReason());

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        auto linkResponse = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            linkResponse->GetStatus(),
            linkResponse->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldKeepRepeatedLinkRequestIdempotentWhileCreating)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        TAutoPtr<IEventHandle> delayedRequest;
        bool requestDelayed = false;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
            {
                if (!requestDelayed &&
                    ev->GetTypeRewrite() ==
                        TEvVolumePrivate::EvUpdateFollowerStateRequest)
                {
                    const auto* msg = ev->Get<
                        TEvVolumePrivate::TEvUpdateFollowerStateRequest>();
                    if (msg->Follower.State ==
                        TFollowerDiskInfo::EState::Created)
                    {
                        requestDelayed = true;
                        delayedRequest = ev.Release();
                        return true;
                    }
                }
                return false;
            });

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return bool(delayedRequest); };
        runtime.DispatchEvents(options);

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        runtime.Send(delayedRequest.Release(), nodeIdx);

        for (ui32 i = 0; i != 2; ++i) {
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldFillCellIdsInLink)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        size_t linkRequestCount = 0;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
            {
                if (ev->GetTypeRewrite() ==
                    TEvVolume::EvLinkLeaderVolumeToFollowerRequest)
                {
                    ++linkRequestCount;
                    const auto& record =
                        ev->Get<TEvVolume::TEvLinkLeaderVolumeToFollowerRequest>()
                            ->Record;
                    UNIT_ASSERT_VALUES_EQUAL("cell-a", record.GetLeaderCellId());
                    UNIT_ASSERT_VALUES_EQUAL(
                        "cell-a",
                        record.GetFollowerCellId());
                }
                return false;
            });

        service.CreateVolumeLink("vol-1", "vol-2");
        UNIT_ASSERT_VALUES_EQUAL_C(
            2,
            linkRequestCount,
            "one for volume proxy and one for volume");

        auto linkStatus = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_PREPARING,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());
        UNIT_ASSERT_C(linkStatus.GetLinkUUID(), linkStatus.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL("cell-a", linkStatus.GetLeaderCellId());
        UNIT_ASSERT_VALUES_EQUAL("cell-a", linkStatus.GetFollowerCellId());

        service.DestroyVolumeLink("vol-1", "vol-2");

        linkStatus = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_NOT_FOUND,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());
    }

    Y_UNIT_TEST(ShouldRejectFollowerInAnotherCell)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        {
            service.SendCreateVolumeLinkRequest("vol-1", "vol-2", "cell-b");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }

        service.CreateVolumeLink("vol-1", "vol-2", "cell-a");

        // Cell ids are matched strictly once filled in.
        auto linkStatus = GetLinkStatus(service, "vol-1", "vol-2", "cell-b");
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_NOT_FOUND,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());
        {
            service.SendDestroyVolumeLinkRequest("vol-1", "vol-2", "cell-b");
            auto response = service.RecvDestroyVolumeLinkResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }
        linkStatus = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_PREPARING,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());

        service.DestroyVolumeLink("vol-1", "vol-2");
    }

    Y_UNIT_TEST(ShouldMatchLinkCreatedWithoutCellIds)
    {
        // A link persisted by a node without a cell id (before the upgrade)
        // must be found by a node that fills cell ids in.
        TTestEnv env(1, 2, 4);
        ui32 oldNodeIdx = SetupTestEnv(env);
        ui32 newNodeIdx = SetupTestEnvWithCellId(env, "cell-a");

        auto& runtime = env.GetRuntime();
        TServiceClient oldService(runtime, oldNodeIdx);
        TServiceClient newService(runtime, newNodeIdx);
        oldService.CreateVolume("vol-1", DefaultBlocksCount);
        oldService.CreateVolume("vol-2", DefaultBlocksCount);

        oldService.CreateVolumeLink("vol-1", "vol-2");

        auto linkStatus = GetLinkStatus(newService, "vol-1", "vol-2", {});
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_PREPARING,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL("", linkStatus.GetLeaderCellId());
        UNIT_ASSERT_VALUES_EQUAL("", linkStatus.GetFollowerCellId());

        // A legacy link was made within one cell: a lookup naming another
        // cell must not find it.
        linkStatus = GetLinkStatus(newService, "vol-1", "vol-2", "cell-b");
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_NOT_FOUND,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());

        newService.DestroyVolumeLink("vol-1", "vol-2");

        linkStatus = GetLinkStatus(oldService, "vol-1", "vol-2", {});
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_NOT_FOUND,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
