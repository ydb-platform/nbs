#include "service_ut.h"

#include <cloud/blockstore/libs/client/config.h>

#include <cloud/blockstore/libs/storage/api/cell_proxy.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/testlib/cell_manager_mock.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>
#include <cloud/blockstore/private/api/protos/checkpoints.pb.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <util/generic/algorithm.h>
#include <util/generic/set.h>
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

// Mock only the CellProxy transport boundary. The volume handlers and their
// source/follower persistence are real; this does not test network or auth.
struct TRemoteLinkMock
{
    TTestActorRuntimeBase& Runtime;
    const ui32 NodeIdx;
    TSet<TActorId> PendingActions;
    ui32 ActionCount = 0;
    ui32 StatusCount = 0;
    std::function<bool(TAutoPtr<IEventHandle>&)> HoldRequest;
    std::function<void(IEventHandle&)> Observe;

    TRemoteLinkMock(TTestActorRuntimeBase& runtime, ui32 nodeIdx)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
    {
        Runtime.SetEventFilter([&](auto&, TAutoPtr<IEventHandle>& event)
        {
            if (Observe) {
                Observe(*event);
            }
            if (event->GetTypeRewrite() ==
                TEvCellProxy::EvDescribeVolumeRequest)
            {
                if (HoldRequest && HoldRequest(event)) {
                    return true;
                }
                const auto* request =
                    event->Get<TEvCellProxy::TEvDescribeVolumeRequest>();
                UNIT_ASSERT_VALUES_EQUAL("cell-b", request->CellId);
                NProto::TVolume volume;
                volume.SetDiskId(request->DiskId);
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);
                volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
                Runtime.Send(
                    new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        new TEvCellProxy::TEvDescribeVolumeResponse(
                            std::move(volume)),
                        0,
                        event->Cookie),
                    NodeIdx,
                    true);
                return true;
            }
            if (event->GetTypeRewrite() ==
                TEvCellProxy::EvExecuteActionRequest)
            {
                if (HoldRequest && HoldRequest(event)) {
                    return true;
                }
                const auto* request =
                    event->Get<TEvCellProxy::TEvExecuteActionRequest>();
                UNIT_ASSERT_VALUES_EQUAL("cell-b", request->CellId);
                if (request->Record.GetAction() == "getlinkstatus") {
                    // These fixtures have no link in the other cell.
                    NProto::TGetLinkStatusResponse status;
                    status.SetStatus(NProto::LINK_STATUS_NOT_FOUND);
                    auto response = std::make_unique<
                        TEvCellProxy::TEvExecuteActionResponse>();
                    UNIT_ASSERT(google::protobuf::util::MessageToJsonString(
                        status, &response->Output).ok());
                    ++StatusCount;
                    Runtime.Send(new IEventHandle(
                        event->Sender, event->Recipient, response.release(),
                        0, event->Cookie), NodeIdx, true);
                    return true;
                }
                UNIT_ASSERT_VALUES_EQUAL(
                    "updatelinkonfollower",
                    request->Record.GetAction());
                auto forward =
                    std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>(
                        request->CallContext);
                const auto status = google::protobuf::util::JsonStringToMessage(
                    request->Record.GetInput(),
                    &forward->Record);
                UNIT_ASSERT_C(status.ok(), status.ToString());
                UNIT_ASSERT(forward->Record.GetLinkUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "cell-b",
                    forward->Record.GetFollowerCellId());
                PendingActions.insert(event->Sender);
                ++ActionCount;
                Runtime.Send(
                    new IEventHandle(
                        MakeVolumeProxyServiceId(),
                        event->Sender,
                        forward.release(),
                        0,
                        event->Cookie),
                    NodeIdx,
                    true);
                return true;
            }
            if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerResponse &&
                PendingActions.contains(event->GetRecipientRewrite()))
            {
                const auto* response =
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerResponse>();
                Runtime.Send(
                    new IEventHandle(
                        event->GetRecipientRewrite(),
                        event->Sender,
                        new TEvCellProxy::TEvExecuteActionResponse(
                            response->GetError()),
                        0,
                        event->Cookie),
                    NodeIdx,
                    true);
                return true;
            }
            return false;
        });
    }
};

std::unique_ptr<TEvVolume::TEvLinkLeaderVolumeToFollowerRequest>
MakeRemoteLinkRequest()
{
    auto request =
        std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerRequest>();
    request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
    request->Record.SetDiskId("vol-1");
    request->Record.SetLeaderCellId("cell-a");
    request->Record.SetFollowerDiskId("vol-2");
    request->Record.SetFollowerCellId("cell-b");
    return request;
}

std::unique_ptr<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest>
MakeRemoteUnlinkRequest(const TString& uuid)
{
    auto request =
        std::make_unique<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest>();
    request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
    request->Record.SetDiskId("vol-1");
    request->Record.SetLeaderCellId("cell-a");
    request->Record.SetFollowerDiskId("vol-2");
    request->Record.SetFollowerCellId("cell-b");
    request->Record.SetLinkUUID(uuid);
    return request;
}

NProto::TGetLinkStatusResponse GetRemoteLinkStatus(
    TServiceClient& service,
    bool follower = false,
    bool includePendingCreation = false)
{
    auto request = std::make_unique<TEvVolume::TEvGetLinkStatusRequest>();
    request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
    request->Record.SetDiskId(follower ? "vol-2" : "vol-1");
    request->Record.SetLeaderDiskId("vol-1");
    request->Record.SetLeaderCellId("cell-a");
    request->Record.SetFollowerDiskId("vol-2");
    request->Record.SetFollowerCellId("cell-b");
    request->Record.SetIncludePendingCreation(includePendingCreation);
    service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
    auto response = service.RecvResponse<TEvVolume::TEvGetLinkStatusResponse>();
    UNIT_ASSERT_C(!HasError(response->GetError()), response->GetErrorReason());
    return response->Record;
}

enum class ETagRemovalRecovery
{
    None,
    Retry,
    Reboot,
    LostRequest,
    LostResponse,
    UnchangedTags,
    Backoff,
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
    TVector<TDuration> retryDelays;
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
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvRetryRemoveSourceDiskIdTag)
            {
                retryDelays.push_back(delay);
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
                ++removalAttempts;
                if (removalAttempts == 1) {
                    firstRequestedTags =
                        event->Get<TEvService::TEvRemoveTagsRequest>()->Tags;
                }
                if (recovery == ETagRemovalRecovery::Backoff &&
                    removalAttempts <= 6)
                {
                    callbackRuntime.Send(
                        new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            new TEvService::TEvRemoveTagsResponse(
                                MakeError(E_REJECTED, "try again")),
                            0,
                            event->Cookie),
                        nodeIdx,
                        true);
                    return true;
                }
                if (removalAttempts == 1) {
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
        UNIT_ASSERT_C(removalAttempts >= 2, removalAttempts);
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

    if (recovery == ETagRemovalRecovery::Backoff) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return cleanupSucceeded;
        };
        runtime.DispatchEvents(options, TDuration::Minutes(2));
        UNIT_ASSERT(cleanupSucceeded);
        const TVector<TDuration> expectedDelays{
            TDuration::Seconds(1),
            TDuration::Seconds(2),
            TDuration::Seconds(4),
            TDuration::Seconds(8),
            TDuration::Seconds(15),
            TDuration::Seconds(15),
            TDuration::Seconds(15)};
        UNIT_ASSERT_C(
            retryDelays.size() >= expectedDelays.size(),
            retryDelays.size());
        for (size_t i = 0; i < expectedDelays.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(expectedDelays[i], retryDelays[i]);
        }
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
    ui32 minimumAttempts = 2;
    if (recovery == ETagRemovalRecovery::None ||
        recovery == ETagRemovalRecovery::LostResponse)
    {
        minimumAttempts = 1;
    } else if (recovery == ETagRemovalRecovery::Backoff) {
        minimumAttempts = 7;
    }
    UNIT_ASSERT_C(removalAttempts >= minimumAttempts, removalAttempts);

    if (recovery == ETagRemovalRecovery::LostResponse) {
        const auto attemptsAfterCompletion = removalAttempts;
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
        UNIT_ASSERT_VALUES_EQUAL(attemptsAfterCompletion, removalAttempts);
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

    Y_UNIT_TEST(ShouldBackOffSourceDiskIdTagRemovalRetries)
    {
        CheckSourceDiskIdTagRemoval(ETagRemovalRecovery::Backoff);
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
        ui32 sourceUnlinkRequests = 0;
        std::unique_ptr<IEventHandle> transferAcknowledgement;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& callbackRuntime,
                TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(callbackRuntime);

                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvLinkOnFollowerDataTransferred)
                {
                    const auto* msg = event->Get<
                        TEvVolumePrivate::TEvLinkOnFollowerDataTransferred>();
                    UNIT_ASSERT_C(
                        !HasError(msg->GetError()),
                        FormatError(msg->GetError()));
                    UNIT_ASSERT(!transferAcknowledgement);
                    transferAcknowledgement.reset(event.Release());
                    return true;
                }
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUnlinkLeaderVolumeFromFollowerRequest)
                {
                    ++sourceUnlinkRequests;
                }
                return false;
            });

        service.MountVolume("vol-1");
        service.CreateVolumeLink("vol-1", "vol-2");
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return transferAcknowledgement != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(transferAcknowledgement);
        const auto before = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT(before.GetLinkUUID());

        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
        const auto rejected = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, rejected->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, sourceUnlinkRequests);
        const auto after = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT_VALUES_EQUAL(before.GetLinkUUID(), after.GetLinkUUID());
        UNIT_ASSERT_EQUAL(before.GetStatus(), after.GetStatus());
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

    Y_UNIT_TEST(ShouldRemoveFollowerCreatedAfterUnlink)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnv(env);
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);
        service.CreateVolume("vol-3", DefaultBlocksCount);

        TString cancelledUUID;
        bool holdCreation = true;
        bool removed = false;
        TVector<std::unique_ptr<IEventHandle>> creations;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& callbackRuntime,
                TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(callbackRuntime);

                if (holdCreation && event->GetTypeRewrite() ==
                                        TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    const auto& record = event->Get<
                        TEvVolume::TEvUpdateLinkOnFollowerRequest>()->Record;
                    if (record.GetDiskId() == "vol-2" &&
                        record.GetAction() == NProto::LINK_ACTION_CREATE)
                    {
                        cancelledUUID = record.GetLinkUUID();
                        creations.emplace_back(event.Release());
                        return true;
                    }
                }
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvLinkOnFollowerDestroyed)
                {
                    const auto* msg = event->Get<
                        TEvVolumePrivate::TEvLinkOnFollowerDestroyed>();
                    if (msg->Link.LinkUUID == cancelledUUID)
                    {
                        UNIT_ASSERT_C(
                            !HasError(msg->GetError()),
                            FormatError(msg->GetError()));
                        removed = true;
                    }
                }
                return false;
            });

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return !creations.empty();
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(!creations.empty());

        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
        const auto destroyed = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_C(
            !HasError(destroyed->GetError()),
            FormatError(destroyed->GetError()));
        const auto cancelled = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, cancelled->GetStatus());

        // The cancelled creator must no longer restrict the source volume.
        service.CreateVolumeLink("vol-1", "vol-3");
        service.DestroyVolumeLink("vol-1", "vol-3");

        // Wait for the original background DESTROY before releasing CREATE.
        options.CustomFinalCondition = [&]
        {
            return removed;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(removed);
        removed = false;
        holdCreation = false;
        for (auto& creation: creations) {
            runtime.SendAsync(creation.release(), nodeIdx);
        }
        options.CustomFinalCondition = [&]
        {
            return removed;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(removed);
        const auto status = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT_EQUAL_C(
            NProto::LINK_STATUS_NOT_FOUND,
            status.GetStatus(),
            status.ShortDebugString());

        // A fresh UUID must be able to use the target after late CREATE cleanup.
        service.CreateVolumeLink("vol-1", "vol-2");
        const auto recreated = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT(recreated.GetLinkUUID());
        UNIT_ASSERT(recreated.GetLinkUUID() != cancelledUUID);
    }

    Y_UNIT_TEST(ShouldPreserveNewLinkAfterCancelledCreationFinishes)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnv(env);
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        std::unique_ptr<IEventHandle> created;
        TString cancelledUUID;
        size_t destructions = 0;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& callbackRuntime,
                TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(callbackRuntime);

                if (!cancelledUUID && event->GetTypeRewrite() ==
                                          TEvVolumePrivate::EvLinkOnFollowerCreated)
                {
                    const auto* msg = event->Get<
                        TEvVolumePrivate::TEvLinkOnFollowerCreated>();
                    UNIT_ASSERT_C(
                        !HasError(msg->GetError()),
                        FormatError(msg->GetError()));
                    cancelledUUID = msg->Link.LinkUUID;
                    created.reset(event.Release());
                    return true;
                }
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvLinkOnFollowerDestroyed)
                {
                    const auto* msg = event->Get<
                        TEvVolumePrivate::TEvLinkOnFollowerDestroyed>();
                    if (msg->Link.LinkUUID == cancelledUUID) {
                        UNIT_ASSERT_C(
                            !HasError(msg->GetError()),
                            FormatError(msg->GetError()));
                        ++destructions;
                    }
                }
                return false;
            });

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return created != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(created);

        service.DestroyVolumeLink("vol-1", "vol-2");
        const auto cancelled = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, cancelled->GetStatus());
        service.CreateVolumeLink("vol-1", "vol-2");
        const auto before = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT(before.GetLinkUUID());
        UNIT_ASSERT(before.GetLinkUUID() != cancelledUUID);

        runtime.SendAsync(created.release(), nodeIdx);
        options.CustomFinalCondition = [&]
        {
            return destructions == 2;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(2, destructions);
        const auto after = GetLinkStatus(service, "vol-1", "vol-2", {});
        UNIT_ASSERT_VALUES_EQUAL(before.GetLinkUUID(), after.GetLinkUUID());
        UNIT_ASSERT_EQUAL(before.GetStatus(), after.GetStatus());
    }

    void CheckCreationErrorAfterUnlink(bool reverseResponses)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnv(env);
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);

        TActorId creatingActor;
        bool holdDescription = true;
        TVector<std::unique_ptr<IEventHandle>> descriptions;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& callbackRuntime,
                TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(callbackRuntime);

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
                    descriptions.emplace_back(event.Release());
                    return true;
                }
                return false;
            });

        service.SendCreateVolumeLinkRequest("vol-1", "missing");
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return descriptions.size() == 2;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(2, descriptions.size());

        service.SendDestroyVolumeLinkRequest("vol-1", "missing");
        const auto destroyed = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_C(
            !HasError(destroyed->GetError()), destroyed->GetErrorReason());

        holdDescription = false;
        for (size_t i = 0; i < descriptions.size(); ++i) {
            const auto index = reverseResponses ? descriptions.size() - i - 1 : i;
            runtime.SendAsync(descriptions[index].release(), nodeIdx);
        }
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

    Y_UNIT_TEST(ShouldNotRestoreCreationErrorAfterUnlink)
    {
        CheckCreationErrorAfterUnlink(false);
    }

    Y_UNIT_TEST(ShouldNotRestoreCreationErrorAfterUnlinkWithReversedResponses)
    {
        CheckCreationErrorAfterUnlink(true);
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

    Y_UNIT_TEST(ShouldMatchConfiguredCellIdsStrictly)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        TRemoteLinkMock remote(runtime, nodeIdx);

        service.CreateVolumeLink("vol-1", "vol-2", "cell-a");

        // Cell ids are matched strictly once filled in.
        auto linkStatus = GetLinkStatus(service, "vol-1", "vol-2", "cell-b");
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_NOT_FOUND,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(1, remote.StatusCount);
        {
            service.SendDestroyVolumeLinkRequest("vol-1", "vol-2", "cell-b");
            auto response = service.RecvDestroyVolumeLinkResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
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
        TRemoteLinkMock remote(runtime, newNodeIdx);
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

        UNIT_ASSERT_VALUES_EQUAL(1, remote.StatusCount);

        newService.DestroyVolumeLink("vol-1", "vol-2");

        linkStatus = GetLinkStatus(oldService, "vol-1", "vol-2", {});
        UNIT_ASSERT_EQUAL_C(
            NProto::ELinkStatus::LINK_STATUS_NOT_FOUND,
            linkStatus.GetStatus(),
            linkStatus.ShortDebugString());
    }
    Y_UNIT_TEST(ShouldRejectRemoteDestroyWithoutSourceUUIDBeforeMutation)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);
        TRemoteLinkMock remote(runtime, nodeIdx);
        ui32 sourceUnlinks = 0;
        remote.Observe = [&](IEventHandle& event)
        {
            if (event.GetTypeRewrite() ==
                TEvVolume::EvUnlinkLeaderVolumeFromFollowerRequest)
            {
                ++sourceUnlinks;
            }
        };

        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2", "cell-b");
        const auto response = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, remote.ActionCount);
        UNIT_ASSERT_VALUES_EQUAL(0, sourceUnlinks);
    }

    Y_UNIT_TEST(ShouldPinPendingRemoteLinkWithoutChangingLegacyStatus)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);
        TRemoteLinkMock remote(runtime, nodeIdx);
        std::unique_ptr<IEventHandle> description;
        bool holdDescription = true;
        remote.HoldRequest = [&](TAutoPtr<IEventHandle>& event)
        {
            if (holdDescription && event->GetTypeRewrite() ==
                                       TEvCellProxy::EvDescribeVolumeRequest)
            {
                description.reset(event.Release());
                return true;
            }
            return false;
        };

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            MakeRemoteLinkRequest());
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return description != nullptr; };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(description);
        const auto legacy = GetRemoteLinkStatus(service);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<ui32>(legacy.GetStatus()));
        UNIT_ASSERT(!legacy.GetLinkUUID());
        const auto pinned = GetRemoteLinkStatus(service, false, true);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_PREPARING),
            static_cast<ui32>(pinned.GetStatus()));
        UNIT_ASSERT(pinned.GetLinkUUID());

        // An old UUID must not cancel the new source-side pending creation.
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            MakeRemoteUnlinkRequest("stale-link-uuid"));
        const auto stale =
            service.RecvResponse<
                TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse>();
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, stale->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            pinned.GetLinkUUID(),
            GetRemoteLinkStatus(service, false, true).GetLinkUUID());

        holdDescription = false;
        runtime.SendAsync(description.release(), nodeIdx);
        const auto created =
            service.RecvResponse<
                TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>();
        UNIT_ASSERT_C(
            !HasError(created->GetError()),
            created->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(
            pinned.GetLinkUUID(),
            created->Record.GetLinkUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            pinned.GetLinkUUID(),
            GetRemoteLinkStatus(service, true).GetLinkUUID());
    }

    Y_UNIT_TEST(ShouldKeepPinnedRemoteDestroyFromRemovingRecreatedLink)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);
        TRemoteLinkMock remote(runtime, nodeIdx);
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            MakeRemoteLinkRequest());
        const auto original =
            service.RecvResponse<
                TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>();
        UNIT_ASSERT_C(
            !HasError(original->GetError()),
            original->GetErrorReason());
        const auto oldUUID = original->Record.GetLinkUUID();
        UNIT_ASSERT(oldUUID);

        TVector<std::unique_ptr<IEventHandle>> heldDestroy;
        TActorId propagationActor;
        TActorId destroyActor;
        ui32 guardedUnlinks = 0;
        bool holdDestroy = true;
        remote.Observe = [&](IEventHandle& event)
        {
            if (!destroyActor &&
                event.GetTypeRewrite() == TEvVolume::EvGetLinkStatusRequest &&
                event.Get<TEvVolume::TEvGetLinkStatusRequest>()
                    ->Record.GetIncludePendingCreation())
            {
                destroyActor = event.Sender;
            }
            if (event.GetTypeRewrite() ==
                    TEvVolume::EvUnlinkLeaderVolumeFromFollowerRequest &&
                event.Sender == destroyActor)
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    oldUUID,
                    event.Get<
                        TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest>()
                        ->Record.GetLinkUUID());
                ++guardedUnlinks;
            }
        };
        remote.HoldRequest = [&](TAutoPtr<IEventHandle>& event)
        {
            if (holdDestroy && event->GetTypeRewrite() ==
                                   TEvCellProxy::EvExecuteActionRequest)
            {
                const auto* request =
                    event->Get<TEvCellProxy::TEvExecuteActionRequest>();
                NProto::TUpdateLinkOnFollowerRequest payload;
                const auto status = google::protobuf::util::JsonStringToMessage(
                    request->Record.GetInput(),
                    &payload);
                UNIT_ASSERT_C(status.ok(), status.ToString());
                if (!propagationActor &&
                    payload.GetAction() == NProto::LINK_ACTION_DESTROY)
                {
                    propagationActor = event->Sender;
                }
                if (event->Sender == propagationActor) {
                    UNIT_ASSERT_VALUES_EQUAL(oldUUID, payload.GetLinkUUID());
                    heldDestroy.emplace_back(event.Release());
                    return true;
                }
            }
            return false;
        };
        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2", "cell-b");
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return !heldDestroy.empty(); };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(!heldDestroy.empty());
        UNIT_ASSERT_VALUES_EQUAL(0, guardedUnlinks);

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            MakeRemoteUnlinkRequest(oldUUID));
        const auto unlinked =
            service.RecvResponse<
                TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse>();
        UNIT_ASSERT_C(
            !HasError(unlinked->GetError()),
            unlinked->GetErrorReason());
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            MakeRemoteLinkRequest());
        const auto recreated =
            service.RecvResponse<
                TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>();
        UNIT_ASSERT_C(
            !HasError(recreated->GetError()),
            recreated->GetErrorReason());
        const auto newUUID = recreated->Record.GetLinkUUID();
        UNIT_ASSERT(newUUID && newUUID != oldUUID);

        holdDestroy = false;
        for (auto& request: heldDestroy) {
            runtime.SendAsync(request.release(), nodeIdx);
        }
        const auto destroyed = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, destroyed->GetStatus());
        UNIT_ASSERT(guardedUnlinks > 0);
        UNIT_ASSERT_VALUES_EQUAL(
            newUUID,
            GetRemoteLinkStatus(service).GetLinkUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            newUUID,
            GetRemoteLinkStatus(service, true).GetLinkUUID());
    }

    Y_UNIT_TEST(ShouldDestroyRemoteLinkWithPinnedUUID)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);
        TRemoteLinkMock remote(runtime, nodeIdx);
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            MakeRemoteLinkRequest());
        const auto created =
            service.RecvResponse<
                TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>();
        UNIT_ASSERT_C(
            !HasError(created->GetError()),
            created->GetErrorReason());

        service.DestroyVolumeLink("vol-1", "vol-2", "cell-b");
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<ui32>(GetRemoteLinkStatus(service).GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<ui32>(GetRemoteLinkStatus(service, true).GetStatus()));
    }

    Y_UNIT_TEST(ShouldLeaveRemoteOrphanUntouchedWhenSourceIsGone)
    {
        TTestEnv env(1, 1, 4);
        const ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-2", DefaultBlocksCount);
        auto request =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        request->Record.SetDiskId("vol-2");
        request->Record.SetLeaderDiskId("vol-1");
        request->Record.SetLeaderCellId("cell-a");
        request->Record.SetFollowerCellId("cell-b");
        request->Record.SetLinkUUID("orphan-link");
        request->Record.SetAction(NProto::LINK_ACTION_CREATE);
        service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
        const auto linked =
            service.RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>();
        UNIT_ASSERT_C(!HasError(linked->GetError()), linked->GetErrorReason());
        TRemoteLinkMock remote(runtime, nodeIdx);

        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2", "cell-b");
        const auto response = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_C(
            HasError(response->GetError()),
            response->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(0, remote.ActionCount);
        UNIT_ASSERT_VALUES_EQUAL(
            "orphan-link",
            GetRemoteLinkStatus(service, true).GetLinkUUID());
    }

    Y_UNIT_TEST(ShouldCreateAndDestroyLinkThroughCellProxyAndFollowerAction)
    {
        TTestEnv env(1, 2, 4);
        auto createCellNode = [&](const TString& cellId)
        {
            env.CreateSubDomain(cellId);
            NProto::TStorageServiceConfig config;
            config.SetSchemeShardDir("/local/" + cellId);
            return env.CreateBlockStoreNode(
                cellId,
                CreateTestStorageConfig(std::move(config)),
                CreateTestDiagnosticsConfig(),
                NYdbStats::CreateVolumesStatsUploaderStub(),
                CreateManuallyPreemptedVolumes(),
                cellId);
        };
        const auto nodeA = createCellNode("cell-a");
        const auto nodeB = createCellNode("cell-b");
        auto& runtime = env.GetRuntime();
        TServiceClient serviceA(runtime, nodeA);
        TServiceClient serviceB(runtime, nodeB);
        serviceA.CreateVolume("vol-1", DefaultBlocksCount);
        serviceB.CreateVolume("vol-2", DefaultBlocksCount);

        // Only networking is mocked. CellProxy, the target service action and
        // both volume handlers execute through their normal actor paths.
        serviceA.SendRequest(
            MakeCellProxyServiceId(),
            std::make_unique<TEvCellProxy::TEvInitializeRequest>(
                CreateCellManagerMock(runtime, {{"cell-b", nodeB}}),
                std::make_shared<NClient::TClientAppConfig>()));
        const auto initialized =
            serviceA.RecvResponse<TEvCellProxy::TEvInitializeResponse>();
        UNIT_ASSERT_C(
            !HasError(initialized->GetError()),
            FormatError(initialized->GetError()));

        // Public cross-cell creation remains disabled until the full copy
        // pipeline is ready; enter the source volume through its private API.
        serviceA.SendRequest(
            MakeVolumeProxyServiceId(),
            MakeRemoteLinkRequest());
        const auto created = serviceA.RecvResponse<
            TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>();
        UNIT_ASSERT_C(
            !HasError(created->GetError()),
            created->GetErrorReason());
        const auto uuid = created->Record.GetLinkUUID();
        UNIT_ASSERT(uuid);
        const auto source = GetRemoteLinkStatus(serviceA);
        const auto follower = GetRemoteLinkStatus(serviceB, true);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_PREPARING),
            static_cast<ui32>(source.GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_PREPARING),
            static_cast<ui32>(follower.GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(uuid, source.GetLinkUUID());
        UNIT_ASSERT_VALUES_EQUAL(uuid, follower.GetLinkUUID());
        UNIT_ASSERT_VALUES_EQUAL("cell-a", follower.GetLeaderCellId());
        UNIT_ASSERT_VALUES_EQUAL("cell-b", follower.GetFollowerCellId());

        serviceA.DestroyVolumeLink("vol-1", "vol-2", "cell-b");
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<ui32>(GetRemoteLinkStatus(serviceA).GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<ui32>(GetRemoteLinkStatus(serviceB, true).GetStatus()));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
