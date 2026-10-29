#include "service_ut.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/storage/api/cell_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/cell_manager_mock.h>
#include <cloud/blockstore/libs/storage/volume/actors/propagate_to_follower.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>
#include <cloud/blockstore/private/api/protos/checkpoints.pb.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <util/generic/size_literals.h>
#include <util/string/ascii.h>

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

ui32 CreateCellNode(TTestEnv& env, const TString& cellId)
{
    env.CreateSubDomain(cellId);
    NProto::TStorageServiceConfig config;
    config.SetSchemeShardDir("/local/" + cellId);
    auto* bandwidth = config.AddLinkedDiskFillBandwidth();
    bandwidth->SetReadBandwidth(1024);
    bandwidth->SetWriteBandwidth(1024);
    bandwidth->SetReadIoDepth(1);
    bandwidth->SetWriteIoDepth(1);
    return env.CreateBlockStoreNode(
        cellId, CreateTestStorageConfig(std::move(config)),
        CreateTestDiagnosticsConfig(),
        NYdbStats::CreateVolumesStatsUploaderStub(),
        CreateManuallyPreemptedVolumes(), cellId);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceLinkVolumeTest)
{
    Y_UNIT_TEST(ShouldFailOnInvalidArgumentVolume)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount * 2);

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

    Y_UNIT_TEST(ShouldDescribeVolumeInAnotherCell)
    {
        TTestEnv env(1, 2, 4);
        const auto nodeA = CreateCellNode(env, "cell-a");
        const auto nodeB = CreateCellNode(env, "cell-b");
        auto& runtime = env.GetRuntime();
        TServiceClient serviceA(runtime, nodeA);
        TServiceClient serviceB(runtime, nodeB);
        serviceB.CreateVolume("follower", 2048, DefaultBlockSize, "", "",
                              NProto::STORAGE_MEDIA_SSD);
        serviceA.SendRequest(MakeCellProxyServiceId(),
            std::make_unique<TEvCellProxy::TEvInitializeRequest>(
                CreateCellManagerMock(runtime, {{"cell-b", nodeB}}),
                std::make_shared<NClient::TClientAppConfig>()));
        UNIT_ASSERT_VALUES_EQUAL(S_OK,
            serviceA.RecvResponse<TEvCellProxy::TEvInitializeResponse>()->GetStatus());

        serviceA.SendRequest(MakeCellProxyServiceId(),
            std::make_unique<TEvCellProxy::TEvDescribeVolumeRequest>("cell-b", "follower"));
        const auto response = serviceA.RecvResponse<TEvCellProxy::TEvDescribeVolumeResponse>();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, response->GetStatus(), FormatError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL("follower", response->Volume.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(2048, response->Volume.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, response->Volume.GetBlockSize());

        // A failed remote lookup must not be retried through the local SSProxy.
        serviceA.CreateVolume("local-only");
        serviceA.SendRequest(MakeCellProxyServiceId(),
            std::make_unique<TEvCellProxy::TEvDescribeVolumeRequest>("cell-b", "local-only"));
        const auto missing = serviceA.RecvResponse<TEvCellProxy::TEvDescribeVolumeResponse>();
        UNIT_ASSERT(IsNotFoundSchemeShardError(missing->GetError()));
    }

    Y_UNIT_TEST(ShouldPropagateFollowerLinkToAnotherCell)
    {
        TTestEnv env(1, 2, 4);
        auto& runtime = env.GetRuntime();
        const auto nodeA = CreateCellNode(env, "cell-a");
        const auto nodeB = CreateCellNode(env, "cell-b");
        TServiceClient serviceA(runtime, nodeA);
        TServiceClient serviceB(runtime, nodeB);
        serviceA.SendRequest(MakeCellProxyServiceId(),
            std::make_unique<TEvCellProxy::TEvInitializeRequest>(
                CreateCellManagerMock(runtime, {{"cell-b", nodeB}}),
                std::make_shared<NClient::TClientAppConfig>()));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            serviceA.RecvResponse<TEvCellProxy::TEvInitializeResponse>()
                ->GetStatus());
        serviceB.CreateVolume("follower");

        TActorId volumeActor;
        const auto context = MakeIntrusive<TCallContext>();
        const TLeaderFollowerLink link{.LinkUUID = "link-uuid",
                                       .LeaderDiskId = "leader",
                                       .LeaderCellId = "cell-a",
                                       .FollowerDiskId = "follower",
                                       .FollowerCellId = "cell-b"};
        NProto::ELinkAction action = NProto::LINK_ACTION_CREATE;
        NProto::TError responseError;
        ui32 delivered = 0;
        bool retry = false;
        runtime.SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvVolume::EvWaitReadyResponse) {
                    volumeActor = event->Sender;
                }
                if (event->GetTypeRewrite() ==
                    TEvService::EvExecuteActionRequest) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        runtime.GetNodeId(nodeB),
                        event->GetRecipientRewrite().NodeId());
                    const auto& record =
                        event->Get<TEvService::TEvExecuteActionRequest>()
                            ->Record;
                    UNIT_ASSERT_VALUES_EQUAL("updatelinkonfollower",
                                             record.GetAction());
                    NProto::TUpdateLinkOnFollowerRequest request;
                    UNIT_ASSERT(
                        google::protobuf::util::JsonStringToMessage(
                            record.GetInput(), &request)
                            .ok());
                    UNIT_ASSERT(!request.HasHeaders());
                }
                if (event->GetTypeRewrite() !=
                    TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    return false;
                }
                const auto* msg =
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                UNIT_ASSERT(msg->CallContext == context);
                if (event->Recipient == MakeVolumeProxyServiceId()) {
                    return false;
                }
                // Intercept at the volume so the test exercises the complete
                // route without starting the follower's link state machine.
                UNIT_ASSERT(event->GetRecipientRewrite() == volumeActor);
                UNIT_ASSERT_VALUES_EQUAL(runtime.GetNodeId(nodeB),
                                         volumeActor.NodeId());
                const auto& record = msg->Record;
                UNIT_ASSERT_VALUES_EQUAL(link.LinkUUID, record.GetLinkUUID());
                UNIT_ASSERT_VALUES_EQUAL(link.LeaderDiskId,
                                         record.GetLeaderDiskId());
                UNIT_ASSERT_VALUES_EQUAL(link.LeaderCellId,
                                         record.GetLeaderCellId());
                UNIT_ASSERT_VALUES_EQUAL(link.FollowerDiskId,
                                         record.GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL(link.FollowerCellId,
                                         record.GetFollowerCellId());
                UNIT_ASSERT_EQUAL(action, record.GetAction());
                UNIT_ASSERT(record.GetHeaders().GetExactDiskIdMatch());
                ++delivered;
                auto response = std::make_unique<
                    TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                    retry && delivered == 1 ? MakeError(E_REJECTED, "retry")
                                            : responseError);
                runtime.Send(new IEventHandle(event->Sender, volumeActor,
                                              response.release(), 0,
                                              event->Cookie));
                return true;
            });
        serviceB.WaitForVolume("follower");
        UNIT_ASSERT(volumeActor);

        using EReason = TPropagateLinkToFollowerActor::EReason;

        const struct
        {
            EReason Reason;
            NProto::ELinkAction Action;
            ui32 Status;
            bool Retry;
        } cases[] = {
            {EReason::Creation, NProto::LINK_ACTION_CREATE, S_OK, false},
            {EReason::DataTransferred, NProto::LINK_ACTION_COMPLETED, S_FALSE,
             false},
            {EReason::Destruction, NProto::LINK_ACTION_DESTROY, S_ALREADY,
             false},
            {EReason::Creation, NProto::LINK_ACTION_CREATE, E_ARGUMENT, false},
            {EReason::Creation, NProto::LINK_ACTION_CREATE, S_OK, true},
        };

        for (const auto& test: cases) {
            action = test.Action;
            responseError = MakeError(test.Status, "follower response");
            responseError.SetFlags(123);
            delivered = 0;
            retry = test.Retry;
            const auto actor = runtime.Register(
                new TPropagateLinkToFollowerActor(
                    "test", CreateRequestInfo(serviceA.GetSender(), 0, context),
                    link, test.Reason), nodeA);
            runtime.EnableScheduleForActor(actor);
            NProto::TError error;
            switch (test.Reason) {
                case EReason::Creation:
                    error =
                        serviceA
                            .RecvResponse<
                                TEvVolumePrivate::TEvLinkOnFollowerCreated>()
                            ->GetError();
                    break;
                case EReason::DataTransferred:
                    error = serviceA
                                .RecvResponse<
                                    TEvVolumePrivate::
                                        TEvLinkOnFollowerDataTransferred>()
                                ->GetError();
                    break;
                case EReason::Destruction:
                    error =
                        serviceA
                            .RecvResponse<
                                TEvVolumePrivate::TEvLinkOnFollowerDestroyed>()
                            ->GetError();
                    break;
            }
            UNIT_ASSERT_VALUES_EQUAL(responseError.SerializeAsString(),
                                     error.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(test.Retry ? 2 : 1, delivered);
        }
    }

    Y_UNIT_TEST(ShouldCopyDataBetweenCellsEndToEnd)
    {
        constexpr ui64 volumeSize = 32_MB;
        constexpr ui32 blocksCount = volumeSize / DefaultBlockSize;
        const TString leaderDiskId = "leader";
        const TString followerDiskId = "follower";

        TTestEnv env(1, 2, 4);
        auto& runtime = env.GetRuntime();

        const auto nodeA = CreateCellNode(env, "cell-a");
        const auto nodeB = CreateCellNode(env, "cell-b");
        TServiceClient serviceA(runtime, nodeA);
        TServiceClient serviceB(runtime, nodeB);
        for (auto* service: {&serviceA, &serviceB}) {
            service->SendRequest(MakeCellProxyServiceId(),
                std::make_unique<TEvCellProxy::TEvInitializeRequest>(
                    CreateCellManagerMock(runtime, {{"cell-a", nodeA}, {"cell-b", nodeB}}),
                    std::make_shared<NClient::TClientAppConfig>()));
            const auto response = service->RecvResponse<TEvCellProxy::TEvInitializeResponse>();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
        serviceA.CreateVolume(leaderDiskId, blocksCount, DefaultBlockSize, "",
                              "", NProto::STORAGE_MEDIA_SSD);
        serviceB.CreateVolume(followerDiskId, blocksCount, DefaultBlockSize, "",
                              "", NProto::STORAGE_MEDIA_SSD);

        auto sessionA =
            serviceA.MountVolume(leaderDiskId)->Record.GetSessionId();
        TVector<char> expected(blocksCount, 0);
        serviceA.WriteBlocks(leaderDiskId, TBlockRange64::WithLength(0, 4),
                             sessionA, 'a');
        std::fill_n(expected.begin(), 4, 'a');
        serviceA.ZeroBlocks(leaderDiskId, 2, sessionA);
        expected[2] = 0;
        serviceA.WriteBlocks(leaderDiskId,
                             TBlockRange64::MakeOneBlock(blocksCount / 2),
                             sessionA, 'b');
        expected[blocksCount / 2] = 'b';
        serviceA.WriteBlocks(leaderDiskId,
                             TBlockRange64::WithLength(blocksCount - 2, 2),
                             sessionA, 'b');
        expected[blocksCount - 2] = expected[blocksCount - 1] = 'b';

        auto sessionB =
            serviceB.MountVolume(followerDiskId)->Record.GetSessionId();
        // Copying zero ranges must also overwrite existing follower data.
        serviceB.WriteBlocks(followerDiskId,
                             TBlockRange64::MakeOneBlock(blocksCount / 4),
                             sessionB, 'x');
        serviceB.UnmountVolume(followerDiskId, sessionB);

        TAutoPtr<IEventHandle> delayedRange;
        bool rangeDelayed = false;
        bool migrationFinished = false;
        ui32 followerActions = 0;
        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvService::EvExecuteActionRequest &&
                    event->GetRecipientRewrite().NodeId() ==
                        runtime.GetNodeId(nodeB))
                {
                    const auto& record =
                        event->Get<TEvService::TEvExecuteActionRequest>()
                            ->Record;
                    if (AsciiEqualsIgnoreCase(record.GetAction(),
                                              "updatelinkonfollower"))
                    {
                        NProto::TUpdateLinkOnFollowerRequest request;
                        UNIT_ASSERT(
                            google::protobuf::util::JsonStringToMessage(
                                record.GetInput(), &request)
                                .ok());
                        UNIT_ASSERT_VALUES_EQUAL(leaderDiskId,
                                                 request.GetLeaderDiskId());
                        UNIT_ASSERT_VALUES_EQUAL(followerDiskId,
                                                 request.GetDiskId());
                        UNIT_ASSERT_VALUES_EQUAL("cell-a",
                                                 request.GetLeaderCellId());
                        UNIT_ASSERT_VALUES_EQUAL("cell-b",
                                                 request.GetFollowerCellId());
                        followerActions |= 1u << request.GetAction();
                    }
                }
                if (event->GetTypeRewrite() ==
                        TEvNonreplPartitionPrivate::EvRangeMigrated &&
                    event->GetRecipientRewrite().NodeId() ==
                        runtime.GetNodeId(nodeA) &&
                    !rangeDelayed)
                {
                    const auto* msg = event->Get<
                        TEvNonreplPartitionPrivate::TEvRangeMigrated>();
                    UNIT_ASSERT_C(!HasError(msg->GetError()),
                                  FormatError(msg->GetError()));
                    UNIT_ASSERT(msg->Range.End < blocksCount - 2);
                    rangeDelayed = true;
                    delayedRange = event.Release();
                    return true;
                }
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvUpdateFollowerStateResponse)
                {
                    const auto& follower =
                        event
                            ->Get<TEvVolumePrivate::
                                      TEvUpdateFollowerStateResponse>()
                            ->Follower;
                    if (follower.Link.LeaderDiskId == leaderDiskId &&
                        follower.Link.FollowerCellId == "cell-b")
                    {
                        migrationFinished =
                            follower.State ==
                            TFollowerDiskInfo::EState::LeadershipTransferred;
                    }
                }
                return false;
            });

        // Readiness test: exercise production routing without forwarding or
        // answering inter-cell requests in the test.
        serviceA.CreateVolumeLink(leaderDiskId, followerDiskId, "cell-b");

        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return rangeDelayed;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT_C(delayedRange, "Background copying did not start");
        const auto preparing =
            GetLinkStatus(serviceA, leaderDiskId, followerDiskId, "cell-b");
        UNIT_ASSERT_EQUAL_C(NProto::LINK_STATUS_PREPARING,
                            preparing.GetStatus(),
                            preparing.ShortDebugString());
        UNIT_ASSERT(preparing.GetLinkUUID());
        UNIT_ASSERT_VALUES_EQUAL("cell-a", preparing.GetLeaderCellId());
        UNIT_ASSERT_VALUES_EQUAL("cell-b", preparing.GetFollowerCellId());

        // The leader's partition restarts when copying begins. Remount before
        // changing ranges that have not yet been copied.
        sessionA = serviceA.MountVolume(leaderDiskId)->Record.GetSessionId();
        serviceA.WriteBlocks(leaderDiskId,
                             TBlockRange64::MakeOneBlock(blocksCount - 2),
                             sessionA, 'c');
        expected[blocksCount - 2] = 'c';
        serviceA.ZeroBlocks(leaderDiskId, blocksCount - 1, sessionA);
        expected[blocksCount - 1] = 0;
        runtime.Send(delayedRange.Release(), nodeA);

        options.CustomFinalCondition = [&]
        {
            return migrationFinished;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(30));
        UNIT_ASSERT_C(migrationFinished, "Cross-cell copying did not finish");
        const auto transferred =
            GetLinkStatus(serviceA, leaderDiskId, followerDiskId, "cell-b");
        UNIT_ASSERT_EQUAL_C(NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED,
                            transferred.GetStatus(),
                            transferred.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(volumeSize, transferred.GetMigratedBytes());
        UNIT_ASSERT_VALUES_EQUAL(preparing.GetLinkUUID(),
                                 transferred.GetLinkUUID());

        serviceA.DestroyVolumeLink(leaderDiskId, followerDiskId, "cell-b");
        const auto unlinked =
            GetLinkStatus(serviceA, leaderDiskId, followerDiskId, "cell-b");
        UNIT_ASSERT_EQUAL_C(NProto::LINK_STATUS_NOT_FOUND, unlinked.GetStatus(),
                            unlinked.ShortDebugString());
        const ui32 expectedActions = (1u << NProto::LINK_ACTION_CREATE) |
                                     (1u << NProto::LINK_ACTION_COMPLETED) |
                                     (1u << NProto::LINK_ACTION_DESTROY);
        // DestroyVolumeLink replies before notifying the follower.
        options.CustomFinalCondition = [&]
        {
            return followerActions == expectedActions;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(expectedActions, followerActions);

        sessionB = serviceB.MountVolume(followerDiskId)->Record.GetSessionId();
        constexpr ui32 readSize = 4096;
        for (ui32 start = 0; start < blocksCount; start += readSize) {
            auto request = serviceB.CreateReadBlocksRequest(
                followerDiskId, start, sessionB);
            request->Record.SetBlocksCount(readSize);
            serviceB.SendRequest(MakeStorageServiceId(), std::move(request));
            const auto response = serviceB.RecvReadBlocksResponse();
            UNIT_ASSERT_C(!HasError(response->GetError()),
                          response->GetErrorReason());
            const auto& buffers = response->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(readSize, buffers.size());
            for (ui32 i = 0; i < readSize; ++i) {
                const auto block =
                    buffers[i].empty() ? GetBlockContent(0) : buffers[i];
                UNIT_ASSERT_VALUES_EQUAL_C(
                    GetBlockContent(expected[start + i]), block,
                    "Block " << start + i);
            }
        }
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

    // Y_UNIT_TEST(ShouldNotRejectSimilarilyNamedDisksFromDifferenCells)
    // {
    //     TTestEnv env(1, 2, 4);
    //     ui32 nodeIdx = SetupTestEnvWithCellId(env, "cell-a");
    //
    //     auto& runtime = env.GetRuntime();
    //     TServiceClient service(runtime, nodeIdx);
    //     service.CreateVolume("vol-1", DefaultBlocksCount);
    //
    //     ui32 nodeIdxB = SetupTestEnvWithCellId(env, "cell-b");
    //
    //     auto& runtimeB = env.GetRuntime();
    //     TServiceClient serviceB(runtimeB, nodeIdxB);
    //     service.CreateVolume("vol-1", DefaultBlocksCount);
    //
    //     {
    //         service.SendCreateVolumeLinkRequest("vol-1", "vol-1", "cell-b");
    //         auto response = service.RecvCreateVolumeLinkResponse();
    //         // Expect E_NOT_IMPLEMENTED instead of E_ARGUMENT
    //         UNIT_ASSERT_VALUES_EQUAL_C(
    //             E_NOT_IMPLEMENTED,
    //             response->GetError().GetCode(),
    //             FormatError(response->GetError()));
    //     }
    // }
}

}   // namespace NCloud::NBlockStore::NStorage
