#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/blockstore/private/api/protos/checkpoints.pb.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceLinkVolumeTest)
{
    struct TCrossShardFixture
    {
        TTestEnv Env{1, 2, 4};
        ui32 SourceNode = 0;
        ui32 TargetNode = 0;
        std::unique_ptr<TServiceClient> Source;
        std::unique_ptr<TServiceClient> Target;

        explicit TCrossShardFixture(
            NCloud::NProto::EStorageMediaKind mediaKind =
                NProto::STORAGE_MEDIA_SSD,
            NCloud::NProto::EStorageMediaKind targetKind =
                NProto::STORAGE_MEDIA_DEFAULT, ui64 blocksCount = 1024 * 1024)
        {
            // Cleanup is scheduled to the symbolic service ID. The test
            // runtime otherwise whitelists only the registered actor IDs.
            Env.GetRuntime().EnableScheduleForActor(MakeStorageServiceId());
            NProto::TStorageServiceConfig proto;
            (*proto.MutableShardDirectories())["source"] = "/local/nbs";
            (*proto.MutableShardDirectories())["target"] = "/local/remote";
            SourceNode = SetupTestEnv(Env, proto);
            Env.CreateSubDomain("remote");
            proto.SetSchemeShardDir("/local/remote");
            TargetNode = Env.CreateBlockStoreNode(
                "remote", CreateTestStorageConfig(proto),
                CreateTestDiagnosticsConfig());
            Source =
                std::make_unique<TServiceClient>(Env.GetRuntime(), SourceNode);
            Target =
                std::make_unique<TServiceClient>(Env.GetRuntime(), TargetNode);
            Source->CreateVolume("disk", blocksCount, DefaultBlockSize, "", "",
                                 mediaKind);

            // Exercise destination creation and WaitReady through the source
            // node's existing NBS API, rather than bypassing the service.
            auto request = Source->CreateCreateVolumeRequest(
                "disk-copy",
                blocksCount,
                DefaultBlockSize,
                "",
                "",
                targetKind == NProto::STORAGE_MEDIA_DEFAULT ? mediaKind
                                                            : targetKind);
            request->Record.MutableHeaders()->SetShardId("target");
            Source->SendRequest(MakeStorageServiceId(), std::move(request));
            const auto response = Source->RecvCreateVolumeResponse();
            UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()),
                          response->GetErrorReason());
        }

        auto CreateLink()
        {
            auto request =
                Source->CreateCreateVolumeLinkRequest("disk", "disk-copy");
            request->Record.SetLeaderShardId("source");
            request->Record.SetFollowerShardId("target");
            Source->SendRequest(MakeStorageServiceId(), std::move(request));
            return Source->RecvCreateVolumeLinkResponse();
        }

        auto DestroyLink(const TString& leader = "disk")
        {
            auto request =
                Source->CreateDestroyVolumeLinkRequest(leader, "disk-copy");
            request->Record.SetLeaderShardId("source");
            request->Record.SetFollowerShardId("target");
            Source->SendRequest(MakeStorageServiceId(), std::move(request));
            return Source->RecvDestroyVolumeLinkResponse();
        }

        NProto::TGetLinkStatusResponse GetStatus(const TString& leader = "disk")
        {
            NProto::TGetLinkStatusRequest request;
            request.SetLeaderDiskId(leader);
            request.SetFollowerDiskId("disk-copy");
            request.SetLeaderShardId("source");
            request.SetFollowerShardId("target");
            TString json;
            UNIT_ASSERT(
                google::protobuf::util::MessageToJsonString(request, &json)
                    .ok());
            const auto response = Source->ExecuteAction("GetLinkStatus", json);
            NProto::TGetLinkStatusResponse status;
            UNIT_ASSERT_C(
                google::protobuf::util::JsonStringToMessage(
                    response->Record.GetOutput(), &status)
                    .ok(), response->Record.GetOutput());
            return status;
        }
    };

    Y_UNIT_TEST(ShouldCreateInspectAndCancelCrossShardSsdAndHddLinks)
    {
        for (const auto kind:
             {NProto::STORAGE_MEDIA_SSD, NProto::STORAGE_MEDIA_HDD})
        {
            TCrossShardFixture fixture(kind);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
                static_cast<int>(fixture.GetStatus().GetStatus()));
            const auto created = fixture.CreateLink();
            UNIT_ASSERT_C(SUCCEEDED(created->GetStatus()),
                          created->GetErrorReason());
            const auto repeated = fixture.CreateLink();
            UNIT_ASSERT_C(SUCCEEDED(repeated->GetStatus()),
                          repeated->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(NProto::LINK_STATUS_PREPARING),
                static_cast<int>(fixture.GetStatus().GetStatus()));

            const auto cancelled = fixture.DestroyLink();
            UNIT_ASSERT_C(SUCCEEDED(cancelled->GetStatus()),
                          cancelled->GetErrorReason());
            const auto repeatedCancel = fixture.DestroyLink();
            UNIT_ASSERT_C(SUCCEEDED(repeatedCancel->GetStatus()),
                          repeatedCancel->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
                static_cast<int>(fixture.GetStatus().GetStatus()));

            // Both volumes survive link cancellation in their own namespace.
            fixture.Source->DescribeVolume("disk", true);
            fixture.Target->DescribeVolume("disk-copy", true);
        }
    }

    Y_UNIT_TEST(ShouldRouteFollowerCleanupWhenCrossShardLeaderIsMissing)
    {
        TCrossShardFixture fixture;
        const auto response = fixture.DestroyLink("missing-leader");
        UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()),
                      response->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(fixture.GetStatus("missing-leader").GetStatus()));
        fixture.Source->DescribeVolume("disk", true);
        fixture.Target->DescribeVolume("disk-copy", true);
    }

    Y_UNIT_TEST(ShouldRejectInvalidCrossShardLinkAddresses)
    {
        TCrossShardFixture fixture;
        for (const auto& shards:
             {std::pair<TString, TString>{"unknown", "target"},
              {"source", "unknown"},
              {"", "target"},
              {"target", ""}})
        {
            auto create = fixture.Source->CreateCreateVolumeLinkRequest(
                "disk", "disk-copy");
            create->Record.SetLeaderShardId(shards.first);
            create->Record.SetFollowerShardId(shards.second);
            fixture.Source->SendRequest(MakeStorageServiceId(),
                                        std::move(create));
            const auto response =
                fixture.Source->RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());

            auto destroy = fixture.Source->CreateDestroyVolumeLinkRequest(
                "disk", "disk-copy");
            destroy->Record.SetLeaderShardId(shards.first);
            destroy->Record.SetFollowerShardId(shards.second);
            fixture.Source->SendRequest(MakeStorageServiceId(),
                                        std::move(destroy));
            const auto removed =
                fixture.Source->RecvDestroyVolumeLinkResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, removed->GetStatus());
        }
        auto emptyLeader =
            fixture.Source->CreateDestroyVolumeLinkRequest("", "disk-copy");
        emptyLeader->Record.SetLeaderShardId("source");
        emptyLeader->Record.SetFollowerShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(),
                                    std::move(emptyLeader));
        const auto response = fixture.Source->RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(fixture.GetStatus().GetStatus()));
    }

    Y_UNIT_TEST(ShouldAddressDescribeAndDestroyInDestinationShard)
    {
        TCrossShardFixture fixture;
        auto describe =
            fixture.Source->CreateDescribeVolumeRequest("disk-copy", true);
        describe->Record.MutableHeaders()->SetShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(),
                                    std::move(describe));
        const auto described = fixture.Source->RecvDescribeVolumeResponse();
        UNIT_ASSERT_C(SUCCEEDED(described->GetStatus()),
                      described->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL("disk-copy",
                                 described->Record.GetVolume().GetDiskId());

        auto destroy = fixture.Source->CreateDestroyVolumeRequest(
            "disk-copy", false, false, 0, true);
        destroy->Record.MutableHeaders()->SetShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(destroy));
        const auto removed = fixture.Source->RecvDestroyVolumeResponse();
        UNIT_ASSERT_C(SUCCEEDED(removed->GetStatus()),
                      removed->GetErrorReason());
        fixture.Target->SendDescribeVolumeRequest("disk-copy", true);
        const auto absent = fixture.Target->RecvDescribeVolumeResponse();
        UNIT_ASSERT(HasError(absent->GetError()));
        fixture.Source->DescribeVolume("disk", true);
    }

    void TestCrossShardCopyData(NCloud::NProto::EStorageMediaKind sourceKind,
                                NCloud::NProto::EStorageMediaKind targetKind,
                                bool online)
    {
        constexpr ui64 blocksCount = 8192;
        TCrossShardFixture fixture(sourceKind, targetKind, blocksCount);
        auto& runtime = fixture.Env.GetRuntime();
        auto mount = fixture.Source->MountVolume("disk");
        TString sourceSession = mount->Record.GetSessionId();
        fixture.Source->WriteBlocks("disk", TBlockRange64::WithLength(0, 16),
                                    sourceSession, 'a');
        fixture.Source->ZeroBlocks("disk", 1, sourceSession);
        fixture.Source->WriteBlocks(
            "disk", TBlockRange64::WithLength(blocksCount - 2, 2),
            sourceSession, 'c');
        if (!online) {
            fixture.Source->UnmountVolume("disk", sourceSession,
                                          NProto::SOURCE_CLIENT);
        }

        struct TProbe
        {
            bool HeldOnce = false;
            bool SourceDeleted = false;
            bool SourceDeleteAcknowledged = false;
            ui32 DestinationWrites = 0;
            std::unique_ptr<IEventHandle> HeldWrite;
        };

        auto probe = std::make_shared<TProbe>();
        runtime.SetObserverFunc(
            [probe, online](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest)
                {
                    const auto& request =
                        event->Get<TEvService::TEvWriteBlocksRequest>()->Record;
                    if (request.GetDiskId() == "disk-copy" &&
                        request.GetHeaders().GetShardId() == "target")
                    {
                        ++probe->DestinationWrites;
                        if (online && !probe->HeldOnce) {
                            probe->HeldOnce = true;
                            probe->HeldWrite.reset(event.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                if (event->GetTypeRewrite() ==
                    TEvService::EvDestroyVolumeRequest) {
                    const auto& request =
                        event->Get<TEvService::TEvDestroyVolumeRequest>()
                            ->Record;
                    if (request.GetDiskId() == "disk") {
                        UNIT_ASSERT_VALUES_EQUAL(
                            "source", request.GetHeaders().GetShardId());
                        UNIT_ASSERT(request.GetHeaders().GetExactDiskIdMatch());
                        probe->SourceDeleted = true;
                    }
                }
                if (event->GetTypeRewrite() ==
                        TEvService::EvDestroyVolumeResponse &&
                    probe->SourceDeleted)
                {
                    const auto* response =
                        event->Get<TEvService::TEvDestroyVolumeResponse>();
                    if (SUCCEEDED(response->GetStatus())) {
                        probe->SourceDeleteAcknowledged = true;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto created = fixture.CreateLink();
        UNIT_ASSERT_C(SUCCEEDED(created->GetStatus()),
                      created->GetErrorReason());

        if (online) {
            TDispatchOptions options;
            options.CustomFinalCondition = [probe]
            {
                return probe->HeldWrite != nullptr;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT_C(probe->HeldWrite,
                          fixture.GetStatus().ShortDebugString());

            // The first migration range is held while writes to a later range
            // are mirrored. The subsequent background copy must not restore
            // old data over the acknowledged foreground write or zero.
            mount = fixture.Source->MountVolume("disk");
            sourceSession = mount->Record.GetSessionId();
            fixture.Source->WriteBlocks(
                "disk", TBlockRange64::MakeOneBlock(blocksCount - 1),
                sourceSession, 'b');
            fixture.Source->ZeroBlocks("disk", blocksCount - 2, sourceSession);
            runtime.Send(probe->HeldWrite.release(), fixture.SourceNode);
        }

        NProto::TGetLinkStatusResponse status;
        bool transferred = false;
        for (ui32 attempt = 0; attempt != 500; ++attempt) {
            status = fixture.GetStatus();
            UNIT_ASSERT_C(status.GetStatus() != NProto::LINK_STATUS_ERROR,
                          status.ShortDebugString());
            if (status.GetStatus() ==
                    NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED ||
                status.GetStatus() == NProto::LINK_STATUS_COMPLETED)
            {
                transferred = true;
                break;
            }
            runtime.DispatchEvents({}, TDuration::MilliSeconds(20));
        }
        UNIT_ASSERT_C(transferred, status.ShortDebugString());
        UNIT_ASSERT(probe->DestinationWrites > 0);
        if (online) {
            fixture.Source->UnmountVolume("disk", sourceSession,
                                          NProto::SOURCE_CLIENT);
        }

        const auto targetMount = fixture.Target->MountVolume("disk-copy");
        auto targetSession = targetMount->Record.GetSessionId();
        auto checkBlock = [&](ui32 index, char expected)
        {
            const auto response =
                fixture.Target->ReadBlocks("disk-copy", index, targetSession);
            UNIT_ASSERT_VALUES_EQUAL(
                1, response->Record.GetBlocks().BuffersSize());
            auto data = response->Record.GetBlocks().GetBuffers(0);
            if (data.empty()) {
                data = TString(DefaultBlockSize, 0);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(
                TString(DefaultBlockSize, expected), data,
                TStringBuilder() << "block " << index << ", online=" << online);
        };
        for (ui32 index = 0; index != 16; ++index) {
            checkBlock(index, index == 1 ? 0 : 'a');
        }
        checkBlock(32, 0);
        checkBlock(blocksCount - 2, online ? 0 : 'c');
        checkBlock(blocksCount - 1, online ? 'b' : 'c');

        const auto destination =
            fixture.Target->DescribeVolume("disk-copy", true);
        UNIT_ASSERT_VALUES_EQUAL(
            blocksCount, destination->Record.GetVolume().GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize, destination->Record.GetVolume().GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(targetKind),
            static_cast<int>(
                destination->Record.GetVolume().GetStorageMediaKind()));

        TDispatchOptions cleanup;
        cleanup.CustomFinalCondition = [probe]
        {
            return probe->SourceDeleteAcknowledged;
        };
        runtime.DispatchEvents(cleanup, TDuration::Minutes(3));
        UNIT_ASSERT_C(probe->SourceDeleteAcknowledged,
                      fixture.GetStatus().ShortDebugString());

        bool completed = false;
        for (ui32 attempt = 0; attempt != 3000; ++attempt) {
            // A volume reboot can orphan an older actor's delete response.
            // Let the live actor's cleanup retry and principal transaction run
            // while advancing time in small steps so node leases stay alive.
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            status = fixture.GetStatus();
            if (status.GetStatus() == NProto::LINK_STATUS_COMPLETED) {
                completed = true;
                break;
            }
        }
        UNIT_ASSERT_C(completed, status.ShortDebugString());
        UNIT_ASSERT(probe->SourceDeleted);
        fixture.Source->SendDescribeVolumeRequest("disk", true);
        const auto absent = fixture.Source->RecvDescribeVolumeResponse();
        UNIT_ASSERT(HasError(absent->GetError()));
        // The low-level test client does not implement the SDK's automatic
        // remount on E_BS_INVALID_SESSION after a tablet restart.
        targetSession =
            fixture.Target->MountVolume("disk-copy")->Record.GetSessionId();
        checkBlock(blocksCount - 1, online ? 'b' : 'c');
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ShouldCompleteCrossShardCopyOfDetachedSsdAndHddDisks)
    {
        for (const auto source:
             {NProto::STORAGE_MEDIA_SSD, NProto::STORAGE_MEDIA_HDD})
        {
            for (const auto target:
                 {NProto::STORAGE_MEDIA_SSD, NProto::STORAGE_MEDIA_HDD})
            {
                TestCrossShardCopyData(source, target, false);
            }
        }
    }

    Y_UNIT_TEST(ShouldMirrorWritesAndZerosDuringCrossShardSsdAndHddCopy)
    {
        for (const auto source:
             {NProto::STORAGE_MEDIA_SSD, NProto::STORAGE_MEDIA_HDD})
        {
            for (const auto target:
                 {NProto::STORAGE_MEDIA_SSD, NProto::STORAGE_MEDIA_HDD})
            {
                TestCrossShardCopyData(source, target, true);
            }
        }
    }

    Y_UNIT_TEST(ShouldKeepLocalNonreplicatedCreationWithTrailingSlash)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetSchemeShardDir("/local/nbs/");
        (*proto.MutableShardDirectories())["local"] = "/local/nbs";
        TTestEnv env;
        const auto node = SetupTestEnv(env, proto);
        TServiceClient service(env.GetRuntime(), node);
        for (const TString shard: {"", "local"}) {
            auto request = service.CreateCreateVolumeRequest(
                "nonrepl-" + shard, 93_GB / DefaultBlockSize, DefaultBlockSize,
                "", "", NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
            request->Record.MutableHeaders()->SetShardId(shard);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            const auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()),
                          response->GetErrorReason());
        }
    }

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
            UNIT_ASSERT_C(E_ARGUMENT, response->GetError().GetCode());
        }
        {
            service.SendCreateVolumeLinkRequest("vol-1", "unknown");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(E_ARGUMENT, response->GetError().GetCode());
        }
        {
            service.SendCreateVolumeLinkRequest("unknown", "vol-1");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(E_ARGUMENT, response->GetError().GetCode());
        }
        {
            service.SendCreateVolumeLinkRequest("vol-2", "vol-1");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(E_ARGUMENT, response->GetError().GetCode());
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
}

}   // namespace NCloud::NBlockStore::NStorage
