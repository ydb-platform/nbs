#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/model/composite_id.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/testlib/test_env.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

using namespace NTestVolume;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const ui64 VolumeBlockCount =
        DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize;

    std::unique_ptr<TTestActorRuntime> Runtime;
    TMap<TString, TVolumeClient*> VolumeClients;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        NProto::TStorageServiceConfig config;
        {
            NProto::TLinkedDiskFillBandwidth defaultBandwidth;
            defaultBandwidth.SetReadBandwidth(100);
            defaultBandwidth.SetReadIoDepth(2);
            defaultBandwidth.SetWriteBandwidth(100);
            defaultBandwidth.SetWriteIoDepth(2);
            config.MutableLinkedDiskFillBandwidth()->Add(
                std::move(defaultBandwidth));
        }
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetMaxShadowDiskFillIoDepth(2);

        auto state = MakeIntrusive<TDiskRegistryState>();
        Runtime = PrepareTestActorRuntime(
            std::move(config),
            std::move(state),
            {},
            {},
            {},
            false   // debugActorRegistration
        );
        for (ui32 i = TBlockStoreComponents::START;
             i < TBlockStoreComponents::END;
             ++i)
        {
            Runtime->SetLogPriority(i, NLog::PRI_DEBUG);
        }

        Runtime->RegisterService(
            MakeVolumeProxyServiceId(),
            Runtime->AllocateEdgeActor(),
            0);

        auto volumeProxyEventFilter =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            if (event->Recipient == MakeVolumeProxyServiceId()) {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    auto* msg =
                        event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                    ForwardToVolume(GetDiskId(*msg), event);
                } else if (
                    event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest)
                {
                    auto* msg = event->Get<TEvService::TEvWriteBlocksRequest>();
                    ForwardToVolume(GetDiskId(*msg), event);
                } else if (
                    event->GetTypeRewrite() == TEvService::EvZeroBlocksRequest)
                {
                    auto* msg = event->Get<TEvService::TEvZeroBlocksRequest>();
                    ForwardToVolume(GetDiskId(*msg), event);
                } else {
                    UNIT_ASSERT_C(
                        false,
                        TStringBuilder() << "Unexpected event type: "
                                         << event->GetTypeName());
                    return false;
                }

                return true;
            }

            return false;
        };

        Runtime->SetEventFilter(volumeProxyEventFilter);
    }

    void ForwardToVolume(const TString& diskId, TAutoPtr<IEventHandle>& event)
    {
        if (auto* volumeClient = VolumeClients.FindPtr(diskId)) {
            (*volumeClient)->ForwardToPipe(event);
        } else {
            UNIT_ASSERT_C(
                false,
                TStringBuilder() << "Unknown disk id: " << diskId);
        }
    }

    void CheckVolumesDataMatch(
        TVolumeClient& volume1,
        TVolumeClient& volume2,
        const NProto::TVolumeClientInfo& clientInfo1,
        const NProto::TVolumeClientInfo& clientInfo2) const
    {
        for (ui64 position = 0; position < VolumeBlockCount; position += 1024) {
            auto range = TBlockRange64::MakeClosedIntervalWithLimit(
                position,
                position + 1024,
                VolumeBlockCount - 1);

            auto response1 =
                volume1.ReadBlocks(range, clientInfo1.GetClientId());
            auto response2 =
                volume2.ReadBlocks(range, clientInfo2.GetClientId());
            const auto& bufs1 = response1->Record.GetBlocks().GetBuffers();
            const auto& bufs2 = response2->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(bufs1.size(), bufs2.size());

            for (int i = 0; i < bufs1.size(); ++i) {
                auto block1 = bufs1[i];
                auto block2 = bufs2[i];

                if (block1.empty()) {
                    block1 = TString(DefaultBlockSize, 0);
                }
                if (block2.empty()) {
                    block2 = TString(DefaultBlockSize, 0);
                }

                UNIT_ASSERT_VALUES_EQUAL_C(
                    block1.size(),
                    block2.size(),
                    TStringBuilder() << "Block #" << position + i);

                for (size_t j = 0; j < block1.size(); ++j) {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        ui32(block1[j]),
                        ui32(block2[j]),
                        TStringBuilder()
                            << "Block #" << position + i << " byte #" << j);
                }
            }
        }
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TLinkedVolumeTest)
{
    Y_UNIT_TEST(ShouldManageFollowerLink)
    {
        const auto volumeBlockCount =
            DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize;

        auto runtime = PrepareTestActorRuntime();
        runtime->RegisterService(
            MakeVolumeProxyServiceId(),
            runtime->AllocateEdgeActor(),
            0);

        TVolumeClient volume1(*runtime, 0, TestVolumeTablets[0]);
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD,
            7 * 1024,   // block count per partition
            "vol1"));
        volume1.WaitReady();

        TVolumeClient volume2(*runtime, 0, TestVolumeTablets[1]);
        volume2.CreateVolume(volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            volumeBlockCount,   // block count per partition
            "vol2"));
        volume2.WaitReady();

        auto forwardRequest = [&](TAutoPtr<IEventHandle>& event, TString diskId)
        {
            if (diskId == "vol1") {
                volume1.ForwardToPipe(event);
            } else if (diskId == "vol2") {
                volume2.ForwardToPipe(event);
            } else {
                UNIT_ASSERT_C(
                    false,
                    TStringBuilder() << "Unknown disk id: " << diskId);
            }
        };

        auto volumeProxyRequestHandler =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            if (event->Recipient == MakeVolumeProxyServiceId()) {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    auto* msg =
                        event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                    forwardRequest(event, msg->Record.GetDiskId());
                }

                return true;
            }
            return false;
        };

        runtime->SetEventFilter(volumeProxyRequestHandler);

        TLeaderFollowerLink link{
            .LinkUUID = "",
            .LeaderDiskId = "vol1",
            .LeaderShardId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerShardId = "su2"};
        {
            // Create link
            auto response = volume1.LinkLeaderVolumeToFollower(link);
            link.LinkUUID = response->Record.GetLinkUUID();

            // Reboot tablet
            volume1.RebootTablet();
            volume1.WaitReady();

            // Should get S_ALREADY since link persisted in local volume
            // database.
            volume1.SendLinkLeaderVolumeToFollowerRequest(link);
            response = volume1.RecvLinkLeaderVolumeToFollowerResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, response->GetStatus());
        }

        {   // Update link state
            using EState = TFollowerDiskInfo::EState;

            auto follower = TFollowerDiskInfo{
                .Link = link,
                .CreatedAt = TInstant::Now(),
                .State = EState::Preparing,
                .MigratedBytes = 1_MB};

            auto response = volume1.UpdateFollowerState(follower);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                EState::Preparing,
                response->Follower.State);
            UNIT_ASSERT_VALUES_EQUAL(1_MB, response->Follower.MigratedBytes);

            follower.MigratedBytes = 2_MB;
            response = volume1.UpdateFollowerState(follower);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                EState::Preparing,
                response->Follower.State);
            UNIT_ASSERT_VALUES_EQUAL(2_MB, response->Follower.MigratedBytes);

            follower.State = EState::DataReady;
            follower.MigratedBytes = 2_MB;
            response = volume1.UpdateFollowerState(follower);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                EState::DataReady,
                response->Follower.State);
            UNIT_ASSERT_VALUES_EQUAL(2_MB, response->Follower.MigratedBytes);

            follower.State = EState::Error;
            follower.MigratedBytes = 2_MB;
            response = volume1.UpdateFollowerState(follower);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(EState::Error, response->Follower.State);
            UNIT_ASSERT_VALUES_EQUAL(2_MB, response->Follower.MigratedBytes);
        }

        {
            // Destroy link
            volume1.UnlinkLeaderVolumeFromFollower(link);

            // Reboot tablet
            volume1.RebootTablet();
            volume1.WaitReady();

            // Should get S_ALREADY since link persisted in local volume
            // database.
            volume1.SendUnlinkLeaderVolumeFromFollowerRequest(link);
            auto response =
                volume1.RecvUnlinkLeaderVolumeFromFollowerResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldRetryPropagateFollowerLink)
    {
        auto runtime = PrepareTestActorRuntime();
        runtime->RegisterService(
            MakeVolumeProxyServiceId(),
            runtime->AllocateEdgeActor(),
            0);

        TVolumeClient volume1(*runtime, 0, TestVolumeTablets[0]);
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD,
            7 * 1024,   // block count per partition
            "vol1"));
        volume1.WaitReady();

        TVolumeClient volume2(*runtime, 0, TestVolumeTablets[1]);
        volume2.CreateVolume(volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD,
            7 * 1024,   // block count per partition
            "vol2"));
        volume2.WaitReady();

        auto forwardRequest = [&](TAutoPtr<IEventHandle>& event, TString diskId)
        {
            if (diskId == "vol1") {
                volume1.ForwardToPipe(event);
            } else if (diskId == "vol2") {
                volume2.ForwardToPipe(event);
            } else {
                UNIT_ASSERT_C(
                    false,
                    TStringBuilder() << "Unknown disk id: " << diskId);
            }
        };

        size_t updateLinkOnFollowerResponseCount = 0;
        auto volumeProxyRequestHandler =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            if (event->Recipient == MakeVolumeProxyServiceId()) {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    auto* msg =
                        event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                    forwardRequest(event, msg->Record.GetDiskId());
                }
                return true;
            }

            if (event->GetTypeRewrite() ==
                TEvVolume::EvUpdateLinkOnFollowerResponse)
            {
                // Drop first 3 responses from follower.
                if (updateLinkOnFollowerResponseCount++ < 2) {
                    return true;
                }
            }
            return false;
        };

        runtime->SetEventFilter(volumeProxyRequestHandler);

        TLeaderFollowerLink link{
            .LinkUUID = "",
            .LeaderDiskId = "vol1",
            .LeaderShardId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerShardId = "su2"};

        // Create link
        volume1.LinkLeaderVolumeToFollower(link);

        // Should get 4 responses from follower.
        UNIT_ASSERT_VALUES_EQUAL(3, updateLinkOnFollowerResponseCount);
    }

    Y_UNIT_TEST(ShouldFailCreateLinkPropagateFollowerLink)
    {
        auto runtime = PrepareTestActorRuntime();
        runtime->RegisterService(
            MakeVolumeProxyServiceId(),
            runtime->AllocateEdgeActor(),
            0);

        TVolumeClient volume1(*runtime, 0, TestVolumeTablets[0]);
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD,
            7 * 1024,   // block count per partition
            "vol1"));
        volume1.WaitReady();

        TVolumeClient volume2(*runtime, 0, TestVolumeTablets[1]);
        volume2.CreateVolume(volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD,
            7 * 1024,   // block count per partition
            "vol2"));
        volume2.WaitReady();

        auto forwardRequest = [&](TAutoPtr<IEventHandle>& event, TString diskId)
        {
            if (diskId == "vol1") {
                volume1.ForwardToPipe(event);
            } else if (diskId == "vol2") {
                volume2.ForwardToPipe(event);
            } else {
                UNIT_ASSERT_C(
                    false,
                    TStringBuilder() << "Unknown disk id: " << diskId);
            }
        };

        auto volumeProxyRequestHandler =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            if (event->Recipient == MakeVolumeProxyServiceId()) {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    auto* msg =
                        event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                    forwardRequest(event, msg->Record.GetDiskId());
                }
                return true;
            }

            if (event->GetTypeRewrite() ==
                TEvVolume::EvUpdateLinkOnFollowerRequest)
            {
                auto response = std::make_unique<
                    TEvVolume::TEvUpdateLinkOnFollowerResponse>(MakeError(
                    E_ARGUMENT,
                    "Simulated non-retriable error on follower"));

                runtime->Send(
                    new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0,   // flags
                        event->Cookie),
                    0);
                return true;
            }
            return false;
        };

        runtime->SetEventFilter(volumeProxyRequestHandler);

        TLeaderFollowerLink link{
            .LinkUUID = "",
            .LeaderDiskId = "vol1",
            .LeaderShardId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerShardId = "su2"};

        // Create link
        volume1.SendLinkLeaderVolumeToFollowerRequest(link);
        auto response = volume1.RecvLinkLeaderVolumeToFollowerResponse();

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
    }

    void DoShouldPrepareFollowerVolume(
        TFixture& fixture,
        NProto::EStorageMediaKind leaderMediaType,
        NProto::EStorageMediaKind followerMediaType,
        bool withCheckpoint)
    {
        // Intercept leader state changes
        TTestActorRuntimeBase::TEventFilter prevFilter;
        std::optional<TFollowerDiskInfo::EState> followerState;
        auto followerStateFilter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvUpdateFollowerStateResponse)
            {
                auto* msg = event->Get<
                    TEvVolumePrivate::TEvUpdateFollowerStateResponse>();
                followerState = msg->Follower.State;
            }

            return prevFilter(runtime, event);
        };
        prevFilter = fixture.Runtime->SetEventFilter(followerStateFilter);

        // Create leader volume
        TVolumeClient volume1(*fixture.Runtime, 0, TestVolumeTablets[0]);
        fixture.VolumeClients["vol1"] = &volume1;
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            leaderMediaType,
            fixture.VolumeBlockCount,   // block count per partition
            "vol1"));
        volume1.WaitReady();

        // registering a writer
        auto clientInfo1 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume1.AddClient(clientInfo1);

        // Create follower volume
        TVolumeClient volume2(*fixture.Runtime, 0, TestVolumeTablets[1]);
        fixture.VolumeClients["vol2"] = &volume2;
        volume2.CreateVolume(volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            followerMediaType,
            fixture.VolumeBlockCount,   // block count per partition
            "vol2"));
        volume2.WaitReady();

        // Write some blocks to leader before migration starts.
        for (ui64 pos = 0; pos < fixture.VolumeBlockCount; pos += 2048) {
            volume1.WriteBlocks(
                TBlockRange64::MakeOneBlock(pos),
                clientInfo1.GetClientId(),
                'a');
        }

        if (withCheckpoint) {
            volume1.CreateCheckpoint("c1");

            // Reconnect pipe since partition has restarted.
            volume1.ReconnectPipe();

            auto status = volume1.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::NOT_READY,
                status->Record.GetCheckpointStatus());
        }

        // Link volumes
        {
            TLeaderFollowerLink link{
                .LinkUUID = "",
                .LeaderDiskId = "vol1",
                .LeaderShardId = "su1",
                .FollowerDiskId = "vol2",
                .FollowerShardId = "su2"};

            auto response = volume1.LinkLeaderVolumeToFollower(link);
            link.LinkUUID = response->Record.GetLinkUUID();

            UNIT_ASSERT_EQUAL(
                TFollowerDiskInfo::EState::Preparing,
                followerState);

            // It is necessary to reconnect the pipe as the previous one broke
            // when the partition was restarted.
            volume1.ReconnectPipe();
            volume1.AddClient(clientInfo1);
        };

        // Send WriteBlocks and ZeroBlocks to leader during migration
        for (ui64 pos = 0; pos < fixture.VolumeBlockCount; pos += 2048) {
            for (;;) {
                auto range = TBlockRange64::MakeOneBlock(pos + 1);
                volume1.SendWriteBlocksRequest(
                    range,
                    clientInfo1.GetClientId(),
                    'b');
                auto response = volume1.RecvWriteBlocksResponse();
                if (response->GetStatus() == S_OK) {
                    break;
                }
                Cout << "WriteBlocks " << range
                     << "failed: " << FormatError(response->GetError()) << Endl;
                fixture.Runtime->DispatchEvents(
                    {},
                    TDuration::MilliSeconds(10));
            }
            for (;;) {
                auto range = TBlockRange64::MakeOneBlock(pos + 2);
                volume1.SendZeroBlocksRequest(range, clientInfo1.GetClientId());
                auto response = volume1.RecvZeroBlocksResponse();
                if (response->GetStatus() == S_OK) {
                    break;
                }
                Cout << "ZeroBlocks " << range
                     << " failed: " << FormatError(response->GetError())
                     << Endl;
                fixture.Runtime->DispatchEvents(
                    {},
                    TDuration::MilliSeconds(10));
            }
        }

        // Waiting for the follower disk to fill up.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return followerState == TFollowerDiskInfo::EState::DataReady;
            };
            fixture.Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT_EQUAL(
                TFollowerDiskInfo::EState::DataReady,
                followerState);
        }

        if (withCheckpoint) {
            for (;;) {
                auto status = volume1.GetCheckpointStatus("c1");
                if (status->Record.GetCheckpointStatus() ==
                    NProto::ECheckpointStatus::READY)
                {
                    break;
                }
                fixture.Runtime->DispatchEvents(
                    {},
                    TDuration::MilliSeconds(100));
            }

            auto status = volume1.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::READY,
                status->Record.GetCheckpointStatus());
        }

        // Make read client for volume2 and check volumes content match.
        auto clientInfo2 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume2.AddClient(clientInfo2);

        fixture
            .CheckVolumesDataMatch(volume1, volume2, clientInfo1, clientInfo2);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_SSD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            false);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_NRD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            false);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_NRD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            false);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_MIRROR, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            false);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_MIRROR_SSD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            NProto::STORAGE_MEDIA_SSD,
            false);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_SSD, TFixture)
    {
        /* TODO(drbasic) vol1 partition can't reboot. Need to figure out why.
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD);
        */
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_SSD_WithCheckpoint, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            true);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
