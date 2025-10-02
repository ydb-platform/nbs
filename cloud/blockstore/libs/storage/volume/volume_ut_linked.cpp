#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/model/composite_id.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/testlib/test_env.h>

#include <cloud/storage/core/libs/common/media.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

using namespace NTestVolume;

////////////////////////////////////////////////////////////////////////////////

enum class ECheckpointBehaviour
{
    None,
    CreateBeforeLink,
    CreateAfterLink,
    CreateThenDelete,
};

enum class ERebootBehaviour
{
    RebootLeader,
    RebootFollower,
    RebootBoth,
};

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    struct TVolumeInfo
    {
        TVolumeClient* VolumeClient = nullptr;
        NProto::TVolumeClientInfo* VolumeClientInfo = nullptr;
    };

    const ui64 VolumeBlockCount =
        DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize / 2;

    std::unique_ptr<TTestActorRuntime> Runtime;

    TMap<TString, TVolumeInfo> Volumes;

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
        config.SetMigrationIndexCachingInterval(16_MB / DefaultBlockSize);

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
    }

    void ForwardToVolume(const TString& diskId, TAutoPtr<IEventHandle>& event)
    {
        if (auto* volume = Volumes.FindPtr(diskId)) {
            volume->VolumeClient->ForwardToPipe(event);
        } else {
            UNIT_ASSERT_C(
                false,
                TStringBuilder() << "Unknown disk id: " << diskId);
        }
    }

    void CheckVolumesDataMatch() const
    {
        const auto* volume1 = Volumes.FindPtr("vol1");
        if (!volume1) {
            UNIT_ASSERT_C(false, "Volume vol1 not found");
            return;
        }
        const auto* volume2 = Volumes.FindPtr("vol2");
        if (!volume2) {
            UNIT_ASSERT_C(false, "Volume vol2 not found");
            return;
        }

        // Make read client for volume2
        auto clientInfo2 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume2->VolumeClient->AddClient(clientInfo2);

        // Read all data and match it.
        for (ui64 position = 0; position < VolumeBlockCount; position += 1024) {
            auto range = TBlockRange64::MakeClosedIntervalWithLimit(
                position,
                position + 1024,
                VolumeBlockCount - 1);

            auto response1 = volume1->VolumeClient->ReadBlocks(
                range,
                volume1->VolumeClientInfo->GetClientId());
            auto response2 = volume2->VolumeClient->ReadBlocks(
                range,
                clientInfo2.GetClientId());
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

    void CreateCheckpoint(NProto::EStorageMediaKind mediaType)
    {
        auto* volume = Volumes.FindPtr("vol1");
        if (!volume) {
            UNIT_ASSERT_C(false, "Volume not found");
            return;
        }

        volume->VolumeClient->CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume->VolumeClient->ReconnectPipe();
        volume->VolumeClient->AddClient(*volume->VolumeClientInfo);

        auto status = volume->VolumeClient->GetCheckpointStatus("c1");
        const auto expectedStatus = IsDiskRegistryMediaKind(mediaType)
                                        ? NProto::ECheckpointStatus::NOT_READY
                                        : NProto::ECheckpointStatus::READY;
        UNIT_ASSERT_EQUAL(expectedStatus, status->Record.GetCheckpointStatus());
    }

    void DeleteCheckpoint()
    {
        auto* volume = Volumes.FindPtr("vol1");
        if (!volume) {
            UNIT_ASSERT_C(false, "Volume not found");
            return;
        }

        volume->VolumeClient->DeleteCheckpointData("c1");
        // Reconnect pipe since partition has restarted.
        volume->VolumeClient->ReconnectPipe();
        volume->VolumeClient->AddClient(*volume->VolumeClientInfo);

        volume->VolumeClient->DeleteCheckpoint("c1");
        // Reconnect pipe since partition has restarted.
        volume->VolumeClient->ReconnectPipe();
        volume->VolumeClient->AddClient(*volume->VolumeClientInfo);
    }

    void WaitCheckpointReady()
    {
        auto* volume = Volumes.FindPtr("vol1");
        if (!volume) {
            UNIT_ASSERT_C(false, "Volume not found");
            return;
        }

        for (size_t i = 0; i < 100; ++i) {
            auto status = volume->VolumeClient->GetCheckpointStatus("c1");
            if (status->Record.GetCheckpointStatus() ==
                NProto::ECheckpointStatus::READY)
            {
                break;
            }
            Runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        }

        auto status = volume->VolumeClient->GetCheckpointStatus("c1");
        UNIT_ASSERT_EQUAL(
            NProto::ECheckpointStatus::READY,
            status->Record.GetCheckpointStatus());
    }

    void WriteBlocks(TBlockRange64 range) const
    {
        const auto* volume = Volumes.FindPtr("vol1");
        if (!volume) {
            UNIT_ASSERT_C(false, "Volume not found");
            return;
        }

        for (size_t i = 0; i < 100; ++i) {
            volume->VolumeClient->SendWriteBlocksRequest(
                range,
                volume->VolumeClientInfo->GetClientId(),
                'b');
            auto response = volume->VolumeClient->RecvWriteBlocksResponse();
            if (response->GetStatus() == S_OK) {
                return;
            }
            Runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
            if (response->GetStatus() == E_REJECTED) {
                Cout << "WriteBlocks " << range.Print() << " failed"
                     << FormatError(response->GetError()) << Endl;
                continue;
            }
            UNIT_ASSERT_C(
                false,
                TStringBuilder() << "WriteBlocks " << range.Print() << "failed"
                                 << FormatError(response->GetError()));
        }
        UNIT_ASSERT_C(
            false,
            TStringBuilder() << "WriteBlocks " << range.Print() << " failed");
    }

    void ZeroBlocks(TBlockRange64 range) const
    {
        const auto* volume = Volumes.FindPtr("vol1");
        if (!volume) {
            UNIT_ASSERT_C(false, "Volume not found");
            return;
        }

        for (size_t i = 0; i < 100; ++i) {
            volume->VolumeClient->SendZeroBlocksRequest(
                range,
                volume->VolumeClientInfo->GetClientId());
            auto response = volume->VolumeClient->RecvZeroBlocksResponse();
            if (response->GetStatus() == S_OK) {
                return;
            }
            Runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
            if (response->GetStatus() == E_REJECTED) {
                continue;
            }
            UNIT_ASSERT_C(
                false,
                TStringBuilder() << "ZeroBlocks " << range.Print() << "failed"
                                 << FormatError(response->GetError()));
        }
        UNIT_ASSERT_C(
            false,
            TStringBuilder() << "ZeroBlocks " << range.Print() << " failed");
    }

    void RebootTablets(ERebootBehaviour rebootBehaviour) const
    {
        const auto* volume1 = Volumes.FindPtr("vol1");
        if (!volume1) {
            UNIT_ASSERT_C(false, "Volume vol1 not found");
            return;
        }
        const auto* volume2 = Volumes.FindPtr("vol2");
        if (!volume2) {
            UNIT_ASSERT_C(false, "Volume vol2 not found");
            return;
        }

        if (rebootBehaviour == ERebootBehaviour::RebootLeader ||
            rebootBehaviour == ERebootBehaviour::RebootBoth)
        {
            volume1->VolumeClient->RebootTablet();
            volume1->VolumeClient->WaitReady();
            volume1->VolumeClient->AddClient(*volume1->VolumeClientInfo);
        }

        if (rebootBehaviour == ERebootBehaviour::RebootFollower ||
            rebootBehaviour == ERebootBehaviour::RebootBoth)
        {
            volume2->VolumeClient->RebootTablet();
            volume2->VolumeClient->WaitReady();
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
        TFixture & fixture,
        NProto::EStorageMediaKind leaderMediaType,
        NProto::EStorageMediaKind followerMediaType,
        ECheckpointBehaviour checkpoint)
    {
        // Intercept leader state changes
        TTestActorRuntimeBase::TEventFilter prevFilter;
        std::optional<TFollowerDiskInfo::EState> followerState;
        std::optional<ui64> migratedBytes;
        auto followerStateFilter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvUpdateFollowerStateResponse)
            {
                auto* msg = event->Get<
                    TEvVolumePrivate::TEvUpdateFollowerStateResponse>();
                followerState = msg->Follower.State;
                migratedBytes = msg->Follower.MigratedBytes;
            }

            return prevFilter(runtime, event);
        };
        prevFilter = fixture.Runtime->SetEventFilter(followerStateFilter);

        // Create leader volume
        TVolumeClient volume1(*fixture.Runtime, 0, TestVolumeTablets[0]);
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
        fixture.Volumes["vol1"] = {
            .VolumeClient = &volume1,
            .VolumeClientInfo = &clientInfo1};

        // Create follower volume
        TVolumeClient volume2(*fixture.Runtime, 0, TestVolumeTablets[1]);
        fixture.Volumes["vol2"] = {
            .VolumeClient = &volume2,
            .VolumeClientInfo = nullptr};
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

        if (checkpoint == ECheckpointBehaviour::CreateBeforeLink) {
            fixture.CreateCheckpoint(leaderMediaType);
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

        if (checkpoint == ECheckpointBehaviour::CreateAfterLink) {
            fixture.CreateCheckpoint(leaderMediaType);
        }

        if (checkpoint == ECheckpointBehaviour::CreateThenDelete) {
            fixture.CreateCheckpoint(leaderMediaType);
            fixture.DeleteCheckpoint();
        }

        // Send WriteBlocks and ZeroBlocks to leader during migration
        for (ui64 pos = 0; pos < fixture.VolumeBlockCount; pos += 2048) {
            fixture.WriteBlocks(TBlockRange64::MakeOneBlock(pos + 1));
            fixture.ZeroBlocks(TBlockRange64::MakeOneBlock(pos + 2));
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

        if (checkpoint == ECheckpointBehaviour::CreateBeforeLink ||
            checkpoint == ECheckpointBehaviour::CreateAfterLink)
        {
            fixture.WaitCheckpointReady();
        }

        // Check volumes content match.
        fixture.CheckVolumesDataMatch();
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_SSD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            ECheckpointBehaviour::None);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_NRD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            ECheckpointBehaviour::None);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_NRD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            ECheckpointBehaviour::None);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_MIRROR, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            ECheckpointBehaviour::None);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_MIRROR_SSD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            NProto::STORAGE_MEDIA_SSD,
            ECheckpointBehaviour::None);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_SSD, TFixture)
    {
        /* TODO(drbasic) vol1 partition can't reboot. Need to figure out
        why. DoShouldPrepareFollowerVolume( *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD);
        */
    }

    Y_UNIT_TEST_F(
        ShouldPrepareFollowerVolume_NRD_SSD_WithCheckpointBefore,
        TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            ECheckpointBehaviour::CreateBeforeLink);
    }

    Y_UNIT_TEST_F(
        ShouldPrepareFollowerVolume_NRD_SSD_WithCheckpointAfter,
        TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            ECheckpointBehaviour::CreateAfterLink);
    }

    Y_UNIT_TEST_F(
        ShouldPrepareFollowerVolume_NRD_SSD_WithCheckpointCreateThenDelete,
        TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            ECheckpointBehaviour::CreateThenDelete);
    }

    Y_UNIT_TEST_F(
        ShouldPrepareFollowerVolume_SSD_NRD_WithCheckpointAfter,
        TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            ECheckpointBehaviour::CreateAfterLink);
    }

    void DoShouldPrepareFollowerVolumeWithReboot(
        TFixture& fixture,
        NProto::EStorageMediaKind leaderMediaType,
        NProto::EStorageMediaKind followerMediaType)
    {
        struct TRestart
        {
            ui64 Position = 0;
            ERebootBehaviour RebootBehaviour = ERebootBehaviour::RebootFollower;
            bool Done = false;
        };
        TRestart restarts[] = {
            {.Position = 16_MB,
             .RebootBehaviour = ERebootBehaviour::RebootFollower},
            {.Position = 32_MB,
             .RebootBehaviour = ERebootBehaviour::RebootLeader},
            {.Position = 48_MB,
             .RebootBehaviour = ERebootBehaviour::RebootBoth},
        };

        // Intercept leader state changes
        TTestActorRuntimeBase::TEventFilter prevFilter;
        std::optional<TFollowerDiskInfo::EState> followerState;
        std::optional<ui64> migratedBytes;
        bool deletionOfTheSourceHasBeenInitiated = false;
        auto followerStateFilter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvUpdateFollowerStateResponse)
            {
                auto* msg = event->Get<
                    TEvVolumePrivate::TEvUpdateFollowerStateResponse>();
                followerState = msg->Follower.State;

                if (msg->Follower.State ==
                        TFollowerDiskInfo::EState::Preparing &&
                    msg->Follower.MigratedBytes)
                {
                    UNIT_ASSERT_C(
                        !migratedBytes.has_value() ||
                            migratedBytes.value() < msg->Follower.MigratedBytes,
                        TStringBuilder()
                            << "Migration progress should advance: "
                            << migratedBytes.value() << " < "
                            << msg->Follower.MigratedBytes);
                    migratedBytes = msg->Follower.MigratedBytes;
                }
            }

            if (event->GetTypeRewrite() == TEvService::EvDestroyVolumeRequest) {
                auto* msg = event->Get<TEvService::TEvDestroyVolumeRequest>();
                UNIT_ASSERT_VALUES_EQUAL("vol1", msg->Record.GetDiskId());
                deletionOfTheSourceHasBeenInitiated = true;
            }

            return prevFilter(runtime, event);
        };
        prevFilter = fixture.Runtime->SetEventFilter(followerStateFilter);

        // Create leader volume
        TVolumeClient volume1(*fixture.Runtime, 0, TestVolumeTablets[0]);
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
        fixture.Volumes["vol1"] = {
            .VolumeClient = &volume1,
            .VolumeClientInfo = &clientInfo1};

        // Create follower volume
        TVolumeClient volume2(*fixture.Runtime, 0, TestVolumeTablets[1]);
        fixture.Volumes["vol2"] = {
            .VolumeClient = &volume2,
            .VolumeClientInfo = nullptr};
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

        // Send WriteBlocks to leader during migration
        for (ui64 pos = 0;; pos += 2048) {
            fixture.WriteBlocks(
                TBlockRange64::MakeOneBlock(
                    ((pos + 1) % fixture.VolumeBlockCount)));

            TDispatchOptions options;
            std::optional<ui64> oldMigratedBytes = migratedBytes;
            options.CustomFinalCondition = [&]
            {
                return migratedBytes != oldMigratedBytes;
            };
            fixture.Runtime->DispatchEvents(
                options,
                TDuration::MilliSeconds(100));

            if (followerState == TFollowerDiskInfo::EState::DataReady) {
                break;
            }

            for (auto& restart: restarts) {
                if (restart.Position != migratedBytes || restart.Done) {
                    continue;
                }
                fixture.RebootTablets(restart.RebootBehaviour);
                restart.Done = true;
            }
        }

        for (const auto& restart: restarts) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                true,
                restart.Done,
                TStringBuilder()
                    << "Restart at " << restart.Position << " failed");
        }

        // Check volumes content match.
        fixture.CheckVolumesDataMatch();

        UNIT_ASSERT_VALUES_EQUAL(true, deletionOfTheSourceHasBeenInitiated);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_NRD_WithReboot, TFixture)
    {
        DoShouldPrepareFollowerVolumeWithReboot(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_SSD_WithReboot, TFixture)
    {
        DoShouldPrepareFollowerVolumeWithReboot(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_NRD_WithReboot, TFixture)
    {
        DoShouldPrepareFollowerVolumeWithReboot(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST_F(
        ShouldPropagateCompleteOnDataReadyAndDestroyOutdatedLeader,
        TFixture)
    {
        // Intercept leader state changes
        TTestActorRuntimeBase::TEventFilter prevFilter;
        std::optional<TFollowerDiskInfo::EState> followerState;
        std::optional<NProto::ELinkAction> propagatedAction;
        size_t volumeDestructionRequestCount = 0;
        NActors::TActorId leaderVolumeActorId;
        bool leaderPoisoned = false;
        auto followerStateFilter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvUpdateFollowerStateResponse)
            {
                auto* msg = event->Get<
                    TEvVolumePrivate::TEvUpdateFollowerStateResponse>();
                followerState = msg->Follower.State;
                leaderVolumeActorId = event->Sender;
            }

            if (event->GetTypeRewrite() ==
                TEvVolume::EvUpdateLinkOnFollowerRequest)
            {
                auto* msg =
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                propagatedAction = msg->Record.GetAction();
            }

            if (event->GetTypeRewrite() == TEvService::EvDestroyVolumeRequest) {
                ++volumeDestructionRequestCount;
                auto* msg = event->Get<TEvService::TEvDestroyVolumeRequest>();
                UNIT_ASSERT_VALUES_EQUAL("vol1", msg->Record.GetDiskId());

                auto response =
                    std::make_unique<TEvService::TEvDestroyVolumeResponse>(
                        MakeError(
                            volumeDestructionRequestCount == 1 ? E_REJECTED
                                                            : S_OK));

                auto responseEvent = std::make_unique<IEventHandle>(
                    event->Sender,
                    event->Recipient,
                    response.release(),
                    0, // flags
                    event->Cookie);
                runtime.SendAsync(responseEvent.release());

                return true;
            }

            if (event->GetTypeRewrite() == NActors::TEvents::TSystem::Poison) {
                if (event->Recipient == leaderVolumeActorId) {
                    leaderPoisoned = true;
                }
            }
            return prevFilter(runtime, event);
        };
        prevFilter = Runtime->SetEventFilter(followerStateFilter);

        // Create leader volume
        TVolumeClient volume1(*Runtime, 0, TestVolumeTablets[0]);
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            VolumeBlockCount,   // block count per partition
            "vol1"));
        volume1.WaitReady();

        // registering a writer
        auto clientInfo1 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume1.AddClient(clientInfo1);
        Volumes["vol1"] = {
            .VolumeClient = &volume1,
            .VolumeClientInfo = &clientInfo1};

        // Create follower volume
        TVolumeClient volume2(*Runtime, 0, TestVolumeTablets[1]);
        Volumes["vol2"] = {
            .VolumeClient = &volume2,
            .VolumeClientInfo = nullptr};
        volume2.CreateVolume(volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            VolumeBlockCount,   // block count per partition
            "vol2"));
        volume2.WaitReady();

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

        // Waiting for the follower disk to fill up.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return followerState == TFollowerDiskInfo::EState::DataReady;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT_EQUAL(
                TFollowerDiskInfo::EState::DataReady,
                followerState);
        }

        // Waiting for the follower disk to become a leader.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return propagatedAction ==
                       NProto::ELinkAction::LINK_ACTION_COMPLETED;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT_EQUAL(
                NProto::ELinkAction::LINK_ACTION_COMPLETED,
                propagatedAction);
        }

        // Waiting for the leader disk destruction initiated and retried after
        // reject.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return volumeDestructionRequestCount == 1;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(1, volumeDestructionRequestCount);
        }

        // Waiting for the leader disk destruction retried after reject.
        {
            TDispatchOptions options;
            Runtime->AdvanceCurrentTime(TDuration::Seconds(10));
            options.CustomFinalCondition = [&]
            {
                return volumeDestructionRequestCount == 2;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));

            UNIT_ASSERT_VALUES_EQUAL(2, volumeDestructionRequestCount);
        }

        // Waiting for the leader volume actor poisoned by
        // FollowerPartitionActor.
        {
            UNIT_ASSERT_VALUES_EQUAL(false, leaderPoisoned);
            TDispatchOptions options;
            Runtime->AdvanceCurrentTime(TDuration::Seconds(30));
            options.CustomFinalCondition = [&]
            {
                return leaderPoisoned;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));

            UNIT_ASSERT_VALUES_EQUAL(false, leaderPoisoned);
        }
    }

    Y_UNIT_TEST_F(ShouldImmediatelyRestartVolumeOnError, TFixture)
    {
        // Intercept leader state changes
        TTestActorRuntimeBase::TEventFilter prevFilter;
        NActors::TActorId leaderVolumeActorId;
        bool leaderPoisoned = false;

        auto followerStateFilter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvUpdateFollowerStateRequest)
            {
                auto* msg = event->Get<
                    TEvVolumePrivate::TEvUpdateFollowerStateRequest>();

                if (msg->Follower.State == TFollowerDiskInfo::EState::DataReady)
                {   // Force error state.
                    UNIT_ASSERT_VALUES_EQUAL(false, leaderPoisoned);
                    msg->Follower.State = TFollowerDiskInfo::EState::Error;
                }
                leaderVolumeActorId = event->Recipient;
            }

            if (event->GetTypeRewrite() == NActors::TEvents::TSystem::Poison) {
                if (event->Recipient == leaderVolumeActorId) {
                    leaderPoisoned = true;
                }
            }
            return prevFilter(runtime, event);
        };
        prevFilter = Runtime->SetEventFilter(followerStateFilter);

        // Create leader volume
        TVolumeClient volume1(*Runtime, 0, TestVolumeTablets[0]);
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            VolumeBlockCount,   // block count per partition
            "vol1"));
        volume1.WaitReady();

        // Create follower volume
        TVolumeClient volume2(*Runtime, 0, TestVolumeTablets[1]);
        Volumes["vol2"] = {
            .VolumeClient = &volume2,
            .VolumeClientInfo = nullptr};
        volume2.CreateVolume(volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            VolumeBlockCount,   // block count per partition
            "vol2"));
        volume2.WaitReady();

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
        };

        // Waiting for the leader volume actor poisoned by
        // FollowerPartitionActor.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return leaderPoisoned;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(true, leaderPoisoned);
        }
    }

    Y_UNIT_TEST_F(ShouldGoIntoErrorStateIfTheFollowerDiskDisappears, TFixture)
    {
       // Intercept leader state changes
        TTestActorRuntimeBase::TEventFilter prevFilter;
        std::optional<TFollowerDiskInfo::EState> followerState;
        NActors::TActorId leaderVolumeActorId;
        bool leaderPoisoned = false;
        auto followerStateFilter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvUpdateFollowerStateResponse)
            {
                const auto* msg = event->Get<
                    TEvVolumePrivate::TEvUpdateFollowerStateResponse>();
                followerState = msg->Follower.State;
                leaderVolumeActorId = event->Sender;
            }

            if (event->GetTypeRewrite() ==
                TEvVolume::EvUpdateLinkOnFollowerRequest)
            {
                const auto* msg =
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();

                if (msg->Record.GetAction() ==
                    NProto::ELinkAction::LINK_ACTION_COMPLETED)
                {
                    auto response = std::make_unique<
                        TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                        MakeSchemeShardError(
                            NKikimrScheme::StatusPathDoesNotExist,
                            "path does not exist"));
                    auto responseEvent = std::make_unique<IEventHandle>(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0,   // flags
                        event->Cookie);
                    runtime.SendAsync(responseEvent.release());

                    return true;
                }
            }

            if (event->GetTypeRewrite() == NActors::TEvents::TSystem::Poison) {
                if (event->Recipient == leaderVolumeActorId) {
                    leaderPoisoned = true;
                }
            }
            return prevFilter(runtime, event);
        };
        prevFilter = Runtime->SetEventFilter(followerStateFilter);

        // Create leader volume
        TVolumeClient volume1(*Runtime, 0, TestVolumeTablets[0]);
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            VolumeBlockCount,   // block count per partition
            "vol1"));
        volume1.WaitReady();

        // Create follower volume
        TVolumeClient volume2(*Runtime, 0, TestVolumeTablets[1]);
        Volumes["vol2"] = {
            .VolumeClient = &volume2,
            .VolumeClientInfo = nullptr};
        volume2.CreateVolume(volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            VolumeBlockCount,   // block count per partition
            "vol2"));
        volume2.WaitReady();

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
        };

        // Waiting for the link with follower going to error state.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return followerState == TFollowerDiskInfo::EState::Error;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT_EQUAL(
                TFollowerDiskInfo::EState::Error,
                followerState);
        }

        // Waiting for the leader volume actor poisoned by
        // FollowerPartitionActor.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return leaderPoisoned;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(true, leaderPoisoned);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
