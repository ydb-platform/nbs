#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/model/composite_id.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_mirror_actor.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_actor.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_mirror_actor.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_actor.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/actors/follower_disk_actor.h>
#include <cloud/blockstore/libs/storage/volume/actors/propagate_to_follower.h>
#include <cloud/blockstore/libs/storage/volume/actors/volume_as_partition_actor.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>
#include <cloud/blockstore/libs/storage/volume/testlib/test_env.h>

#include <cloud/storage/core/libs/common/media.h>

#include <util/system/hostname.h>
#include <util/system/type_name.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

using namespace NTestVolume;

////////////////////////////////////////////////////////////////////////////////

enum class ERebootBehaviour
{
    RebootLeader,
    RebootFollower,
    RebootBoth,
};

enum class ECheckpointBeforeLink
{
    None,
    DataPresent,
    Deleted,
};

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPoisonableTestActor final
    : public TActorBootstrapped<TPoisonableTestActor>
{
public:
    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);
        Y_UNUSED(ctx);
    }

private:
    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        ctx.Send(
            ev->Sender,
            new TEvents::TEvPoisonTaken(),
            0,   // flags
            ev->Cookie);
        Die(ctx);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
                break;
        }
    }
};

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

    TVector<char> ExpectedBlockContent = TVector<char>(VolumeBlockCount, 0);

    std::unique_ptr<TTestActorRuntime> Runtime;

    TMap<TString, TVolumeInfo> Volumes;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        SetUpRuntime({});
    }

    void SetUpRuntime(NProto::TStorageServiceConfig config)
    {
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

    void SetExpectedBlockContent(TBlockRange64 range, char content)
    {
        for (ui64 block = range.Start; block <= range.End; ++block) {
            ExpectedBlockContent[block] = content;
        }
    }

    void CheckCopiedData() const
    {
        const auto* volume = Volumes.FindPtr("vol2");
        UNIT_ASSERT_C(volume, "Target volume not found");

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume->VolumeClient->AddClient(clientInfo);

        for (ui64 position = 0; position < VolumeBlockCount; position += 1024) {
            const auto range = TBlockRange64::MakeClosedIntervalWithLimit(
                position,
                position + 1024,
                VolumeBlockCount - 1);
            const auto response = volume->VolumeClient->ReadBlocks(
                range,
                clientInfo.GetClientId());
            const auto& buffers = response->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(range.Size(), buffers.size());
            for (int i = 0; i < buffers.size(); ++i) {
                const auto& block = buffers[i];
                const auto expected = ExpectedBlockContent[position + i];
                if (block.empty()) {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        0,
                        ui32(expected),
                        TStringBuilder() << "Block #" << position + i);
                    continue;
                }

                UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, block.size());
                for (size_t j = 0; j < block.size(); ++j) {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        ui32(expected),
                        ui32(block[j]),
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

    [[nodiscard]] bool WriteBlocks(
        TBlockRange64 range,
        std::optional<TFollowerDiskInfo::EState>* followerState = nullptr)
    {
        using EState = TFollowerDiskInfo::EState;

        const auto* volume = Volumes.FindPtr("vol1");
        if (!volume) {
            UNIT_ASSERT_C(false, "Volume not found");
            return false;
        }

        for (size_t i = 0; i < 100; ++i) {
            if (followerState && followerState->has_value() &&
                (followerState->value() == EState::DataReady ||
                 followerState->value() == EState::LeadershipTransferred))
            {
                return false;
            }
            volume->VolumeClient->SendWriteBlocksRequest(
                range,
                volume->VolumeClientInfo->GetClientId(),
                'b');
            auto response = volume->VolumeClient->RecvWriteBlocksResponse();
            if (response->GetStatus() == S_OK) {
                SetExpectedBlockContent(range, 'b');
                return true;
            }
            if (response->GetStatus() == E_BS_INVALID_SESSION) {
                return false;
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
        return false;
    }

    [[nodiscard]] bool ZeroBlocks(TBlockRange64 range)
    {
        const auto* volume = Volumes.FindPtr("vol1");
        if (!volume) {
            UNIT_ASSERT_C(false, "Volume not found");
            return false;
        }

        for (size_t i = 0; i < 100; ++i) {
            volume->VolumeClient->SendZeroBlocksRequest(
                range,
                volume->VolumeClientInfo->GetClientId());
            auto response = volume->VolumeClient->RecvZeroBlocksResponse();
            if (response->GetStatus() == S_OK) {
                SetExpectedBlockContent(range, 0);
                return true;
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
        return false;
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

enum class ELeaderRemovalPoint
{
    PromotionCommit,
    TagRemoval,
    DestructionDelay,
};

void CheckLeaderRemovalDuringPromotion(ELeaderRemovalPoint removalPoint)
{
    auto runtime = PrepareTestActorRuntime();
    TVolumeClient volume(*runtime);
    auto configRequest = volume.CreateUpdateVolumeConfigRequest(
        0,
        0,
        0,
        0,
        false,
        1,
        NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
        DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize,
        "vol2",
        "cloud",
        "folder",
        1,
        0,
        MakeTag("source-disk-id", "vol1"));
    auto config = configRequest->Record;
    volume.CreateVolume(std::move(configRequest));
    volume.RecvUpdateVolumeConfigResponse();
    volume.WaitReady();

    const auto clientInfo = CreateVolumeClientInfo(
        NProto::VOLUME_ACCESS_READ_WRITE,
        NProto::VOLUME_MOUNT_LOCAL,
        0);
    if (removalPoint == ELeaderRemovalPoint::PromotionCommit) {
        volume.AddClient(clientInfo);
    }

    auto makeLinkRequest = [](NProto::ELinkAction action)
    {
        auto request =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        request->Record.SetDiskId("vol2");
        request->Record.SetLeaderDiskId("vol1");
        request->Record.SetLinkUUID("test-link");
        request->Record.SetAction(action);
        return request;
    };
    auto getLinkStatus = [&]
    {
        auto request = std::make_unique<TEvVolume::TEvGetLinkStatusRequest>();
        request->Record.SetDiskId("vol2");
        request->Record.SetLeaderDiskId("vol1");
        request->Record.SetFollowerDiskId("vol2");
        volume.SendToPipe(std::move(request));
        return static_cast<ui32>(
            volume.RecvResponse<TEvVolume::TEvGetLinkStatusResponse>()
                ->Record.GetStatus());
    };

    volume.SendToPipe(makeLinkRequest(NProto::LINK_ACTION_CREATE));
    auto created =
        volume.RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>();
    UNIT_ASSERT_C(!HasError(created->GetError()), created->GetErrorReason());

    bool holdCommits = removalPoint == ELeaderRemovalPoint::PromotionCommit;
    TVector<std::unique_ptr<IEventHandle>> commits;
    std::unique_ptr<IEventHandle> tagRemoval;
    THashMap<ui64, ui32> responses;
    ui32 sourceDestructionRequests = 0;
    runtime->SetEventFilter(
        [&](TTestActorRuntimeBase& callbackRuntime,
            TAutoPtr<IEventHandle>& event)
        {
            if (holdCommits &&
                event->GetTypeRewrite() == TEvTablet::EvCommitResult &&
                event->Get<TEvTablet::TEvCommitResult>()->TabletID ==
                    TestVolumeTablets[0])
            {
                commits.emplace_back(event.Release());
                return true;
            }
            if (event->GetTypeRewrite() == TEvService::EvRemoveTagsRequest) {
                tagRemoval.reset(event.Release());
                return true;
            }
            if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerResponse &&
                event->Recipient == volume.GetSender())
            {
                responses[event->Cookie] =
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerResponse>()
                        ->GetStatus();
                return true;
            }
            if (event->GetTypeRewrite() == TEvService::EvDestroyVolumeRequest) {
                ++sourceDestructionRequests;
                callbackRuntime.Send(new IEventHandle(
                    event->Sender,
                    event->Recipient,
                    new TEvService::TEvDestroyVolumeResponse(),
                    0,
                    event->Cookie));
                return true;
            }
            return false;
        });

    volume.SendToPipe(makeLinkRequest(NProto::LINK_ACTION_COMPLETED), 1);
    TDispatchOptions options;
    options.CustomFinalCondition = [&]
    {
        return holdCommits ? !commits.empty() : tagRemoval != nullptr;
    };
    runtime->DispatchEvents(options, TDuration::Seconds(1));
    UNIT_ASSERT(responses.empty());

    TVector<std::unique_ptr<IEventHandle>> promotionCommits;
    if (holdCommits) {
        promotionCommits.swap(commits);
    }

    if (removalPoint != ELeaderRemovalPoint::PromotionCommit) {
        volume.SendToPipe(makeLinkRequest(NProto::LINK_ACTION_COMPLETED), 5);
        for (ui64 cookie = 10; cookie < 110; ++cookie) {
            volume.SendToPipe(
                makeLinkRequest(NProto::LINK_ACTION_COMPLETED),
                cookie);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED),
            getLinkStatus());
        UNIT_ASSERT(!responses.contains(1));
        UNIT_ASSERT(responses.contains(5));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, responses.at(5));
        for (ui64 cookie = 10; cookie < 110; ++cookie) {
            UNIT_ASSERT_C(responses.contains(cookie), cookie);
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, responses.at(cookie));
        }
    }

    auto completeTagRemoval = [&]
    {
        // The fake StorageService does not apply the tag change.
        config.MutableVolumeConfig()->ClearTagsStr();
        config.MutableVolumeConfig()->SetVersion(2);
        auto update = std::make_unique<TEvBlockStore::TEvUpdateVolumeConfig>();
        update->Record = config;
        volume.SendToPipe(std::move(update));
        const auto updated = volume.RecvUpdateVolumeConfigResponse();
        UNIT_ASSERT(updated->Record.GetStatus() == NKikimrBlockStore::OK);
        volume.ReconnectPipe();
        volume.WaitReady();
        runtime->Send(new IEventHandle(
            tagRemoval->Sender,
            tagRemoval->Recipient,
            new TEvService::TEvRemoveTagsResponse(),
            0,
            tagRemoval->Cookie));
        options.CustomFinalCondition = [&]
        {
            return responses.contains(1);
        };
        runtime->DispatchEvents(options, TDuration::Seconds(1));
        UNIT_ASSERT_C(
            responses.contains(1),
            "No initial leadership transfer response after tag removal");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, responses.at(1));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, responses.at(5));

        volume.SendToPipe(makeLinkRequest(NProto::LINK_ACTION_COMPLETED), 7);
        options.CustomFinalCondition = [&]
        {
            return responses.contains(7);
        };
        runtime->DispatchEvents(options, TDuration::Seconds(1));
        UNIT_ASSERT(responses.contains(7));
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, responses.at(7));
    };

    if (removalPoint == ELeaderRemovalPoint::DestructionDelay) {
        completeTagRemoval();
    }
    volume.SendToPipe(makeLinkRequest(NProto::LINK_ACTION_DESTROY), 2);
    if (holdCommits) {
        volume.SendToPipe(makeLinkRequest(NProto::LINK_ACTION_COMPLETED), 4);
        // Commit confirmations are held: readers must still see the original
        // Following row, even after RemoveLeader executes.
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_PREPARING),
            getLinkStatus());
        UNIT_ASSERT_C(
            responses.contains(4),
            "No response to promotion during link removal");
        UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, responses.at(4));

        // Complete promotion while keeping removal unconfirmed. A cancelled
        // promotion must not allow client writes in this window.
        for (auto& commit: promotionCommits) {
            runtime->Send(commit.release());
        }
        options.CustomFinalCondition = [&]
        {
            return responses.contains(1);
        };
        runtime->DispatchEvents(options, TDuration::Seconds(1));
        UNIT_ASSERT(responses.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, responses.at(1));
        UNIT_ASSERT(!responses.contains(2));
        volume.SendWriteBlocksRequest(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            'a');
        const auto writeResponse = volume.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_BS_INVALID_SESSION,
            writeResponse->GetStatus(),
            FormatError(writeResponse->GetError()));
        UNIT_ASSERT(!responses.contains(2));

        holdCommits = false;
        for (auto& commit: commits) {
            runtime->Send(commit.release());
        }
    }
    options.CustomFinalCondition = [&]
    {
        return responses.contains(2);
    };
    runtime->DispatchEvents(options, TDuration::Seconds(1));
    const bool cancellationAllowed =
        removalPoint == ELeaderRemovalPoint::PromotionCommit;
    UNIT_ASSERT_C(responses.contains(2), "No response to link removal");
    UNIT_ASSERT_VALUES_EQUAL(
        cancellationAllowed ? S_OK : E_INVALID_STATE,
        responses.at(2));
    if (cancellationAllowed) {
        UNIT_ASSERT(responses.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, responses.at(1));
        UNIT_ASSERT(!tagRemoval);
        volume.ReconnectPipe();
        volume.WaitReady();
    } else if (removalPoint == ELeaderRemovalPoint::TagRemoval) {
        UNIT_ASSERT(!responses.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, responses.at(5));
        completeTagRemoval();
    }

    volume.SendToPipe(makeLinkRequest(NProto::LINK_ACTION_COMPLETED), 3);
    options.CustomFinalCondition = [&]
    {
        return responses.contains(3);
    };
    runtime->DispatchEvents(options, TDuration::Seconds(1));
    UNIT_ASSERT_C(responses.contains(3), "No response to repeated promotion");
    UNIT_ASSERT_VALUES_EQUAL(
        cancellationAllowed ? E_CANCELLED : S_ALREADY,
        responses.at(3));

    runtime->DispatchEvents(TDispatchOptions(), TDuration::Minutes(1));
    if (cancellationAllowed) {
        UNIT_ASSERT_VALUES_EQUAL(0, sourceDestructionRequests);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_NOT_FOUND),
            getLinkStatus());
    } else {
        UNIT_ASSERT(sourceDestructionRequests > 0);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_COMPLETED),
            getLinkStatus());
    }

    volume.RebootTablet();
    volume.WaitReady();
    volume.SendToPipe(makeLinkRequest(NProto::LINK_ACTION_DESTROY), 6);
    options.CustomFinalCondition = [&]
    {
        return responses.contains(6);
    };
    runtime->DispatchEvents(options, TDuration::Seconds(1));
    UNIT_ASSERT_C(
        responses.contains(6),
        "No response to link removal after reboot");
    UNIT_ASSERT_VALUES_EQUAL(
        cancellationAllowed ? S_ALREADY : E_INVALID_STATE,
        responses.at(6));
}

struct TPullStatisticsFixture: public TFixture
{
    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        NProto::TStorageServiceConfig config;
        config.SetUsePullSchemeForVolumeStatistics(true);
        SetUpRuntime(std::move(config));
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TUpdateLinkOnFollowerTest)
{
    struct TEnv
    {
        std::unique_ptr<TTestActorRuntime> Runtime = PrepareTestActorRuntime();
        TVolumeClient Volume{*Runtime};
        const TLeaderFollowerLink Link{
            .LinkUUID = "link-uuid",
            .LeaderDiskId = "leader",
            .LeaderCellId = "cell-a",
            .FollowerDiskId = "vol0",
            .FollowerCellId = "cell-a"};

        TEnv()
        {
            Volume.UpdateVolumeConfig();
            Volume.WaitReady();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                UpdateLink(Link, NProto::LINK_ACTION_CREATE));
            CheckLinkStatus(NProto::LINK_STATUS_PREPARING);
        }

        ui32 UpdateLink(
            const TLeaderFollowerLink& link,
            NProto::ELinkAction action)
        {
            auto request =
                std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
            request->Record.SetDiskId(link.FollowerDiskId);
            request->Record.SetLinkUUID(link.LinkUUID);
            request->Record.SetLeaderDiskId(link.LeaderDiskId);
            request->Record.SetLeaderCellId(link.LeaderCellId);
            request->Record.SetFollowerCellId(link.FollowerCellId);
            request->Record.SetAction(action);
            Volume.SendToPipe(std::move(request));
            return Volume.RecvResponse<
                TEvVolume::TEvUpdateLinkOnFollowerResponse>()->GetStatus();
        }

        void CheckLinkStatus(NProto::ELinkStatus expectedStatus)
        {
            // Query by the original disk and cell ids, without a UUID that
            // could hide an accidental change to the persisted identity.
            auto request =
                std::make_unique<TEvVolume::TEvGetLinkStatusRequest>();
            request->Record.SetDiskId(Link.FollowerDiskId);
            request->Record.SetLeaderDiskId(Link.LeaderDiskId);
            request->Record.SetLeaderCellId(Link.LeaderCellId);
            request->Record.SetFollowerDiskId(Link.FollowerDiskId);
            request->Record.SetFollowerCellId(Link.FollowerCellId);
            Volume.SendToPipe(std::move(request));
            auto response =
                Volume.RecvResponse<TEvVolume::TEvGetLinkStatusResponse>();
            UNIT_ASSERT_C(
                !HasError(response->GetError()),
                response->GetErrorReason());
            const auto& record = response->Record;
            UNIT_ASSERT_EQUAL_C(
                expectedStatus,
                record.GetStatus(),
                record.ShortDebugString());
            if (expectedStatus != NProto::LINK_STATUS_NOT_FOUND) {
                UNIT_ASSERT_VALUES_EQUAL(Link.LinkUUID, record.GetLinkUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    Link.LeaderCellId,
                    record.GetLeaderCellId());
                UNIT_ASSERT_VALUES_EQUAL(
                    Link.FollowerCellId,
                    record.GetFollowerCellId());
            }
        }

        void Reboot()
        {
            Volume.RebootTablet();
            Volume.WaitReady();
        }

        void CheckCompletion(const TLeaderFollowerLink& requestLink)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                UpdateLink(requestLink, NProto::LINK_ACTION_COMPLETED));
            CheckLinkStatus(NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED);
            Reboot();
            CheckLinkStatus(NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED);

            // Repeating the completion must still find the original record.
            UNIT_ASSERT_VALUES_EQUAL(
                S_ALREADY,
                UpdateLink(Link, NProto::LINK_ACTION_COMPLETED));
        }
    };

    void DoShouldPersistLinkDestruction(bool withUuid)
    {
        TEnv env;
        auto requestLink = env.Link;
        if (!withUuid) {
            requestLink.LinkUUID.clear();
        }

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            env.UpdateLink(requestLink, NProto::LINK_ACTION_DESTROY));
        env.CheckLinkStatus(NProto::LINK_STATUS_NOT_FOUND);
        env.Reboot();
        env.CheckLinkStatus(NProto::LINK_STATUS_NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            env.UpdateLink(requestLink, NProto::LINK_ACTION_DESTROY));
    }

    Y_UNIT_TEST(ShouldPersistLinkDestructionWithUuid)
    {
        DoShouldPersistLinkDestruction(true);
    }

    Y_UNIT_TEST(ShouldPersistLinkDestructionWithoutUuid)
    {
        DoShouldPersistLinkDestruction(false);
    }

    Y_UNIT_TEST(ShouldPreserveLinkIdentityOnCompletion)
    {
        TEnv env;
        env.CheckCompletion(env.Link);
    }

    Y_UNIT_TEST(ShouldPreserveLinkIdentityOnCompletionWithoutUuid)
    {
        TEnv env;
        auto requestLink = env.Link;
        requestLink.LinkUUID.clear();
        env.CheckCompletion(requestLink);
    }

    Y_UNIT_TEST(ShouldPreserveLinkIdentityOnCompletionWithoutCells)
    {
        TEnv env;
        auto requestLink = env.Link;
        requestLink.LeaderCellId.clear();
        requestLink.FollowerCellId.clear();
        env.CheckCompletion(requestLink);
    }

    Y_UNIT_TEST(ShouldPreserveLinkIdentityOnCompletionWithAnotherLeaderCell)
    {
        TEnv env;
        auto requestLink = env.Link;
        requestLink.LeaderCellId = "another-cell";
        env.CheckCompletion(requestLink);
    }

    Y_UNIT_TEST(ShouldPreserveLinkIdentityOnCompletionWithAnotherLeaderDisk)
    {
        TEnv env;
        auto requestLink = env.Link;
        requestLink.LeaderDiskId = "another-leader";
        env.CheckCompletion(requestLink);
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLinkedVolumeTest)
{
    Y_UNIT_TEST(ShouldKeepOneRetryTimerForRetriableLinkErrors)
    {
        auto runtime = PrepareTestActorRuntime();
        const auto sender = runtime->AllocateEdgeActor();
        ui32 attempts = 0;
        ui32 retryTimers = 0;
        TActorId propagationActor;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& callbackRuntime,
                TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() !=
                        TEvVolume::EvUpdateLinkOnFollowerRequest ||
                    event->Recipient != MakeVolumeProxyServiceId())
                {
                    return false;
                }
                ++attempts;
                callbackRuntime.SendAsync(new IEventHandle(
                    event->Sender,
                    event->Recipient,
                    new TEvVolume::TEvUpdateLinkOnFollowerResponse(
                        attempts <= 3 ? MakeError(E_REJECTED, "try again")
                                      : MakeError(S_OK)), 0, event->Cookie));
                return true;
            });
        TTestActorRuntimeBase::TScheduledEventFilter previousScheduledFilter;
        previousScheduledFilter = runtime->SetScheduledEventFilter(
            [&](TTestActorRuntimeBase& callbackRuntime,
                TAutoPtr<IEventHandle>& event,
                TDuration delay, TInstant& deadline)
            {
                if (event->Recipient == propagationActor &&
                    event->GetTypeRewrite() == TEvents::TEvWakeup::EventType)
                {
                    ++retryTimers;
                }
                return previousScheduledFilter(
                    callbackRuntime, event, delay, deadline);
            });
        propagationActor = runtime->Register(new TPropagateLinkToFollowerActor(
            "test",
            CreateRequestInfo(sender, 0, MakeIntrusive<TCallContext>()),
            TLeaderFollowerLink{
                .LinkUUID = "link",
                .LeaderDiskId = "source",
                .FollowerDiskId = "target"},
            TPropagateLinkToFollowerActor::EReason::DataTransferred));
        runtime->EnableScheduleForActor(propagationActor);

        TAutoPtr<IEventHandle> response;
        runtime
            ->GrabEdgeEvent<TEvVolumePrivate::TEvLinkOnFollowerDataTransferred>(
                response, TDuration::Seconds(10));
        UNIT_ASSERT(response);
        UNIT_ASSERT(!HasError(
            response->Get<TEvVolumePrivate::TEvLinkOnFollowerDataTransferred>()
                ->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(4, attempts);
        UNIT_ASSERT_VALUES_EQUAL(attempts, retryTimers);
    }

    Y_UNIT_TEST(ShouldRemoveLeaderWhileCreationCommitIsPending)
    {
        for (const bool exactLink: {false, true}) {
            auto runtime = PrepareTestActorRuntime();
            TVolumeClient volume(*runtime);
            volume.CreateVolume(volume.CreateUpdateVolumeConfigRequest());
            volume.RecvUpdateVolumeConfigResponse();
            volume.WaitReady();

            auto makeLinkRequest =
                [](NProto::ELinkAction action, const TString& uuid)
            {
                auto request = std::make_unique<
                    TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                request->Record.SetDiskId("vol0");
                request->Record.SetLeaderDiskId("source");
                request->Record.SetLinkUUID(uuid);
                request->Record.SetAction(action);
                return request;
            };

            bool holdCommits = true;
            TVector<std::unique_ptr<IEventHandle>> commits;
            THashMap<ui64, ui32> responses;
            runtime->SetEventFilter(
                [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
                {
                    if (holdCommits &&
                        event->GetTypeRewrite() == TEvTablet::EvCommitResult &&
                        event->Get<TEvTablet::TEvCommitResult>()->TabletID ==
                            TestVolumeTablets[0])
                    {
                        commits.emplace_back(event.Release());
                        return true;
                    }
                    if (event->GetTypeRewrite() ==
                            TEvVolume::EvUpdateLinkOnFollowerResponse &&
                        event->Recipient == volume.GetSender())
                    {
                        responses[event->Cookie] =
                            event
                                ->Get<TEvVolume::
                                          TEvUpdateLinkOnFollowerResponse>()
                                ->GetStatus();
                        return true;
                    }
                    return false;
                });

            auto waitResponse = [&](ui64 cookie)
            {
                TDispatchOptions options;
                options.CustomFinalCondition = [&]
                {
                    return responses.contains(cookie);
                };
                runtime->DispatchEvents(options, TDuration::Seconds(1));
                UNIT_ASSERT_C(responses.contains(cookie), cookie);
                return responses.at(cookie);
            };

            volume.SendToPipe(
                makeLinkRequest(NProto::LINK_ACTION_CREATE, "test-link"), 1);
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !commits.empty();
            };
            runtime->DispatchEvents(options, TDuration::Seconds(1));
            UNIT_ASSERT(!commits.empty());
            UNIT_ASSERT(responses.empty());
            TVector<std::unique_ptr<IEventHandle>> creationCommits;
            creationCommits.swap(commits);

            volume.SendToPipe(
                makeLinkRequest(
                    NProto::LINK_ACTION_DESTROY, exactLink ? "test-link" : ""),
                2);
            volume.SendToPipe(
                makeLinkRequest(NProto::LINK_ACTION_COMPLETED, "test-link"), 3);
            UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, waitResponse(3));
            UNIT_ASSERT(!responses.contains(2));

            for (auto& commit: creationCommits) {
                runtime->Send(commit.release());
            }
            UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, waitResponse(1));
            UNIT_ASSERT(!responses.contains(2));

            // Creation has completed, but cancellation must keep exclusive
            // operations blocked until the removal is committed.
            volume.SendToPipe(
                makeLinkRequest(NProto::LINK_ACTION_CREATE, "next-link"), 4);
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, waitResponse(4));

            holdCommits = false;
            for (auto& commit: commits) {
                runtime->Send(commit.release());
            }
            UNIT_ASSERT_VALUES_EQUAL(S_OK, waitResponse(2));

            volume.ReconnectPipe();
            volume.WaitReady();
            volume.RebootTablet();
            volume.WaitReady();
            auto status =
                std::make_unique<TEvVolume::TEvGetLinkStatusRequest>();
            status->Record.SetDiskId("vol0");
            status->Record.SetLeaderDiskId("source");
            status->Record.SetFollowerDiskId("vol0");
            volume.SendToPipe(std::move(status));
            const ui32 linkStatus =
                volume.RecvResponse<TEvVolume::TEvGetLinkStatusResponse>()
                    ->Record.GetStatus();
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::LINK_STATUS_NOT_FOUND), linkStatus);

            volume.SendToPipe(
                makeLinkRequest(NProto::LINK_ACTION_CREATE, "next-link"), 5);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, waitResponse(5));
        }
    }

    Y_UNIT_TEST(ShouldRemoveLeaderWhilePromotionCommitIsPending)
    {
        CheckLeaderRemovalDuringPromotion(ELeaderRemovalPoint::PromotionCommit);
    }

    Y_UNIT_TEST(ShouldRejectUnlinkWhileRemovingSourceDiskIdTag)
    {
        CheckLeaderRemovalDuringPromotion(ELeaderRemovalPoint::TagRemoval);
    }

    Y_UNIT_TEST(ShouldRejectUnlinkAfterLeadershipTransfer)
    {
        CheckLeaderRemovalDuringPromotion(
            ELeaderRemovalPoint::DestructionDelay);
    }

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
            .LeaderCellId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerCellId = "su2"};
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
                .MediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
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

            follower.State = EState::LeadershipTransferred;
            response = volume1.UpdateFollowerState(follower);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                EState::LeadershipTransferred,
                response->Follower.State);
            UNIT_ASSERT_VALUES_EQUAL(2_MB, response->Follower.MigratedBytes);

            for (ui32 attempt = 0; attempt < 2; ++attempt) {
                volume1.SendUnlinkLeaderVolumeFromFollowerRequest(link);
                auto unlinked =
                    volume1.RecvUnlinkLeaderVolumeFromFollowerResponse();
                UNIT_ASSERT_VALUES_EQUAL(
                    E_INVALID_STATE,
                    unlinked->GetStatus());
                if (attempt == 0) {
                    volume1.RebootTablet();
                    volume1.WaitReady();
                }
            }

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

            // Delayed migration replies must not recreate the removed link.
            for (const auto state:
                 {TFollowerDiskInfo::EState::Preparing,
                  TFollowerDiskInfo::EState::DataReady,
                  TFollowerDiskInfo::EState::LeadershipTransferred,
                  TFollowerDiskInfo::EState::Error})
            {
                volume1.SendUpdateFollowerStateRequest(
                    TFollowerDiskInfo{
                        .Link = link,
                        .CreatedAt = TInstant::Now(),
                        .State = state});
                auto delayed = volume1.RecvUpdateFollowerStateResponse();
                UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, delayed->GetStatus());
                UNIT_ASSERT_VALUES_EQUAL(state, delayed->Follower.State);
            }

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
        auto followerConfig = volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD,
            7 * 1024,   // block count per partition
            "vol2");
        followerConfig->Record.MutablePartitions(0)->SetTabletId(
            TestPartitionTablets[1]);
        volume2.CreateVolume(std::move(followerConfig));
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
                // Drop the first two responses from the follower.
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
            .LeaderCellId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerCellId = "su2"};

        // Create link
        volume1.LinkLeaderVolumeToFollower(link);

        // Two responses are lost; the third completes link creation.
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
            .LeaderCellId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerCellId = "su2"};

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
        bool isMultipartition = false,
        bool verifySourcePartitionStopped = false,
        ECheckpointBeforeLink checkpoint = ECheckpointBeforeLink::None)
    {
        TActorId latestSourcePartitionActor;
        TActorId migrationSourcePartitionActor;
        TTestActorRuntimeBase::TRegistrationObserver prevRegistrationObserver;

        if (verifySourcePartitionStopped) {
            prevRegistrationObserver =
                fixture.Runtime->SetRegistrationObserverFunc({});
            fixture.Runtime->SetRegistrationObserverFunc(
                [&](TTestActorRuntimeBase& runtime,
                    const TActorId& parentId,
                    const TActorId& actorId)
                {
                    if (prevRegistrationObserver) {
                        prevRegistrationObserver(runtime, parentId, actorId);
                    }
                    if (migrationSourcePartitionActor) {
                        return;
                    }
                    auto* actor = runtime.FindActor(actorId);
                    if (dynamic_cast<TNonreplicatedPartitionActor*>(actor) ||
                        dynamic_cast<TMirrorPartitionActor*>(actor))
                    {
                        latestSourcePartitionActor = actorId;
                    } else if (dynamic_cast<TFollowerDiskActor*>(actor)) {
                        migrationSourcePartitionActor =
                            latestSourcePartitionActor;
                    }
                });
        }

        const ui32 leaderPartitionCount =
            isMultipartition && leaderMediaType == NProto::STORAGE_MEDIA_SSD
                ? 2
                : 1;
        const ui32 followerPartitionCount =
            isMultipartition && followerMediaType == NProto::STORAGE_MEDIA_SSD
                ? 2
                : 1;

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
            fixture.VolumeBlockCount /
                leaderPartitionCount,   // block count per partition
            "vol1",
            "cloud",
            "folder",
            leaderPartitionCount,              // partition count
            leaderPartitionCount > 1 ? 2 : 0   // blocks per stripe
            ));
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
            fixture.VolumeBlockCount /
                followerPartitionCount,   // block count per partition
            "vol2",
            "cloud",
            "folder",
            followerPartitionCount,              // partition count,
            followerPartitionCount > 1 ? 2 : 0   // blocks per stripe
            ));
        volume2.WaitReady();

        // Write some blocks to leader before migration starts.
        for (ui64 pos = 0; pos < fixture.VolumeBlockCount; pos += 2048) {
            volume1.WriteBlocks(
                TBlockRange64::MakeOneBlock(pos),
                clientInfo1.GetClientId(),
                'a');
            fixture.SetExpectedBlockContent(
                TBlockRange64::MakeOneBlock(pos),
                'a');
        }

        if (checkpoint != ECheckpointBeforeLink::None) {
            fixture.CreateCheckpoint(leaderMediaType);
        }
        if (checkpoint == ECheckpointBeforeLink::Deleted) {
            fixture.DeleteCheckpoint();
        }

        // Link volumes
        TLeaderFollowerLink link{
            .LinkUUID = "",
            .LeaderDiskId = "vol1",
            .LeaderCellId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerCellId = "su2"};
        {
            volume1.SendLinkLeaderVolumeToFollowerRequest(link);
            auto response = volume1.RecvLinkLeaderVolumeToFollowerResponse();

            if (checkpoint == ECheckpointBeforeLink::DataPresent) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_TRY_AGAIN,
                    response->GetStatus(),
                    response->GetErrorReason());
                return;
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
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
        size_t writtenBlockCount = 0;
        for (ui64 pos = 0; pos < fixture.VolumeBlockCount; pos += 2048) {
            bool success = fixture.WriteBlocks(TBlockRange64::MakeOneBlock(pos + 1));
            writtenBlockCount += success ? 1 : 0;
            success = fixture.ZeroBlocks(TBlockRange64::MakeOneBlock(pos + 2));
            writtenBlockCount += success ? 1 : 0;
        }
        UNIT_ASSERT_LE(1, writtenBlockCount);

        // Waiting for the follower disk to fill up.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return followerState == TFollowerDiskInfo::EState::LeadershipTransferred;
            };
            fixture.Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT_EQUAL(
                TFollowerDiskInfo::EState::LeadershipTransferred,
                followerState);
            // It is necessary to reconnect the pipe as the previous one broke
            // when the volume was restarted.
            volume1.ReconnectPipe();
            volume1.WaitReady();
        }

        // The source stays fenced while copied data is checked.
        volume1.SendUnlinkLeaderVolumeFromFollowerRequest(link);
        const auto unlinked =
            volume1.RecvUnlinkLeaderVolumeFromFollowerResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, unlinked->GetStatus());
        fixture.CheckCopiedData();

        if (verifySourcePartitionStopped) {
            const bool sourcePartitionStopped =
                migrationSourcePartitionActor &&
                !fixture.Runtime->FindActor(migrationSourcePartitionActor);

            fixture.Runtime->SetRegistrationObserverFunc(
                std::move(prevRegistrationObserver));

            UNIT_ASSERT_C(
                migrationSourcePartitionActor,
                "Failed to capture the source partition used by migration");
            UNIT_ASSERT_C(
                sourcePartitionStopped,
                TStringBuilder()
                    << "Source partition " << migrationSourcePartitionActor
                    << " is still alive after leader cutover");
        }
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_SSD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD);
    }

    Y_UNIT_TEST_F(ShouldRejectLinkWhileCheckpointDataIsPresent, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            false,
            false,
            ECheckpointBeforeLink::DataPresent);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolumeAfterCheckpointDeletion, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            false,
            false,
            ECheckpointBeforeLink::Deleted);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_NRD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_NRD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_MIRROR, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_MIRROR3);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_MIRROR_SSD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            NProto::STORAGE_MEDIA_SSD);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_MultiPartition_NRD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            true);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_SSD_MultiPartition, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            true);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_SSD_SSD, TFixture)
    {
        /* TODO(drbasic) vol1 partition can't reboot. Need to figure out
        why. DoShouldPrepareFollowerVolume( *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD);
        */
    }

    void DoShouldPrepareFollowerVolumeWithReboot(
        TFixture& fixture,
        NProto::EStorageMediaKind leaderMediaType,
        NProto::EStorageMediaKind followerMediaType,
        bool isMultipartition = false)
    {
        using EState = TFollowerDiskInfo::EState;

        const ui32 leaderPartitionCount =
            isMultipartition && leaderMediaType == NProto::STORAGE_MEDIA_SSD
                ? 2
                : 1;
        const ui32 followerPartitionCount =
            isMultipartition && followerMediaType == NProto::STORAGE_MEDIA_SSD
                ? 2
                : 1;

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

                if (msg->Follower.State == EState::Preparing &&
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
            fixture.VolumeBlockCount /
                leaderPartitionCount,   // block count per partition
            "vol1",
            "cloud",
            "folder",
            leaderPartitionCount,              // partition count
            leaderPartitionCount > 1 ? 2 : 0   // blocks per stripe
            ));
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
            fixture.VolumeBlockCount /
                followerPartitionCount,   // block count per partition
            "vol2",
            "cloud",
            "folder",
            followerPartitionCount,              // partition count
            followerPartitionCount > 1 ? 2 : 0   // blocks per stripe
            ));
        volume2.WaitReady();

        // Write some blocks to leader before migration starts.
        for (ui64 pos = 0; pos < fixture.VolumeBlockCount; pos += 2048) {
            volume1.WriteBlocks(
                TBlockRange64::MakeOneBlock(pos),
                clientInfo1.GetClientId(),
                'a');
            fixture.SetExpectedBlockContent(
                TBlockRange64::MakeOneBlock(pos),
                'a');
        }

        // Link volumes
        TLeaderFollowerLink link{
            .LinkUUID = "",
            .LeaderDiskId = "vol1",
            .LeaderCellId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerCellId = "su2"};
        {
            auto response = volume1.LinkLeaderVolumeToFollower(link);
            link.LinkUUID = response->Record.GetLinkUUID();

            UNIT_ASSERT_EQUAL(EState::Preparing, followerState);

            // It is necessary to reconnect the pipe as the previous one broke
            // when the partition was restarted.
            volume1.ReconnectPipe();
            volume1.AddClient(clientInfo1);
        };

        // Send WriteBlocks to leader during migration
        size_t writtenBlockCount = 0;
        for (ui64 pos = 0;; pos += 2048) {
            bool success = fixture.WriteBlocks(
                TBlockRange64::MakeOneBlock(
                    ((pos + 1) % fixture.VolumeBlockCount)),
                &followerState);
            writtenBlockCount += success ? 1 : 0;

            TDispatchOptions options;
            std::optional<ui64> oldMigratedBytes = migratedBytes;
            options.CustomFinalCondition = [&]
            {
                return migratedBytes != oldMigratedBytes;
            };
            fixture.Runtime->DispatchEvents(
                options,
                TDuration::MilliSeconds(100));

            if (followerState == EState::DataReady ||
                followerState == EState::LeadershipTransferred)
            {
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
        UNIT_ASSERT_LE(1, writtenBlockCount);

        for (const auto& restart: restarts) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                true,
                restart.Done,
                TStringBuilder()
                    << "Restart at " << restart.Position << " failed");
        }

        {   // The source disk should be deleted after 30 seconds.
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return deletionOfTheSourceHasBeenInitiated;
            };
            fixture.Runtime->AdvanceCurrentTime(TDuration::Seconds(30));
            fixture.Runtime->DispatchEvents(options, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(true, deletionOfTheSourceHasBeenInitiated);
        }

        volume1.ReconnectPipe();
        volume1.WaitReady();

        // The source stays fenced while copied data is checked.
        volume1.SendUnlinkLeaderVolumeFromFollowerRequest(link);
        const auto unlinked =
            volume1.RecvUnlinkLeaderVolumeFromFollowerResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, unlinked->GetStatus());
        volume1.ReconnectPipe();
        fixture.CheckCopiedData();
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

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_MutiSSD_NRD_WithReboot, TFixture)
    {
        DoShouldPrepareFollowerVolumeWithReboot(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            true);
    }

    Y_UNIT_TEST_F(ShouldPrepareFollowerVolume_NRD_MutiSSD_WithReboot, TFixture)
    {
        DoShouldPrepareFollowerVolumeWithReboot(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            true);
    }

    Y_UNIT_TEST_F(ShouldStopSourcePartitionAfterMigration_NRD, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD,
            false,   // isMultipartition
            true     // verifySourcePartitionStopped
        );
    }

    Y_UNIT_TEST_F(ShouldStopSourcePartitionAfterMigration_MIRROR, TFixture)
    {
        DoShouldPrepareFollowerVolume(
            *this,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            NProto::STORAGE_MEDIA_SSD,
            false,   // isMultipartition
            true     // verifySourcePartitionStopped
        );
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
            }

            if (event->GetTypeRewrite() ==
                TEvVolume::EvUpdateLinkOnFollowerRequest)
            {
                auto* msg =
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                propagatedAction = msg->Record.GetAction();
            }

            if (event->GetTypeRewrite() == TEvVolume::EvWaitReadyRequest) {
                auto* msg = event->Get<TEvVolume::TEvWaitReadyRequest>();
                if (msg->Record.GetDiskId() == "vol1") {
                    leaderVolumeActorId = event->Recipient;
                }
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
        volume1.WaitReady("vol1");

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
                .LeaderCellId = "su1",
                .FollowerDiskId = "vol2",
                .FollowerCellId = "su2"};

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
                return followerState == TFollowerDiskInfo::EState::LeadershipTransferred;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT_EQUAL(
                TFollowerDiskInfo::EState::LeadershipTransferred,
                followerState);

            // Leader volume actor should be poisoned
            UNIT_ASSERT_VALUES_EQUAL(true, leaderPoisoned);
            leaderPoisoned = false;

            // It is necessary to reconnect the pipe as the previous one broke
            // when the partition was restarted.
            volume1.ReconnectPipe();
            volume1.WaitReady("vol1");
            volume1.AddClient(clientInfo1);
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
            Runtime->AdvanceCurrentTime(TDuration::Seconds(30));
            Runtime->DispatchEvents(options, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(1, volumeDestructionRequestCount);
        }

        // Waiting for the leader volume actor poisoned by
        // FollowerPartitionActor after 30 seconds.
        {
            UNIT_ASSERT_VALUES_EQUAL(false, leaderPoisoned);
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return leaderPoisoned;
            };
            Runtime->AdvanceCurrentTime(TDuration::Seconds(30));
            Runtime->DispatchEvents(options, TDuration::Seconds(1));
            Runtime->AdvanceCurrentTime(TDuration::Seconds(30));
            Runtime->DispatchEvents(options, TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(true, leaderPoisoned);
        }

        // Waiting for the leader disk destruction retried after reject.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return volumeDestructionRequestCount == 2;
            };
            Runtime->AdvanceCurrentTime(TDuration::Seconds(30));
            Runtime->DispatchEvents(options, TDuration::Seconds(1));
            Runtime->AdvanceCurrentTime(TDuration::Seconds(30));
            Runtime->DispatchEvents(options, TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(2, volumeDestructionRequestCount);
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
                .LeaderCellId = "su1",
                .FollowerDiskId = "vol2",
                .FollowerCellId = "su2"};

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
                .LeaderCellId = "su1",
                .FollowerDiskId = "vol2",
                .FollowerCellId = "su2"};

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

    Y_UNIT_TEST_F(
        ShouldPerformIoWithPredefinedCopyVolumeClientIdWhenPreparing,
        TFixture)
    {
        TTestActorRuntimeBase::TEventFilter prevFilter;
        std::optional<TFollowerDiskInfo::EState> followerState;
        std::optional<ui64> migratedBytes;
        bool stealDataTransferredPropagation = true;
        std::unique_ptr<IEventHandle> stolenDataTransferredRequest;
        std::unique_ptr<IEventHandle> tagRemovalRequest;

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

            if (event->GetTypeRewrite() ==
                TEvVolume::EvUpdateLinkOnFollowerRequest)
            {
                const auto* msg =
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();

                if (stealDataTransferredPropagation &&
                    msg->Record.GetAction() ==
                        NProto::ELinkAction::LINK_ACTION_COMPLETED)
                {
                    stolenDataTransferredRequest.reset(event.Release());
                    return true;
                }
            }

            if (event->GetTypeRewrite() == TEvService::EvRemoveTagsRequest) {
                tagRemovalRequest.reset(event.Release());
                return true;
            }

            return prevFilter(runtime, event);
        };
        prevFilter = Runtime->SetEventFilter(followerStateFilter);

        // Leader
        TVolumeClient volume1(*Runtime, 0, TestVolumeTablets[0]);
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD,
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

        // Follower
        TVolumeClient volume2(*Runtime, 0, TestVolumeTablets[1]);
        auto followerConfigRequest = volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            VolumeBlockCount,   // block count per partition
            "vol2",
            "cloud",
            "folder",
            1,                                  // partitionCount
            0,                                  // blocksPerStripe
            MakeTag("source-disk-id", "vol1")   // Created as follower for vol1
        );
        auto followerConfig = followerConfigRequest->Record;
        volume2.CreateVolume(std::move(followerConfigRequest));
        volume2.WaitReady();
        Volumes["vol2"] = {
            .VolumeClient = &volume2,
            .VolumeClientInfo = nullptr};

        auto checkRead = [](TVolumeClient& volume,
                            const TStringBuf& clientId,
                            EWellKnownResultCodes expectedResponse)
        {
            volume.SendReadBlocksRequest(
                TBlockRange64::MakeOneBlock(0),
                TString(clientId));
            auto response = volume.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedResponse,
                response->GetStatus(),
                FormatError(response->GetError()));
        };
        auto checkWrite = [&](TVolumeClient& volume,
                              const TStringBuf& clientId,
                              EWellKnownResultCodes expectedResponse)
        {
            volume.SendWriteBlocksRequest(
                TBlockRange64::MakeOneBlock(VolumeBlockCount - 1),
                TString(clientId),
                'a');
            auto response = volume.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedResponse,
                response->GetStatus(),
                FormatError(response->GetError()));
        };
        auto checkZero = [&](TVolumeClient& volume,
                             const TStringBuf& clientId,
                             EWellKnownResultCodes expectedResponse)
        {
            volume.SendZeroBlocksRequest(
                TBlockRange64::MakeOneBlock(VolumeBlockCount - 1),
                TString(clientId));
            auto response = volume.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedResponse,
                response->GetStatus(),
                FormatError(response->GetError()));
        };

        {       // volume1 and volume2 are not linked
            {   // vol1 accepted IO with "normal" clientId.
                checkRead(volume1, clientInfo1.GetClientId(), S_OK);
                checkWrite(volume1, clientInfo1.GetClientId(), S_OK);
                checkZero(volume1, clientInfo1.GetClientId(), S_OK);
            }
            {   // vol1 rejected IO with CopyVolumeClientId.
                checkRead(volume1, CopyVolumeClientId, E_ARGUMENT);
                checkWrite(volume1, CopyVolumeClientId, E_REJECTED);
                checkZero(volume1, CopyVolumeClientId, E_REJECTED);
            }
            {   // vol2 accepted write-IO with CopyVolumeClientId.
                checkRead(volume2, CopyVolumeClientId, E_ARGUMENT);
                checkWrite(volume2, CopyVolumeClientId, S_OK);
                checkZero(volume2, CopyVolumeClientId, S_OK);
            }
            {   // vol2 rejected IO with "normal" clientId.
                checkRead(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
                checkWrite(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
                checkZero(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
            }
        }

        // Create link
        TLeaderFollowerLink link{
            .LinkUUID = "",
            .LeaderDiskId = "vol1",
            .FollowerDiskId = "vol2"};
        {
            // Create link
            auto response = volume1.LinkLeaderVolumeToFollower(link);
            link.LinkUUID = response->Record.GetLinkUUID();

            volume1.ReconnectPipe();
            volume1.AddClient(clientInfo1);
        }

        {   // State of leader ELeadershipStatus::Principal
            // State of follower ELeadershipStatus::Follower
            {   // Leader accepted IO with "normal" clientid.
                checkRead(volume1, clientInfo1.GetClientId(), S_OK);
                checkWrite(volume1, clientInfo1.GetClientId(), S_OK);
                checkZero(volume1, clientInfo1.GetClientId(), S_OK);
            }
            {   // Leader declined IO with predefined CopyVolumeClientId
                checkRead(volume1, CopyVolumeClientId, E_ARGUMENT);
                checkWrite(volume1, CopyVolumeClientId, E_REJECTED);
                checkZero(volume1, CopyVolumeClientId, E_REJECTED);
            }
            {   // Follower accepted IO with predefined CopyVolumeClientId
                // only for WriteBlocks and ZeroBlocks.
                checkRead(volume2, CopyVolumeClientId, E_ARGUMENT);
                checkWrite(volume2, CopyVolumeClientId, S_OK);
                checkZero(volume2, CopyVolumeClientId, S_OK);
            }
            {   // Follower rejected IO with "normal" clientId.
                checkRead(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
                checkWrite(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
                checkZero(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
            }
        }

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

            // Wait for propagation ELinkAction::LINK_ACTION_COMPLETED request
            options.CustomFinalCondition = [&]() -> bool
            {
                return stolenDataTransferredRequest != nullptr;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT(stolenDataTransferredRequest);
        }

        {   // State of leader ELeadershipStatus::LeadershipTransferring
            // State of follower ELeadershipStatus::Follower

            // Leader rejected IO from "normal" client
            {
                checkRead(volume1, clientInfo1.GetClientId(), E_REJECTED);
                checkWrite(volume1, clientInfo1.GetClientId(), E_REJECTED);
                checkZero(volume1, clientInfo1.GetClientId(), E_REJECTED);
            }
            {   // vol1 rejected IO with CopyVolumeClientId.
                checkRead(volume1, CopyVolumeClientId, E_ARGUMENT);
                checkWrite(volume1, CopyVolumeClientId, E_PRECONDITION_FAILED);
                checkZero(volume1, CopyVolumeClientId, E_PRECONDITION_FAILED);
            }
            {   // Follower accepted IO with predefined CopyVolumeClientId
                // only for WriteBlocks and ZeroBlocks.
                checkRead(volume2, CopyVolumeClientId, E_ARGUMENT);
                checkWrite(volume2, CopyVolumeClientId, S_OK);
                checkZero(volume2, CopyVolumeClientId, S_OK);
            }
            {   // Follower rejected IO with "normal" clientId.
                checkRead(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
                checkWrite(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
                checkZero(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
            }
        }

        // Waiting for leadership transferring.
        {
            // return stolen propagate DataTransferred request.
            stealDataTransferredPropagation = false;
            Runtime->Send(stolenDataTransferredRequest.release());

            // Emulate RemoveTags: the fake StorageService does not process
            // requests, so persist the new config before replying ourselves.
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return tagRemovalRequest != nullptr;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT(tagRemovalRequest);

            followerConfig.MutableVolumeConfig()->ClearTagsStr();
            followerConfig.MutableVolumeConfig()->SetVersion(
                followerConfig.GetVolumeConfig().GetVersion() + 1);
            auto updateConfig =
                std::make_unique<TEvBlockStore::TEvUpdateVolumeConfig>();
            updateConfig->Record = followerConfig;
            volume2.SendToPipe(std::move(updateConfig));
            auto updated = volume2.RecvUpdateVolumeConfigResponse();
            UNIT_ASSERT(updated->Record.GetStatus() == NKikimrBlockStore::OK);
            Runtime->Send(new IEventHandle(
                tagRemovalRequest->Sender,
                tagRemovalRequest->Recipient,
                new TEvService::TEvRemoveTagsResponse(),
                0,
                tagRemovalRequest->Cookie));

            options.CustomFinalCondition = [&]
            {
                return followerState ==
                       TFollowerDiskInfo::EState::LeadershipTransferred;
            };
            Runtime->DispatchEvents(options, TDuration::Seconds(10));
            UNIT_ASSERT_EQUAL(
                TFollowerDiskInfo::EState::LeadershipTransferred,
                followerState);

            // It is necessary to reconnect the pipe as the previous one broke
            // when the volume was restarted.
            volume1.ReconnectPipe();
            volume1.WaitReady();
        }

        {   // State of leader ELeadershipStatus::LeadershipTransferring
            // State of follower ELeadershipStatus::Follower

            // Leader declined IO from "normal" client
            {
                checkRead(
                    volume1,
                    clientInfo1.GetClientId(),
                    E_BS_INVALID_SESSION);
                checkWrite(
                    volume1,
                    clientInfo1.GetClientId(),
                    E_BS_INVALID_SESSION);
                checkZero(
                    volume1,
                    clientInfo1.GetClientId(),
                    E_BS_INVALID_SESSION);
            }

            {   // vol1 rejected IO with CopyVolumeClientId.
                checkRead(volume1, CopyVolumeClientId, E_ARGUMENT);
                checkWrite(volume1, CopyVolumeClientId, E_PRECONDITION_FAILED);
                checkZero(volume1, CopyVolumeClientId, E_PRECONDITION_FAILED);
            }

            {   // vol2 rejected IO with CopyVolumeClientId.
                checkRead(volume2, CopyVolumeClientId, E_ARGUMENT);
                checkWrite(volume2, CopyVolumeClientId, E_REJECTED);
                checkZero(volume2, CopyVolumeClientId, E_REJECTED);
            }

            {   // vol2 rejected IO with unknown clientid.
                checkRead(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
                checkWrite(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
                checkZero(volume2, clientInfo1.GetClientId(), E_BS_INVALID_SESSION);
            }

            volume2.AddClient(clientInfo1);
            {   // vol2 accepted IO with "normal" clientId.
                checkRead(volume2, clientInfo1.GetClientId(), S_OK);
                checkWrite(volume2, clientInfo1.GetClientId(), S_OK);
                checkZero(volume2, clientInfo1.GetClientId(), S_OK);
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldCompleteGracefulShutdownDuringFollowerMigration,
        TFixture)
    {
        TActorId latestSourcePartitionActor;
        TActorId migrationSourcePartitionActor;
        TActorId followerDiskActor;
        TActorId followerPartitionActor;

        auto prevRegistrationObserver =
            Runtime->SetRegistrationObserverFunc({});
        Runtime->SetRegistrationObserverFunc(
            [&](TTestActorRuntimeBase& runtime,
                const TActorId& parentId,
                const TActorId& actorId)
            {
                if (prevRegistrationObserver) {
                    prevRegistrationObserver(runtime, parentId, actorId);
                }

                auto* actor = runtime.FindActor(actorId);
                if (!followerDiskActor &&
                    (dynamic_cast<TNonreplicatedPartitionActor*>(actor) ||
                     dynamic_cast<TMirrorPartitionActor*>(actor)))
                {
                    latestSourcePartitionActor = actorId;
                } else if (
                    !followerDiskActor &&
                    dynamic_cast<TFollowerDiskActor*>(actor))
                {
                    followerDiskActor = actorId;
                    migrationSourcePartitionActor = latestSourcePartitionActor;
                } else if (
                    followerDiskActor && !followerPartitionActor &&
                    parentId == followerDiskActor)
                {
                    followerPartitionActor = actorId;
                }
            });

        TTestActorRuntimeBase::TEventFilter prevEventFilter;
        TTestActorRuntimeBase::TScheduledEventFilter prevScheduledEventFilter;
        std::optional<TFollowerDiskInfo::EState> followerState;
        bool followerResponseDropped = false;
        bool shutdownTimeoutScheduled = false;
        auto scheduledEventFilter =
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event,
                TDuration delay,
                TInstant& deadline)
        {
            if (event->GetTypeRewrite() == TEvents::TEvWakeup::EventType &&
                event->Recipient == followerPartitionActor)
            {
                shutdownTimeoutScheduled = true;
            }

            return prevScheduledEventFilter
                       ? prevScheduledEventFilter(
                             runtime,
                             event,
                             delay,
                             deadline)
                       : false;
        };
        prevScheduledEventFilter =
            Runtime->SetScheduledEventFilter(scheduledEventFilter);

        auto eventFilter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvUpdateFollowerStateResponse)
            {
                const auto* msg = event->Get<
                    TEvVolumePrivate::TEvUpdateFollowerStateResponse>();
                followerState = msg->Follower.State;
            }

            const bool isFollowerWriteResponse =
                event->GetTypeRewrite() == TEvService::EvWriteBlocksResponse ||
                event->GetTypeRewrite() == TEvService::EvZeroBlocksResponse;
            if (!followerResponseDropped && followerPartitionActor &&
                event->Recipient == followerPartitionActor &&
                isFollowerWriteResponse)
            {
                followerResponseDropped = true;
                return true;
            }

            return prevEventFilter ? prevEventFilter(runtime, event) : false;
        };
        prevEventFilter = Runtime->SetEventFilter(eventFilter);

        TVolumeClient volume1(*Runtime, 0, TestVolumeTablets[0]);
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            VolumeBlockCount,
            "vol1"));
        volume1.WaitReady();

        auto clientInfo1 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume1.AddClient(clientInfo1);
        Volumes["vol1"] = {
            .VolumeClient = &volume1,
            .VolumeClientInfo = &clientInfo1};

        TVolumeClient volume2(*Runtime, 0, TestVolumeTablets[1]);
        volume2.CreateVolume(volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            NProto::STORAGE_MEDIA_SSD,
            VolumeBlockCount,
            "vol2"));
        volume2.WaitReady();
        Volumes["vol2"] = {
            .VolumeClient = &volume2,
            .VolumeClientInfo = nullptr};

        volume1.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo1.GetClientId(),
            'a');

        TLeaderFollowerLink link{
            .LinkUUID = "",
            .LeaderDiskId = "vol1",
            .LeaderCellId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerCellId = "su2"};
        auto linkResponse = volume1.LinkLeaderVolumeToFollower(link);
        link.LinkUUID = linkResponse->Record.GetLinkUUID();

        volume1.ReconnectPipe();
        volume1.AddClient(clientInfo1);

        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return followerResponseDropped;
        };
        Runtime->DispatchEvents(options, TDuration::Seconds(10));

        UNIT_ASSERT_C(followerState, "Follower state was not observed");
        UNIT_ASSERT_VALUES_EQUAL(
            TFollowerDiskInfo::EState::Preparing,
            *followerState);
        UNIT_ASSERT_C(
            migrationSourcePartitionActor && followerDiskActor &&
                followerPartitionActor,
            "Failed to capture the follower migration actor tree");
        UNIT_ASSERT_C(
            followerResponseDropped,
            "Failed to leave a follower request in flight");

        volume1.SendGracefulShutdownRequest();
        options.CustomFinalCondition = [&]
        {
            return shutdownTimeoutScheduled;
        };
        Runtime->DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT_C(
            shutdownTimeoutScheduled,
            "Follower partition did not schedule its shutdown timeout");

        Runtime->AdvanceCurrentTime(TDuration::Minutes(1));
        auto shutdownResponse =
            volume1.RecvGracefulShutdownResponse(TDuration::Seconds(1));

        const bool sourcePartitionStopped =
            !Runtime->FindActor(migrationSourcePartitionActor);
        const bool followerDiskStopped = !Runtime->FindActor(followerDiskActor);
        const bool followerPartitionStopped =
            !Runtime->FindActor(followerPartitionActor);

        Runtime->SetEventFilter(std::move(prevEventFilter));
        Runtime->SetScheduledEventFilter(
            std::move(prevScheduledEventFilter));
        Runtime->SetRegistrationObserverFunc(
            std::move(prevRegistrationObserver));

        UNIT_ASSERT_C(
            shutdownResponse,
            "Graceful shutdown did not complete after a stuck follower "
            "request");
        UNIT_ASSERT_C(
            sourcePartitionStopped,
            "Source partition is still alive after graceful shutdown");
        UNIT_ASSERT_C(
            followerDiskStopped,
            "Follower disk actor is still alive after graceful shutdown");
        UNIT_ASSERT_C(
            followerPartitionStopped,
            "Follower partition adapter is still alive after graceful "
            "shutdown");
    }

    void DoShouldOwnSourcePartitionAfterRecovery(
        TFixture & fixture,
        TFollowerDiskInfo::EState persistedState,
        bool retryTransfer = false, bool completeTransfer = true)
    {
        auto config = std::make_shared<TStorageConfig>(
            NProto::TStorageServiceConfig{},
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto sourcePartitionActor =
            fixture.Runtime->Register(new TPoisonableTestActor());
        const auto leaderVolumeActor = fixture.Runtime->AllocateEdgeActor();
        const auto poisoner = fixture.Runtime->AllocateEdgeActor();

        auto followerDiskActor = std::make_unique<TFollowerDiskActor>(
            TLogTitle(
                GetCycleCount(),
                TLogTitle::TVolume{
                    .TabletId = TestVolumeTablets[0],
                    .DiskId = "vol1"}),
            std::move(config),
            CreateTestDiagnosticsConfig(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            TFollowerDiskActorParams{
                .LeaderMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                .LeaderDiskId = "vol1",
                .LeaderBlockCount = fixture.VolumeBlockCount,
                .LeaderBlockSize = DefaultBlockSize,
                .LeaderVolumeActorId = leaderVolumeActor,
                .LeaderPartitionActorId = sourcePartitionActor,
                .ClientId = "client",
                .FollowerDiskInfo = {
                    .Link =
                        {.LinkUUID = "link-id",
                         .LeaderDiskId = "vol1",
                         .LeaderCellId = "leader-cell",
                         .FollowerDiskId = "vol2",
                         .FollowerCellId = "follower-cell"},
                    .CreatedAt = TInstant::Now(),
                    .State = persistedState,
                    .MediaKind = NProto::STORAGE_MEDIA_SSD}});

        const auto followerDiskActorId =
            fixture.Runtime->Register(followerDiskActor.release());

        ui32 propagationAttempts = 0;
        const TVector<TDuration> retryDelays{
            TDuration::Seconds(1),
            TDuration::Seconds(2),
            TDuration::Seconds(4),
            TDuration::Seconds(8),
            TDuration::Seconds(15),
            TDuration::Seconds(15)};
        std::optional<TFollowerDiskInfo::EState> requestedState;
        if (retryTransfer) {
            fixture.Runtime->EnableScheduleForActor(followerDiskActorId);
            fixture.Runtime->SetEventFilter(
                [&](TTestActorRuntimeBase& runtime,
                    TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvVolume::EvUpdateLinkOnFollowerRequest)
                    {
                        ++propagationAttempts;
                        // Keep the regression bounded if errors are retried
                        // immediately instead of waiting for a timer.
                        if (propagationAttempts > retryDelays.size() + 1) {
                            return true;
                        }
                        runtime.SendAsync(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            new TEvVolume::TEvUpdateLinkOnFollowerResponse(
                                MakeError(
                                    completeTransfer &&
                                            propagationAttempts ==
                                                retryDelays.size() + 1
                                        ? S_ALREADY
                                        : E_CANCELLED)), 0, event->Cookie));
                        return true;
                    }
                    if (event->GetTypeRewrite() ==
                        TEvVolumePrivate::EvUpdateFollowerStateRequest)
                    {
                        requestedState =
                            event
                                ->Get<TEvVolumePrivate::
                                          TEvUpdateFollowerStateRequest>()
                                ->Follower.State;
                    }
                    return false;
                });
        }

        fixture.Runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        if (retryTransfer) {
            UNIT_ASSERT_VALUES_EQUAL(1, propagationAttempts);
            UNIT_ASSERT(!requestedState);
            for (size_t i = 0; i < retryDelays.size(); ++i) {
                fixture.Runtime->AdvanceCurrentTime(retryDelays[i]);
                fixture.Runtime->DispatchEvents(
                    {}, TDuration::MilliSeconds(10));
                UNIT_ASSERT_VALUES_EQUAL(i + 2, propagationAttempts);
                if (i + 1 < retryDelays.size() || !completeTransfer) {
                    UNIT_ASSERT(!requestedState);
                }
            }
            if (completeTransfer) {
                UNIT_ASSERT(requestedState);
                UNIT_ASSERT_VALUES_EQUAL(
                    TFollowerDiskInfo::EState::LeadershipTransferred,
                    *requestedState);
            }
        }

        const ui32 attemptsBeforeShutdown = propagationAttempts;
        fixture.Runtime->Send(new IEventHandle(
            followerDiskActorId, poisoner, new TEvents::TEvPoisonPill()));

        TAutoPtr<IEventHandle> handle;
        fixture.Runtime->GrabEdgeEvent<TEvents::TEvPoisonTaken>(
            handle, TDuration::Seconds(1));

        UNIT_ASSERT_C(handle, "Follower disk actor did not complete shutdown");
        UNIT_ASSERT_C(
            !fixture.Runtime->FindActor(sourcePartitionActor),
            TStringBuilder() << "Source partition " << sourcePartitionActor
                             << " is still alive after recovery from "
                             << ToString(persistedState));
        if (retryTransfer) {
            fixture.Runtime->AdvanceCurrentTime(TDuration::Seconds(30));
            fixture.Runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
            UNIT_ASSERT_VALUES_EQUAL(
                attemptsBeforeShutdown, propagationAttempts);
        }
    }

    Y_UNIT_TEST_F(ShouldIgnoreCancelledFollowerUpdatesDuringShutdown, TFixture)
    {
        auto config = std::make_shared<TStorageConfig>(
            NProto::TStorageServiceConfig{},
            std::make_shared<NFeatures::TFeaturesConfig>());
        const auto sourcePartitionActor = Runtime->AllocateEdgeActor();
        const auto leaderVolumeActor = Runtime->AllocateEdgeActor();
        const auto poisoner = Runtime->AllocateEdgeActor();
        ui32 leaderPoisonRequests = 0;
        ui32 propagationAttempts = 0;
        Runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);

                if (event->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType &&
                    event->Recipient == leaderVolumeActor)
                {
                    ++leaderPoisonRequests;
                }
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    ++propagationAttempts;
                    return true;
                }
                return false;
            });
        Runtime->SetScheduledEventFilter(
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event,
                TDuration delay,
                TInstant& deadline)
            {
                Y_UNUSED(runtime);
                Y_UNUSED(delay);
                Y_UNUSED(deadline);

                if (event->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType &&
                    event->Recipient == leaderVolumeActor)
                {
                    ++leaderPoisonRequests;
                }
                return false;
            });

        const auto followerDiskActor = Runtime->Register(new TFollowerDiskActor(
            TLogTitle(
                GetCycleCount(),
                TLogTitle::TVolume{
                    .TabletId = TestVolumeTablets[0],
                    .DiskId = "vol1"}),
            std::move(config),
            CreateTestDiagnosticsConfig(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            TFollowerDiskActorParams{
                .LeaderMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                .LeaderDiskId = "vol1",
                .LeaderBlockCount = VolumeBlockCount,
                .LeaderBlockSize = DefaultBlockSize,
                .LeaderVolumeActorId = leaderVolumeActor,
                .LeaderPartitionActorId = sourcePartitionActor,
                .ClientId = "client",
                .FollowerDiskInfo = {
                    .Link =
                        {.LinkUUID = "link-id",
                         .LeaderDiskId = "vol1",
                         .FollowerDiskId = "vol2"},
                    .CreatedAt = TInstant::Now(),
                    .State = TFollowerDiskInfo::EState::DataReady,
                    .MediaKind = NProto::STORAGE_MEDIA_SSD}}));

        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return propagationAttempts == 1;
        };
        Runtime->DispatchEvents(options, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, propagationAttempts);

        auto checkCancelledUpdates = [&]
        {
            for (const auto state:
                 {TFollowerDiskInfo::EState::DataReady,
                  TFollowerDiskInfo::EState::LeadershipTransferred,
                  TFollowerDiskInfo::EState::Error})
            {
                auto response = std::make_unique<
                    TEvVolumePrivate::TEvUpdateFollowerStateResponse>(
                    MakeError(E_CANCELLED, "Follower link was removed"));
                response->Follower.State = state;
                Runtime->Send(new IEventHandle(
                    followerDiskActor,
                    leaderVolumeActor,
                    response.release()));
            }

            // A forwarded message confirms that the preceding replies were handled.
            Runtime->Send(new IEventHandle(
                followerDiskActor,
                leaderVolumeActor,
                new TEvVolume::TEvRWClientIdChanged("client")));
            TAutoPtr<IEventHandle> handle;
            Runtime->GrabEdgeEvent<TEvVolume::TEvRWClientIdChanged>(
                handle,
                TDuration::Seconds(1));
            UNIT_ASSERT(handle);
            UNIT_ASSERT_VALUES_EQUAL(0, leaderPoisonRequests);
            UNIT_ASSERT_VALUES_EQUAL(1, propagationAttempts);
        };

        checkCancelledUpdates();
        Runtime->Send(new IEventHandle(
            followerDiskActor,
            poisoner,
            new TEvents::TEvPoisonPill()));
        TAutoPtr<IEventHandle> poison;
        Runtime->GrabEdgeEvent<TEvents::TEvPoisonPill>(
            poison,
            TDuration::Seconds(1));
        UNIT_ASSERT(poison);
        UNIT_ASSERT(poison->Recipient == sourcePartitionActor);
        UNIT_ASSERT(Runtime->FindActor(followerDiskActor));

        checkCancelledUpdates();
        Runtime->Send(new IEventHandle(
            followerDiskActor,
            sourcePartitionActor,
            new TEvents::TEvPoisonTaken()));
        TAutoPtr<IEventHandle> stopped;
        Runtime->GrabEdgeEvent<TEvents::TEvPoisonTaken>(
            stopped,
            TDuration::Seconds(1));
        UNIT_ASSERT(stopped);
        UNIT_ASSERT(stopped->Recipient == poisoner);
    }

    Y_UNIT_TEST_F(ShouldOwnSourcePartitionAfterDataReadyRecovery, TFixture)
    {
        DoShouldOwnSourcePartitionAfterRecovery(
            *this,
            TFollowerDiskInfo::EState::DataReady);
    }

    Y_UNIT_TEST_F(
        ShouldRetryCancelledLeadershipTransferAndAcceptAlready,
        TFixture)
    {
        DoShouldOwnSourcePartitionAfterRecovery(
            *this,
            TFollowerDiskInfo::EState::DataReady,
            true);
    }

    Y_UNIT_TEST_F(
        ShouldBackOffPersistentCancelledLeadershipTransfer,
        TFixture)
    {
        DoShouldOwnSourcePartitionAfterRecovery(
            *this,
            TFollowerDiskInfo::EState::DataReady,
            true,
            false);
    }

    Y_UNIT_TEST_F(
        ShouldOwnSourcePartitionAfterLeadershipTransferredRecovery,
        TFixture)
    {
        DoShouldOwnSourcePartitionAfterRecovery(
            *this,
            TFollowerDiskInfo::EState::LeadershipTransferred);
    }

    Y_UNIT_TEST_F(ShouldOwnSourcePartitionAfterErrorRecovery, TFixture)
    {
        DoShouldOwnSourcePartitionAfterRecovery(
            *this,
            TFollowerDiskInfo::EState::Error);
    }

    template <typename TRequest>
    void CheckRequestForwardedToLeaderPartition(
        TTestActorRuntime& runtime,
        TActorId followerDiskActor,
        TActorId leaderPartitionActor,
        ui64 cookie)
    {
        const auto statisticsCollector = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(
            followerDiskActor,
            statisticsCollector,
            new TRequest(),
            0,   // flags
            cookie));

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TRequest>(handle, TDuration::Seconds(1));

        UNIT_ASSERT_C(
            handle,
            TStringBuilder() << TypeName<TRequest>()
                             << " was not forwarded to the leader partition");
        UNIT_ASSERT_VALUES_EQUAL(leaderPartitionActor, handle->Recipient);
        // The leader partition must reply directly to the collector.
        UNIT_ASSERT_VALUES_EQUAL(statisticsCollector, handle->Sender);
        UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
    }

    void DoShouldForwardPullStatisticsRequestsToLeaderPartition(
        TFixture& fixture,
        TFollowerDiskInfo::EState persistedState)
    {
        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetUsePullSchemeForVolumeStatistics(true);
        auto config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>());

        const auto leaderPartitionActor = fixture.Runtime->AllocateEdgeActor();
        const auto leaderVolumeActor = fixture.Runtime->AllocateEdgeActor();

        const auto followerDiskActor =
            fixture.Runtime->Register(new TFollowerDiskActor(
                TLogTitle(
                    GetCycleCount(),
                    TLogTitle::TVolume{
                        .TabletId = TestVolumeTablets[0],
                        .DiskId = "vol1"}),
                std::move(config),
                CreateTestDiagnosticsConfig(),
                CreateProfileLogStub(),
                CreateBlockDigestGeneratorStub(),
                TFollowerDiskActorParams{
                    .LeaderMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    .LeaderDiskId = "vol1",
                    .LeaderBlockCount = fixture.VolumeBlockCount,
                    .LeaderBlockSize = DefaultBlockSize,
                    .LeaderVolumeActorId = leaderVolumeActor,
                    .LeaderPartitionActorId = leaderPartitionActor,
                    .ClientId = "client",
                    .FollowerDiskInfo = {
                        .Link =
                            {.LinkUUID = "link-id",
                             .LeaderDiskId = "vol1",
                             .LeaderCellId = "leader-cell",
                             .FollowerDiskId = "vol2",
                             .FollowerCellId = "follower-cell"},
                        .CreatedAt = TInstant::Now(),
                        .State = persistedState,
                        .MediaKind = NProto::STORAGE_MEDIA_SSD}}));

        fixture.Runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // DiskRegistry-based leader.
        CheckRequestForwardedToLeaderPartition<
            TEvNonreplPartitionPrivate::
                TEvGetDiskRegistryBasedPartCountersRequest>(
            *fixture.Runtime,
            followerDiskActor,
            leaderPartitionActor,
            1);

        // BlobStorage-based leader.
        CheckRequestForwardedToLeaderPartition<
            TEvPartitionCommonPrivate::TEvGetPartCountersRequest>(
            *fixture.Runtime,
            followerDiskActor,
            leaderPartitionActor,
            2);
    }

    Y_UNIT_TEST_F(
        ShouldForwardPullStatisticsRequestsToLeaderPartitionWhilePreparing,
        TFixture)
    {
        DoShouldForwardPullStatisticsRequestsToLeaderPartition(
            *this,
            TFollowerDiskInfo::EState::Preparing);
    }

    Y_UNIT_TEST_F(
        ShouldForwardPullStatisticsRequestsToLeaderPartitionWhenDataReady,
        TFixture)
    {
        DoShouldForwardPullStatisticsRequestsToLeaderPartition(
            *this,
            TFollowerDiskInfo::EState::DataReady);
    }

    Y_UNIT_TEST_F(
        ShouldForwardPullStatisticsRequestsToLeaderPartitionWhenLeadershipTransferred,
        TFixture)
    {
        DoShouldForwardPullStatisticsRequestsToLeaderPartition(
            *this,
            TFollowerDiskInfo::EState::LeadershipTransferred);
    }

    Y_UNIT_TEST_F(
        ShouldForwardPullStatisticsRequestsToLeaderPartitionInErrorState,
        TFixture)
    {
        DoShouldForwardPullStatisticsRequestsToLeaderPartition(
            *this,
            TFollowerDiskInfo::EState::Error);
    }

    void DoShouldPullLeaderStatisticsWhileFollowerIsPreparing(
        TFixture& fixture,
        NProto::EStorageMediaKind leaderMediaType,
        NProto::EStorageMediaKind followerMediaType)
    {
        auto& runtime = *fixture.Runtime;

        std::optional<TFollowerDiskInfo::EState> followerState;
        auto followerStateObserver = runtime.AddObserver<
            TEvVolumePrivate::TEvUpdateFollowerStateResponse>(
            [&](TEvVolumePrivate::TEvUpdateFollowerStateResponse::TPtr& ev)
            { followerState = ev->Get()->Follower.State; });

        TVolumeClient volume1(runtime, 0, TestVolumeTablets[0]);
        volume1.CreateVolume(volume1.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            leaderMediaType,
            fixture.VolumeBlockCount,
            "vol1"));
        volume1.WaitReady();

        auto clientInfo1 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume1.AddClient(clientInfo1);
        fixture.Volumes["vol1"] = {
            .VolumeClient = &volume1,
            .VolumeClientInfo = &clientInfo1};

        TVolumeClient volume2(runtime, 0, TestVolumeTablets[1]);
        volume2.CreateVolume(volume2.CreateUpdateVolumeConfigRequest(
            0,
            0,
            0,
            0,
            false,
            1,
            followerMediaType,
            fixture.VolumeBlockCount,
            "vol2"));
        volume2.WaitReady();
        fixture.Volumes["vol2"] = {
            .VolumeClient = &volume2,
            .VolumeClientInfo = nullptr};

        TLeaderFollowerLink link{
            .LinkUUID = "",
            .LeaderDiskId = "vol1",
            .LeaderCellId = "su1",
            .FollowerDiskId = "vol2",
            .FollowerCellId = "su2"};
        volume1.LinkLeaderVolumeToFollower(link);
        UNIT_ASSERT_EQUAL(TFollowerDiskInfo::EState::Preparing, followerState);

        // The partitions were restarted to wrap them with the follower actor.
        volume1.ReconnectPipe();
        volume1.AddClient(clientInfo1);

        UNIT_ASSERT(fixture.WriteBlocks(TBlockRange64::MakeOneBlock(0)));

        ui32 followerDiskRequestCount = 0;
        ui32 followerPartitionRequestCount = 0;
        auto onRequest = [&](const TActorId& recipient)
        {
            auto* actor = runtime.FindActor(recipient);
            if (dynamic_cast<TFollowerDiskActor*>(actor)) {
                ++followerDiskRequestCount;
            }
            if (dynamic_cast<TVolumeAsPartitionActor*>(actor)) {
                ++followerPartitionRequestCount;
            }
        };
        auto requestObserver = runtime.AddObserver<
            TEvPartitionCommonPrivate::TEvGetPartCountersRequest>(
            [&](TEvPartitionCommonPrivate::TEvGetPartCountersRequest::TPtr& ev)
            { onRequest(ev->GetRecipientRewrite()); });
        auto drRequestObserver =
            runtime.AddObserver<TEvNonreplPartitionPrivate::
                                    TEvGetDiskRegistryBasedPartCountersRequest>(
                [&](TEvNonreplPartitionPrivate::
                        TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev)
                { onRequest(ev->GetRecipientRewrite()); });

        TStringBuilder errors;
        auto responseObserver = runtime.AddObserver<
            TEvPartitionCommonPrivate::TEvPartCountersCombined>(
            [&](TEvPartitionCommonPrivate::TEvPartCountersCombined::TPtr& ev)
            {
                if (HasError(ev->Get()->Error)) {
                    errors << FormatError(ev->Get()->Error) << "; ";
                }
            });
        auto drResponseObserver = runtime.AddObserver<
            TEvNonreplPartitionPrivate::
                TEvGetDiskRegistryBasedPartCountersResponse>(
            [&](TEvNonreplPartitionPrivate::
                    TEvGetDiskRegistryBasedPartCountersResponse::TPtr& ev)
            {
                if (HasError(ev->Get()->Error)) {
                    errors << FormatError(ev->Get()->Error) << "; ";
                }
            });

        volume1.SendToPipe(std::make_unique<
                           TEvStatsService::TEvGetServiceStatisticsRequest>());

        TAutoPtr<IEventHandle> handle;
        auto* response = runtime.GrabEdgeEventRethrow<
            TEvStatsService::TEvGetServiceStatisticsResponse>(
            handle,
            TDuration::Seconds(5));
        UNIT_ASSERT(handle);

        // Make sure the statistics were pulled while the data was transferring.
        UNIT_ASSERT_EQUAL(TFollowerDiskInfo::EState::Preparing, followerState);

        UNIT_ASSERT_LT(0, followerDiskRequestCount);
        // The follower volume reports its own statistics.
        UNIT_ASSERT_VALUES_EQUAL(0, followerPartitionRequestCount);
        UNIT_ASSERT_C(errors.empty(), errors);

        ui64 writeBytes = 0;
        for (const auto& partCounters: response->PartsCounters) {
            UNIT_ASSERT_VALUES_EQUAL("vol1", partCounters.DiskId);
            writeBytes += partCounters.DiskCounters->RequestCounters.WriteBlocks
                              .RequestBytes;
        }
        UNIT_ASSERT_LE(DefaultBlockSize, writeBytes);
    }

    Y_UNIT_TEST_F(
        ShouldPullLeaderStatisticsWhileFollowerIsPreparing_SSD_NRD,
        TPullStatisticsFixture)
    {
        DoShouldPullLeaderStatisticsWhileFollowerIsPreparing(
            *this,
            NProto::STORAGE_MEDIA_SSD,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST_F(
        ShouldPullLeaderStatisticsWhileFollowerIsPreparing_NRD_SSD,
        TPullStatisticsFixture)
    {
        DoShouldPullLeaderStatisticsWhileFollowerIsPreparing(
            *this,
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
