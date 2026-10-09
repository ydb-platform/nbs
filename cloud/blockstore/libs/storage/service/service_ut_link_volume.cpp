#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/testlib/disk_agent_mock.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/volume/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/blockstore/private/api/protos/checkpoints.pb.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/core/protos/schemeshard/operations.pb.h>
#include <contrib/ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceLinkVolumeTest)
{
    struct TExecutorQueueProbe
    {
        bool Block = true;
        bool SeedExecuted = false;
        bool CancellationHandled = false;
        ui32 DestructionRequests = 0;
        TVector<std::unique_ptr<IEventHandle>> Activations;
    };

    class TExecutorQueueSeedTx final
        : public NKikimr::NTabletFlatExecutor::ITransaction
    {
    private:
        const std::shared_ptr<TExecutorQueueProbe> Probe;

    public:
        explicit TExecutorQueueSeedTx(
            std::shared_ptr<TExecutorQueueProbe> probe)
            : Probe(std::move(probe))
        {}

        bool Execute(TTransactionContext& tx, const TActorContext& ctx) override
        {
            Y_UNUSED(tx);
            Y_UNUSED(ctx);
            Probe->SeedExecuted = true;
            return true;
        }

        void Complete(const TActorContext& ctx) override
        {
            Y_UNUSED(ctx);
        }
    };

    class TExecutorQueueSeedActor final
        : public TActorBootstrapped<TExecutorQueueSeedActor>
    {
    private:
        NKikimr::NTabletFlatExecutor::NFlatExecutorSetup::IExecutor* const
            Executor;
        const TActorId ExecutorId;
        const std::shared_ptr<TExecutorQueueProbe> Probe;

    public:
        TExecutorQueueSeedActor(
            NKikimr::NTabletFlatExecutor::NFlatExecutorSetup::IExecutor*
                executor, TActorId executorId,
            std::shared_ptr<TExecutorQueueProbe> probe)
            : Executor(executor)
            , ExecutorId(executorId)
            , Probe(std::move(probe))
        {}

        void Bootstrap(const TActorContext& ctx)
        {
            const TActorContext executorCtx(ctx.Mailbox, ctx.ExecutorThread,
                                            ctx.EventStart, ExecutorId);
            // Enqueue, not Execute: the seed creates a real activation queue.
            Executor->Enqueue(new TExecutorQueueSeedTx(Probe), executorCtx);
            Die(ctx);
        }
    };

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
                NProto::STORAGE_MEDIA_DEFAULT, ui64 blocksCount = 1024 * 1024,
            bool createOnTarget = false, bool disableGc = false)
        {
            // Cleanup is scheduled to the symbolic service ID. The test
            // runtime otherwise whitelists only the registered actor IDs.
            Env.GetRuntime().EnableScheduleForActor(MakeStorageServiceId());
            NProto::TStorageServiceConfig proto;
            (*proto.MutableShardDirectories())["source"] = "/local/nbs";
            (*proto.MutableShardDirectories())["target"] = "/local/remote";
            (*proto.MutableShardDirectories())["source-alias"] = "/local/nbs/";
            (*proto.MutableShardDirectories())["target-alias"] =
                "/local/remote/";
            proto.SetDisableStartPartitionsForGc(disableGc);
            proto.SetAllocationUnitNonReplicatedSSD(1);
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

            if (createOnTarget) {
                Target->CreateVolume(
                    "disk-copy",
                    blocksCount,
                    DefaultBlockSize,
                    "",
                    "",
                    targetKind == NProto::STORAGE_MEDIA_DEFAULT ? mediaKind
                                                                : targetKind);
                return;
            }

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

        NProto::TGetLinkStatusResponse GetStatusFor(
            TServiceClient& client, const TString& leader,
            const TString& leaderShard, const TString& follower,
            const TString& followerShard)
        {
            NProto::TGetLinkStatusRequest request;
            request.SetLeaderDiskId(leader);
            request.SetFollowerDiskId(follower);
            request.SetLeaderShardId(leaderShard);
            request.SetFollowerShardId(followerShard);
            TString json;
            UNIT_ASSERT(
                google::protobuf::util::MessageToJsonString(request, &json)
                    .ok());
            for (ui32 attempt = 0; attempt != 100; ++attempt) {
                client.SendExecuteActionRequest("GetLinkStatus", json);
                const auto response = client.RecvExecuteActionResponse();
                if (SUCCEEDED(response->GetStatus())) {
                    NProto::TGetLinkStatusResponse status;
                    UNIT_ASSERT_C(
                        google::protobuf::util::JsonStringToMessage(
                            response->Record.GetOutput(), &status)
                            .ok(), response->Record.GetOutput());
                    return status;
                }
                UNIT_ASSERT_C(
                    response->GetStatus() == E_REJECTED ||
                        response->GetStatus() == E_TIMEOUT ||
                        response->GetStatus() == E_BS_INVALID_SESSION ||
                        response->GetStatus() == E_TRY_AGAIN,
                    response->GetErrorReason());
                Env.GetRuntime().DispatchEvents({},
                                                TDuration::MilliSeconds(100));
            }
            UNIT_FAIL("Link status did not recover after tablet restart");
            return {};
        }

        NProto::TGetLinkStatusResponse GetStatus(const TString& leader = "disk")
        {
            return GetStatusFor(*Source, leader, "source", "disk-copy",
                                "target");
        }

        NProto::ELinkStatus GetTargetStatus()
        {
            for (ui32 attempt = 0; attempt != 100; ++attempt) {
                auto request =
                    std::make_unique<TEvVolume::TEvGetLinkStatusRequest>();
                request->Record.SetDiskId("disk-copy");
                request->Record.SetLeaderDiskId("disk");
                request->Record.SetLeaderShardId("source");
                request->Record.SetFollowerDiskId("disk-copy");
                request->Record.SetFollowerShardId("target");
                request->Record.MutableHeaders()->SetShardId("target");
                request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
                Source->SendRequest(MakeVolumeProxyServiceId(),
                                    std::move(request));
                const auto response =
                    Source->RecvResponse<TEvVolume::TEvGetLinkStatusResponse>();
                if (SUCCEEDED(response->GetStatus())) {
                    return response->Record.GetStatus();
                }
                UNIT_ASSERT_C(
                    response->GetStatus() == E_REJECTED ||
                        response->GetStatus() == E_TRY_AGAIN ||
                        response->GetStatus() == E_TIMEOUT,
                    response->GetErrorReason());
                Env.GetRuntime().DispatchEvents({},
                                                TDuration::MilliSeconds(100));
            }
            UNIT_FAIL("Target link status did not recover");
            return NProto::LINK_STATUS_NOT_FOUND;
        }

        ui64 GetTabletId(const TString& disk, const TString& shard)
        {
            Source->SendRequest(
                MakeSSProxyServiceId(),
                std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
                    disk, true, shard));
            const auto response =
                Source->RecvResponse<TEvSSProxy::TEvDescribeVolumeResponse>();
            UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()),
                          response->GetErrorReason());
            return response->PathDescription.GetBlockStoreVolumeDescription()
                .GetVolumeTabletId();
        }
    };

    Y_UNIT_TEST(ShouldRestoreDurableCancellationAfterSourceRestart)
    {
        TCrossShardFixture fixture;
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> lostDestroy;
        bool captured = false, restoredAck = false;
        ui32 attempts = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvVolume::EvUpdateLinkOnFollowerRequest &&
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                            ->Record.GetAction() == NProto::LINK_ACTION_DESTROY)
                {
                    ++attempts;
                    if (!captured) {
                        captured = true;
                        lostDestroy.reset(event.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvLinkOnFollowerDestroyed)
                {
                    restoredAck = SUCCEEDED(
                        event
                            ->Get<
                                TEvVolumePrivate::TEvLinkOnFollowerDestroyed>()
                            ->GetStatus());
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.DestroyLink()->GetStatus());
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return lostDestroy != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(lostDestroy);
        const auto cancelled =
            lostDestroy->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                ->Record;
        UNIT_ASSERT(cancelled.GetLinkUUID());
        NKikimr::RebootTablet(runtime, fixture.GetTabletId("disk", "source"),
                              fixture.Source->GetSender(), fixture.SourceNode);
        // Either recovery or a repeat request must redeliver the retained UUID.
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, fixture.DestroyLink()->GetStatus());
        options.CustomFinalCondition = [&]
        {
            return restoredAck;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(restoredAck && attempts >= 2);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(fixture.GetTargetStatus()));
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        auto stale =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        stale->Record = cancelled;
        fixture.Source->SendRequest(MakeVolumeProxyServiceId(),
                                    std::move(stale));
        UNIT_ASSERT_C(
            SUCCEEDED(
                fixture.Source
                    ->RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>()
                    ->GetStatus()), "Old cancel retry failed");
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(fixture.GetStatus().GetStatus()));
    }

    Y_UNIT_TEST(ShouldRetainCancellationUntilDestinationAckIsCommitted)
    {
        TCrossShardFixture fixture;
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> acknowledgement;
        bool captured = false, done = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (!captured &&
                    event->GetTypeRewrite() ==
                        TEvVolumePrivate::EvLinkOnFollowerDestroyed)
                {
                    captured = true;
                    acknowledgement.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (captured && event->GetTypeRewrite() ==
                                    TEvVolumePrivate::EvLinkOnFollowerDestroyed)
                {
                    done = true;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.DestroyLink()->GetStatus());
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return acknowledgement != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(acknowledgement);
        NKikimr::RebootTablet(runtime, fixture.GetTabletId("disk", "source"),
                              fixture.Source->GetSender(), fixture.SourceNode);
        options.CustomFinalCondition = [&]
        {
            return done;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(done);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        // The old owner's ACK cannot remove the new generation.
        acknowledgement.reset();
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(fixture.GetStatus().GetStatus()));
    }

    Y_UNIT_TEST(ShouldRejectCreateForRecreatedDestinationIncarnation)
    {
        TCrossShardFixture fixture;
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> oldCreate;
        bool captured = false, cancelled = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (!captured &&
                    event->GetTypeRewrite() ==
                        TEvVolume::EvUpdateLinkOnFollowerRequest &&
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                            ->Record.GetAction() == NProto::LINK_ACTION_CREATE)
                {
                    captured = true;
                    oldCreate.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvLinkOnFollowerDestroyed)
                {
                    cancelled = true;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        auto request =
            fixture.Source->CreateCreateVolumeLinkRequest("disk", "disk-copy");
        request->Record.SetLeaderShardId("source");
        request->Record.SetFollowerShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return oldCreate != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(oldCreate);
        const auto oldRecord =
            oldCreate->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()->Record;
        UNIT_ASSERT_VALUES_EQUAL(fixture.GetTabletId("disk-copy", "target"),
                                 oldRecord.GetFollowerTabletId());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.DestroyLink()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            fixture.Source->RecvCreateVolumeLinkResponse()->GetStatus());
        options.CustomFinalCondition = [&]
        {
            return cancelled;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(cancelled);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        fixture.Target->DestroyVolume("disk-copy", false, false, 0, true);
        fixture.Target->CreateVolume("disk-copy", 1024 * 1024);
        UNIT_ASSERT(
            fixture.GetTabletId("disk-copy", "target") !=
            oldRecord.GetFollowerTabletId());
        auto stale =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        stale->Record = oldRecord;
        fixture.Source->SendRequest(MakeVolumeProxyServiceId(),
                                    std::move(stale));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            fixture.Source
                ->RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>()
                ->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(fixture.GetTargetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        runtime.Send(oldCreate.release(), fixture.SourceNode);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(fixture.GetStatus().GetStatus()));
    }

    Y_UNIT_TEST(ShouldRejectConditionalLocalDrDeleteBeforeMutations)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                                   NProto::STORAGE_MEDIA_SSD,
                                   1_GB / DefaultBlockSize);
        auto& runtime = fixture.Env.GetRuntime();
        ui32 mutations = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvDiskRegistry::EvMarkDiskForCleanupRequest ||
                    event->GetTypeRewrite() ==
                        TEvDiskRegistry::EvDeallocateDiskRequest ||
                    event->GetTypeRewrite() ==
                        TEvVolume::EvGracefulShutdownRequest)
                {
                    ++mutations;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        auto request = fixture.Source->CreateDestroyVolumeRequest(
            "disk", false, false, 0, true);
        request->Record.SetExpectedVolumeTabletId(
            fixture.GetTabletId("disk", "source"));
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_IMPLEMENTED,
            fixture.Source->RecvDestroyVolumeResponse()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, mutations);
        fixture.Source->DescribeVolume("disk", true);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        // The established, unconditional local DR API remains supported.
        fixture.Source->DestroyVolume("disk", false, false, 0, true);
    }

    Y_UNIT_TEST(ShouldNotMutateDrReplacementBetweenGuardedDescribeAndStat)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_SSD,
                                   1_GB / DefaultBlockSize);
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> heldStat;
        bool captured = false;
        ui32 mutations = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (!captured &&
                    event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->Get<TEvService::TEvStatVolumeRequest>()
                            ->Record.GetDiskId() == "disk")
                {
                    captured = true;
                    heldStat.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (event->GetTypeRewrite() ==
                        TEvDiskRegistry::EvMarkDiskForCleanupRequest ||
                    event->GetTypeRewrite() ==
                        TEvDiskRegistry::EvDeallocateDiskRequest ||
                    event->GetTypeRewrite() ==
                        TEvVolume::EvGracefulShutdownRequest)
                {
                    ++mutations;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        const auto original = fixture.GetTabletId("disk", "source");
        auto request = fixture.Source->CreateDestroyVolumeRequest(
            "disk", false, false, 0, true);
        request->Record.SetExpectedVolumeTabletId(original);
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return heldStat != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(heldStat);
        fixture.Source->DestroyVolume("disk", false, false, 0, true);
        fixture.Source->CreateVolume("disk", 1_GB / DefaultBlockSize,
                                     DefaultBlockSize, "", "",
                                     NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        UNIT_ASSERT(fixture.GetTabletId("disk", "source") != original);
        const auto recipient = heldStat->GetRecipientRewrite();
        const auto sender = heldStat->Sender;
        const auto cookie = heldStat->Cookie;
        auto body = heldStat->ReleaseBase();
        heldStat.reset();
        runtime.Send(new IEventHandle(recipient, sender, body.Release(), 0,
                                      cookie), fixture.SourceNode);
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_IMPLEMENTED,
            fixture.Source->RecvDestroyVolumeResponse()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, mutations);
        fixture.Source->DescribeVolume("disk", true);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    void TestCleanupHandoffAfterCancellation(bool queuedPrincipal)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_HDD, 8192, false,
                                   true);
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> oldDelete;
        bool captured = false, responseObserved = false, newDelete = false;
        TString oldUuid;
        TActorId targetOwner;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvVolumePrivate::EvUpdateFollowerStateRequest &&
                    event->Get<
                             TEvVolumePrivate::TEvUpdateFollowerStateRequest>()
                            ->Follower.Link.LeaderDiskId == "disk")
                {
                    oldUuid = event
                                  ->Get<TEvVolumePrivate::
                                            TEvUpdateFollowerStateRequest>()
                                  ->Follower.Link.LinkUUID;
                }
                if (event->GetTypeRewrite() ==
                    TEvService::EvDestroyVolumeRequest) {
                    const auto& record =
                        event->Get<TEvService::TEvDestroyVolumeRequest>()
                            ->Record;
                    if (!captured && record.GetDiskId() == "disk") {
                        captured = true;
                        targetOwner = event->Sender;
                        oldDelete.reset(event.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    if (record.GetDiskId() == "source-b") {
                        newDelete = true;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return oldDelete != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(60));
        UNIT_ASSERT(oldDelete && oldUuid && targetOwner);
        const auto oldCookie = oldDelete->Cookie;
        std::shared_ptr<TExecutorQueueProbe> queue;
        if (queuedPrincipal) {
            auto* tablet = dynamic_cast<
                NKikimr::NTabletFlatExecutor::NFlatExecutorSetup::ITablet*>(
                runtime.FindActor(targetOwner));
            UNIT_ASSERT(tablet);
            const auto executorId = tablet->ExecutorID();
            auto* executor = dynamic_cast<
                NKikimr::NTabletFlatExecutor::NFlatExecutorSetup::IExecutor*>(
                runtime.FindActor(executorId));
            UNIT_ASSERT(executor);
            queue = std::make_shared<TExecutorQueueProbe>();
            runtime.SetObserverFunc(
                [&, queue, executorId](TAutoPtr<IEventHandle>& event)
                {
                    if (queue->Block &&
                        event->GetRecipientRewrite() == executorId &&
                        event->GetTypeRewrite() ==
                            EventSpaceBegin(NKikimr::TKikimrEvents::ES_PRIVATE))
                    {
                        queue->Activations.emplace_back(event.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    if (event->GetTypeRewrite() ==
                            TEvService::EvDestroyVolumeResponse &&
                        event->GetRecipientRewrite() == targetOwner &&
                        event->Cookie == oldCookie)
                    {
                        responseObserved = true;
                    }
                    if (event->GetTypeRewrite() ==
                            TEvService::EvDestroyVolumeRequest &&
                        event->Get<TEvService::TEvDestroyVolumeRequest>()
                                ->Record.GetDiskId() == "source-b")
                    {
                        newDelete = true;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });
            runtime.Register(
                new TExecutorQueueSeedActor(executor, executorId, queue),
                fixture.TargetNode);
            options.CustomFinalCondition = [queue]
            {
                return !queue->Activations.empty();
            };
            runtime.DispatchEvents(options, TDuration::Seconds(3));
            UNIT_ASSERT(!queue->Activations.empty() && !queue->SeedExecuted);
        }
        NTestVolume::TVolumeClient target(
            runtime, fixture.TargetNode,
            fixture.GetTabletId("disk-copy", "target"));
        auto cancel =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        auto& record = cancel->Record;
        record.SetDiskId("disk-copy");
        record.SetLeaderDiskId("disk");
        record.SetLeaderShardId("source");
        record.SetFollowerShardId("target");
        record.SetLinkUUID(oldUuid);
        record.SetAction(NProto::LINK_ACTION_DESTROY);
        record.SetRequireCancellable(false);
        target.SendToPipe(std::move(cancel));
        if (queuedPrincipal) {
            // A same-pipe status reply proves the remove handler ran while its
            // transaction was still queued, before the delete response arrives.
            auto status =
                std::make_unique<TEvVolume::TEvGetLinkStatusRequest>();
            status->Record.SetDiskId("disk-copy");
            status->Record.SetLinkUUID(oldUuid);
            target.SendToPipe(std::move(status));
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED),
                static_cast<int>(
                    target.RecvGetLinkStatusResponse()->Record.GetStatus()));
            runtime.Send(
                new IEventHandle(targetOwner,
                                 runtime.AllocateEdgeActor(fixture.TargetNode),
                                 new TEvService::TEvDestroyVolumeResponse(), 0,
                                 oldCookie), fixture.TargetNode, true);
            options.CustomFinalCondition = [&]
            {
                return responseObserved;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(3));
            UNIT_ASSERT(responseObserved && !queue->SeedExecuted);
            queue->Block = false;
            for (auto& activation: queue->Activations) {
                runtime.Send(activation.release(), fixture.TargetNode);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK, target.RecvUpdateLinkOnFollowerResponse()->GetStatus());
        fixture.Source->CreateVolume("source-b", 8192);
        auto create = fixture.Source->CreateCreateVolumeLinkRequest(
            "source-b", "disk-copy");
        create->Record.SetLeaderShardId("source");
        create->Record.SetFollowerShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(create));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK, fixture.Source->RecvCreateVolumeLinkResponse()->GetStatus());
        bool complete = false;
        for (ui32 attempt = 0; attempt != 1000; ++attempt) {
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            if (fixture
                    .GetStatusFor(*fixture.Source, "source-b", "source",
                                  "disk-copy", "target")
                    .GetStatus() == NProto::LINK_STATUS_COMPLETED)
            {
                complete = true;
                break;
            }
        }
        UNIT_ASSERT(complete && newDelete);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        // The old response must not consume the next generation's state.
        runtime.Send(
            new IEventHandle(targetOwner,
                             runtime.AllocateEdgeActor(fixture.TargetNode),
                             new TEvService::TEvDestroyVolumeResponse(), 0,
                             oldCookie), fixture.TargetNode);
        fixture.Source->DescribeVolume("disk", true);
    }

    Y_UNIT_TEST(ShouldReleaseCleanupSlotOnCommittedUnrestrictedCancel)
    {
        TestCleanupHandoffAfterCancellation(false);
    }

    Y_UNIT_TEST(ShouldHandoffCleanupAfterRejectedQueuedPrincipalTransition)
    {
        TestCleanupHandoffAfterCancellation(true);
    }

    Y_UNIT_TEST(ShouldKeepExistingLocalDrCopyCleanupWorkflow)
    {
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1);
        TTestEnvState state;
        TTestEnv env(1, 1, 4, 1, state);
        const auto node = SetupTestEnv(env, config);
        auto& runtime = env.GetRuntime();
        runtime.EnableScheduleForActor(MakeStorageServiceId());
        // The service fixture's default DR devices are not backed by a data
        // agent. Register accessible devices for the actual legacy copy.
        google::protobuf::RepeatedPtrField<NProto::TDeviceConfig> devices;
        for (auto& device: state.DiskRegistryState->Devices) {
            device.SetNodeId(runtime.GetNodeId(node));
            *devices.Add() = device;
        }
        runtime.RegisterService(
            MakeDiskAgentServiceId(runtime.GetNodeId(node)),
            runtime.Register(new TDiskAgentMock(std::move(devices)), node),
            node);
        TServiceClient client(runtime, node);
        client.CreateVolume("legacy", 1_GB / DefaultBlockSize, DefaultBlockSize,
                            "", "", NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        client.CreateVolume("legacy-copy", 1_GB / DefaultBlockSize,
                            DefaultBlockSize, "", "",
                            NProto::STORAGE_MEDIA_SSD);
        bool oldMode = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvService::EvDestroyVolumeRequest &&
                    event->Get<TEvService::TEvDestroyVolumeRequest>()
                            ->Record.GetDiskId() == "legacy")
                {
                    oldMode = event->Get<TEvService::TEvDestroyVolumeRequest>()
                                  ->Record.GetExpectedVolumeTabletId() == 0;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        client.CreateVolumeLink("legacy", "legacy-copy");
        NProto::TGetLinkStatusRequest query;
        query.SetLeaderDiskId("legacy");
        query.SetFollowerDiskId("legacy-copy");
        TString json;
        UNIT_ASSERT(
            google::protobuf::util::MessageToJsonString(query, &json).ok());
        bool complete = false;
        for (ui32 attempt = 0; attempt != 2000; ++attempt) {
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            client.SendExecuteActionRequest("GetLinkStatus", json);
            const auto response = client.RecvExecuteActionResponse();
            if (HasError(response->GetError())) {
                UNIT_ASSERT_C(
                    response->GetStatus() == E_REJECTED ||
                        response->GetStatus() == E_TRY_AGAIN ||
                        response->GetStatus() == E_TIMEOUT,
                    response->GetErrorReason());
                continue;
            }
            NProto::TGetLinkStatusResponse status;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                            response->Record.GetOutput(), &status)
                            .ok());
            if (status.GetStatus() == NProto::LINK_STATUS_COMPLETED) {
                complete = true;
                break;
            }
        }
        UNIT_ASSERT(complete && oldMode);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        client.SendDescribeVolumeRequest("legacy", true);
        UNIT_ASSERT(HasError(client.RecvDescribeVolumeResponse()->GetError()));
        client.DescribeVolume("legacy-copy", true);
    }

    Y_UNIT_TEST(ShouldRejectRemoteServiceRequestsBeforeLocalSessionLookup)
    {
        TCrossShardFixture fixture;
        const auto mounted = fixture.Source->MountVolume("disk");
        fixture.Source->WriteBlocks("disk", TBlockRange64::MakeOneBlock(0),
                                    mounted->Record.GetSessionId(), 'a');
        // A local cached session must not make an unsupported remote request
        // bypass the shard boundary, even with the same physical DiskId.
#define EXPECT_REMOTE_REJECTED(name)                                           \
    {                                                                          \
        auto request = std::make_unique<TEvService::TEv##name##Request>();     \
        request->Record.SetDiskId("disk");                                     \
        request->Record.MutableHeaders()->SetShardId("target");                \
        fixture.Source->SendRequest(MakeStorageServiceId(),                    \
                                    std::move(request));                       \
        const auto response = fixture.Source->Recv##name##Response();          \
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_IMPLEMENTED, response->GetStatus());    \
    }
        EXPECT_REMOTE_REJECTED(ResizeVolume)
        EXPECT_REMOTE_REJECTED(AlterVolume)
        EXPECT_REMOTE_REJECTED(AssignVolume)
        EXPECT_REMOTE_REJECTED(MountVolume)
        EXPECT_REMOTE_REJECTED(UnmountVolume)
        EXPECT_REMOTE_REJECTED(ReadBlocks)
        EXPECT_REMOTE_REJECTED(WriteBlocks)
        EXPECT_REMOTE_REJECTED(ZeroBlocks)
        EXPECT_REMOTE_REJECTED(CreateCheckpoint)
        EXPECT_REMOTE_REJECTED(DeleteCheckpoint)
#undef EXPECT_REMOTE_REJECTED
        auto resize =
            fixture.Source->CreateResizeVolumeRequest("disk", 2 * 1024 * 1024);
        resize->Record.MutableHeaders()->SetShardId("unknown");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(resize));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            fixture.Source->RecvResizeVolumeResponse()->GetStatus());
        resize =
            fixture.Source->CreateResizeVolumeRequest("disk", 2 * 1024 * 1024);
        resize->Record.MutableHeaders()->SetShardId("source-alias");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(resize));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK, fixture.Source->RecvResizeVolumeResponse()->GetStatus());
        const auto read = fixture.Source->ReadBlocks(
            "disk", 0, mounted->Record.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'a'),
                                 read->Record.GetBlocks().GetBuffers(0));
    }

    Y_UNIT_TEST(ShouldNotUseLocalSessionForRemoteStat)
    {
        TCrossShardFixture fixture;
        fixture.Target->CreateVolume("disk", 4096);
        fixture.Source->MountVolume("disk");
        auto request = fixture.Source->CreateStatVolumeRequest("disk");
        request->Record.MutableHeaders()->SetShardId("target");
        request->Record.SetNoPartition(true);
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        const auto response = fixture.Source->RecvStatVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(4096,
                                 response->Record.GetVolume().GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            1024 * 1024,
            fixture.Source->DescribeVolume("disk")
                ->Record.GetVolume()
                .GetBlocksCount());
    }

    Y_UNIT_TEST(ShouldRejectRemoteDiskRegistryDeletionBeforeSideEffects)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                                   NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                                   1_GB / DefaultBlockSize, true);
        fixture.Source->CreateVolume("disk-copy", 1_GB / DefaultBlockSize,
                                     DefaultBlockSize, "", "",
                                     NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        ui32 registryCalls = 0;
        fixture.Env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvDiskRegistry::EvMarkDiskForCleanupRequest ||
                    event->GetTypeRewrite() ==
                        TEvDiskRegistry::EvDeallocateDiskRequest)
                {
                    ++registryCalls;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        for (bool sync: {false, true}) {
            auto request = fixture.Source->CreateDestroyVolumeRequest(
                "disk-copy", false, sync, 0, true);
            request->Record.MutableHeaders()->SetShardId("target");
            fixture.Source->SendRequest(MakeStorageServiceId(),
                                        std::move(request));
            const auto response = fixture.Source->RecvDestroyVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_IMPLEMENTED, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0, registryCalls);
            fixture.Source->DescribeVolume("disk-copy", true);
            fixture.Target->DescribeVolume("disk-copy", true);
        }
        fixture.Env.GetRuntime().SetObserverFunc(
            TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ShouldNotDeallocateLocalDiskForMissingRemoteSyncDelete)
    {
        TCrossShardFixture fixture;
        ui32 deallocations = 0;
        fixture.Env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistry::EvDeallocateDiskRequest)
                {
                    ++deallocations;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        auto request = fixture.Source->CreateDestroyVolumeRequest(
            "disk", false, true, 0, true);
        request->Record.MutableHeaders()->SetShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        const auto response = fixture.Source->RecvDestroyVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, deallocations);
        fixture.Source->DescribeVolume("disk", true);
        fixture.Env.GetRuntime().SetObserverFunc(
            TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ShouldRejectCrossShardDiskRegistryLinksBeforePersistence)
    {
        for (const auto kinds:
             {std::pair{NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                        NProto::STORAGE_MEDIA_SSD},
              std::pair{NProto::STORAGE_MEDIA_SSD,
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED},
              std::pair{NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED}})
        {
            TCrossShardFixture fixture(kinds.first, kinds.second,
                                       1_GB / DefaultBlockSize, true);
            const auto response = fixture.CreateLink();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_IMPLEMENTED, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
                static_cast<int>(fixture.GetStatus().GetStatus()));
            fixture.Source->DescribeVolume("disk", true);
            fixture.Target->DescribeVolume("disk-copy", true);
        }
    }

    Y_UNIT_TEST(ShouldFindCrossShardLinkThroughEquivalentAliases)
    {
        TCrossShardFixture fixture;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        auto request =
            fixture.Source->CreateCreateVolumeLinkRequest("disk", "disk-copy");
        request->Record.SetLeaderShardId("source-alias");
        request->Record.SetFollowerShardId("target-alias");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            fixture.Source->RecvCreateVolumeLinkResponse()->GetStatus());
        const auto status =
            fixture.GetStatusFor(*fixture.Source, "disk", "source-alias",
                                 "disk-copy", "target-alias");
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(status.GetStatus()));
        auto cancel =
            fixture.Source->CreateDestroyVolumeLinkRequest("disk", "disk-copy");
        cancel->Record.SetLeaderShardId("source-alias");
        cancel->Record.SetFollowerShardId("target-alias");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(cancel));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK, fixture.Source->RecvDestroyVolumeLinkResponse()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(fixture.GetStatus().GetStatus()));
    }

    Y_UNIT_TEST(ShouldFindLegacyLocalLinkThroughExplicitLocalAliases)
    {
        TCrossShardFixture fixture;
        fixture.Source->CreateVolume("local-copy", 1024 * 1024);
        fixture.Source->CreateVolumeLink("disk", "local-copy");
        const auto status = fixture.GetStatusFor(
            *fixture.Source, "disk", "source", "local-copy", "source-alias");
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(status.GetStatus()));
        auto cancel = fixture.Source->CreateDestroyVolumeLinkRequest(
            "disk", "local-copy");
        cancel->Record.SetLeaderShardId("source");
        cancel->Record.SetFollowerShardId("source-alias");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(cancel));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK, fixture.Source->RecvDestroyVolumeLinkResponse()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(
                fixture
                    .GetStatusFor(*fixture.Source, "disk", "", "local-copy", "")
                    .GetStatus()));
    }

    Y_UNIT_TEST(ShouldRejectLateProgressAfterCancellationAndLinkRecreation)
    {
        TCrossShardFixture fixture;
        auto& runtime = fixture.Env.GetRuntime();
        TFollowerDiskInfo oldFollower;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvUpdateFollowerStateRequest)
                {
                    oldFollower = event
                                      ->Get<TEvVolumePrivate::
                                                TEvUpdateFollowerStateRequest>()
                                      ->Follower;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT(oldFollower.Link.LinkUUID);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.DestroyLink()->GetStatus());
        NTestVolume::TVolumeClient source(
            runtime, fixture.SourceNode, fixture.GetTabletId("disk", "source"));
        for (const auto state:
             {TFollowerDiskInfo::EState::Preparing,
              TFollowerDiskInfo::EState::DataReady,
              TFollowerDiskInfo::EState::LeadershipTransferred,
              TFollowerDiskInfo::EState::Error})
        {
            oldFollower.State = state;
            source.SendUpdateFollowerStateRequest(oldFollower);
            const auto response = source.RecvUpdateFollowerStateResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, response->GetStatus());
        }
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(fixture.GetStatus().GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        oldFollower.State = TFollowerDiskInfo::EState::DataReady;
        source.SendUpdateFollowerStateRequest(oldFollower);
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            source.RecvUpdateFollowerStateResponse()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(fixture.GetStatus().GetStatus()));
    }

    Y_UNIT_TEST(ShouldFenceQueuedCancellationOnBothTablets)
    {
        TCrossShardFixture fixture;
        auto& runtime = fixture.Env.GetRuntime();
        TFollowerDiskInfo follower;
        TActorId sourceOwner, targetOwner;
        TVector<std::unique_ptr<IEventHandle>> copyWrites;
        auto holdCopy = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() == TEvService::EvZeroBlocksRequest &&
                event->Get<TEvService::TEvZeroBlocksRequest>()
                        ->Record.GetHeaders()
                        .GetShardId() == "target")
            {
                copyWrites.emplace_back(event.Release());
                return true;
            }
            if (event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest &&
                event->Get<TEvService::TEvWriteBlocksRequest>()
                        ->Record.GetHeaders()
                        .GetShardId() == "target")
            {
                copyWrites.emplace_back(event.Release());
                return true;
            }
            return false;
        };
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvUpdateFollowerStateRequest)
                {
                    follower = event
                                   ->Get<TEvVolumePrivate::
                                             TEvUpdateFollowerStateRequest>()
                                   ->Follower;
                    sourceOwner = event->GetRecipientRewrite();
                }
                if (event->GetTypeRewrite() ==
                        TEvVolume::EvUpdateLinkOnFollowerRequest &&
                    event->GetRecipientRewrite().NodeId() &&
                    dynamic_cast<NKikimr::NTabletFlatExecutor::
                                     NFlatExecutorSetup::ITablet*>(
                        runtime.FindActor(event->GetRecipientRewrite())))
                {
                    targetOwner = event->GetRecipientRewrite();
                }
                if (holdCopy(event)) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        UNIT_ASSERT(sourceOwner && targetOwner);

        auto holdExecutor = [&](TActorId owner, ui32 node)
        {
            auto* tablet = dynamic_cast<
                NKikimr::NTabletFlatExecutor::NFlatExecutorSetup::ITablet*>(
                runtime.FindActor(owner));
            UNIT_ASSERT(tablet);
            const auto executorId = tablet->ExecutorID();
            auto* executor = dynamic_cast<
                NKikimr::NTabletFlatExecutor::NFlatExecutorSetup::IExecutor*>(
                runtime.FindActor(executorId));
            UNIT_ASSERT(executor);
            auto probe = std::make_shared<TExecutorQueueProbe>();
            runtime.SetObserverFunc(
                [&, probe, owner, executorId](TAutoPtr<IEventHandle>& event)
                {
                    // Pinned executor's first private event is
                    // ActivateExecution.
                    if (probe->Block &&
                        event->GetRecipientRewrite() == executorId &&
                        event->GetTypeRewrite() ==
                            EventSpaceBegin(NKikimr::TKikimrEvents::ES_PRIVATE))
                    {
                        probe->Activations.emplace_back(event.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    if (event->GetTypeRewrite() ==
                            TEvVolume::EvUpdateLinkOnFollowerRequest &&
                        event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                                ->Record.GetAction() ==
                            NProto::LINK_ACTION_DESTROY)
                    {
                        ++probe->DestructionRequests;
                        if (event->GetRecipientRewrite() == owner) {
                            probe->CancellationHandled = true;
                        }
                    }
                    if (event->GetRecipientRewrite() == owner &&
                        event->GetTypeRewrite() ==
                            TEvVolume::EvUnlinkLeaderVolumeFromFollowerRequest)
                    {
                        probe->CancellationHandled = true;
                    }
                    if (holdCopy(event)) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });
            runtime.Register(
                new TExecutorQueueSeedActor(executor, executorId, probe), node);
            TDispatchOptions options;
            options.CustomFinalCondition = [probe]
            {
                return !probe->Activations.empty();
            };
            runtime.DispatchEvents(options, TDuration::Seconds(3));
            UNIT_ASSERT(!probe->Activations.empty());
            UNIT_ASSERT(!probe->SeedExecuted);
            return probe;
        };
        auto releaseExecutor =
            [&](const std::shared_ptr<TExecutorQueueProbe>& probe, ui32 node)
        {
            probe->Block = false;
            for (auto& event: probe->Activations) {
                runtime.Send(event.release(), node);
            }
        };
        auto query =
            [&](NTestVolume::TVolumeClient& client, const TString& disk)
        {
            auto request =
                std::make_unique<TEvVolume::TEvGetLinkStatusRequest>();
            request->Record.SetDiskId(disk);
            request->Record.SetLeaderDiskId("disk");
            request->Record.SetLeaderShardId("source");
            request->Record.SetFollowerDiskId("disk-copy");
            request->Record.SetFollowerShardId("target");
            request->Record.SetLinkUUID(follower.Link.LinkUUID);
            client.SendToPipe(std::move(request));
            return client.RecvGetLinkStatusResponse()->Record.GetStatus();
        };

        NTestVolume::TVolumeClient source(
            runtime, fixture.SourceNode, fixture.GetTabletId("disk", "source"));
        const auto sourceProbe = holdExecutor(sourceOwner, fixture.SourceNode);
        follower.State = TFollowerDiskInfo::EState::DataReady;
        source.SendUpdateFollowerStateRequest(follower);
        auto cancel =
            source.CreateUnlinkLeaderVolumeFromFollowerRequest(follower.Link);
        cancel->Record.SetRequireCancellable(true);
        source.SendToPipe(std::move(cancel));
        // This reply is after the cancellation handler on the same pipe,
        // while the update and removal transactions are still unexecuted.
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(query(source, "disk")));
        UNIT_ASSERT(sourceProbe->CancellationHandled);
        UNIT_ASSERT(!sourceProbe->SeedExecuted);
        UNIT_ASSERT_VALUES_EQUAL(0, sourceProbe->DestructionRequests);
        releaseExecutor(sourceProbe, fixture.SourceNode);
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK, source.RecvUpdateFollowerStateResponse()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            source.RecvUnlinkLeaderVolumeFromFollowerResponse()->GetStatus());
        UNIT_ASSERT(sourceProbe->SeedExecuted);
        UNIT_ASSERT_VALUES_EQUAL(0, sourceProbe->DestructionRequests);

        NTestVolume::TVolumeClient target(
            runtime, fixture.TargetNode,
            fixture.GetTabletId("disk-copy", "target"));
        const auto targetProbe = holdExecutor(targetOwner, fixture.TargetNode);
        auto update =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        update->Record.SetDiskId("disk-copy");
        update->Record.SetLeaderDiskId("disk");
        update->Record.SetLeaderShardId("source");
        update->Record.SetFollowerShardId("target");
        update->Record.SetLinkUUID(follower.Link.LinkUUID);
        update->Record.SetAction(NProto::LINK_ACTION_COMPLETED);
        auto remove =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        remove->Record = update->Record;
        remove->Record.SetAction(NProto::LINK_ACTION_DESTROY);
        remove->Record.SetRequireCancellable(true);
        target.SendToPipe(std::move(update));
        target.SendToPipe(std::move(remove));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(query(target, "disk-copy")));
        UNIT_ASSERT(targetProbe->CancellationHandled);
        UNIT_ASSERT(!targetProbe->SeedExecuted);
        releaseExecutor(targetProbe, fixture.TargetNode);
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK, target.RecvUpdateLinkOnFollowerResponse()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            target.RecvUpdateLinkOnFollowerResponse()->GetStatus());
        UNIT_ASSERT(targetProbe->SeedExecuted);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ShouldServeRemoteMountAfterCancellingRebootedCopyOnlySource)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_SSD, 1024 * 1024,
                                   false, true);
        auto& runtime = fixture.Env.GetRuntime();
        TVector<std::unique_ptr<IEventHandle>> writes;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvService::EvWriteBlocksRequest &&
                    event->Get<TEvService::TEvWriteBlocksRequest>()
                            ->Record.GetHeaders()
                            .GetShardId() == "target")
                {
                    writes.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        NKikimr::RebootTablet(runtime, fixture.GetTabletId("disk", "source"),
                              fixture.Source->GetSender(), fixture.SourceNode);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(fixture.GetStatus().GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.DestroyLink()->GetStatus());
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        for (auto& write: writes) {
            runtime.Send(write.release(), fixture.SourceNode);
        }
        auto mount = fixture.Source->CreateMountVolumeRequest("disk");
        mount->Record.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(mount));
        const auto mounted = fixture.Source->RecvMountVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, mounted->GetStatus());
        const auto session = mounted->Record.GetSessionId();
        NTestVolume::TVolumeClient source(
            runtime, fixture.SourceNode, fixture.GetTabletId("disk", "source"));
        source.WaitReady();
        fixture.Source->WriteBlocks("disk", TBlockRange64::MakeOneBlock(0),
                                    session, 'b');
        const auto read = fixture.Source->ReadBlocks("disk", 0, session);
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'b'),
                                 read->Record.GetBlocks().GetBuffers(0));
    }

    Y_UNIT_TEST(ShouldResumeIdleDestinationCleanupAfterRebootWithoutMountOrGc)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_HDD, 8192, false,
                                   true);
        auto& runtime = fixture.Env.GetRuntime();
        bool oldDeleteDropped = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (!oldDeleteDropped &&
                    event->GetTypeRewrite() ==
                        TEvService::EvDestroyVolumeRequest &&
                    event->Get<TEvService::TEvDestroyVolumeRequest>()
                            ->Record.GetDiskId() == "disk")
                {
                    oldDeleteDropped = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return oldDeleteDropped;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(60));
        UNIT_ASSERT(oldDeleteDropped);
        fixture.Source->DescribeVolume("disk", true);
        NKikimr::RebootTablet(runtime,
                              fixture.GetTabletId("disk-copy", "target"),
                              fixture.Target->GetSender(), fixture.TargetNode);
        bool complete = false;
        for (ui32 attempt = 0; attempt != 1000; ++attempt) {
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            const auto status = fixture.GetStatus();
            if (status.GetStatus() == NProto::LINK_STATUS_COMPLETED) {
                complete = true;
                break;
            }
        }
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT(complete);
        fixture.Source->SendDescribeVolumeRequest("disk", true);
        UNIT_ASSERT(
            HasError(fixture.Source->RecvDescribeVolumeResponse()->GetError()));
        fixture.Target->DescribeVolume("disk-copy", true);
    }

    Y_UNIT_TEST(ShouldRejectForeignDiskRegistryDescribeBeforeDeviceLookup)
    {
        for (bool localAllocation: {false, true}) {
            TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                       NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                                       1_GB / DefaultBlockSize, true);
            if (localAllocation) {
                fixture.Source->CreateVolume(
                    "disk-copy", 1_GB / DefaultBlockSize, DefaultBlockSize, "",
                    "", NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
            }
            ui32 lookups = 0;
            fixture.Env.GetRuntime().SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvDiskRegistry::EvDescribeDiskRequest)
                    {
                        ++lookups;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });
            auto request =
                fixture.Source->CreateDescribeVolumeRequest("disk-copy", true);
            request->Record.MutableHeaders()->SetShardId("target");
            fixture.Source->SendRequest(MakeStorageServiceId(),
                                        std::move(request));
            UNIT_ASSERT_VALUES_EQUAL(
                E_NOT_IMPLEMENTED,
                fixture.Source->RecvDescribeVolumeResponse()->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0, lookups);
            fixture.Env.GetRuntime().SetObserverFunc(
                TTestActorRuntime::DefaultObserverFunc);
            fixture.Target->DescribeVolume("disk-copy", true);
        }
    }

    Y_UNIT_TEST(ShouldFencePendingCreateCancellationByItsOriginalUuid)
    {
        TCrossShardFixture fixture;
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> description, destruction;
        TString cancelledUuid;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (!description &&
                    event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeResponse &&
                    event->Get<TEvSSProxy::TEvDescribeVolumeResponse>()
                            ->PathDescription.GetBlockStoreVolumeDescription()
                            .GetVolumeConfig()
                            .GetDiskId() == "disk-copy")
                {
                    description.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (event->GetTypeRewrite() ==
                        TEvVolume::EvUpdateLinkOnFollowerRequest &&
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                            ->Record.GetAction() == NProto::LINK_ACTION_DESTROY)
                {
                    cancelledUuid =
                        event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                            ->Record.GetLinkUUID();
                    destruction.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        auto create =
            fixture.Source->CreateCreateVolumeLinkRequest("disk", "disk-copy");
        create->Record.SetLeaderShardId("source");
        create->Record.SetFollowerShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(create));
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return description != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(description);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.DestroyLink()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            fixture.Source->RecvCreateVolumeLinkResponse()->GetStatus());
        options.CustomFinalCondition = [&]
        {
            return destruction != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(destruction);
        UNIT_ASSERT(cancelledUuid);

        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        runtime.Send(destruction.release(), fixture.SourceNode);
        runtime.Send(description.release(), fixture.SourceNode);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(fixture.GetStatus().GetStatus()));
    }

    Y_UNIT_TEST(ShouldPersistCancelledUuidBeforeLateCreateAndReboot)
    {
        TCrossShardFixture fixture;
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> heldCreate;
        bool destinationCancelled = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (!heldCreate &&
                    event->GetTypeRewrite() ==
                        TEvVolume::EvUpdateLinkOnFollowerRequest &&
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                            ->Record.GetAction() == NProto::LINK_ACTION_CREATE)
                {
                    heldCreate.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvLinkOnFollowerDestroyed)
                {
                    destinationCancelled = SUCCEEDED(
                        event
                            ->Get<
                                TEvVolumePrivate::TEvLinkOnFollowerDestroyed>()
                            ->GetStatus());
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        auto create =
            fixture.Source->CreateCreateVolumeLinkRequest("disk", "disk-copy");
        create->Record.SetLeaderShardId("source");
        create->Record.SetFollowerShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(create));
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return heldCreate != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(heldCreate);
        const auto oldRecord =
            heldCreate->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                ->Record;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.DestroyLink()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            fixture.Source->RecvCreateVolumeLinkResponse()->GetStatus());
        options.CustomFinalCondition = [&]
        {
            return destinationCancelled;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(destinationCancelled);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        // A status lookup on the target observes the committed tombstone.
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(fixture
                                 .GetStatusFor(*fixture.Target, "disk",
                                               "source", "disk-copy", "target")
                                 .GetStatus()));
        NKikimr::RebootTablet(runtime,
                              fixture.GetTabletId("disk-copy", "target"),
                              fixture.Target->GetSender(), fixture.TargetNode);
        auto stale =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        stale->Record = oldRecord;
        fixture.Source->SendRequest(MakeVolumeProxyServiceId(),
                                    std::move(stale));
        const auto rejected =
            fixture.Source
                ->RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, rejected->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        stale = std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        stale->Record = oldRecord;
        fixture.Source->SendRequest(MakeVolumeProxyServiceId(),
                                    std::move(stale));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            fixture.Source
                ->RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>()
                ->GetStatus());
        runtime.Send(heldCreate.release(), fixture.SourceNode);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_PREPARING),
            static_cast<int>(fixture.GetStatus().GetStatus()));
    }

    Y_UNIT_TEST(ShouldNotRestartMountedSourceOnIdempotentCancel)
    {
        TCrossShardFixture fixture;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.DestroyLink()->GetStatus());
        const auto mount = fixture.Source->MountVolume("disk");
        const auto session = mount->Record.GetSessionId();
        fixture.Source->WriteBlocks("disk", TBlockRange64::MakeOneBlock(0),
                                    session, 'b');
        ui32 poisons = 0;
        fixture.Env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvents::TEvPoisonPill::EventType) {
                    ++poisons;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, fixture.DestroyLink()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, poisons);
        const auto read = fixture.Source->ReadBlocks("disk", 0, session);
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'b'),
                                 read->Record.GetBlocks().GetBuffers(0));
        fixture.Env.GetRuntime().SetObserverFunc(
            TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ShouldPreserveUnrestrictedInternalUnlinkOnDestination)
    {
        TCrossShardFixture fixture;
        TString uuid;
        bool propagated = false;
        fixture.Env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvUpdateFollowerStateRequest)
                {
                    uuid = event
                               ->Get<TEvVolumePrivate::
                                         TEvUpdateFollowerStateRequest>()
                               ->Follower.Link.LinkUUID;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        fixture.Env.GetRuntime().SetObserverFunc(
            TTestActorRuntime::DefaultObserverFunc);
        auto update =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
        update->Record.SetDiskId("disk-copy");
        update->Record.SetLeaderDiskId("disk");
        update->Record.SetLeaderShardId("source");
        update->Record.SetFollowerShardId("target");
        update->Record.SetLinkUUID(uuid);
        update->Record.SetAction(NProto::LINK_ACTION_COMPLETED);
        update->Record.MutableHeaders()->SetShardId("target");
        update->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        fixture.Source->SendRequest(MakeVolumeProxyServiceId(),
                                    std::move(update));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            fixture.Source
                ->RecvResponse<TEvVolume::TEvUpdateLinkOnFollowerResponse>()
                ->GetStatus());
        NTestVolume::TVolumeClient source(
            fixture.Env.GetRuntime(), fixture.SourceNode,
            fixture.GetTabletId("disk", "source"));
        fixture.Env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolumePrivate::EvLinkOnFollowerDestroyed)
                {
                    propagated = SUCCEEDED(
                        event
                            ->Get<
                                TEvVolumePrivate::TEvLinkOnFollowerDestroyed>()
                            ->GetStatus());
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        source.UnlinkLeaderVolumeFromFollower(
            TLeaderFollowerLink{.LinkUUID = uuid, .LeaderDiskId = "disk",
                                .LeaderShardId = "source",
                                .FollowerDiskId = "disk-copy",
                                .FollowerShardId = "target"});
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return propagated;
        };
        fixture.Env.GetRuntime().DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(propagated);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(fixture.GetTargetStatus()));
        fixture.Env.GetRuntime().SetObserverFunc(
            TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(fixture.GetStatus().GetStatus()));
        fixture.Source->DescribeVolume("disk", true);
    }

    Y_UNIT_TEST(ShouldVerifyLegacyCleanupUuidBeforeConditionalDelete)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_HDD, 8192, false,
                                   true);
        auto& runtime = fixture.Env.GetRuntime();
        bool probed = false, guarded = false, incomplete = false;
        TActorId probeOwner;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    auto& record =
                        event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                            ->Record;
                    if (record.GetAction() == NProto::LINK_ACTION_CREATE) {
                        // Emulate a relationship persisted by the previous
                        // version.
                        record.SetLeaderTabletId(0);
                    }
                }
                if (event->GetTypeRewrite() ==
                        TEvVolume::EvGetLinkStatusRequest &&
                    event->Get<TEvVolume::TEvGetLinkStatusRequest>()
                        ->Record.GetLinkUUID())
                {
                    probed = true;
                    if (!probeOwner) {
                        probeOwner = event->Sender;
                    }
                }
                if (event->GetTypeRewrite() ==
                        TEvService::EvDestroyVolumeRequest &&
                    event->Get<TEvService::TEvDestroyVolumeRequest>()
                            ->Record.GetDiskId() == "disk")
                {
                    guarded = event->Get<TEvService::TEvDestroyVolumeRequest>()
                                  ->Record.GetExpectedVolumeTabletId() != 0;
                }
                if (!incomplete && probeOwner &&
                    event->GetTypeRewrite() ==
                        TEvVolume::EvGetLinkStatusResponse &&
                    event->GetRecipientRewrite() == probeOwner)
                {
                    auto& record =
                        event->Get<TEvVolume::TEvGetLinkStatusResponse>()
                            ->Record;
                    record.ClearVolumeTabletId();
                    record.ClearLinkUUID();
                    incomplete = true;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        for (ui32 attempt = 0; attempt != 1000 && !incomplete; ++attempt) {
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(incomplete);
        UNIT_ASSERT(!guarded);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED),
            static_cast<int>(fixture.GetTargetStatus()));
        fixture.Source->DescribeVolume("disk", true);
        bool complete = false;
        for (ui32 attempt = 0; attempt != 1000; ++attempt) {
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            if (fixture.GetStatus().GetStatus() ==
                NProto::LINK_STATUS_COMPLETED)
            {
                complete = true;
                break;
            }
        }
        UNIT_ASSERT(complete);
        UNIT_ASSERT(probed && guarded);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ShouldNotDeleteRecreatedSourceFromAnOldCleanupTimer)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_HDD, 8192, false,
                                   true);
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> timer;
        ui32 deletes = 0;
        bool capturedTimer = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (!capturedTimer &&
                    event->GetTypeRewrite() ==
                        TEvVolumePrivate::EvDestroyOutdatedLeader)
                {
                    capturedTimer = true;
                    timer.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (event->GetTypeRewrite() ==
                        TEvService::EvDestroyVolumeRequest &&
                    event->Get<TEvService::TEvDestroyVolumeRequest>()
                            ->Record.GetDiskId() == "disk")
                {
                    ++deletes;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return timer != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(60));
        UNIT_ASSERT(timer);
        // The reloaded actor can start partitions as well as restore cleanup.
        NKikimr::RebootTablet(runtime,
                              fixture.GetTabletId("disk-copy", "target"),
                              fixture.Target->GetSender(), fixture.TargetNode);
        fixture.Target->MountVolume("disk-copy");
        runtime.Send(timer.release(), fixture.TargetNode);
        // Do not capture the new actor's timer.
        bool complete = false;
        for (ui32 attempt = 0; attempt != 1000; ++attempt) {
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            if (fixture.GetStatus().GetStatus() ==
                NProto::LINK_STATUS_COMPLETED)
            {
                complete = true;
                break;
            }
        }
        UNIT_ASSERT(complete);
        UNIT_ASSERT_VALUES_EQUAL(1, deletes);
        fixture.Source->CreateVolume("disk", 8192);
        const auto mounted = fixture.Source->MountVolume("disk");
        fixture.Source->WriteBlocks("disk", TBlockRange64::MakeOneBlock(0),
                                    mounted->Record.GetSessionId(), 'b');
        for (ui32 attempt = 0; attempt != 700; ++attempt) {
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(1));
        }
        const auto remount = fixture.Source->MountVolume("disk");
        const auto read = fixture.Source->ReadBlocks(
            "disk", 0, remount->Record.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'b'),
                                 read->Record.GetBlocks().GetBuffers(0));
        UNIT_ASSERT_VALUES_EQUAL(1, deletes);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ShouldFenceSchemaCleanupAgainstRecreatedIncarnation)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_HDD, 8192, false,
                                   true);
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> conditionalDrop;
        NProto::TDestroyVolumeRequest staleDelete;
        bool captured = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvService::EvDestroyVolumeRequest) {
                    const auto& record =
                        event->Get<TEvService::TEvDestroyVolumeRequest>()
                            ->Record;
                    if (record.GetDiskId() == "disk" &&
                        record.GetExpectedVolumeTabletId())
                    {
                        staleDelete = record;
                    }
                }
                if (!captured && event->GetTypeRewrite() ==
                                     TEvSSProxy::EvModifySchemeRequest)
                {
                    const auto& scheme =
                        event->Get<TEvSSProxy::TEvModifySchemeRequest>()
                            ->ModifyScheme;
                    if (scheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume &&
                        scheme.GetDrop().GetName() == "disk" &&
                        scheme.ApplyIfSize())
                    {
                        UNIT_ASSERT(scheme.GetDrop().GetId());
                        captured = true;
                        conditionalDrop.reset(event.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        const auto originalTablet = fixture.GetTabletId("disk", "source");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return conditionalDrop != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(60));
        UNIT_ASSERT(conditionalDrop);
        UNIT_ASSERT_VALUES_EQUAL(originalTablet,
                                 staleDelete.GetExpectedVolumeTabletId());
        fixture.Source->DestroyVolume("disk", false, false, 0, true);
        fixture.Source->CreateVolume("disk", 8192);
        UNIT_ASSERT(fixture.GetTabletId("disk", "source") != originalTablet);
        const auto mounted = fixture.Source->MountVolume("disk");
        const auto session = mounted->Record.GetSessionId();
        fixture.Source->WriteBlocks("disk", TBlockRange64::MakeOneBlock(0),
                                    session, 'b');
        // Normalize the service-ID rewrite before reinjection; this message
        // originated on the target NBS node, not the source shard's node.
        const auto dropRecipient = conditionalDrop->GetRecipientRewrite();
        const auto dropSender = conditionalDrop->Sender;
        const auto dropCookie = conditionalDrop->Cookie;
        auto dropBody = conditionalDrop->ReleaseBase();
        conditionalDrop.reset();
        runtime.Send(new IEventHandle(dropRecipient, dropSender,
                                      dropBody.Release(), 0, dropCookie),
                     fixture.TargetNode);
        bool complete = false;
        for (ui32 attempt = 0; attempt != 1500; ++attempt) {
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            if (fixture.GetTargetStatus() == NProto::LINK_STATUS_COMPLETED) {
                complete = true;
                break;
            }
        }
        UNIT_ASSERT(complete);
        auto request = std::make_unique<TEvService::TEvDestroyVolumeRequest>();
        request->Record = staleDelete;
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            fixture.Source->RecvDestroyVolumeResponse()->GetStatus());
        const auto remount = fixture.Source->MountVolume("disk");
        const auto read = fixture.Source->ReadBlocks(
            "disk", 0, remount->Record.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'b'),
                                 read->Record.GetBlocks().GetBuffers(0));
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ShouldApplyAuthoritativeErrorForTheSameCopyUuid)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_SSD, 8192);
        auto& runtime = fixture.Env.GetRuntime();
        std::unique_ptr<IEventHandle> ready;
        bool rebootRequested = false;
        TActorId wrapper, owner;
        TFollowerDiskInfo follower;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (!ready &&
                    event->GetTypeRewrite() ==
                        TEvVolumePrivate::EvUpdateFollowerStateRequest)
                {
                    const auto& info =
                        event
                            ->Get<TEvVolumePrivate::
                                      TEvUpdateFollowerStateRequest>()
                            ->Follower;
                    if (info.State == TFollowerDiskInfo::EState::DataReady) {
                        follower = info;
                        wrapper = event->Sender;
                        owner = event->Recipient;
                        ready.reset(event.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                if (event->GetTypeRewrite() ==
                        TEvents::TEvPoisonPill::EventType &&
                    event->Sender == wrapper && event->Recipient == owner)
                {
                    rebootRequested = true;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fixture.CreateLink()->GetStatus());
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return ready != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(30));
        UNIT_ASSERT(ready);
        NTestVolume::TVolumeClient source(
            runtime, fixture.SourceNode, fixture.GetTabletId("disk", "source"));
        follower.State = TFollowerDiskInfo::EState::Error;
        source.UpdateFollowerState(follower);
        runtime.Send(ready.release(), fixture.SourceNode);
        options.CustomFinalCondition = [&]
        {
            return rebootRequested;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(rebootRequested);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_ERROR),
            static_cast<int>(fixture.GetStatus().GetStatus()));
        const auto mounted = fixture.Source->MountVolume("disk");
        fixture.Source->WriteBlocks("disk", TBlockRange64::MakeOneBlock(0),
                                    mounted->Record.GetSessionId(), 'b');
        const auto read = fixture.Source->ReadBlocks(
            "disk", 0, mounted->Record.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'b'),
                                 read->Record.GetBlocks().GetBuffers(0));
    }

    Y_UNIT_TEST(ShouldBindLegacyCreatedLinkToDestinationOnRecovery)
    {
        TCrossShardFixture fixture;
        auto& runtime = fixture.Env.GetRuntime();
        const auto targetTabletId = fixture.GetTabletId("disk-copy", "target");
        TVector<std::unique_ptr<IEventHandle>> creates;
        bool legacyCreated = false;
        bool recovering = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (!legacyCreated &&
                    event->GetTypeRewrite() ==
                        TEvVolumePrivate::EvUpdateFollowerStateRequest)
                {
                    auto& follower =
                        event
                            ->Get<TEvVolumePrivate::
                                      TEvUpdateFollowerStateRequest>()
                            ->Follower;
                    if (follower.State == TFollowerDiskInfo::EState::Created) {
                        // Emulate a Created row persisted before the
                        // destination incarnation field was introduced.
                        follower.Link.FollowerTabletId = 0;
                        legacyCreated = true;
                    }
                }
                if (event->GetTypeRewrite() ==
                        TEvVolume::EvUpdateLinkOnFollowerRequest &&
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                            ->Record.GetAction() == NProto::LINK_ACTION_CREATE)
                {
                    if (recovering) {
                        UNIT_ASSERT_VALUES_EQUAL(
                            targetTabletId,
                            event
                                ->Get<
                                    TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                                ->Record.GetFollowerTabletId());
                    }
                    creates.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        auto request =
            fixture.Source->CreateCreateVolumeLinkRequest("disk", "disk-copy");
        request->Record.SetLeaderShardId("source");
        request->Record.SetFollowerShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return !creates.empty();
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(legacyCreated);
        UNIT_ASSERT(!creates.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            creates.front()
                ->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                ->Record.GetFollowerTabletId());

        const auto oldCreates = creates.size();
        recovering = true;
        NKikimr::RebootTablet(runtime, fixture.GetTabletId("disk", "source"),
                              fixture.Source->GetSender(), fixture.SourceNode);
        options.CustomFinalCondition = [&]
        {
            return creates.size() > oldCreates;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT_C(creates.size() > oldCreates,
                      "Recovered Created link did not persist its destination");
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(
        ShouldRevalidateCreatedCrossShardLinkAfterDestinationReplacement)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_SSD,
                                   1_GB / DefaultBlockSize);
        auto& runtime = fixture.Env.GetRuntime();
        TVector<std::unique_ptr<IEventHandle>> creates;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvVolume::EvUpdateLinkOnFollowerRequest &&
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>()
                            ->Record.GetAction() == NProto::LINK_ACTION_CREATE)
                {
                    creates.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        auto request =
            fixture.Source->CreateCreateVolumeLinkRequest("disk", "disk-copy");
        request->Record.SetLeaderShardId("source");
        request->Record.SetFollowerShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return !creates.empty();
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
        UNIT_ASSERT(!creates.empty());
        fixture.Target->DestroyVolume("disk-copy", false, false, 0, true);
        fixture.Target->CreateVolume("disk-copy", 1_GB / DefaultBlockSize,
                                     DefaultBlockSize, "", "",
                                     NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        NKikimr::RebootTablet(runtime, fixture.GetTabletId("disk", "source"),
                              fixture.Source->GetSender(), fixture.SourceNode);
        bool error = false;
        for (ui32 attempt = 0; attempt != 100; ++attempt) {
            const auto status = fixture.GetStatus();
            if (status.GetStatus() == NProto::LINK_STATUS_ERROR) {
                error = true;
                break;
            }
            runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT(error);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        for (auto& create: creates) {
            runtime.Send(create.release(), fixture.SourceNode);
        }
        auto stat = fixture.Target->CreateStatVolumeRequest("disk-copy");
        stat->Record.SetNoPartition(true);
        fixture.Target->SendRequest(MakeStorageServiceId(), std::move(stat));
        UNIT_ASSERT(!fixture.Target->RecvStatVolumeResponse()
                         ->Record.GetIsVolumeOperationRestricted());
    }

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
                                bool online, bool restartSource = false,
                                bool restartTarget = false,
                                bool reverseCopy = false)
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
            if (restartTarget) {
                NKikimr::RebootTablet(
                    runtime, fixture.GetTabletId("disk-copy", "target"),
                    fixture.Target->GetSender(), fixture.TargetNode);
            }
            if (restartSource) {
                NKikimr::RebootTablet(
                    runtime, fixture.GetTabletId("disk", "source"),
                    fixture.Source->GetSender(), fixture.SourceNode);
            }
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
        if (status.GetStatus() == NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED) {
            const auto lateCancel = fixture.DestroyLink();
            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, lateCancel->GetStatus());
        }
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
        const auto completedCancel = fixture.DestroyLink();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, completedCancel->GetStatus());

        fixture.Source->SendDescribeVolumeRequest("disk", true);
        const auto absent = fixture.Source->RecvDescribeVolumeResponse();
        UNIT_ASSERT(HasError(absent->GetError()));
        // The low-level test client does not implement the SDK's automatic
        // remount on E_BS_INVALID_SESSION after a tablet restart.
        targetSession =
            fixture.Target->MountVolume("disk-copy")->Record.GetSessionId();
        checkBlock(blocksCount - 1, online ? 'b' : 'c');
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        if (reverseCopy) {
            fixture.Target->UnmountVolume("disk-copy", targetSession,
                                          NProto::SOURCE_CLIENT);
            auto createBack = fixture.Source->CreateCreateVolumeRequest(
                "disk", blocksCount, DefaultBlockSize, "", "", sourceKind);
            createBack->Record.MutableHeaders()->SetShardId("source");
            fixture.Source->SendRequest(MakeStorageServiceId(),
                                        std::move(createBack));
            const auto recreated = fixture.Source->RecvCreateVolumeResponse();
            UNIT_ASSERT_C(SUCCEEDED(recreated->GetStatus()),
                          recreated->GetErrorReason());

            auto linkBack = fixture.Target->CreateCreateVolumeLinkRequest(
                "disk-copy", "disk");
            linkBack->Record.SetLeaderShardId("target");
            linkBack->Record.SetFollowerShardId("source");
            fixture.Target->SendRequest(MakeStorageServiceId(),
                                        std::move(linkBack));
            const auto linked = fixture.Target->RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(SUCCEEDED(linked->GetStatus()),
                          linked->GetErrorReason());

            bool backCompleted = false;
            for (ui32 attempt = 0; attempt != 4000; ++attempt) {
                status = fixture.GetStatusFor(*fixture.Target, "disk-copy",
                                              "target", "disk", "source");
                UNIT_ASSERT_C(status.GetStatus() != NProto::LINK_STATUS_ERROR,
                              status.ShortDebugString());
                if (status.GetStatus() == NProto::LINK_STATUS_COMPLETED) {
                    backCompleted = true;
                    break;
                }
                runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
                runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            }
            UNIT_ASSERT_C(backCompleted, status.ShortDebugString());
            const auto backMount = fixture.Source->MountVolume("disk");
            const auto backSession = backMount->Record.GetSessionId();
            for (const ui32 index:
                 {0u, 1u, ui32(blocksCount - 2), ui32(blocksCount - 1)})
            {
                const auto response =
                    fixture.Source->ReadBlocks("disk", index, backSession);
                auto data = response->Record.GetBlocks().GetBuffers(0);
                if (data.empty()) {
                    data = TString(DefaultBlockSize, 0);
                }
                const char expected = index == 0                 ? 'a'
                                      : index == blocksCount - 1 ? 'b'
                                                                 : 0;
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, expected),
                                         data);
            }
            fixture.Target->SendDescribeVolumeRequest("disk-copy", true);
            const auto oldCopy = fixture.Target->RecvDescribeVolumeResponse();
            UNIT_ASSERT(HasError(oldCopy->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldResumeCrossShardCopyAfterSourceTabletRestart)
    {
        TestCrossShardCopyData(NProto::STORAGE_MEDIA_SSD,
                               NProto::STORAGE_MEDIA_HDD, true, true, false);
    }

    Y_UNIT_TEST(ShouldResumeCrossShardCopyAfterTargetTabletRestart)
    {
        TestCrossShardCopyData(NProto::STORAGE_MEDIA_HDD,
                               NProto::STORAGE_MEDIA_SSD, true, false, true);
    }

    Y_UNIT_TEST(ShouldResumeCrossShardCopyAfterBothTabletRestarts)
    {
        TestCrossShardCopyData(NProto::STORAGE_MEDIA_SSD,
                               NProto::STORAGE_MEDIA_SSD, true, true, true);
    }

    Y_UNIT_TEST(ShouldCopySameLogicalDiskBackToOriginalShard)
    {
        TestCrossShardCopyData(NProto::STORAGE_MEDIA_SSD,
                               NProto::STORAGE_MEDIA_HDD, true, false, false,
                               true);
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

    Y_UNIT_TEST(ShouldRejectNonlocalDiskRegistryCreationBeforeAllocation)
    {
        TCrossShardFixture fixture;
        auto request = fixture.Source->CreateCreateVolumeRequest(
            "remote-nrd", 93_GB / DefaultBlockSize, DefaultBlockSize, "", "",
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        request->Record.MutableHeaders()->SetShardId("target");
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(request));
        const auto response = fixture.Source->RecvCreateVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_IMPLEMENTED, response->GetStatus());
        fixture.Target->SendDescribeVolumeRequest("remote-nrd", true);
        const auto absent = fixture.Target->RecvDescribeVolumeResponse();
        UNIT_ASSERT(HasError(absent->GetError()));
    }

    Y_UNIT_TEST(ShouldCancelDetachedCopyWithoutLosingSourceData)
    {
        TCrossShardFixture fixture(NProto::STORAGE_MEDIA_SSD,
                                   NProto::STORAGE_MEDIA_HDD, 8192);
        auto& runtime = fixture.Env.GetRuntime();
        auto mount = fixture.Source->MountVolume("disk");
        const auto sourceSession = mount->Record.GetSessionId();
        fixture.Source->WriteBlocks("disk", TBlockRange64::MakeOneBlock(0),
                                    sourceSession, 'a');
        fixture.Source->UnmountVolume("disk", sourceSession,
                                      NProto::SOURCE_CLIENT);

        struct TProbe
        {
            bool Captured = false;
            std::unique_ptr<IEventHandle> Write;
        };

        auto probe = std::make_shared<TProbe>();
        runtime.SetObserverFunc(
            [probe](TAutoPtr<IEventHandle>& event)
            {
                if (!probe->Captured &&
                    event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest)
                {
                    const auto& request =
                        event->Get<TEvService::TEvWriteBlocksRequest>()->Record;
                    if (request.GetHeaders().GetShardId() == "target") {
                        probe->Captured = true;
                        probe->Write.reset(event.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        const auto linked = fixture.CreateLink();
        UNIT_ASSERT_C(SUCCEEDED(linked->GetStatus()), linked->GetErrorReason());
        TDispatchOptions options;
        options.CustomFinalCondition = [probe]
        {
            return probe->Write != nullptr;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(probe->Write);
        const auto cancelled = fixture.DestroyLink();
        UNIT_ASSERT_C(SUCCEEDED(cancelled->GetStatus()),
                      cancelled->GetErrorReason());
        runtime.Send(probe->Write.release(), fixture.SourceNode);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        const auto status = fixture.GetStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::LINK_STATUS_NOT_FOUND),
            static_cast<int>(status.GetStatus()));
        mount = fixture.Source->MountVolume("disk");
        const auto read =
            fixture.Source->ReadBlocks("disk", 0, mount->Record.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'a'),
                                 read->Record.GetBlocks().GetBuffers(0));
        auto stat = fixture.Source->CreateStatVolumeRequest("disk");
        stat->Record.SetNoPartition(true);
        fixture.Source->SendRequest(MakeStorageServiceId(), std::move(stat));
        const auto unrestricted = fixture.Source->RecvStatVolumeResponse();
        UNIT_ASSERT(!unrestricted->Record.GetIsVolumeOperationRestricted());
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
