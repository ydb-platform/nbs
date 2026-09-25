#include "service_ut.h"

#include "service_events_private.h"

#include <cloud/blockstore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/hive_proxy/tablet_boot_info.h>

#include <contrib/ydb/core/base/hive.h>
#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/base/tablet_resolver.h>
#include <contrib/ydb/core/node_whiteboard/node_whiteboard.h>

#include <google/protobuf/util/json_util.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NNodeWhiteboard;
using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 TabletId = 0x0100000000001234ULL;
constexpr ui32 Generation = 42;
using TResponse = NPrivateProto::TGetTabletStateResponse;
using TWhiteboardInfo = NKikimrWhiteboard::TTabletStateInfo;

TResponse GetTabletState(TServiceClient& service, ui64 tabletId)
{
    NPrivateProto::TGetTabletStateRequest request;
    request.SetTabletId(tabletId);
    TString input;
    UNIT_ASSERT(
        google::protobuf::util::MessageToJsonString(request, &input).ok());
    const auto response = service.ExecuteAction("GetTabletState", input);
    TResponse result;
    UNIT_ASSERT(
        google::protobuf::util::JsonStringToMessage(
            response->Record.GetOutput(),
            &result)
            .ok());
    UNIT_ASSERT_VALUES_EQUAL(result.GetTabletId(), tabletId);
    return result;
}

void AssertNoHiveOrBootRequest(ui32 type)
{
    UNIT_ASSERT(type != TEvHive::EvRequestHiveInfo);
    UNIT_ASSERT(type != TEvHive::EvGetTabletStorageInfo);
    UNIT_ASSERT(type != TEvHive::EvInitiateTabletExternalBoot);
    UNIT_ASSERT(type != TEvHive::EvLockTabletExecution);
    UNIT_ASSERT(type != TEvHiveProxy::EvBootExternalRequest);
    UNIT_ASSERT(type != TEvHiveProxy::EvLockTabletRequest);
    UNIT_ASSERT(type != TEvHiveProxy::EvGetStorageInfoRequest);
}

// Use the real service dispatch and pipe client, controlling only responses
// from the resolver, tablet, whiteboard and local backup.
struct TFixture
{
    TTestEnv Env;
    const ui32 NodeIndex = SetupTestEnv(Env);
    TTestActorRuntime& Runtime = Env.GetRuntime();
    TServiceClient Service{Runtime, NodeIndex};
    const TActorId TabletActor = Runtime.AllocateEdgeActor(NodeIndex);
    const TActorId SystemTabletActor = Runtime.AllocateEdgeActor(NodeIndex);
    const TActorId StaleSystemTabletActor =
        Runtime.AllocateEdgeActor(NodeIndex);
    TActorId ServerId = TabletActor;
    TActorId ProbeActor;
    TActorId PipeClient;

    NKikimrWhiteboard::TEvTabletStateResponse Whiteboard;
    TVector<TTabletBootInfo> BootInfos;
    ui32 BackupError = S_OK;
    bool ResolveFails = false;
    bool ConnectFails = false;
    bool StaleFirstAddress = false;
    bool ConnectedToFollower = false;
    bool DropWhiteboard = false;
    bool DropBackup = false;
    bool UndeliverWhiteboard = false;
    bool UndeliverBackup = false;
    bool TimeoutAfterPipeResult = false;
    bool DisconnectAfterConnect = false;
    bool HoldResolver = false;
    bool ResolverSeen = false;
    bool WhiteboardSeen = false;
    bool BackupSeen = false;
    bool DeadlineSent = false;
    ui32 ResolveRequests = 0;
    ui32 ConnectRequests = 0;

    TFixture()
    {
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& event)
                                { return Observe(event); });
    }

    ~TFixture()
    {
        Runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    void SetLocalState(TWhiteboardInfo::ETabletState state)
    {
        Whiteboard.Clear();
        auto* info = Whiteboard.AddTabletStateInfo();
        info->SetTabletId(TabletId);
        info->SetFollowerId(0);
        info->SetLeader(true);
        info->SetState(state);
        info->SetGeneration(Generation - 1);
    }

    void AddBootInfo(ui64 tabletId = TabletId)
    {
        TTabletBootInfo info;
        info.StorageInfoProto.SetTabletID(tabletId);
        info.SuggestedGeneration = Generation - 2;
        BootInfos.push_back(std::move(info));
    }

    TResponse Query(ui32 expectedResolveRequests = 1)
    {
        const auto result = GetTabletState(Service, TabletId);
        UNIT_ASSERT_VALUES_EQUAL(
            result.GetLocalNodeId(),
            Runtime.GetNodeId(NodeIndex));
        UNIT_ASSERT_VALUES_EQUAL(ResolveRequests, expectedResolveRequests);
        return result;
    }

    void SendEvent(TAutoPtr<IEventHandle> event)
    {
        // SetupTestEnv creates NBS on a separate runtime node. Local events
        // must originate there to avoid interconnect serialization.
        UNIT_ASSERT_VALUES_EQUAL(
            event->Recipient.NodeId(),
            Runtime.GetNodeId(NodeIndex));
        // Observers run before the intercepted event is delivered. Queue
        // follow-up events so they cannot overtake that event.
        Runtime.SendAsync(std::move(event), NodeIndex);
    }

    template <typename T>
    void Reply(const IEventHandle& request, std::unique_ptr<T> response)
    {
        SendEvent(new IEventHandle(
            request.Sender,
            request.Recipient,
            response.release(),
            0,
            request.Cookie));
    }

    void SendWhiteboard()
    {
        auto response =
            std::make_unique<TEvWhiteboard::TEvTabletStateResponse>();
        response->Record = Whiteboard;
        SendEvent(new IEventHandle(
            ProbeActor,
            MakeNodeWhiteboardServiceId(Runtime.GetNodeId(NodeIndex)),
            response.release()));
    }

    void SendDeadline()
    {
        if (!DeadlineSent) {
            DeadlineSent = true;
            SendEvent(new IEventHandle(
                ProbeActor,
                TabletActor,
                new TEvents::TEvWakeup()));
        }
    }

    void MaybeExpireUnresolvedProbe()
    {
        if (HoldResolver && ResolverSeen && WhiteboardSeen && BackupSeen) {
            SendDeadline();
        }
    }

    TTestActorRuntime::EEventAction Observe(TAutoPtr<IEventHandle>& event)
    {
        const auto type = event->GetTypeRewrite();
        // No tablet operation is requested by these tests. In particular, a
        // failed pipe probe must not fall back to checking state in Hive.
        AssertNoHiveOrBootRequest(type);

        if (type == TEvTabletResolver::EvForward &&
            event->Get<TEvTabletResolver::TEvForward>()->TabletID == TabletId)
        {
            const auto& request = *event->Get<TEvTabletResolver::TEvForward>();
            UNIT_ASSERT(!request.ResolveFlags.AllowFollower());
            UNIT_ASSERT(!request.Ev);
            ++ResolveRequests;
            ResolverSeen = true;
            PipeClient = event->Sender;
            if (!HoldResolver) {
                if (ResolveFails) {
                    Reply(
                        *event,
                        std::make_unique<TEvTabletResolver::TEvForwardResult>(
                            NKikimrProto::ERROR,
                            TabletId));
                } else {
                    Reply(
                        *event,
                        std::make_unique<TEvTabletResolver::TEvForwardResult>(
                            TabletId,
                            TabletActor,
                            StaleFirstAddress && ResolveRequests == 1
                                ? StaleSystemTabletActor
                                : SystemTabletActor,
                            1));
                }
            }
            MaybeExpireUnresolvedProbe();
            return TTestActorRuntime::EEventAction::DROP;
        }

        if (type == TEvTabletPipe::EvConnect &&
            event->Get<TEvTabletPipe::TEvConnect>()->Record.GetTabletId() ==
                TabletId)
        {
            ++ConnectRequests;
            if (StaleFirstAddress && event->Recipient == StaleSystemTabletActor)
            {
                Reply(
                    *event,
                    std::make_unique<TEvents::TEvUndelivered>(
                        type,
                        TEvents::TEvUndelivered::ReasonActorUnknown));
                return TTestActorRuntime::EEventAction::DROP;
            }
            // NBS accepts pipe connections through the system tablet.
            UNIT_ASSERT(event->Recipient == SystemTabletActor);
            Reply(
                *event,
                std::make_unique<TEvTabletPipe::TEvConnectResult>(
                    ConnectFails ? NKikimrProto::ERROR : NKikimrProto::OK,
                    TabletId,
                    event->Sender,
                    ConnectFails ? TActorId() : ServerId,
                    !ConnectedToFollower,
                    Generation,
                    TString()));
            return TTestActorRuntime::EEventAction::DROP;
        }

        if (type == TEvWhiteboard::EvTabletStateRequest) {
            const auto& request =
                event->Get<TEvWhiteboard::TEvTabletStateRequest>()->Record;
            if (request.FilterTabletIdSize() == 1 &&
                request.GetFilterTabletId(0) == TabletId)
            {
                ProbeActor = event->Sender;
                WhiteboardSeen = true;
                if (UndeliverWhiteboard) {
                    Reply(
                        *event,
                        std::make_unique<TEvents::TEvUndelivered>(
                            type,
                            TEvents::TEvUndelivered::ReasonActorUnknown));
                } else if (!DropWhiteboard) {
                    SendWhiteboard();
                }
                MaybeExpireUnresolvedProbe();
                return TTestActorRuntime::EEventAction::DROP;
            }
        }

        if (type == TEvHiveProxy::EvGetTabletBootInfosRequest &&
            event->Get<TEvHiveProxy::TEvGetTabletBootInfosRequest>()
                    ->TabletId == TabletId)
        {
            ProbeActor = event->Sender;
            BackupSeen = true;
            if (UndeliverBackup) {
                Reply(
                    *event,
                    std::make_unique<TEvents::TEvUndelivered>(
                        type,
                        TEvents::TEvUndelivered::ReasonActorUnknown));
            } else if (!DropBackup) {
                auto response = std::make_unique<
                    TEvHiveProxy::TEvGetTabletBootInfosResponse>(
                    MakeError(BackupError));
                response->TabletBootInfos = BootInfos;
                Reply(*event, std::move(response));
            }
            MaybeExpireUnresolvedProbe();
            return TTestActorRuntime::EEventAction::DROP;
        }

        if (type == TEvTabletPipe::EvClientConnected &&
            event->Get<TEvTabletPipe::TEvClientConnected>()->TabletId ==
                TabletId)
        {
            ProbeActor = event->Recipient;
            if (DisconnectAfterConnect) {
                SendEvent(new IEventHandle(
                    ProbeActor,
                    PipeClient,
                    new TEvTabletPipe::TEvClientDestroyed(
                        TabletId,
                        PipeClient,
                        ServerId)));
                SendWhiteboard();
            } else if (TimeoutAfterPipeResult) {
                SendDeadline();
            }
        }

        return TTestActorRuntime::DefaultObserverFunc(event);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGetTabletStateTest)
{
    Y_UNIT_TEST(ShouldConfirmRunningTabletWithoutBootInfo)
    {
        TFixture fixture;
        const auto result = fixture.Query();
        UNIT_ASSERT(result.GetState() == TResponse::RUNNING);
        UNIT_ASSERT_VALUES_EQUAL(
            result.GetLeaderNodeId(),
            fixture.ServerId.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(result.GetLeaderGeneration(), Generation);
        UNIT_ASSERT(result.GetLocalState() == TResponse::LOCAL_NOT_OBSERVED);
        UNIT_ASSERT(result.GetBootInfoState() == TResponse::BOOT_INFO_MISSING);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ConnectRequests, 1);
    }

    Y_UNIT_TEST(ShouldFindRunningTabletAfterStaleResolverAddress)
    {
        TFixture fixture;
        fixture.StaleFirstAddress = true;
        const auto result = fixture.Query(2);
        UNIT_ASSERT_C(
            result.GetState() == TResponse::RUNNING,
            result.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            result.GetLeaderNodeId(),
            fixture.ServerId.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(result.GetLeaderGeneration(), Generation);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ConnectRequests, 2);
    }

    Y_UNIT_TEST(ShouldKeepLeaderAndLocalObservationsIndependent)
    {
        TFixture fixture;
        fixture.SetLocalState(TWhiteboardInfo::Dead);
        fixture.AddBootInfo();
        const auto result = fixture.Query();
        UNIT_ASSERT(result.GetState() == TResponse::RUNNING);
        UNIT_ASSERT(result.GetLocalState() == TResponse::LOCAL_STOPPED);
        UNIT_ASSERT_VALUES_EQUAL(result.GetLocalGeneration(), Generation - 1);
        UNIT_ASSERT(
            result.GetBootInfoState() == TResponse::BOOT_INFO_AVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(
            result.GetSuggestedGeneration(),
            Generation - 2);
    }

    Y_UNIT_TEST(ShouldReportStartingAndStoppedObservationsWithoutBooting)
    {
        const std::pair<TWhiteboardInfo::ETabletState, TResponse::ELocalState>
            cases[] = {
                {TWhiteboardInfo::Created, TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::ResolveStateStorage,
                 TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::Candidate, TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::BlockBlobStorage, TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::RebuildGraph, TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::WriteZeroEntry, TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::Restored, TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::Discover, TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::Lock, TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::ResolveLeader, TResponse::LOCAL_STARTING},
                {TWhiteboardInfo::Dead, TResponse::LOCAL_STOPPED},
                {TWhiteboardInfo::Stopped, TResponse::LOCAL_STOPPED},
                {TWhiteboardInfo::Deleted, TResponse::LOCAL_STOPPED},
                {TWhiteboardInfo::Active, TResponse::LOCAL_ACTIVE},
                {TWhiteboardInfo::Reserved14, TResponse::LOCAL_UNKNOWN},
            };

        for (const auto& [observed, expected]: cases) {
            TFixture fixture;
            fixture.ResolveFails = true;
            fixture.SetLocalState(observed);
            const auto result = fixture.Query(2);
            UNIT_ASSERT(result.GetState() == TResponse::UNKNOWN);
            UNIT_ASSERT(result.GetLocalState() == expected);
            UNIT_ASSERT_VALUES_EQUAL(result.GetLeaderNodeId(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.ConnectRequests, 0);
        }
    }

    Y_UNIT_TEST(ShouldNotInferLivenessFromBackupOrCachedAddress)
    {
        TFixture fixture;
        fixture.ConnectFails = true;
        fixture.SetLocalState(TWhiteboardInfo::Active);
        fixture.AddBootInfo();
        const auto result = fixture.Query(2);
        UNIT_ASSERT(result.GetState() == TResponse::UNKNOWN);
        UNIT_ASSERT(result.GetLocalState() == TResponse::LOCAL_ACTIVE);
        UNIT_ASSERT(
            result.GetBootInfoState() == TResponse::BOOT_INFO_AVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(result.GetLeaderGeneration(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ConnectRequests, 2);
    }

    Y_UNIT_TEST(ShouldNotTreatFollowerAsRunningLeader)
    {
        TFixture fixture;
        fixture.ConnectedToFollower = true;
        fixture.SetLocalState(TWhiteboardInfo::Active);
        auto* info = fixture.Whiteboard.MutableTabletStateInfo(0);
        info->SetFollowerId(1);
        info->SetLeader(false);
        const auto result = fixture.Query();
        UNIT_ASSERT(result.GetState() == TResponse::UNKNOWN);
        UNIT_ASSERT(result.GetLocalState() == TResponse::LOCAL_NOT_OBSERVED);
    }

    Y_UNIT_TEST(ShouldReportUnknownForAmbiguousWhiteboardResponse)
    {
        for (bool duplicate: {false, true}) {
            TFixture fixture;
            fixture.SetLocalState(TWhiteboardInfo::Active);
            if (duplicate) {
                *fixture.Whiteboard.AddTabletStateInfo() =
                    fixture.Whiteboard.GetTabletStateInfo(0);
            } else {
                fixture.Whiteboard.MutableTabletStateInfo(0)->ClearState();
            }
            const auto result = fixture.Query();
            UNIT_ASSERT(result.GetLocalState() == TResponse::LOCAL_UNKNOWN);
        }
    }

    Y_UNIT_TEST(ShouldRejectUnexpectedPackedWhiteboardResponse)
    {
        TFixture fixture;
        fixture.Whiteboard.SetPacked5("unexpected format");
        const auto result = fixture.Query();
        UNIT_ASSERT(result.GetLocalState() == TResponse::LOCAL_UNKNOWN);
    }

    Y_UNIT_TEST(ShouldDistinguishMissingBackupFromUnavailableBackup)
    {
        for (ui32 error: {E_PRECONDITION_FAILED, E_REJECTED}) {
            TFixture fixture;
            fixture.BackupError = error;
            const auto result = fixture.Query();
            UNIT_ASSERT(result.GetState() == TResponse::RUNNING);
            UNIT_ASSERT(
                result.GetBootInfoState() ==
                (error == E_PRECONDITION_FAILED
                     ? TResponse::BOOT_INFO_NOT_CONFIGURED
                     : TResponse::BOOT_INFO_UNKNOWN));
            UNIT_ASSERT(!result.GetBootInfoMessage().empty());
        }
    }

    Y_UNIT_TEST(ShouldRejectUnexpectedBackupEntries)
    {
        TFixture fixture;
        fixture.AddBootInfo(TabletId + 1);
        const auto result = fixture.Query();
        UNIT_ASSERT(result.GetBootInfoState() == TResponse::BOOT_INFO_UNKNOWN);
    }

    Y_UNIT_TEST(ShouldReturnPartialDiagnosticsOnTimeout)
    {
        TFixture fixture;
        fixture.DropWhiteboard = true;
        fixture.DropBackup = true;
        fixture.TimeoutAfterPipeResult = true;
        const auto result = fixture.Query();
        UNIT_ASSERT(fixture.DeadlineSent);
        UNIT_ASSERT_C(
            result.GetState() == TResponse::RUNNING,
            result.DebugString());
        UNIT_ASSERT(result.GetLocalState() == TResponse::LOCAL_UNKNOWN);
        UNIT_ASSERT(result.GetBootInfoState() == TResponse::BOOT_INFO_UNKNOWN);
    }

    Y_UNIT_TEST(ShouldBoundUnresolvedProbeWithoutDeclaringTabletStopped)
    {
        TFixture fixture;
        fixture.HoldResolver = true;
        fixture.AddBootInfo();
        const auto result = fixture.Query();
        UNIT_ASSERT(fixture.DeadlineSent);
        UNIT_ASSERT(result.GetState() == TResponse::UNKNOWN);
        UNIT_ASSERT(
            result.GetBootInfoState() == TResponse::BOOT_INFO_AVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ConnectRequests, 0);
    }

    Y_UNIT_TEST(ShouldHandleUnavailableDiagnosticServices)
    {
        TFixture fixture;
        fixture.UndeliverWhiteboard = true;
        fixture.UndeliverBackup = true;
        const auto result = fixture.Query();
        UNIT_ASSERT(result.GetState() == TResponse::RUNNING);
        UNIT_ASSERT(result.GetLocalState() == TResponse::LOCAL_UNKNOWN);
        UNIT_ASSERT(result.GetBootInfoState() == TResponse::BOOT_INFO_UNKNOWN);
    }

    Y_UNIT_TEST(ShouldForgetConnectionLostWhileCollectingDiagnostics)
    {
        TFixture fixture;
        fixture.DropWhiteboard = true;
        fixture.DisconnectAfterConnect = true;
        const auto result = fixture.Query();
        UNIT_ASSERT(result.GetState() == TResponse::UNKNOWN);
        UNIT_ASSERT_VALUES_EQUAL(result.GetLeaderNodeId(), 0);
        UNIT_ASSERT_VALUES_EQUAL(result.GetLeaderGeneration(), 0);
    }

    Y_UNIT_TEST(ShouldRejectInvalidRequestsBeforeProbing)
    {
        TFixture fixture;
        for (const TString input:
             {"not json",
              "{}",
              "{\"TabletId\":0}",
              "{\"TabletId\":1,\"TimeoutMs\":30001}",
              "{\"TabletId\":1,\"TimeoutMs\":-1}",
              "{\"TabletId\":1,\"UnexpectedField\":true}"})
        {
            fixture.Service.SendExecuteActionRequest("GetTabletState", input);
            const auto response = fixture.Service.RecvExecuteActionResponse();
            UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.ResolveRequests, 0);
        UNIT_ASSERT(!fixture.WhiteboardSeen);
        UNIT_ASSERT(!fixture.BackupSeen);
    }

    Y_UNIT_TEST(ShouldInspectMountedVolumeWithoutRestartingIt)
    {
        TTestEnv env;
        const auto nodeIndex = SetupTestEnv(env);
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIndex);
        service.CreateVolume();

        ui64 tabletId = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvServicePrivate::EvVolumeTabletStatus)
                {
                    tabletId =
                        event->Get<TEvServicePrivate::TEvVolumeTabletStatus>()
                            ->TabletId;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        service.MountVolume();
        UNIT_ASSERT(tabletId);

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                const auto type = event->GetTypeRewrite();
                AssertNoHiveOrBootRequest(type);
                if (type == TEvWhiteboard::EvTabletStateRequest) {
                    // The liveness check must work independently of local
                    // monitoring records, using the actual mounted tablet.
                    runtime.SendAsync(
                        new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            new TEvWhiteboard::TEvTabletStateResponse(),
                            0,
                            event->Cookie),
                        nodeIndex);
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto result = GetTabletState(service, tabletId);
        UNIT_ASSERT_C(
            result.GetState() == TResponse::RUNNING,
            result.DebugString());
        UNIT_ASSERT(result.GetLeaderNodeId() == runtime.GetNodeId(nodeIndex));
        UNIT_ASSERT(result.GetLeaderGeneration());
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
