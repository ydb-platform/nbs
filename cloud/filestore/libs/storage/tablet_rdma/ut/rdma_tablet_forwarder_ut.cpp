#include <cloud/filestore/libs/rdma/transport.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet_rdma/rdma_server_handler.h>
#include <cloud/filestore/libs/storage/tablet_rdma/rdma_tablet_forwarder.h>
#include <cloud/filestore/libs/storage/tablet_rdma/rdma_tablet_registry.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/context.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDeadActor final
    : public TActorBootstrapped<TDeadActor>
{
public:
    void Bootstrap()
    {
        PassAway();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPipe
{
    TActorId Sender;
    TActorId ClientId;
    TActorId ServerId;
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TTestEnv Env;
    TTestActorRuntime& Runtime;
    const ui32 NodeIdx;
    const ui64 TabletId;
    TIndexTabletClient Tablet;

    const TRdmaTabletRegistryPtr Registry;
    const TRdmaForwarderCountersPtr Counters;
    TActorId ForwarderId;
    IRdmaTabletRequestHandlerPtr Handler;
    TPipe Pipe;

    explicit TEnv(TDuration requestTimeout)
        : TEnv(requestTimeout, NProto::TStorageConfig())
    {}

    TEnv(TDuration requestTimeout, NProto::TStorageConfig storageConfig)
        : Env({}, std::move(storageConfig))
        , Runtime(Env.GetRuntime())
        , NodeIdx(Env.AddDynamicNode())
        , TabletId(Env.BootIndexTablet(NodeIdx))
        , Tablet(Runtime, NodeIdx, TabletId)
        , Registry(Env.GetRdmaTabletRegistry(Runtime.GetNodeId(NodeIdx)))
        , Counters(std::make_shared<TRdmaForwarderCounters>(
              MakeIntrusive<NMonitoring::TDynamicCounters>()))
    {
        ForwarderId = Runtime.Register(
            CreateRdmaTabletForwarderActor(requestTimeout, Counters).release(),
            NodeIdx);
        Runtime.EnableScheduleForActor(ForwarderId);
        Registry->AttachForwarder(ForwarderId);

        Handler = CreateActorSystemRdmaRequestHandler(
            Runtime.GetActorSystem(NodeIdx),
            ForwarderId,
            Registry,
            Counters);

        Tablet.InitSession("client", "session");
        Pipe = ConnectPipe();
    }

    TFuture<TTabletTransportResponse> Send(
        const TActorId& pipeServerId,
        ui32 methodId,
        TStringBuf body)
    {
        NProtoPrivate::TTabletRdmaRequest envelope;
        envelope.SetMethodId(methodId);
        envelope.MutablePipeServerId()->SetRawX1(pipeServerId.RawX1());
        envelope.MutablePipeServerId()->SetRawX2(pipeServerId.RawX2());

        return Handler->HandleRequest(
            envelope,
            body,
            MakeIntrusive<TCallContextBase>(ui64{0}));
    }

    template <typename TRequest>
    TFuture<TTabletTransportResponse> Send(
        std::unique_ptr<TRequest> request,
        const TActorId& pipeServerId)
    {
        TString body;
        Y_ABORT_UNLESS(request->Record.SerializeToString(&body));
        return Send(pipeServerId, TRequest::EventType, body);
    }

    template <typename TRequest>
    TFuture<TTabletTransportResponse> Send(std::unique_ptr<TRequest> request)
    {
        return Send(std::move(request), Pipe.ServerId);
    }

    TTabletTransportResponse Wait(TFuture<TTabletTransportResponse> future)
    {
        Runtime.DispatchEvents(
            {.CustomFinalCondition = [&] { return future.HasValue(); }},
            TDuration::Seconds(30));
        UNIT_ASSERT(future.HasValue());
        return future.ExtractValue();
    }

    template <typename TRecord>
    static const TRecord& Record(const TTabletTransportResponse& response)
    {
        UNIT_ASSERT_C(
            !HasError(response.Error),
            FormatError(response.Error));
        UNIT_ASSERT(response.Response);
        return static_cast<const TRecord&>(*response.Response);
    }

    TPipe ConnectPipe()
    {
        TPipe pipe;
        pipe.Sender = Runtime.AllocateEdgeActor(NodeIdx);
        pipe.ClientId = Runtime.ConnectToPipe(
            TabletId,
            pipe.Sender,
            NodeIdx,
            GetPipeConfigWithRetries());

        const auto clientId = pipe.ClientId;
        auto connected =
            Runtime.GrabEdgeEventIf<TEvTabletPipe::TEvClientConnected>(
                pipe.Sender,
                [=](const auto& ev)
                { return ev->Get()->ClientId == clientId; });
        pipe.ServerId = connected->Get()->ServerId;
        UNIT_ASSERT(pipe.ServerId);

        WaitUntil([&] { return !!Registry->FindTablet(pipe.ServerId); });
        return pipe;
    }

    void DropResponsesToForwarder(ui32 eventType, bool* dropped)
    {
        const auto forwarderId = ForwarderId;
        Runtime.SetObserverFunc(
            [=](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == eventType &&
                    event->Recipient == forwarderId)
                {
                    *dropped = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
    }

    void WaitUntil(std::function<bool()> condition)
    {
        if (!condition()) {
            Runtime.DispatchEvents(
                {.CustomFinalCondition = condition},
                TDuration::Seconds(30));
        }
        UNIT_ASSERT(condition());
    }

    TString GetStorageStatsBody()
    {
        TString body;
        Y_ABORT_UNLESS(
            Tablet.CreateGetStorageStatsRequest()->Record.SerializeToString(
                &body));
        return body;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRdmaTabletForwarderTest)
{
    Y_UNIT_TEST(ShouldForwardNodeRequestsToTablet)
    {
        TEnv env(TDuration::Zero());

        auto created = env.Wait(env.Send(env.Tablet.CreateCreateNodeRequest(
            TCreateNodeArgs::File(RootNodeId, "test"))));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TEvService::EvCreateNodeResponse),
            created.MethodId);
        const ui64 nodeId = env.Record<NProto::TCreateNodeResponse>(created)
            .GetNode()
            .GetId();
        UNIT_ASSERT(nodeId != InvalidNodeId);

        auto attr = env.Wait(env.Send(
            env.Tablet.CreateGetNodeAttrRequest(RootNodeId, "test")));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TEvService::EvGetNodeAttrResponse),
            attr.MethodId);
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId,
            env.Record<NProto::TGetNodeAttrResponse>(attr).GetNode().GetId());

        auto viaPipe = env.Tablet.GetNodeAttr(RootNodeId, "test");
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId,
            viaPipe->Record.GetNode().GetId());

        UNIT_ASSERT_VALUES_EQUAL(2, env.Counters->Requests->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->InFlightRequests->Val());
    }

    Y_UNIT_TEST(ShouldForwardDataRequestsToTablet)
    {
        TEnv env(TDuration::Zero());

        const ui64 nodeId =
            CreateNode(env.Tablet, TCreateNodeArgs::File(RootNodeId, "file"));
        const ui64 handle = CreateHandle(env.Tablet, nodeId);

        auto written = env.Wait(env.Send(
            env.Tablet.CreateWriteDataRequest(handle, 0, 4_KB, 'a')));
        env.Record<NProto::TWriteDataResponse>(written);

        auto read = env.Wait(
            env.Send(env.Tablet.CreateReadDataRequest(handle, 0, 4_KB)));
        const auto& record = env.Record<NProto::TReadDataResponse>(read);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(4_KB, 'a'),
            record.GetBuffer().substr(record.GetBufferOffset()));

        UNIT_ASSERT_VALUES_EQUAL(
            TString(4_KB, 'a'),
            ReadData(env.Tablet, handle, 4_KB));
    }

    Y_UNIT_TEST(ShouldForwardTabletRequestsToTablet)
    {
        TEnv env(TDuration::Zero());

        CreateNode(env.Tablet, TCreateNodeArgs::File(RootNodeId, "file"));

        auto stats = env.Wait(
            env.Send(env.Tablet.CreateGetStorageStatsRequest()));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TEvIndexTablet::EvGetStorageStatsResponse),
            stats.MethodId);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Record<NProtoPrivate::TGetStorageStatsResponse>(stats)
                .GetStats()
                .GetUsedNodesCount());
    }

    Y_UNIT_TEST(ShouldDeliverRequestsOnBehalfOfClientPipe)
    {
        using TEvRequest = TEvIndexTablet::TEvGetStorageStatsRequest;

        TEnv env(TDuration::Zero());

        const auto tabletActorId = env.Registry->FindTablet(env.Pipe.ServerId);
        UNIT_ASSERT(tabletActorId);

        TActorId recipient;
        TActorId target;
        auto observer = env.Runtime.AddObserver<TEvRequest>(
            [&](TEvRequest::TPtr& ev)
            {
                recipient = ev->Recipient;
                target = ev->GetRecipientRewrite();
            });

        auto stats = env.Wait(
            env.Send(env.Tablet.CreateGetStorageStatsRequest()));
        env.Record<NProtoPrivate::TGetStorageStatsResponse>(stats);

        UNIT_ASSERT_VALUES_EQUAL(env.Pipe.ServerId, recipient);
        UNIT_ASSERT_VALUES_EQUAL(tabletActorId, target);
    }

    Y_UNIT_TEST(ShouldRejectRequestsWithoutPipeServer)
    {
        TEnv env(TDuration::Zero());

        auto future = env.Send(
            TActorId(),
            TEvIndexTablet::EvGetStorageStatsRequest,
            env.GetStorageStatsBody());
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_ARGUMENT),
            future.GetValue().Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Requests->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Errors->Val());
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedMethods)
    {
        TEnv env(TDuration::Zero());

        UNIT_ASSERT(!IsRdmaTabletMethod(1));
        UNIT_ASSERT(
            !IsRdmaTabletMethod(TEvIndexTablet::EvCreateSessionRequest));
        UNIT_ASSERT(IsRdmaTabletMethod(TEvService::EvCreateNodeRequest));
        UNIT_ASSERT(
            IsRdmaTabletMethod(TEvIndexTablet::EvGetStorageStatsRequest));

        auto unknown = env.Send(env.Pipe.ServerId, 1, "");
        UNIT_ASSERT(unknown.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_NOT_IMPLEMENTED),
            unknown.GetValue().Error.GetCode());

        auto createSession = env.Send(
            env.Pipe.ServerId,
            TEvIndexTablet::EvCreateSessionRequest,
            "");
        UNIT_ASSERT(createSession.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_NOT_IMPLEMENTED),
            createSession.GetValue().Error.GetCode());

        UNIT_ASSERT_VALUES_EQUAL(2, env.Counters->Requests->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, env.Counters->Errors->Val());
    }

    Y_UNIT_TEST(ShouldRejectMalformedBodies)
    {
        TEnv env(TDuration::Zero());

        auto future = env.Send(
            env.Pipe.ServerId,
            TEvService::EvCreateNodeRequest,
            TString(11, '\xff'));
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_ARGUMENT),
            future.GetValue().Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Requests->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Errors->Val());
    }

    Y_UNIT_TEST(ShouldRejectRequestsForUnknownPipes)
    {
        TEnv env(TDuration::Zero());

        auto unknown = env.Send(
            env.Runtime.AllocateEdgeActor(env.NodeIdx),
            TEvIndexTablet::EvGetStorageStatsRequest,
            env.GetStorageStatsBody());
        UNIT_ASSERT(unknown.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            unknown.GetValue().Error.GetCode());

        env.Runtime.ClosePipe(env.Pipe.ClientId, env.Pipe.Sender, env.NodeIdx);
        env.WaitUntil(
            [&] { return !env.Registry->FindTablet(env.Pipe.ServerId); });

        auto disconnected = env.Send(
            env.Pipe.ServerId,
            TEvIndexTablet::EvGetStorageStatsRequest,
            env.GetStorageStatsBody());
        UNIT_ASSERT(disconnected.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            disconnected.GetValue().Error.GetCode());

        UNIT_ASSERT_VALUES_EQUAL(2, env.Counters->Requests->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, env.Counters->Errors->Val());
    }

    Y_UNIT_TEST(ShouldRejectRequestsUndeliveredToTablet)
    {
        TEnv env(TDuration::Zero());

        const auto deadActorId =
            env.Runtime.Register(new TDeadActor(), env.NodeIdx);
        env.Runtime.DispatchEvents({}, TDuration::MilliSeconds(10));

        const auto pipeServerId = env.Runtime.AllocateEdgeActor(env.NodeIdx);
        env.Registry->RegisterPipe(pipeServerId, deadActorId);

        auto response = env.Wait(env.Send(
            env.Tablet.CreateGetStorageStatsRequest(),
            pipeServerId));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->InFlightRequests->Val());
    }

    Y_UNIT_TEST(ShouldRejectInFlightRequestsWhenTabletRestarts)
    {
        TEnv env(TDuration::Zero());

        bool dropped = false;
        env.DropResponsesToForwarder(
            TEvIndexTablet::EvGetStorageStatsResponse,
            &dropped);

        auto future = env.Send(env.Tablet.CreateGetStorageStatsRequest());
        env.WaitUntil([&] { return dropped; });
        UNIT_ASSERT(!future.HasValue());

        env.Tablet.RebootTablet();

        auto response = env.Wait(future);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->InFlightRequests->Val());

        env.Runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);
        env.Tablet.WaitReady();
        env.Pipe = env.ConnectPipe();

        auto stats = env.Wait(
            env.Send(env.Tablet.CreateGetStorageStatsRequest()));
        env.Record<NProtoPrivate::TGetStorageStatsResponse>(stats);
    }

    Y_UNIT_TEST(ShouldTimeOutRequests)
    {
        TEnv env(TDuration::Seconds(4));

        bool dropped = false;
        env.DropResponsesToForwarder(
            TEvIndexTablet::EvGetStorageStatsResponse,
            &dropped);

        auto response = env.Wait(
            env.Send(env.Tablet.CreateGetStorageStatsRequest()));
        UNIT_ASSERT(dropped);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_TIMEOUT),
            response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->InFlightRequests->Val());
    }

    Y_UNIT_TEST(ShouldRejectRequestsWhenForwarderStops)
    {
        TEnv env(TDuration::Zero());

        bool dropped = false;
        env.DropResponsesToForwarder(
            TEvIndexTablet::EvGetStorageStatsResponse,
            &dropped);

        auto inFlight = env.Send(env.Tablet.CreateGetStorageStatsRequest());
        env.WaitUntil([&] { return dropped; });

        env.Runtime.Send(
            new IEventHandle(
                env.ForwarderId,
                TActorId(),
                new TEvents::TEvPoisonPill()),
            env.NodeIdx);

        auto response = env.Wait(inFlight);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->InFlightRequests->Val());

        auto late = env.Wait(
            env.Send(env.Tablet.CreateGetStorageStatsRequest()));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            late.Error.GetCode());
    }

    Y_UNIT_TEST(ShouldRejectResponsesWithPayload)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetExternalReadDataPayload(true);

        TEnv env(TDuration::Zero(), std::move(storageConfig));

        const ui64 nodeId =
            CreateNode(env.Tablet, TCreateNodeArgs::File(RootNodeId, "file"));
        const ui64 handle = CreateHandle(env.Tablet, nodeId);
        env.Tablet.WriteData(handle, 0, 4_KB, 'a');

        auto response = env.Wait(
            env.Send(env.Tablet.CreateReadDataRequest(handle, 0, 4_KB)));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_NOT_IMPLEMENTED),
            response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Errors->Val());
    }

    Y_UNIT_TEST(ShouldKeepFileSystemEventsOfClientPipe)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetFileSystemEventsEnabled(true);
        storageConfig.SetFileSystemEventSubscriptionsSealPeriod(
            TDuration::Minutes(10).MilliSeconds());
        storageConfig.SetFileSystemEventSubscriptionsCacheTTLMargin(
            TDuration::Minutes(1).MilliSeconds());

        TEnv env(TDuration::Zero(), std::move(storageConfig));

        const ui64 nodeId =
            CreateNode(env.Tablet, TCreateNodeArgs::File(RootNodeId, "file"));

        env.Runtime.SendToPipe(
            env.Pipe.ClientId,
            env.Pipe.Sender,
            env.Tablet.CreateGetNodeAttrRequest(RootNodeId, "file").release(),
            env.NodeIdx);
        auto viaPipe =
            env.Runtime.GrabEdgeEvent<TEvService::TEvGetNodeAttrResponse>(
                env.Pipe.Sender);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Minutes(9).MilliSeconds(),
            viaPipe->Get()->Record.GetNodeCacheTTL());

        auto attr = env.Wait(env.Send(
            env.Tablet.CreateGetNodeAttrRequest(RootNodeId, "file")));
        const auto& record = env.Record<NProto::TGetNodeAttrResponse>(attr);
        UNIT_ASSERT_VALUES_EQUAL(nodeId, record.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(0, record.GetNodeCacheTTL());
        UNIT_ASSERT_VALUES_EQUAL(0, record.GetNodeRefCacheTTL());

        NProto::TFileSystemEvent event;
        event.MutableInvalidateNode()->SetNodeId(nodeId);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Tablet.GenerateFileSystemEvent(event)->Record.GetClientCount());
        UNIT_ASSERT(
            env.Runtime.GrabEdgeEvent<TEvIndexTabletProxy::TEvFileSystemEvent>(
                env.Pipe.Sender,
                TDuration::Seconds(5)));
    }
}

}   // namespace NCloud::NFileStore::NStorage
