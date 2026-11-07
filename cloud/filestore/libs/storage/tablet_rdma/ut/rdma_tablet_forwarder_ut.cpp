#include <cloud/filestore/libs/rdma/transport.h>
#include <cloud/filestore/libs/storage/tablet_rdma/rdma_server_handler.h>
#include <cloud/filestore/libs/storage/tablet_rdma/rdma_tablet_forwarder.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/common/context.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TTestEnv Env;
    TTestActorRuntime& Runtime;
    const ui32 NodeIdx;
    const ui64 TabletId;
    TIndexTabletClient Tablet;

    TRdmaForwarderCountersPtr Counters;
    TActorId ForwarderId;
    IRdmaTabletRequestHandlerPtr Handler;

    explicit TEnv(TDuration requestTimeout)
        : Runtime(Env.GetRuntime())
        , NodeIdx(Env.AddDynamicNode())
        , TabletId(Env.BootIndexTablet(NodeIdx))
        , Tablet(Runtime, NodeIdx, TabletId)
        , Counters(std::make_shared<TRdmaForwarderCounters>(
              MakeIntrusive<NMonitoring::TDynamicCounters>()))
    {
        ForwarderId = Runtime.Register(
            CreateRdmaTabletForwarderActor(requestTimeout, Counters).release(),
            NodeIdx);
        Runtime.EnableScheduleForActor(ForwarderId);

        Handler = CreateActorSystemRdmaRequestHandler(
            Runtime.GetActorSystem(NodeIdx),
            ForwarderId,
            Counters);

        Tablet.InitSession("client", "session");
    }

    TFuture<TTabletTransportResponse> Send(
        ui64 tabletId,
        ui32 methodId,
        TStringBuf body)
    {
        return Handler->HandleRequest(
            tabletId,
            methodId,
            body,
            MakeIntrusive<TCallContextBase>(ui64{0}));
    }

    template <typename TRequest>
    TFuture<TTabletTransportResponse> Send(std::unique_ptr<TRequest> request)
    {
        TString body;
        Y_ABORT_UNLESS(request->Record.SerializeToString(&body));
        return Send(TabletId, TRequest::EventType, body);
    }

    TTabletTransportResponse Wait(const TFuture<TTabletTransportResponse>& f)
    {
        Runtime.DispatchEvents(
            {.CustomFinalCondition = [&] { return f.HasValue(); }},
            TDuration::Seconds(30));
        UNIT_ASSERT(f.HasValue());
        return f.GetValue();
    }

    template <typename TRecord>
    const TRecord& Record(const TTabletTransportResponse& response) const
    {
        UNIT_ASSERT_C(
            !HasError(response.Error),
            FormatError(response.Error));
        UNIT_ASSERT(response.ResponseProto);
        return static_cast<const TRecord&>(*response.ResponseProto);
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

    void WaitUntil(const bool* condition)
    {
        Runtime.DispatchEvents(
            {.CustomFinalCondition = [=] { return *condition; }},
            TDuration::Seconds(30));
        UNIT_ASSERT(*condition);
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
        UNIT_ASSERT_VALUES_EQUAL(2, env.Counters->OkReplies->Val());
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

    Y_UNIT_TEST(ShouldRejectRequestsWithoutTabletId)
    {
        TEnv env(TDuration::Zero());

        TString body;
        Y_ABORT_UNLESS(env.Tablet.CreateGetStorageStatsRequest()
                           ->Record.SerializeToString(&body));

        auto future = env.Send(
            0,
            TEvIndexTablet::EvGetStorageStatsRequest,
            body);
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_ARGUMENT),
            future.GetValue().Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->InvalidRequests->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->Requests->Val());
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

        auto unknown = env.Send(env.TabletId, 1, "");
        UNIT_ASSERT(unknown.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            unknown.GetValue().Error.GetCode());

        auto createSession = env.Send(
            env.TabletId,
            TEvIndexTablet::EvCreateSessionRequest,
            "");
        UNIT_ASSERT(createSession.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            createSession.GetValue().Error.GetCode());

        UNIT_ASSERT_VALUES_EQUAL(2, env.Counters->UnsupportedMethods->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->Requests->Val());
    }

    Y_UNIT_TEST(ShouldRejectMalformedBodies)
    {
        TEnv env(TDuration::Zero());

        auto future = env.Send(
            env.TabletId,
            TEvService::EvCreateNodeRequest,
            TString(11, '\xff'));
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            future.GetValue().Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->ParseErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->Requests->Val());
    }

    Y_UNIT_TEST(ShouldRejectInFlightRequestsWhenTabletRestarts)
    {
        TEnv env(TDuration::Zero());

        bool dropped = false;
        env.DropResponsesToForwarder(
            TEvIndexTablet::EvGetStorageStatsResponse,
            &dropped);

        auto future = env.Send(env.Tablet.CreateGetStorageStatsRequest());
        env.WaitUntil(&dropped);
        UNIT_ASSERT(!future.HasValue());

        env.Tablet.RebootTablet();

        auto response = env.Wait(future);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->PipeFailures->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Counters->InFlightRequests->Val());

        env.Runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);

        auto stats = env.Wait(
            env.Send(env.Tablet.CreateGetStorageStatsRequest()));
        env.Record<NProtoPrivate::TGetStorageStatsResponse>(stats);
    }

    Y_UNIT_TEST(ShouldRejectRequestsWhenPipeCannotConnect)
    {
        TEnv env(TDuration::Zero());

        const auto forwarderId = env.ForwarderId;
        env.Runtime.SetObserverFunc(
            [=](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvTabletPipe::EvClientConnected &&
                    event->Recipient == forwarderId)
                {
                    auto* msg = event->Get<TEvTabletPipe::TEvClientConnected>();
                    const_cast<NKikimrProto::EReplyStatus&>(msg->Status) =
                        NKikimrProto::ERROR;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response = env.Wait(
            env.Send(env.Tablet.CreateGetStorageStatsRequest()));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->PipeFailures->Val());
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
        UNIT_ASSERT_VALUES_EQUAL(1, env.Counters->Timeouts->Val());
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
        env.WaitUntil(&dropped);

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

        TTestEnv testEnv({}, storageConfig);
        auto& runtime = testEnv.GetRuntime();
        const ui32 nodeIdx = testEnv.AddDynamicNode();
        const ui64 tabletId = testEnv.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        const ui64 nodeId =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "file"));
        const ui64 handle = CreateHandle(tablet, nodeId);
        tablet.WriteData(handle, 0, 4_KB, 'a');

        auto counters = std::make_shared<TRdmaForwarderCounters>(
            MakeIntrusive<NMonitoring::TDynamicCounters>());
        auto forwarderId = runtime.Register(
            CreateRdmaTabletForwarderActor(TDuration::Zero(), counters)
                .release(),
            nodeIdx);
        auto handler = CreateActorSystemRdmaRequestHandler(
            runtime.GetActorSystem(nodeIdx),
            forwarderId,
            counters);

        TString body;
        Y_ABORT_UNLESS(tablet.CreateReadDataRequest(handle, 0, 4_KB)
                           ->Record.SerializeToString(&body));
        auto future = handler->HandleRequest(
            tabletId,
            TEvService::EvReadDataRequest,
            body,
            MakeIntrusive<TCallContextBase>(ui64{0}));

        runtime.DispatchEvents(
            {.CustomFinalCondition = [&] { return future.HasValue(); }},
            TDuration::Seconds(30));
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_REJECTED),
            future.GetValue().Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, counters->ErrorReplies->Val());
    }
}

}   // namespace NCloud::NFileStore::NStorage
