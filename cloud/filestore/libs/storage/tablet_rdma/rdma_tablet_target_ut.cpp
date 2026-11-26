#include "rdma_tablet_target.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/rdma/protocol.h>
#include <cloud/filestore/libs/rdma_test/test_server.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_actor_system.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

using namespace NCloud::NStorage::NRdma;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestServer final
    : public IServer
{
    const IServerPtr Server;

    ui32 Started = 0;
    ui32 Stopped = 0;
    TString StartError;

    explicit TTestServer(IServerPtr server)
        : Server(std::move(server))
    {}

    IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        IServerHandlerPtr handler) override
    {
        if (!Server) {
            return nullptr;
        }
        return Server->StartEndpoint(std::move(host), port, std::move(handler));
    }

    IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        IServerEndpointHandlerPtr endpointHandler) override
    {
        if (!Server) {
            return nullptr;
        }
        return Server->StartEndpoint(
            std::move(host),
            port,
            std::move(endpointHandler));
    }

    void Start() override
    {
        ++Started;
        if (StartError) {
            ythrow yexception() << StartError;
        }
    }

    void Stop() override
    {
        ++Stopped;
    }

    void DumpHtml(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TStorageConfig RdmaEnabledConfig()
{
    NProto::TStorageConfig config;
    config.SetTabletDirectRdmaEnabled(true);
    return config;
}

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TTestEnv Env;
    TTestActorRuntime& Runtime;
    const ui32 NodeIdx;
    const ui64 TabletId;
    TIndexTabletClient Tablet;

    const TRdmaTabletRegistryPtr Registry;

    bool Responded = false;
    size_t ResponseBytes = 0;
    bool Failed = false;
    TString In;
    TString Out = TString(64_KB, 0);

    const std::shared_ptr<TTestRdmaServer> Endpoints;

    TEnv()
        : Env({}, RdmaEnabledConfig())
        , Runtime(Env.GetRuntime())
        , NodeIdx(Env.AddDynamicNode())
        , TabletId(Env.BootIndexTablet(NodeIdx))
        , Tablet(Runtime, NodeIdx, TabletId)
        , Registry(Env.GetRdmaTabletRegistry(Runtime.GetNodeId(NodeIdx)))
        , Endpoints(std::make_shared<TTestRdmaServer>(
            [this](void* context, size_t responseBytes)
            {
                Y_UNUSED(context);
                Responded = true;
                ResponseBytes = responseBytes;
            },
            [this](void* context, ui32 error, TStringBuf message)
            {
                Y_UNUSED(context);
                Y_UNUSED(error);
                Y_UNUSED(message);
                Failed = true;
            }))
    {
        Tablet.InitSession("client", "session");
    }

    IStartablePtr CreateTarget(IServerPtr server)
    {
        return CreateTarget(std::move(server), NProto::TRdmaConfig());
    }

    IStartablePtr CreateTarget(IServerPtr server, NProto::TRdmaConfig config)
    {
        return CreateRdmaTabletTarget(
            config,
            CreateLoggingService("console"),
            MakeIntrusive<TTestActorSystem>(Runtime, NodeIdx),
            std::move(server),
            Registry);
    }

    TActorId ConnectPipe()
    {
        const auto sender = Runtime.AllocateEdgeActor(NodeIdx);
        const auto clientId = Runtime.ConnectToPipe(
            TabletId,
            sender,
            NodeIdx,
            GetPipeConfigWithRetries());

        auto connected =
            Runtime.GrabEdgeEventIf<TEvTabletPipe::TEvClientConnected>(
                sender,
                [=](const auto& ev)
                { return ev->Get()->ClientId == clientId; });
        const auto serverId = connected->Get()->ServerId;

        WaitUntil([&] { return !!Registry->FindTablet(serverId); });
        return serverId;
    }

    template <typename TRequest>
    void Send(std::unique_ptr<TRequest> request, const TActorId& pipeServerId)
    {
        NProtoPrivate::TTabletRdmaRequest envelope;
        envelope.SetMethodId(TRequest::EventType);
        envelope.MutablePipeServerId()->SetRawX1(pipeServerId.RawX1());
        envelope.MutablePipeServerId()->SetRawX2(pipeServerId.RawX2());

        TString body;
        Y_ABORT_UNLESS(request->Record.SerializeToString(&body));

        NCloud::TBlockDataRef dataRef(body.data(), body.size());
        In = TString(
            TProtoMessageSerializer::MessageByteSize(envelope, body.size()),
            0);
        TProtoMessageSerializer::SerializeWithData(
            In,
            TTabletRdmaProtocol::TabletRequest,
            0,   // flags
            envelope,
            NCloud::TBlockDataRefSpan(&dataRef, 1));

        auto handler = Endpoints->GetHandler();
        handler->HandleRequest(nullptr, handler->CreateCallContext(), In, Out);

        WaitUntil([&] { return Responded || Failed; });
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRdmaTabletTargetTest)
{
    Y_UNIT_TEST(ShouldServeTabletRequests)
    {
        TEnv env;

        auto server = std::make_shared<TTestServer>(env.Endpoints);
        auto target = env.CreateTarget(server);
        target->Start();

        UNIT_ASSERT_VALUES_EQUAL(1, server->Started);
        UNIT_ASSERT_VALUES_EQUAL("::", env.Endpoints->GetHost());
        UNIT_ASSERT_VALUES_EQUAL(10021, env.Endpoints->GetPort());

        env.Send(
            env.Tablet.CreateGetNodeAttrRequest(RootNodeId),
            env.ConnectPipe());

        UNIT_ASSERT(env.Responded);
        auto parsed = TTabletRdmaProtocol::Serializer()->Parse(
            TStringBuf(env.Out).Head(env.ResponseBytes));
        UNIT_ASSERT_C(!HasError(parsed), FormatError(parsed.GetError()));

        const auto response = parsed.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TTabletRdmaProtocol::TabletResponse),
            response.MsgId);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TEvService::EvGetNodeAttrResponse),
            static_cast<const NProtoPrivate::TTabletRdmaResponse&>(
                *response.Proto).GetMethodId());

        NProto::TGetNodeAttrResponse record;
        UNIT_ASSERT(record.ParseFromArray(
            response.Data.data(),
            response.Data.size()));
        UNIT_ASSERT_C(!HasError(record), FormatError(record.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(RootNodeId, record.GetNode().GetId());

        target->Stop();
        UNIT_ASSERT_VALUES_EQUAL(1, server->Stopped);
    }

    Y_UNIT_TEST(ShouldListenOnConfiguredAddress)
    {
        TEnv env;

        NProto::TRdmaConfig config;
        config.SetListenAddress("::1");
        config.SetListenPort(10022);

        auto target = env.CreateTarget(
            std::make_shared<TTestServer>(env.Endpoints),
            std::move(config));
        target->Start();

        UNIT_ASSERT_VALUES_EQUAL("::1", env.Endpoints->GetHost());
        UNIT_ASSERT_VALUES_EQUAL(10022, env.Endpoints->GetPort());
    }

    Y_UNIT_TEST(ShouldReportCritEventAndStopServerWhenEndpointIsNotStarted)
    {
        TEnv env;

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto critEvents = counters->GetCounter(
            "AppCriticalEvents/TabletRdmaCritEvent",
            true);

        auto server = std::make_shared<TTestServer>(nullptr);
        auto target = env.CreateTarget(server);
        target->Start();

        UNIT_ASSERT_VALUES_EQUAL(1, critEvents->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, server->Started);
        UNIT_ASSERT_VALUES_EQUAL(1, server->Stopped);
    }

    Y_UNIT_TEST(ShouldReportCritEventAndStopServerWhenServerIsNotStarted)
    {
        TEnv env;

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto critEvents = counters->GetCounter(
            "AppCriticalEvents/TabletRdmaCritEvent",
            true);

        auto server = std::make_shared<TTestServer>(env.Endpoints);
        server->StartError = "start failed";
        auto target = env.CreateTarget(server);
        target->Start();

        UNIT_ASSERT_VALUES_EQUAL(1, critEvents->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, server->Stopped);
        UNIT_ASSERT(!env.Endpoints->GetHandler());
    }
}

}   // namespace NCloud::NFileStore::NStorage
