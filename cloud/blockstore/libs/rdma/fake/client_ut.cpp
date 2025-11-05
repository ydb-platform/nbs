#include "client.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/testlib/disk_registry_proxy_mock.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>
#include <contrib/ydb/library/actors/core/actorsystem.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>

namespace NCloud::NBlockStore {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;
using namespace NStorage;
using namespace std::chrono_literals;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestActorSystem
    : IActorSystem
{
    TTestBasicRuntime& Runtime;
    TProgramShouldContinue ProgramShouldContinue;

    ILoggingServicePtr Logging = CreateLoggingService("console");
    IMonitoringServicePtr Monitoring = CreateMonitoringServiceStub();

    explicit TTestActorSystem(TTestBasicRuntime& runtime)
        : Runtime(runtime)
    {}

    // IStartable

    void Start() override
    {}

    void Stop() override
    {}

    // ILoggingService

    TLog CreateLog(const TString& component) override
    {
        return Logging->CreateLog(component);
    }

    // IMonitoringService

    IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override
    {
        return Monitoring->RegisterIndexPage(path, title);
    }

    void RegisterMonPage(IMonPagePtr page) override
    {
        Monitoring->RegisterMonPage(std::move(page));
    }

    IMonPagePtr GetMonPage(const TString& path) override
    {
        return Monitoring->GetMonPage(path);
    }

    TDynamicCountersPtr GetCounters() override
    {
        return Monitoring->GetCounters();
    }

    // IActorSystem

    TActorId Register(IActorPtr actor, TStringBuf executorName) override
    {
        Y_ABORT_IF(executorName);

        return Runtime.Register(actor.release());
    }

    bool Send(const TActorId& recipient, IEventBasePtr event) override
    {
        Runtime.SendAsync(
            new IEventHandle{recipient, TActorId{}, event.release()});

        return true;
    }

    TProgramShouldContinue& GetProgramShouldContinue() override
    {
        return ProgramShouldContinue;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    std::optional<TTestBasicRuntime> Runtime;
    IActorSystemPtr ActorSystem;
    NRdma::IClientPtr RdmaClient;
    THashMap<TString, ui32> NodeMap;

    std::function<bool(const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr&)>
        HandleWriteDeviceBlocks;

    std::function<bool(const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr&)>
        HandleReadDeviceBlocks;

    std::function<bool(const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr&)>
        HandleZeroDeviceBlocks;

    std::function<bool(
        const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr&)>
        HandleChecksumDeviceBlocks;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        NodeMap["node-0001.nbs-dev.net"] = 100500;

        Runtime.emplace();

        Runtime->AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        Runtime->SetLogPriority(TBlockStoreComponents::RDMA, NLog::PRI_TRACE);

        Runtime->SetRegistrationObserverFunc(
            [](auto& runtime, const auto& parentId, const auto& actorId)
            {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        Runtime->AddLocalService(
            MakeDiskRegistryProxyServiceId(),
            TActorSetupCmd(
                new TDiskRegistryProxyMock(MakeIntrusive<TDiskRegistryState>()),
                TMailboxType::Simple,
                0));

        SetupTabletServices(*Runtime);

        ActorSystem = MakeIntrusive<TTestActorSystem>(*Runtime);
        ActorSystem->Start();

        RdmaClient = CreateFakeRdmaClient(ActorSystem);
        RdmaClient->Start();

        Runtime->DispatchEvents({}, 10ms);

        Runtime->SetEventFilter(
            [this](auto&, TAutoPtr<IEventHandle>& ev)
            {
#define INVOKE_HANDLER_FUNC(ns, name)                                          \
    case ns::Ev##name##Request:                                                \
        if (Handle##name) {                                                    \
            auto* ptr = reinterpret_cast<ns::TEv##name##Request::TPtr*>(&ev);  \
            return Handle##name(*ptr);                                         \
        }
                switch (ev->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvGetAgentNodeIdRequest:
                        HandleGetAgentNodeId(
                            *reinterpret_cast<
                                TEvDiskRegistry::TEvGetAgentNodeIdRequest::
                                    TPtr*>(&ev));
                        return true;
                    INVOKE_HANDLER_FUNC(TEvDiskAgent, WriteDeviceBlocks)
                    INVOKE_HANDLER_FUNC(TEvDiskAgent, ReadDeviceBlocks)
                    INVOKE_HANDLER_FUNC(TEvDiskAgent, ZeroDeviceBlocks)
                    INVOKE_HANDLER_FUNC(TEvDiskAgent, ChecksumDeviceBlocks)
                    default:
                        return false;
                }
#undef INVOKE_HANDLER_FUNC
                return false;
            });
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override
    {
        RdmaClient->Stop();
        RdmaClient.reset();

        ActorSystem->Stop();
        ActorSystem.Reset();
    }

    void HandleGetAgentNodeId(
        const TEvDiskRegistry::TEvGetAgentNodeIdRequest::TPtr& ev)
    {
        auto* msg = ev->Get();
        auto* nodeId = NodeMap.FindPtr(msg->Record.GetAgentId());

        auto response =
            std::make_unique<TEvDiskRegistry::TEvGetAgentNodeIdResponse>();
        if (nodeId) {
            response->Record.SetNodeId(*nodeId);
        } else {
            response->Record.MutableError()->SetCode(E_NOT_FOUND);
        }

        Runtime->SendAsync(new IEventHandle{
            ev->Sender,
            TActorId{},
            response.release(),
            0,   // flags
            ev->Cookie});
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestClientHandler
    : public NRdma::IClientHandler
{
    std::function<void(NRdma::TClientRequestPtr, ui32, size_t)> Impl;

public:
    template <typename TFn>
    TTestClientHandler(TFn&& fn)
        : Impl(std::forward<TFn>(fn))
    {}

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        Impl(std::move(req), status, responseBytes);
    }
};

template <typename TFn>
NRdma::IClientHandlerPtr Handler(TFn&& fn)
{
    return std::make_shared<TTestClientHandler>(std::forward<TFn>(fn));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFakeRdmaClientTest)
{
    Y_UNIT_TEST_F(ShouldWriteDeviceBlocks, TFixture)
    {
        NProto::TWriteDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->SetClientId("client");
        deviceRequest.SetDeviceUUID("uuid");
        deviceRequest.SetStartIndex(42);
        deviceRequest.SetBlockSize(DefaultBlockSize);

        const TString requestBuffer{DefaultBlockSize, 'X'};
        TSgList sglist{
            TBlockDataRef{requestBuffer.data(), requestBuffer.size()}};

        HandleWriteDeviceBlocks = [&](const auto& ev)
        {
            auto* msg = ev->Get();

            UNIT_ASSERT_VALUES_EQUAL(
                deviceRequest.GetHeaders().GetClientId(),
                msg->Record.GetHeaders().GetClientId());

            UNIT_ASSERT_VALUES_EQUAL(
                deviceRequest.GetDeviceUUID(),
                msg->Record.GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(
                deviceRequest.GetStartIndex(),
                msg->Record.GetStartIndex());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlockSize,
                msg->Record.GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(1, msg->Record.GetBlocks().BuffersSize());
            UNIT_ASSERT_EQUAL(
                requestBuffer,
                msg->Record.GetBlocks().GetBuffers(0));

            auto response =
                std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksResponse>();
            Runtime->SendAsync(new IEventHandle{
                ev->Sender,
                TActorId{},
                response.release(),
                0,   // flags
                ev->Cookie});

            return true;
        };

        auto future = RdmaClient->StartEndpoint("node-0001.nbs-dev.net", 10022);
        Runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(future.Wait(15s));
        NRdma::IClientEndpointPtr ep = future.GetValueSync();
        UNIT_ASSERT(ep);

        std::optional<NProto::TError> responseError;

        auto handler = Handler([&] (auto request, ui32 status, size_t len) {
            auto buffer = request->ResponseBuffer.Head(len);

            UNIT_ASSERT_EQUAL(NRdma::RDMA_PROTO_OK, status);

            auto* serializer = TBlockStoreProtocol::Serializer();
            auto [result, error] = serializer->Parse(buffer);

            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

            const auto& proto =
                static_cast<NProto::TWriteDeviceBlocksResponse&>(*result.Proto);

            responseError = proto.GetError();
        });

        auto [request, error] = ep->AllocateRequest(
            handler,
            nullptr,   // context
            NRdma::TProtoMessageSerializer::MessageByteSize(
                deviceRequest,
                DefaultBlockSize),
            4_KB);

        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        NRdma::TProtoMessageSerializer::SerializeWithData(
            request->RequestBuffer,
            TBlockStoreProtocol::WriteDeviceBlocksRequest,
            0,   // flags
            deviceRequest,
            sglist);

        ep->SendRequest(std::move(request), MakeIntrusive<TCallContext>());

        Runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(responseError);
        UNIT_ASSERT_EQUAL_C(
            S_OK,
            responseError->GetCode(),
            FormatError(*responseError));
    }

    Y_UNIT_TEST_F(ShouldReadDeviceBlocks, TFixture)
    {
        NProto::TReadDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->SetClientId("client");
        deviceRequest.SetDeviceUUID("uuid");
        deviceRequest.SetStartIndex(42);
        deviceRequest.SetBlockSize(DefaultBlockSize);
        deviceRequest.SetBlocksCount(13);

        const ui64 requestByteCount =
            deviceRequest.GetBlockSize() * deviceRequest.GetBlocksCount();

        HandleReadDeviceBlocks = [&](const auto& ev)
        {
            auto* msg = ev->Get();

            UNIT_ASSERT_VALUES_EQUAL(
                deviceRequest.GetHeaders().GetClientId(),
                msg->Record.GetHeaders().GetClientId());

            UNIT_ASSERT_VALUES_EQUAL(
                deviceRequest.GetDeviceUUID(),
                msg->Record.GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(
                deviceRequest.GetStartIndex(),
                msg->Record.GetStartIndex());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlockSize,
                msg->Record.GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                deviceRequest.GetBlocksCount(),
                msg->Record.GetBlocksCount());

            auto response =
                std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksResponse>();

            response->Record.MutableBlocks()->MutableBuffers()->Reserve(
                msg->Record.GetBlocksCount());

            for (ui32 i = 0; i != msg->Record.GetBlocksCount(); ++i) {
                auto& buf = *response->Record.MutableBlocks()->AddBuffers();
                buf.resize(DefaultBlockSize, 'a' + i);
            }

            Runtime->SendAsync(new IEventHandle{
                ev->Sender,
                TActorId{},
                response.release(),
                0,   // flags
                ev->Cookie});

            return true;
        };

        auto future = RdmaClient->StartEndpoint("node-0001.nbs-dev.net", 10022);
        Runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(future.Wait(15s));
        NRdma::IClientEndpointPtr ep = future.GetValueSync();
        UNIT_ASSERT(ep);

        std::optional<NProto::TReadDeviceBlocksResponse> deviceResponse;

        auto handler = Handler([&] (auto request, ui32 status, size_t len) {
            auto buffer = request->ResponseBuffer.Head(len);

            UNIT_ASSERT_EQUAL(NRdma::RDMA_PROTO_OK, status);

            auto* serializer = TBlockStoreProtocol::Serializer();
            auto [result, error] = serializer->Parse(buffer);

            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

            deviceResponse =
                static_cast<NProto::TReadDeviceBlocksResponse&>(*result.Proto);
            auto& blocks = *deviceResponse->MutableBlocks();

            TSgList dst = ResizeIOVector(
                blocks,
                deviceRequest.GetBlocksCount(),
                deviceRequest.GetBlockSize());

            UNIT_ASSERT_EQUAL(requestByteCount, result.Data.size());

            SgListCopy(
                TBlockDataRef{result.Data.data(), result.Data.size()},
                dst);
        });

        auto [request, error] = ep->AllocateRequest(
            handler,
            nullptr,   // context
            NRdma::TProtoMessageSerializer::MessageByteSize(
                deviceRequest,
                DefaultBlockSize),
            4_KB + requestByteCount);

        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        NRdma::TProtoMessageSerializer::Serialize(
            request->RequestBuffer,
            TBlockStoreProtocol::ReadDeviceBlocksRequest,
            0,   // flags
            deviceRequest);

        ep->SendRequest(std::move(request), MakeIntrusive<TCallContext>());

        Runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(deviceResponse);
        UNIT_ASSERT_EQUAL_C(
            S_OK,
            deviceResponse->GetError().GetCode(),
            FormatError(deviceResponse->GetError()));

        UNIT_ASSERT_VALUES_EQUAL(
            deviceRequest.GetBlocksCount(),
            deviceResponse->GetBlocks().BuffersSize());

        for (ui32 i = 0; i != deviceResponse->GetBlocks().BuffersSize(); ++i) {
            const auto& buf = deviceResponse->GetBlocks().GetBuffers(i);
            UNIT_ASSERT_EQUAL(TString(DefaultBlockSize, 'a' + i), buf);
        }
    }

    Y_UNIT_TEST_F(ShouldCancelRequest, TFixture)
    {
        NProto::TWriteDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->SetClientId("client");
        deviceRequest.SetDeviceUUID("uuid");
        deviceRequest.SetStartIndex(42);
        deviceRequest.SetBlockSize(DefaultBlockSize);

        const TString requestBuffer{DefaultBlockSize, 'X'};
        TSgList sglist{
            TBlockDataRef{requestBuffer.data(), requestBuffer.size()}};

        auto writeSentPromise = NewPromise<void>();
        auto writeSent = writeSentPromise.GetFuture();

        HandleWriteDeviceBlocks =
            [writeSentPromise =
                 std::move(writeSentPromise)](const auto& ev) mutable
        {
            Y_UNUSED(ev);

            writeSentPromise.SetValue();
            return true;
        };

        auto future = RdmaClient->StartEndpoint("node-0001.nbs-dev.net", 10022);
        Runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(future.Wait(15s));
        NRdma::IClientEndpointPtr ep = future.GetValueSync();
        UNIT_ASSERT(ep);

        NProto::TError responseError;

        auto handler = Handler(
            [&](NRdma::TClientRequestPtr request, ui32 status, size_t len)
            {
                UNIT_ASSERT_EQUAL(NRdma::RDMA_PROTO_FAIL, status);

                const bool parsed = responseError.ParseFromArray(
                    request->ResponseBuffer.Data(),
                    len);

                UNIT_ASSERT(parsed);
            });

        auto [request, error] = ep->AllocateRequest(
            handler,
            nullptr,   // context
            NRdma::TProtoMessageSerializer::MessageByteSize(
                deviceRequest,
                DefaultBlockSize),
            4_KB);

        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        NRdma::TProtoMessageSerializer::SerializeWithData(
            request->RequestBuffer,
            TBlockStoreProtocol::WriteDeviceBlocksRequest,
            0,   // flags
            deviceRequest,
            sglist);

        auto reqId =
            ep->SendRequest(std::move(request), MakeIntrusive<TCallContext>());

        Runtime->DispatchEvents({}, 10ms);
        writeSent.Wait();

        ep->CancelRequest(reqId);
        Runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, responseError.GetCode());
    }
}

}   // namespace NCloud::NBlockStore
