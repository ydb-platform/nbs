#include <cloud/blockstore/libs/service_rdma/rdma_target.h>
#include <cloud/blockstore/libs/service_rdma/rdma_protocol.h>

#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestEndpoint: public NCloud::NStorage::NRdma::IServerEndpoint
{
    NThreading::TPromise<void> Done{NThreading::NewPromise<void>()};

public:
    void SendResponse(void* context, size_t responseBytes) override
    {
        Y_UNUSED(context);
        Y_UNUSED(responseBytes);
        Done.SetValue();
    }

    void SendError(void* context, ui32 error, TStringBuf message) override
    {
        Y_UNUSED(context);
        Y_UNUSED(error);
        Y_UNUSED(message);
        Done.SetValue();
    }

    NThreading::TFuture<void> WaitDone()
    {
        return Done.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestServer: public NCloud::NStorage::NRdma::IServer
{
    std::shared_ptr<TTestEndpoint> Endpoint;
    NCloud::NStorage::NRdma::IServerHandlerPtr Handler;

public:
    NCloud::NStorage::NRdma::IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        NCloud::NStorage::NRdma::IServerHandlerPtr handler) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        Handler = std::move(handler);
        Endpoint = std::make_shared<TTestEndpoint>();
        return Endpoint;
    }

    void Start() override {}
    void Stop() override {}
    void DumpHtml(IOutputStream& out) const override { Y_UNUSED(out); }

    NCloud::NStorage::NRdma::IServerHandlerPtr GetHandler() const
    {
        return Handler;
    }

    std::shared_ptr<TTestEndpoint> GetEndpoint() const
    {
        return Endpoint;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    std::shared_ptr<TTestServer> Server;
    IStartablePtr Target;

    NCloud::NStorage::NRdma::IServerHandlerPtr GetHandler() const
    {
        return Server->GetHandler();
    }

    std::shared_ptr<TTestEndpoint> GetEndpoint() const
    {
        return Server->GetEndpoint();
    }
};

TTestEnv CreateTestEnv(IBlockStorePtr service)
{
    auto server = std::make_shared<TTestServer>();

    NProto::TRdmaTarget rdmaTargetProto;
    auto config =
        std::make_shared<TBlockstoreServerRdmaTargetConfig>(rdmaTargetProto);

    auto logging = CreateLoggingService("console");
    auto traceSerializer = CreateTraceSerializerStub();

    auto target = CreateBlockstoreServerRdmaTarget(
        config,
        std::move(logging),
        std::move(traceSerializer),
        server,
        std::move(service));

    target->Start();

    return TTestEnv{std::move(server), std::move(target)};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestHandlerTest)
{
    Y_UNIT_TEST(ShouldCloseGuardedSgListOnReadBlocksComplete)
    {
        auto service = std::make_shared<TTestService>();

        TGuardedSgList capturedSgList;
        bool handlerCalled = false;

        service->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            capturedSgList = request->Sglist;
            handlerCalled = true;
            return NThreading::MakeFuture(NProto::TReadBlocksLocalResponse{});
        };

        auto env = CreateTestEnv(service);
        auto handler = env.GetHandler();
        auto endpoint = env.GetEndpoint();

        // Serialize a ReadBlocks request (no data)
        NProto::TReadBlocksRequest request;
        request.SetBlockSize(4096);
        request.SetBlocksCount(1);

        const size_t inSize =
            NCloud::NStorage::NRdma::TProtoMessageSerializer::MessageByteSize(
                request,
                0);
        TString inBuf(inSize, 0);
        NCloud::NStorage::NRdma::TProtoMessageSerializer::Serialize(
            inBuf,
            TBlockStoreServerProtocol::EvReadBlocksRequest,
            0,
            request);

        // Output buffer: header + proto + 1 block of data
        TString outBuf(8_KB, 0);

        auto doneFuture = endpoint->WaitDone();
        handler->HandleRequest(
            nullptr,
            handler->CreateCallContext(),
            inBuf,
            outBuf);

        doneFuture.Wait();

        UNIT_ASSERT_C(handlerCalled, "ReadBlocksLocal handler was not called");

        // guardedSgList.Close() must have been called before SendResponse,
        // so Acquire() on any copy of it must return a false guard.
        auto guard = capturedSgList.Acquire();
        UNIT_ASSERT_C(
            !guard,
            "guardedSgList was not closed after ReadBlocks completed");

        env.Target->Stop();
    }

    Y_UNIT_TEST(ShouldCloseGuardedSgListOnWriteBlocksComplete)
    {
        auto service = std::make_shared<TTestService>();

        TGuardedSgList capturedSgList;
        bool handlerCalled = false;

        service->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            capturedSgList = request->Sglist;
            handlerCalled = true;
            return NThreading::MakeFuture(NProto::TWriteBlocksLocalResponse{});
        };

        auto env = CreateTestEnv(service);
        auto handler = env.GetHandler();
        auto endpoint = env.GetEndpoint();

        // Serialize a WriteBlocks request with 1 block of data
        NProto::TWriteBlocksRequest request;
        request.SetBlockSize(4096);

        const TString blockData(4096, 'X');
        const TBlockDataRef dataRef{blockData.data(), blockData.size()};
        const TBlockDataRefSpan dataSpan{&dataRef, 1};

        const size_t inSize =
            NCloud::NStorage::NRdma::TProtoMessageSerializer::MessageByteSize(
                request,
                blockData.size());
        TString inBuf(inSize, 0);
        NCloud::NStorage::NRdma::TProtoMessageSerializer::SerializeWithData(
            inBuf,
            TBlockStoreServerProtocol::EvWriteBlocksRequest,
            0,
            request,
            dataSpan);

        // Output buffer: header + proto (no data in response)
        TString outBuf(4_KB, 0);

        auto doneFuture = endpoint->WaitDone();
        handler->HandleRequest(
            nullptr,
            handler->CreateCallContext(),
            inBuf,
            outBuf);

        doneFuture.Wait();

        UNIT_ASSERT_C(handlerCalled, "WriteBlocksLocal handler was not called");

        // guardedSgList.Close() must have been called before SendResponse,
        // so Acquire() on any copy of it must return a false guard.
        auto guard = capturedSgList.Acquire();
        UNIT_ASSERT_C(
            !guard,
            "guardedSgList was not closed after WriteBlocks completed");

        env.Target->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
