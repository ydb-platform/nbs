#include <cloud/blockstore/libs/client_rdma/protocol.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;
using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4096;

////////////////////////////////////////////////////////////////////////////////

class TRequest: public NRdma::TClientRequest
{
public:
    TRequest(
            NRdma::IClientHandlerPtr handler,
            std::unique_ptr<NRdma::TNullContext> context)
        : NRdma::TClientRequest(std::move(handler), std::move(context))
    {}

    ~TRequest() override
    {
        delete[] RequestBuffer.data();
        delete[] ResponseBuffer.data();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestClientEndpoint: public NRdma::IClientEndpoint
{
    using TReadBlocksRequestCallback =
        std::function<void(NProto::TReadBlocksRequest*)>;
    using TWriteBlocksRequestCallback =
        std::function<void(NProto::TWriteBlocksRequest*)>;


    TReadBlocksRequestCallback ReadBlocksCheck;
    TWriteBlocksRequestCallback WriteBlocksCheck;

    ui64 NextRequestId = 0;

    TResultOrError<NRdma::TClientRequestPtr> AllocateRequest(
        NRdma::IClientHandlerPtr handler,
        std::unique_ptr<NRdma::TNullContext> context,
        size_t requestBytes,
        size_t responseBytes) override
    {
        auto req = std::make_unique<TRequest>(
            std::move(handler),
            std::move(context));
        req->RequestBuffer = {new char[requestBytes], requestBytes};
        req->ResponseBuffer = {new char[responseBytes], responseBytes};

        return NRdma::TClientRequestPtr(std::move(req));
    }

    ui64 SendRequest(
        NRdma::TClientRequestPtr req,
        TCallContextBasePtr callContext) override
    {
        Y_UNUSED(callContext);

        auto reqId = ++NextRequestId;

        auto* serializer = TBlockStoreProtocol::Serializer();
        auto [result, err] = serializer->Parse(req->RequestBuffer);
        UNIT_ASSERT_C(!HasError(err), err.GetMessage());

        size_t responseBytes = 0;
        switch (result.MsgId) {
            case TBlockStoreProtocol::ReadBlocksRequest: {
                NProto::TReadBlocksResponse response;

                using TProto = NProto::TReadBlocksRequest;
                auto* request = static_cast<TProto*>(result.Proto.get());

                if (ReadBlocksCheck) {
                    ReadBlocksCheck(request);
                }

                const size_t dataBytes =
                    request->GetBlockSize() * request->GetBlocksCount();
                TString payload(dataBytes, 0);
                TSgList payloadSgList{
                    {payload.data(), payload.size()},
                };

                responseBytes = NRdma::TProtoMessageSerializer::
                    SerializeWithData(
                        req->ResponseBuffer,
                        TBlockStoreProtocol::ReadBlocksResponse,
                        0,
                        response,
                        payloadSgList);
                break;
            }
            case TBlockStoreProtocol::WriteBlocksRequest: {
                NProto::TWriteBlocksResponse response;

                using TProto = NProto::TWriteBlocksRequest;
                auto* request = static_cast<TProto*>(result.Proto.get());

                if (WriteBlocksCheck) {
                    WriteBlocksCheck(request);
                }

                responseBytes = NRdma::TProtoMessageSerializer::Serialize(
                    req->ResponseBuffer,
                    TBlockStoreProtocol::WriteBlocksResponse,
                    0,
                    response);
                break;
            }
            default: {
                Y_ABORT_UNLESS(false, "unsupported MsgId: %u", result.MsgId);
            }
        }

        auto* handler = req->Handler.get();
        handler->HandleResponse(
            std::move(req),
            NRdma::RDMA_PROTO_OK,
            responseBytes);

        return reqId;
    }

    void CancelRequest(ui64 reqId) override
    {
        Y_UNUSED(reqId);
    }

    TFuture<void> Stop() override
    {
        return MakeFuture();
    }

    void TryForceReconnect() override
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestRdmaClient: public NRdma::IClient
{
    std::shared_ptr<TTestClientEndpoint> Endpoint;
    NThreading::TPromise<NRdma::IClientEndpointPtr> Promise;

    TTestRdmaClient()
    {
        Endpoint = std::make_shared<TTestClientEndpoint>();
    }

    TFuture<NRdma::IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);

        return MakeFuture<NRdma::IClientEndpointPtr>(Endpoint);
    }

    void Start() override
    {}

    void Stop() override
    {}

    void DumpHtml(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    }

    bool IsAlignedDataEnabled() const override
    {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    ILoggingServicePtr Logging = CreateLoggingService("console");
    ITraceSerializerPtr TraceSerializer = CreateTraceSerializerStub();
    ITaskQueuePtr TaskQueue = CreateTaskQueueStub();
    std::shared_ptr<TTestRdmaClient> Client =
        std::make_shared<TTestRdmaClient>();
    std::shared_ptr<TTestService> VolumeClient =
        std::make_shared<TTestService>();

    TRdmaEndpointConfig Config{"localhost", 12345};

    IBlockStorePtr CreateEndpointClient()
    {
        return CreateRdmaEndpointClient(
            Logging,
            Client,
            VolumeClient,
            TraceSerializer,
            TaskQueue,
            Config);
    }

    IBlockStorePtr CreateDataEndpoint()
    {
        auto future = CreateRdmaDataEndpointAsync(
            Logging,
            Client,
            TraceSerializer,
            TaskQueue,
            Config);
        UNIT_ASSERT_C(future.HasValue(), "Value not set");
        auto result = future.GetValue();
        UNIT_ASSERT_C(!HasError(result.GetError()), result.GetError().GetMessage());
        return result.GetResult();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<NProto::TReadBlocksLocalRequest> MakeReadRequest(
    ui64 startIndex,
    ui32 blocksCount,
    TVector<TString>& blocks)
{
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blocksCount);
    request->BlockSize = DefaultBlockSize;

    TSgList sglist;
    blocks.resize(blocksCount);
    for (ui32 i = 0; i < blocksCount; ++i) {
        blocks[i].resize(DefaultBlockSize, 0);
        sglist.emplace_back(
            const_cast<char*>(blocks[i].data()),
            blocks[i].size());
    }
    request->Sglist = TGuardedSgList(std::move(sglist));

    return request;
}

std::shared_ptr<NProto::TWriteBlocksLocalRequest> MakeWriteRequest(
    ui64 startIndex,
    const TVector<TString>& data)
{
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->BlockSize = DefaultBlockSize;

    TSgList sglist;
    size_t bytesCount = 0;
    for (const auto& b: data) {
        sglist.emplace_back(b.data(), b.size());
        bytesCount += b.size();
    }

    UNIT_ASSERT_C(
        bytesCount % request->BlockSize == 0,
        "WriteBlocks request is not multiple of BlockSize");

    request->BlocksCount = static_cast<ui32>(bytesCount / DefaultBlockSize);
    request->Sglist = TGuardedSgList(std::move(sglist));

    return request;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRdmaClientTest)
{
    Y_UNIT_TEST(ShouldSetBlockSizeInHeader)
    {
        TTestEnv env;
        auto endpoint = env.CreateDataEndpoint();

        env.Client->Endpoint->ReadBlocksCheck =
            [] (NProto::TReadBlocksRequest* request)
            {
                UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, request->GetBlockSize());
            };

        env.Client->Endpoint->WriteBlocksCheck =
            [] (NProto::TWriteBlocksRequest* request)
            {
                UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, request->GetBlockSize());
            };

        {
            TVector<TString> blocks;
            auto request = MakeReadRequest(0, 1, blocks);
            auto future = endpoint->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));
            UNIT_ASSERT_C(future.HasValue(), "Value not set");
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                future.GetValue().GetError().GetCode());
        }

        {
            TVector<TString> blocks;
            blocks.push_back(TString(DefaultBlockSize, 'q'));
            auto request = MakeWriteRequest(0,  blocks);
            auto future = endpoint->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));
            UNIT_ASSERT_C(future.HasValue(), "Value not set");
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                future.GetValue().GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldFailReadWhenSglistClosed)
    {
        TTestEnv env;
        auto endpoint = env.CreateDataEndpoint();

        TVector<TString> blocks;
        auto request = MakeReadRequest(0, 1, blocks);
        request->Sglist.Close();

        auto future = endpoint->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            future.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldFailWriteWhenSglistClosed)
    {
        TTestEnv env;
        auto endpoint = env.CreateDataEndpoint();

        TVector<TString> blocks;
        blocks.push_back(TString(DefaultBlockSize, 'w'));
        auto request = MakeWriteRequest(0,  blocks);
        request->Sglist.Close();

        auto future = endpoint->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            future.GetValue().GetError().GetCode());
    }
}

}   // namespace NCloud::NBlockStore::NClient
