#include "storage.h"

#include "probes.h"
#include "protocol.h"

#include <cloud/blockstore/tools/testing/rdma-test/protocol.pb.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/task_queue.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

LWTRACE_USING(BLOCKSTORE_TEST_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MAX_PROTO_SIZE = 4*1024;

////////////////////////////////////////////////////////////////////////////////

struct IRequestHandler: public NRdma::TNullContext
{
    virtual void HandleResponse(TStringBuf buffer) = 0;
    virtual void HandleError(ui32 error, TString message) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksHandler final
    : public IRequestHandler
{
private:
    TCallContextPtr CallContext;
    TReadBlocksRequestPtr Request;
    TGuardedSgList GuardedSgList;
    TPromise<NProto::TError> Response;

    NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreProtocol::Serializer();

public:
    TReadBlocksHandler(
            TCallContextPtr callContext,
            TReadBlocksRequestPtr request,
            TGuardedSgList guardedSgList,
            TPromise<NProto::TError> response)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , GuardedSgList(std::move(guardedSgList))
        , Response(std::move(response))
    {}

    TResultOrError<NRdma::TClientRequestPtr> PrepareRequest(
        NRdma::IClientEndpoint& endpoint,
        NRdma::IClientHandlerPtr handler)
    {
        LWTRACK(
            RdmaPrepareRequest,
            CallContext->LWOrbit,
            CallContext->RequestId);

        size_t dataSize = Request->GetBlockSize() * Request->GetBlocksCount();

        auto [req, err] = endpoint.AllocateRequest(
            handler,
            nullptr,
            NRdma::TProtoMessageSerializer::MessageByteSize(*Request, 0),
            MAX_PROTO_SIZE + dataSize);

        if (HasError(err)) {
            return err;
        }

        NRdma::TProtoMessageSerializer::Serialize(
            req->RequestBuffer,
            TBlockStoreProtocol::ReadBlocksRequest,
            0,   // flags
            *Request);

        return std::move(req);
    }

    void HandleResponse(TStringBuf buffer) override
    {
        LWTRACK(
            RdmaHandleResponse,
            CallContext->LWOrbit,
            CallContext->RequestId);

        auto resultOrError = Serializer->Parse(buffer);
        if (HasError(resultOrError)) {
            Response.SetValue(resultOrError.GetError());
            return;
        }

        const auto& response = resultOrError.GetResult();
        Y_ENSURE(response.MsgId == TBlockStoreProtocol::ReadBlocksResponse);

        auto guard = GuardedSgList.Acquire();
        Y_ABORT_UNLESS(guard);

        size_t bytesCopied = CopyMemory(guard.Get(), response.Data);
        Y_ENSURE(bytesCopied == response.Data.length());

        Response.SetValue({});
    }

    void HandleError(ui32 error, TString message) override
    {
        Response.SetValue(MakeError(error, std::move(message)));
    }

    void SetError(NProto::TError error)
    {
        Response.SetValue(std::move(error));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBlocksHandler final
    : public IRequestHandler
{
private:
    TCallContextPtr CallContext;
    TWriteBlocksRequestPtr Request;
    TGuardedSgList GuardedSgList;
    TPromise<NProto::TError> Response;

    NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreProtocol::Serializer();

public:
    TWriteBlocksHandler(
            TCallContextPtr callContext,
            TWriteBlocksRequestPtr request,
            TGuardedSgList guardedSgList,
            TPromise<NProto::TError> response)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , GuardedSgList(std::move(guardedSgList))
        , Response(std::move(response))
    {}

    TResultOrError<NRdma::TClientRequestPtr> PrepareRequest(
        NRdma::IClientEndpoint& endpoint,
        NRdma::IClientHandlerPtr handler)
    {
        size_t dataSize = Request->GetBlockSize() * Request->GetBlocksCount();

        auto [req, err] = endpoint.AllocateRequest(
            std::move(handler),
            nullptr,
            NRdma::TProtoMessageSerializer::MessageByteSize(*Request, dataSize),
            MAX_PROTO_SIZE);

        if (HasError(err)) {
            return err;
        }

        auto guard = GuardedSgList.Acquire();
        Y_ABORT_UNLESS(guard);

        const auto& sglist = guard.Get();
        NRdma::TProtoMessageSerializer::SerializeWithData(
            req->RequestBuffer,
            TBlockStoreProtocol::WriteBlocksRequest,
            0,   // flags
            *Request,
            sglist);

        return std::move(req);
    }

    void HandleResponse(TStringBuf buffer) override
    {
        auto resultOrError = Serializer->Parse(buffer);
        if (HasError(resultOrError)) {
            Response.SetValue(resultOrError.GetError());
            return;
        }

        const auto& response = resultOrError.GetResult();
        Y_ENSURE(response.MsgId == TBlockStoreProtocol::WriteBlocksResponse);

        Response.SetValue({});
    }

    void HandleError(ui32 error, TString message) override
    {
        Response.SetValue(MakeError(error, std::move(message)));
    }

    void SetError(NProto::TError error)
    {
        Response.SetValue(std::move(error));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRdmaStorage final
    : public IStorage
    , public NRdma::IClientHandler
    , public std::enable_shared_from_this<TRdmaStorage>
{
private:
    ITaskQueuePtr TaskQueue;
    NRdma::IClientEndpointPtr Endpoint;

public:
    static std::shared_ptr<TRdmaStorage> Create(ITaskQueuePtr taskQueue)
    {
        return std::shared_ptr<TRdmaStorage>{
            new TRdmaStorage(std::move(taskQueue))};
    }

    ~TRdmaStorage()
    {
        Stop();
    }

    void Init(NRdma::IClientEndpointPtr endpoint)
    {
        Endpoint = std::move(endpoint);
    }

    void Start() override {}

    void Stop() override
    {
        if (Endpoint) {
            Endpoint->Stop();
        }
    }

    TFuture<NProto::TError> ReadBlocks(
        TCallContextPtr callContext,
        TReadBlocksRequestPtr request,
        TGuardedSgList sglist) override
    {
        return PerformIO<TReadBlocksHandler>(
            std::move(callContext),
            std::move(request),
            std::move(sglist));
    }

    TFuture<NProto::TError> WriteBlocks(
        TCallContextPtr callContext,
        TWriteBlocksRequestPtr request,
        TGuardedSgList sglist) override
    {
        return PerformIO<TWriteBlocksHandler>(
            std::move(callContext),
            std::move(request),
            std::move(sglist));
    }

private:
    TRdmaStorage(ITaskQueuePtr taskQueue)
        : TaskQueue(std::move(taskQueue))
    {}

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        std::unique_ptr<IRequestHandler> handler(
            static_cast<IRequestHandler*>(req->Context.release()));

        TaskQueue->ExecuteSimple(
            [status = status,
             responseBytes = responseBytes,
             req = std::move(req),
             handler = std::move(handler)]()
            {
                auto buffer = req->ResponseBuffer.Head(responseBytes);
                if (status == NRdma::RDMA_PROTO_OK) {
                    handler->HandleResponse(buffer);
                } else {
                    auto error = NRdma::ParseError(buffer);
                    handler->HandleError(error.GetCode(), error.GetMessage());
                }
            });
    }

    template <typename THandler, typename TRequestPtr>
    TFuture<NProto::TError> PerformIO(
        TCallContextPtr callContext,
        TRequestPtr request,
        TGuardedSgList sglist)
    {
        auto response = NewPromise<NProto::TError>();
        auto future = response.GetFuture();

        auto handler = std::make_unique<THandler>(
            callContext,
            std::move(request),
            std::move(sglist),
            std::move(response));

        TaskQueue->ExecuteSimple(
            [callContext = std::move(callContext),
             self = shared_from_this(),
             handler = std::move(handler)]() mutable
            {
                auto [req, err] =
                    handler->PrepareRequest(*self->Endpoint, self);

                if (HasError(err)) {
                    handler->SetError(std::move(err));
                    return;
                }

                req->Context = std::move(handler);
                self->Endpoint->SendRequest(
                    std::move(req),
                    std::move(callContext));
            });

        return future;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateRdmaStorage(
    NRdma::IClientPtr client,
    ITaskQueuePtr taskQueue,
    const TString& address,
    ui32 port)
{
    auto storage = TRdmaStorage::Create(std::move(taskQueue));

    auto startEndpoint = client->StartEndpoint(address, port);
    storage->Init(startEndpoint.GetValue(WAIT_TIMEOUT));

    return storage;
}

}   // namespace NCloud::NBlockStore
