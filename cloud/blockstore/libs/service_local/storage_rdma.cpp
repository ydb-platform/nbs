#include "storage_rdma.h"

#include "compound_storage.h"
#include "rdma_protocol.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/rdma/client.h>
#include <cloud/blockstore/libs/rdma/error.h>
#include <cloud/blockstore/libs/rdma/protobuf.h>
#include <cloud/blockstore/libs/rdma/protocol.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <util/generic/map.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;
using namespace NServer;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MAX_PROTO_SIZE = 4096;

////////////////////////////////////////////////////////////////////////////////

struct IRequestHandler
{
    virtual ~IRequestHandler() = default;

    virtual void HandleResponse(TStringBuf buffer) = 0;
    virtual void HandleError(ui32 error, TStringBuf message) = 0;
};

using IRequestHandlerPtr = std::unique_ptr<IRequestHandler>;

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksHandler final
    : public IRequestHandler
{
public:
    using TRequest = NProto::TReadBlocksLocalRequest;
    using TResponse = NProto::TReadBlocksLocalResponse;

private:
    const TCallContextPtr CallContext;
    const std::shared_ptr<TRequest> Request;

    NProto::TReadDeviceBlocksRequest Proto;
    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer = TBlockStoreProtocol::Serializer();

public:
    TReadBlocksHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            TString uuid,
            size_t blockSize)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
    {
        Proto.SetDeviceUUID(uuid);
        Proto.SetBlockSize(blockSize);
        Proto.SetStartIndex(Request->GetStartIndex());
        Proto.SetBlocksCount(Request->GetBlocksCount());
        Proto.SetSessionId(Request->GetSessionId());
    }

    size_t GetRequestSize() const
    {
        return Serializer->MessageByteSize(Proto, 0);
    }

    size_t GetResponseSize() const
    {
        return MAX_PROTO_SIZE + Proto.GetBlockSize() * Proto.GetBlocksCount();
    }

    TFuture<TResponse> GetResponse() const
    {
        return Response.GetFuture();
    }

    size_t PrepareRequest(TStringBuf buffer)
    {
        return Serializer->Serialize(
            buffer,
            TBlockStoreProtocol::ReadDeviceBlocksRequest,
            Proto,
            TContIOVector(nullptr, 0));
    }

    void HandleResponse(TStringBuf buffer) override
    {
        auto resultOrError = Serializer->Parse(buffer);
        if (HasError(resultOrError)) {
            Response.SetValue(TErrorResponse(resultOrError.GetError()));
            return;
        }

        const auto& response = resultOrError.GetResult();
        Y_ENSURE(response.MsgId == TBlockStoreProtocol::ReadDeviceBlocksResponse);

        CopyData(Request->Sglist, response.Data);

        auto orig = static_cast<NProto::TReadDeviceBlocksResponse&>(*response.Proto);
        TResponse proto;
        proto.MutableError()->Swap(orig.MutableError());
        Response.SetValue(std::move(proto));
    }

    void HandleError(ui32 error, TStringBuf message) override
    {
        Response.SetValue(TErrorResponse(error, TString(message)));
    }

private:
    static void CopyData(TGuardedSgList& guardedSgList, TStringBuf data)
    {

        auto guard = guardedSgList.Acquire();
        Y_ENSURE(guard);

        const char* ptr = data.data();
        size_t bytesLeft = data.size();

        for (auto buffer: guard.Get()) {
            size_t len = Min(bytesLeft, buffer.Size());
            Y_ENSURE(len);

            memcpy((char*)buffer.Data(), ptr, len);
            ptr += len;
            bytesLeft -= len;
        }

        Y_ENSURE(bytesLeft == 0);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBlocksHandler final
    : public IRequestHandler
{
public:
    using TRequest = NProto::TWriteBlocksLocalRequest;
    using TResponse = NProto::TWriteBlocksLocalResponse;

private:
    const TCallContextPtr CallContext;
    const std::shared_ptr<TRequest> Request;

    NProto::TWriteDeviceBlocksRequest Proto;
    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer = TBlockStoreProtocol::Serializer();

public:
    TWriteBlocksHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            TString uuid,
            size_t blockSize)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
    {
        Proto.SetDeviceUUID(uuid);
        Proto.SetBlockSize(blockSize);
        Proto.SetStartIndex(Request->GetStartIndex());
        Proto.SetSessionId(Request->GetSessionId());
    }

    size_t GetRequestSize() const
    {
        return Serializer->MessageByteSize(
            Proto,
            Request->BlockSize * Request->BlocksCount);
    }

    size_t GetResponseSize() const
    {
        return MAX_PROTO_SIZE;
    }

    TFuture<TResponse> GetResponse() const
    {
        return Response.GetFuture();
    }

    size_t PrepareRequest(TStringBuf buffer)
    {
        auto guard = Request->Sglist.Acquire();
        Y_ENSURE(guard);

        const auto& sglist = guard.Get();

        return Serializer->Serialize(
            buffer,
            TBlockStoreProtocol::WriteDeviceBlocksRequest,
            Proto,
            TContIOVector((IOutputStream::TPart*)sglist.data(), sglist.size()));
    }

    void HandleResponse(TStringBuf buffer) override
    {
        auto resultOrError = Serializer->Parse(buffer);
        if (HasError(resultOrError)) {
            Response.SetValue(TErrorResponse(resultOrError.GetError()));
            return;
        }

        const auto& response = resultOrError.GetResult();
        Y_ENSURE(response.MsgId == TBlockStoreProtocol::WriteDeviceBlocksResponse);
        Y_ENSURE(response.Data.length() == 0);

        auto orig = static_cast<NProto::TWriteDeviceBlocksResponse&>(*response.Proto);
        TResponse proto;
        proto.MutableError()->Swap(orig.MutableError());
        Response.SetValue(std::move(proto));
    }

    void HandleError(ui32 error, TStringBuf message) override
    {
        Response.SetValue(TErrorResponse(error, TString(message)));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TZeroBlocksHandler final
    : public IRequestHandler
{
public:
    using TRequest = NProto::TZeroBlocksRequest;
    using TResponse = NProto::TZeroBlocksResponse;

private:
    const TCallContextPtr CallContext;
    const std::shared_ptr<TRequest> Request;

    NProto::TZeroDeviceBlocksRequest Proto;
    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer = TBlockStoreProtocol::Serializer();

public:
    TZeroBlocksHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            TString uuid,
            size_t blockSize)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
    {
        Proto.SetDeviceUUID(uuid);
        Proto.SetBlockSize(blockSize);
        Proto.SetStartIndex(Request->GetStartIndex());
        Proto.SetBlocksCount(Request->GetBlocksCount());
        Proto.SetSessionId(Request->GetSessionId());
    }

    size_t GetRequestSize() const
    {
        return Serializer->MessageByteSize(Proto, 0);
    }

    size_t GetResponseSize() const
    {
        return MAX_PROTO_SIZE;
    }

    TFuture<TResponse> GetResponse() const
    {
        return Response.GetFuture();
    }

    size_t PrepareRequest(TStringBuf buffer)
    {
        return Serializer->Serialize(
            buffer,
            TBlockStoreProtocol::ZeroDeviceBlocksRequest,
            Proto,
            TContIOVector(nullptr, 0));
    }

    void HandleResponse(TStringBuf buffer) override
    {
        auto resultOrError = Serializer->Parse(buffer);
        if (HasError(resultOrError)) {
            Response.SetValue(TErrorResponse(resultOrError.GetError()));
            return;
        }

        const auto& response = resultOrError.GetResult();
        Y_ENSURE(response.MsgId == TBlockStoreProtocol::ZeroDeviceBlocksResponse);
        Y_ENSURE(response.Data.length() == 0);

        auto orig = static_cast<NProto::TZeroDeviceBlocksResponse&>(*response.Proto);
        TResponse proto;
        proto.MutableError()->Swap(orig.MutableError());
        Response.SetValue(std::move(proto));
    }

    void HandleError(ui32 error, TStringBuf message) override
    {
        Response.SetValue(TErrorResponse(error, TString(message)));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRdmaStorage
    : public IStorage
    , public NRdma::IClientHandler
{
private:
    const TString Uuid;
    const ui64 BlockSize;

    ITaskQueuePtr TaskQueue;
    NRdma::IClientEndpointPtr Endpoint;

public:
    TRdmaStorage(
            TString uuid,
            ui64 blockSize,
            ITaskQueuePtr taskQueue)
        : Uuid(std::move(uuid))
        , BlockSize(blockSize)
        , TaskQueue(std::move(taskQueue))
    {}

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
    {
        return HandleRequest<TReadBlocksHandler>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
    {
        return HandleRequest<TWriteBlocksHandler>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request)
    {
        return HandleRequest<TZeroBlocksHandler>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method)
    {
        Y_UNUSED(method);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount)
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void Init(NRdma::IClientEndpointPtr endpoint)
    {
        Endpoint = std::move(endpoint);
    }

private:
    template <typename T>
    TFuture<typename T::TResponse> HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request)
    {
        return TaskQueue->Execute([=] {
            return DoHandleRequest<T>(std::move(callContext), std::move(request));
        });
    }

    template <typename T>
    TFuture<typename T::TResponse> DoHandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request)
    {
        auto handler = std::make_unique<T>(
            callContext,
            std::move(request),
            Uuid,
            BlockSize);

        auto [req, err] = Endpoint->AllocateRequest(
            handler.get(),
            handler->GetRequestSize(),
            handler->GetResponseSize());

        if (HasError(err)) {
            return MakeFuture<typename T::TResponse>(TErrorResponse(err));
        }

        handler->PrepareRequest(req->RequestBuffer);
        Endpoint->SendRequest(std::move(req), callContext);

        auto response = handler->GetResponse();

        handler.release();  // ownership transferred
        return response;
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes)
    {
        TaskQueue->ExecuteSimple([=, req = std::move(req)] () mutable {
            DoHandleResponse(std::move(req), status, responseBytes);
        });
    }

    void DoHandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes)
    {
        IRequestHandlerPtr handler(static_cast<IRequestHandler*>(req->Context));

        try {
            auto buffer = req->ResponseBuffer.Head(responseBytes);
            if (status == NRdma::RDMA_PROTO_OK) {
                handler->HandleResponse(buffer);
            } else {
                auto error = NRdma::ParseError(buffer);
                handler->HandleError(error.GetCode(), error.GetMessage());
            }
        } catch (...) {
            handler->HandleError(E_FAIL, CurrentExceptionMessage());
        }

        Endpoint->FreeRequest(std::move(req));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRdmaStorageProvider final
    : public IStorageProvider
{
private:
    struct TEndpoint
    {
        TString Host;
        ui32 Port;

        bool operator<(const TEndpoint& other) const
        {
            return std::tie(Host, Port) < std::tie(other.Host, other.Port);
        }
    };

    const IServerStatsPtr ServerStats;

    NRdma::IClientPtr Client;
    ITaskQueuePtr TaskQueue;

    TMap<TEndpoint, std::weak_ptr<TRdmaStorage>> Storages;
    TAdaptiveLock Lock;

public:
    TRdmaStorageProvider(
            IServerStatsPtr serverStats,
            NRdma::IClientPtr client,
            ITaskQueuePtr taskQueue)
        : ServerStats(std::move(serverStats))
        , Client(std::move(client))
        , TaskQueue(std::move(taskQueue))
    {}

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        // TODO
        Y_UNUSED(accessMode);

        const auto kind = volume.GetStorageMediaKind();
        if (kind != NProto::STORAGE_MEDIA_SSD_NONREPLICATED) {
            return MakeFuture<IStoragePtr>(nullptr);
        }

        for (const auto& device: volume.GetDevices()) {
            if (device.GetRdmaEndpoint().GetHost().empty()) {
                // agent doesn't support rdma
                return MakeFuture<IStoragePtr>(nullptr);
            }
        }

        ui64 offset = 0;

        TVector<ui64> offsets;
        TVector<IStoragePtr> storages;
        TVector<TFuture<NRdma::IClientEndpointPtr>> endpoints;

        for (const auto& device: volume.GetDevices()) {
            auto ep = TEndpoint{
                device.GetRdmaEndpoint().GetHost(),
                device.GetRdmaEndpoint().GetPort()};

            with_lock (Lock) {
                std::shared_ptr<TRdmaStorage> storage;

                auto it = Storages.find(ep);
                if (it != Storages.end()) {
                    storage = it->second.lock();
                }

                if (!storage) {
                    storage = std::make_shared<TRdmaStorage>(
                        device.GetDeviceUUID(),
                        volume.GetBlockSize(),
                        TaskQueue);

                    auto endpoint = Client->StartEndpoint(ep.Host, ep.Port, storage)
                        .Subscribe([=] (const auto& future) {
                            storage->Init(future.GetValue());
                        });

                    endpoints.emplace_back(std::move(endpoint));
                    Storages.emplace(ep, storage);
                }

                offset += device.GetBlockCount();
                offsets.emplace_back(offset);
                storages.emplace_back(storage);
            }
        }

        if (endpoints.empty()) {
            return MakeFuture<IStoragePtr>(nullptr);
        }

        return WaitAll(endpoints)
            .Apply([=] (const auto& future) {
                Y_UNUSED(future);

                return CreateCompoundStorage(
                    std::move(storages),
                    std::move(offsets),
                    volume.GetBlockSize(),
                    volume.GetDiskId(),
                    std::move(clientId),
                    ServerStats);
            });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateRdmaStorageProvider(
    IServerStatsPtr serverStats,
    NRdma::IClientPtr client)
{
    // TODO
    auto threadPool = CreateThreadPool("RDMA", 1);
    threadPool->Start();

    return std::make_shared<TRdmaStorageProvider>(
        std::move(serverStats),
        std::move(client),
        std::move(threadPool));
}

}   // namespace NCloud::NBlockStore::NStorage
