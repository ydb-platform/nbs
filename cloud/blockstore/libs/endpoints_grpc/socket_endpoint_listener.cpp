#include "socket_endpoint_listener.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/server/client_storage_factory.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/uds/endpoint_poller.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;
using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEndpointServiceBase
    : public IBlockStore
{
public:
#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
        {                                                                      \
            return CreateUnsupportedResponse<NProto::T##name##Response>(       \
                std::move(callContext),                                        \
                std::move(request));                                           \
        }                                                                      \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename TResponse, typename TRequest>
    TFuture<TResponse> CreateUnsupportedResponse(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        auto requestType = GetBlockStoreRequest<TRequest>();
        const auto& requestName = GetBlockStoreRequestName(requestType);

        return MakeFuture<TResponse>(TErrorResponse(
            E_FAIL,
            TStringBuilder()
                << "Unsupported endpoint request: " << requestName.Quote()));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointService final
    : public TEndpointServiceBase
{
private:
    const TString DiskId;
    const ui32 BlockSize;
    const TStorageAdapter StorageAdapter;
    const ISessionPtr Session;

    const std::shared_ptr<NProto::TMountVolumeResponse> LastMountResponsePtr;

public:
    TEndpointService(TString diskId, ui32 blockSize, ISessionPtr session)
        : DiskId(std::move(diskId))
        , BlockSize(blockSize)
        , StorageAdapter(
              session,
              blockSize,
              true,                // normalize,
              TDuration::Zero(),   // maxRequestDuration
              TDuration::Zero()    // shutdownTimeout
              )
        , Session(std::move(session))
        , LastMountResponsePtr(std::make_shared<NProto::TMountVolumeResponse>())
    {
        *LastMountResponsePtr = TErrorResponse(E_FAIL, "Not mounted yet");
    }

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        Y_UNUSED(callContext);

        auto response = ValidateRequest<NProto::TMountVolumeResponse>(*request);
        if (HasError(response)) {
            return MakeFuture(response);
        }

        return Session->EnsureVolumeMounted().Apply(
            [lastMountResponse = LastMountResponsePtr]
            (TFuture<NProto::TMountVolumeResponse> f)
            {
                const auto& response = f.GetValue();
                if (HasError(response) && !HasError(*lastMountResponse)) {
                    return *lastMountResponse;
                }

                lastMountResponse->CopyFrom(response);
                return response;
            });
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override
    {
        Y_UNUSED(callContext);

        auto response = ValidateRequest<NProto::TUnmountVolumeResponse>(*request);
        if (HasError(response)) {
            return MakeFuture(response);
        }

        auto& error = *response.MutableError();
        error.SetCode(S_FALSE);
        error.SetMessage("Emulated unmount response");
        return MakeFuture(response);
    }

    TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request) override
    {
        auto res = ValidateRequest<NProto::TReadBlocksResponse>(*request);
        if (HasError(res)) {
            return MakeFuture(res);
        }

        return StorageAdapter.ReadBlocks(
            Now(),
            std::move(callContext),
            std::move(request),
            BlockSize,
            {}   // no data buffer
        );
    }

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override
    {
        auto response = ValidateRequest<NProto::TWriteBlocksResponse>(*request);
        if (HasError(response)) {
            return MakeFuture(response);
        }

        return StorageAdapter.WriteBlocks(
            Now(),
            std::move(callContext),
            std::move(request),
            BlockSize,
            {}   // no data buffer
        );
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        auto response = ValidateRequest<NProto::TZeroBlocksResponse>(*request);
        if (HasError(response)) {
            return MakeFuture(response);
        }

        return StorageAdapter.ZeroBlocks(
            Now(),
            std::move(callContext),
            std::move(request),
            BlockSize);
    }

private:
    template <typename TResponse, typename TRequest>
    TResponse ValidateRequest(TRequest& request)
    {
        const auto& diskId = request.GetDiskId();
        if (diskId != DiskId) {
            return TErrorResponse(
                E_ARGUMENT,
                TStringBuilder() << "invalid disk id: " << diskId);
        }

        return TResponse();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSocketEndpointListener final
    : public ISocketEndpointListener
{
private:
    const ui32 SocketBacklog;
    const ui32 SocketAccessMode;

    std::unique_ptr<NStorage::NServer::TEndpointPoller> EndpointPoller;
    IClientStorageFactoryPtr ClientStorageFactory;

    TLog Log;

public:
    TSocketEndpointListener(
            ILoggingServicePtr logging,
            ui32 socketBacklog,
            ui32 socketAccessMode)
        : SocketBacklog(socketBacklog)
        , SocketAccessMode(socketAccessMode)
    {
        Log = logging->CreateLog("BLOCKSTORE_SERVER");
    }

    ~TSocketEndpointListener()
    {
        Stop();
    }

    void SetClientStorageFactory(IClientStorageFactoryPtr factory) override
    {
        Y_ABORT_UNLESS(!EndpointPoller);
        ClientStorageFactory = std::move(factory);
        EndpointPoller = std::make_unique<NStorage::NServer::TEndpointPoller>();
    }

    void Start() override
    {
        Y_ABORT_UNLESS(EndpointPoller);
        EndpointPoller->Start();
    }

    void Stop() override
    {
        if (EndpointPoller) {
            EndpointPoller->Stop();
        }
    }

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        ISessionPtr session) override
    {
        auto sessionService = std::make_shared<TEndpointService>(
            request.GetDiskId(),
            volume.GetBlockSize(),
            std::move(session));

        auto error = EndpointPoller->StartListenEndpoint(
            request.GetUnixSocketPath(),
            SocketBacklog,
            SocketAccessMode,
            false,  // multiClient
            NProto::SOURCE_FD_DATA_CHANNEL,
            ClientStorageFactory->CreateClientStorage(
                std::move(sessionService)));

        return MakeFuture(std::move(error));
    }

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request, volume, session);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> StopEndpoint(
        const TString& socketPath) override
    {
        auto error = EndpointPoller->StopListenEndpoint(socketPath);
        return MakeFuture(std::move(error));
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        Y_UNUSED(socketPath);
        Y_UNUSED(volume);
        return {};
    }

    TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture<NProto::TError>();
    }

    NProto::TError CancelEndpointInFlightRequests(
        const TString& socketPath) override
    {
        Y_UNUSED(socketPath);
        return MakeError(
            E_NOT_IMPLEMENTED,
            "Can't cancel in-flight requests for GRPC endpoint");
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISocketEndpointListenerPtr CreateSocketEndpointListener(
    ILoggingServicePtr logging,
    ui32 socketBacklog,
    ui32 socketAccessMode)
{
    return std::make_unique<TSocketEndpointListener>(
        std::move(logging),
        socketBacklog,
        socketAccessMode);
}

}   // namespace NCloud::NBlockStore::NServer
