#include "connection.h"

#include "endpoint_router.h"
#include "transport_switcher.h"
#include "remote_storage.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCellConnection;
using TCellConnectionPtr = std::shared_ptr<TCellConnection>;

////////////////////////////////////////////////////////////////////////////////

// Wraps the per-client control service so that mount responses can be read on
// their way back. Holds the connection alive: the endpoint above may release
// its handle while requests are still in flight.
class TControlService final
    : public TBlockStoreImpl<TControlService, IBlockStore>
{
private:
    const IBlockStorePtr Impl;
    const TCellConnectionPtr Connection;

public:
    TControlService(IBlockStorePtr impl, TCellConnectionPtr connection)
        : Impl(std::move(impl))
        , Connection(std::move(connection))
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Impl->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request);
};

////////////////////////////////////////////////////////////////////////////////

class TCellConnection final
    : public ICellConnection
    , public std::enable_shared_from_this<TCellConnection>
{
private:
    const TCellHostPoolPtr Pool;
    const TCellHostConfig HostConfig;
    const ICellConnectionObserverPtr Observer;

    const IBlockStorePtr ControlService;
    const IBlockStorePtr DataEndpoint;

public:
    TCellConnection(
            TCellHostPoolPtr pool,
            TCellHostConfig hostConfig,
            ICellConnectionObserverPtr observer,
            IBlockStorePtr controlService,
            IBlockStorePtr dataEndpoint)
        : Pool(std::move(pool))
        , HostConfig(std::move(hostConfig))
        , Observer(std::move(observer))
        , ControlService(std::move(controlService))
        , DataEndpoint(std::move(dataEndpoint))
    {}

    ~TCellConnection() override
    {
        Pool->ReleaseControlChannel(HostConfig.GetFqdn());
    }

    TString GetHost() const override
    {
        return HostConfig.GetFqdn();
    }

    // Built on demand rather than cached: the wrapper owns the connection, so
    // storing it here would close a reference cycle.
    IBlockStorePtr GetService() override
    {
        return std::make_shared<TControlService>(
            ControlService,
            shared_from_this());
    }

    IStoragePtr GetStorage() override
    {
        return CreateRemoteStorage(DataEndpoint);
    }

    void OnMountResponse(
        const NProto::TMountVolumeResponse& response) noexcept
    {
        const auto& fqdn = response.GetTabletHost();
        if (!fqdn) {
            // the serving cell is older than this field
            return;
        }

        if (fqdn != HostConfig.GetFqdn() && Observer) {
            Observer->OnTabletHostChanged(fqdn);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TFuture<typename TMethod::TResponse> TControlService::Execute(
    TCallContextPtr callContext,
    std::shared_ptr<typename TMethod::TRequest> request)
{
    auto future = TMethod::Execute(
        Impl.get(),
        std::move(callContext),
        std::move(request));

    if constexpr (std::is_same_v<TMethod, TBlockStoreMountVolumeMethod>) {
        return future.Apply(
            [connection = Connection](const auto& f)
            {
                const auto& response = f.GetValue();
                if (!HasError(response)) {
                    connection->OnMountResponse(response);
                }
                return response;
            });
    } else {
        return future;
    }
}

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateGrpcDataEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService)
{
    // a channel of its own, so that it dies with the endpoint rather than
    // being shared by everyone talking to this host
    const auto securePort = hostConfig.GetSecureGrpcPort();
    auto endpoint = bootstrap.GrpcClient->CreateDataEndpoint(
        hostConfig.GetFqdn(),
        securePort ? securePort : hostConfig.GetGrpcPort(),
        securePort != 0);

    return endpoint ? std::move(endpoint) : controlService;
}

// Serves data over gRPC right away and switches over to RDMA as soon as it is
// up, so that setting up RDMA does not hold the connection back.
IBlockStorePtr CreateSwitchingDataEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService)
{
    auto router = CreateEndpointRouter(
        CreateGrpcDataEndpoint(bootstrap, hostConfig, controlService));

    StartTransportSwitching(
        router,
        [bootstrap, hostConfig]
        {
            return bootstrap.EndpointsSetup->SetupHostRdmaEndpoint(
                bootstrap,
                hostConfig);
        },
        bootstrap.Timer,
        bootstrap.Scheduler,
        bootstrap.Logging,
        hostConfig.GetFqdn(),
        TTransportSwitcherConfig{});

    return router;
}

NThreading::TFuture<TResultOrError<IBlockStorePtr>> SetupDataEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService)
{
    switch (hostConfig.GetTransport()) {
        case NProto::CELL_DATA_TRANSPORT_RDMA:
            if (!hostConfig.GetGrpcDataFallbackEnabled()) {
                return bootstrap.EndpointsSetup->SetupHostRdmaEndpoint(
                    bootstrap,
                    hostConfig);
            }

            return MakeFuture(TResultOrError<IBlockStorePtr>(
                CreateSwitchingDataEndpoint(
                    bootstrap,
                    hostConfig,
                    controlService)));

        case NProto::CELL_DATA_TRANSPORT_GRPC:
            return MakeFuture(TResultOrError<IBlockStorePtr>(
                CreateGrpcDataEndpoint(bootstrap, hostConfig, controlService)));

        default:
            return MakeFuture(TResultOrError<IBlockStorePtr>(MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Unsupported cell data transport "
                    << NProto::ECellDataTransport_Name(
                           hostConfig.GetTransport()))));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCellConnectionFuture CreateCellConnection(
    TCellHostPoolPtr pool,
    TCellHostConfig hostConfig,
    TBootstrap bootstrap,
    NClient::TClientAppConfigPtr clientConfig,
    ICellConnectionObserverPtr observer)
{
    auto fqdn = hostConfig.GetFqdn();
    auto controlFuture = pool->AcquireControlChannel(fqdn);

    return controlFuture.Apply(
        [pool = std::move(pool),
         hostConfig = std::move(hostConfig),
         bootstrap = std::move(bootstrap),
         clientConfig = std::move(clientConfig),
         observer = std::move(observer),
         fqdn = std::move(fqdn)](const auto& f) mutable -> TCellConnectionFuture
        {
            auto controlEndpoint = f.GetValue();
            if (!controlEndpoint) {
                pool->ReleaseControlChannel(fqdn);
                return MakeFuture(TResultOrError<ICellConnectionPtr>(MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Can't set up a control channel to " << fqdn)));
            }

            auto controlService = controlEndpoint->CreateClientEndpoint(
                clientConfig->GetClientId(),
                clientConfig->GetInstanceId());

            return SetupDataEndpoint(bootstrap, hostConfig, controlService)
                .Apply(
                    [pool = std::move(pool),
                     hostConfig = std::move(hostConfig),
                     observer = std::move(observer),
                     controlService = std::move(controlService),
                     fqdn = std::move(fqdn)](
                        const auto& f) mutable
                        -> TResultOrError<ICellConnectionPtr>
                    {
                        const auto& result = f.GetValue();
                        if (HasError(result)) {
                            pool->ReleaseControlChannel(fqdn);
                            return result.GetError();
                        }

                        return ICellConnectionPtr(
                            std::make_shared<TCellConnection>(
                                std::move(pool),
                                std::move(hostConfig),
                                std::move(observer),
                                std::move(controlService),
                                result.GetResult()));
                    });
        });
}

}   // namespace NCloud::NBlockStore::NCells
