#include "server.h"
#include "cloud/blockstore/config/client.pb.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/nbd/client_handler.h>
#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/libs/nbd/netlink_device.h>
#include <cloud/blockstore/libs/nbd/server.h>
#include <cloud/blockstore/libs/nbd/server_handler.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/public/api/grpc/endpoint_proxy.grpc.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/initializer.h>
#include <cloud/storage/core/libs/uds/client_storage.h>
#include <cloud/storage/core/libs/uds/endpoint_poller.h>

#include <library/cpp/logger/log.h>

#include <contrib/libs/grpc/include/grpcpp/impl/codegen/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/status.h>
#include <contrib/libs/grpc/include/grpcpp/security/server_credentials.h>
#include <contrib/libs/grpc/include/grpcpp/server.h>
#include <contrib/libs/grpc/include/grpcpp/server_builder.h>
#include <contrib/libs/grpc/include/grpcpp/server_posix.h>

#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/printf.h>
#include <util/thread/factory.h>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& name)
{
    return TFileInput(name).ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestContextBase
{
    bool Done = false;

    virtual ~TRequestContextBase() = default;
};

struct TStartRequestContext: TRequestContextBase
{
    NProto::TStartProxyEndpointRequest Request;
    NProto::TStartProxyEndpointResponse Response;
    grpc::ServerContext ServerContext;
    grpc::ServerAsyncResponseWriter<NProto::TStartProxyEndpointResponse> Writer;

    TStartRequestContext(
            NProto::TBlockStoreEndpointProxy::AsyncService& service,
            grpc::ServerCompletionQueue& cq)
        : Writer(&ServerContext)
    {
        service.RequestStartProxyEndpoint(
            &ServerContext,
            &Request,
            &Writer,
            &cq,
            &cq,
            this
        );
    }
};

struct TStopRequestContext: TRequestContextBase
{
    NProto::TStopProxyEndpointRequest Request;
    NProto::TStopProxyEndpointResponse Response;
    grpc::ServerContext ServerContext;
    grpc::ServerAsyncResponseWriter<NProto::TStopProxyEndpointResponse> Writer;

    TStopRequestContext(
            NProto::TBlockStoreEndpointProxy::AsyncService& service,
            grpc::ServerCompletionQueue& cq)
        : Writer(&ServerContext)
    {
        service.RequestStopProxyEndpoint(
            &ServerContext,
            &Request,
            &Writer,
            &cq,
            &cq,
            this
        );
    }
};

struct TListRequestContext: TRequestContextBase
{
    NProto::TListProxyEndpointsRequest Request;
    NProto::TListProxyEndpointsResponse Response;
    grpc::ServerContext ServerContext;
    grpc::ServerAsyncResponseWriter<NProto::TListProxyEndpointsResponse> Writer;

    TListRequestContext(
            NProto::TBlockStoreEndpointProxy::AsyncService& service,
            grpc::ServerCompletionQueue& cq)
        : Writer(&ServerContext)
    {
        service.RequestListProxyEndpoints(
            &ServerContext,
            &Request,
            &Writer,
            &cq,
            &cq,
            this
        );
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStorageProxy final: public IStorage
{
private:
    const IBlockStorePtr Service;

public:
    explicit TStorageProxy(IBlockStorePtr service)
        : Service(std::move(service))
    {}

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return Service->ZeroBlocks(
            std::move(callContext),
            std::move(request));
    }

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return Service->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return Service->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return NThreading::MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() override
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TClientStorage: NStorage::NServer::IClientStorage
{
    grpc::Server& Server;

    explicit TClientStorage(grpc::Server& server)
        : Server(server)
    {}

    void AddClient(
        const TSocketHolder& clientSocket,
        NCloud::NProto::ERequestSource source) override
    {
        Y_UNUSED(source);

        grpc::AddInsecureChannelFromFd(&Server, clientSocket);
    }

    void RemoveClient(const TSocketHolder& clientSocket) override
    {
        Y_UNUSED(clientSocket);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TServer: IEndpointProxyServer
{
    TGrpcInitializer GrpcInitializer;

    const TEndpointProxyServerConfig Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    NClient::TClientAppConfigPtr ClientConfig;
    TLog Log;

    NStorage::NServer::TEndpointPoller EndpointPoller;

    NProto::TBlockStoreEndpointProxy::AsyncService Service;
    std::unique_ptr<grpc::ServerCompletionQueue> CQ;
    std::unique_ptr<grpc::Server> Server;
    THolder<IThreadFactory::IThread> Thread;

    NBD::IServerPtr NbdServer;
    struct TEndpoint
    {
        IBlockStorePtr Client;
        NBD::IDevicePtr NbdDevice;
        std::unique_ptr<TNetworkAddress> ListenAddress;

        TString SockPath;
        TString NbdDevicePath;

        NBD::TStorageOptions NbdOptions;
    };
    THashMap<TString, std::shared_ptr<TEndpoint>> Socket2Endpoint;

    NBD::IClientPtr NbdClient;

    TServer(
            TEndpointProxyServerConfig config,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging)
        : Config(std::move(config))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Log(Logging->CreateLog("BLOCKSTORE_ENDPOINT_PROXY"))
    {
        NProto::TClientAppConfig clientConfig;
        clientConfig.MutableClientConfig()->SetRetryTimeout(
            TDuration::Days(1).MilliSeconds());
        ClientConfig = std::make_shared<NClient::TClientAppConfig>(
            std::move(clientConfig));
    }

    ~TServer() override
    {
        Stop();
    }

    void Wait()
    {
        Thread->Join();
    }

    void PreStart()
    {
        NbdClient = NBD::CreateClient(Logging, 4);
        NbdClient->Start();

        NBD::TServerConfig serverConfig {
            .ThreadsCount = 4,
            .MaxInFlightBytesPerThread = 1_GB,
            .Affinity = {}
        };

        NbdServer = CreateServer(Logging, serverConfig);
        NbdServer->Start();

        grpc::ServerBuilder sb;
        if (Config.Port) {
            const auto addr = Sprintf("0.0.0.0:%u", Config.Port);
            sb.AddListeningPort(addr, grpc::InsecureServerCredentials());
            STORAGE_INFO("Added addr " << addr);
        }

        if (Config.SecurePort) {
            grpc::SslServerCredentialsOptions options(
                GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY);

            options.pem_root_certs = ReadFile(Config.RootCertsFile);
            options.pem_key_cert_pairs.push_back({
                ReadFile(Config.KeyFile),
                ReadFile(Config.CertFile)
            });

            const auto secureAddr = Sprintf("0.0.0.0:%u", Config.SecurePort);

            sb.AddListeningPort(secureAddr, grpc::SslServerCredentials(options));
            STORAGE_INFO("Added secure addr " << secureAddr);
        }

        sb.RegisterService(&Service);
        STORAGE_INFO("Registered service");
        CQ = sb.AddCompletionQueue();
        Server = sb.BuildAndStart();

        Y_ENSURE(Server, "failed to start server");

        if (Config.UnixSocketPath) {
            EndpointPoller.Start();

            const int socketBacklog = 128;
            auto error = EndpointPoller.StartListenEndpoint(
                Config.UnixSocketPath,
                socketBacklog,
                S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR, // accessMode
                true, // multiClient
                NProto::SOURCE_FD_CONTROL_CHANNEL,
                std::make_shared<TClientStorage>(*Server));

            Y_ENSURE_EX(!HasError(error), yexception() << FormatError(error));
        }
    }

    void Loop()
    {
        STORAGE_INFO("Starting loop");

        new TStartRequestContext(Service, *CQ);
        new TStopRequestContext(Service, *CQ);
        new TListRequestContext(Service, *CQ);

        void* tag;
        bool ok;
        while (CQ->Next(&tag, &ok)) {
            auto* requestContext = static_cast<TRequestContextBase*>(tag);

            if (requestContext->Done || !ok) {
                delete requestContext;
                continue;
            }

            auto* startRequestContext =
                dynamic_cast<TStartRequestContext*>(requestContext);
            if (startRequestContext) {
                new TStartRequestContext(Service, *CQ);
                ProcessRequest(startRequestContext);
                continue;
            }

            auto* stopRequestContext =
                dynamic_cast<TStopRequestContext*>(requestContext);
            if (stopRequestContext) {
                new TStopRequestContext(Service, *CQ);
                ProcessRequest(stopRequestContext);
                continue;
            }

            auto* listRequestContext =
                dynamic_cast<TListRequestContext*>(requestContext);
            if (listRequestContext) {
                new TListRequestContext(Service, *CQ);
                ProcessRequest(listRequestContext);
                continue;
            }
        }

        STORAGE_INFO("Exiting loop");
    }

    /*
     *  Logging/error helpers
     */

    template <typename TRequest>
    void RequestReceived(const TRequest& request)
    {
        STORAGE_INFO(request.ShortDebugString().Quote() << "- Received");
    }

    template <typename TResponse>
    void ShitHappened(TResponse& response)
    {
        *response.MutableError() = MakeError(
            E_FAIL,
            TStringBuilder() << "Shit happened: "
            << CurrentExceptionMessage());
    }

    template <typename TRequest, typename TResponse>
    void Already(
        const TRequest& request,
        const TString& message,
        TResponse& response)
    {
        *response.MutableError() = MakeError(S_ALREADY, message);

        STORAGE_INFO(request.ShortDebugString().Quote() << " - " << message);
    }

    template <typename TRequest, typename TResponse>
    void RequestCompleted(const TRequest& request, const TResponse& response)
    {
        if (HasError(response.GetError())) {
            STORAGE_ERROR(
                request.ShortDebugString().Quote() << " - Got error "
                << FormatError(response.GetError()).Quote())
        } else {
            STORAGE_INFO(
                request.ShortDebugString().Quote() << " - Success "
                << FormatError(response.GetError()).Quote())
        }
    }

    template <typename TRequest>
    void ResponseSent(const TRequest& request)
    {
        STORAGE_INFO(request.ShortDebugString().Quote() << " - Response sent");
    }

    /*
     *  Request handling
     */

    static bool ValidateRequest(
        const NProto::TStartProxyEndpointRequest& request,
        NProto::TStartProxyEndpointResponse& response)
    {
        TString message;
        if (!request.GetUnixSocketPath()) {
            message = "UnixSocketPath not specified";
        } else if (!request.GetBlockSize()) {
            message = "BlockSize not specified";
        } else if (!request.GetBlocksCount()) {
            message = "BlocksCount not specified";
        }

        if (message) {
            *response.MutableError() =
                MakeError(E_ARGUMENT, std::move(message));
            return false;
        }

        return true;
    }

    void DoProcessRequest(
        const NProto::TStartProxyEndpointRequest& request,
        TEndpoint& ep,
        NProto::TStartProxyEndpointResponse& response)
    {
        if (!ValidateRequest(request, response)) {
            return;
        }

        STORAGE_INFO(request.ShortDebugString().Quote()
            << " - Validated request");

        TNetworkAddress connectAddress(
            TUnixSocketPath{request.GetUnixSocketPath()});
        ep.Client = NbdClient->CreateEndpoint(
            connectAddress,
            NBD::CreateClientHandler(Logging),
            CreateBlockStoreStub());
        ep.Client->Start();
        STORAGE_INFO(request.ShortDebugString().Quote()
            << " - Started NBD client endpoint");

        auto retryPolicy = CreateRetryPolicy(ClientConfig);
        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();
        ep.Client = CreateDurableClient(
            ClientConfig,
            std::move(ep.Client),
            std::move(retryPolicy),
            Logging,
            Timer,
            Scheduler,
            requestStats,
            volumeStats);

        ep.Client->Start();
        STORAGE_INFO(request.ShortDebugString().Quote()
            << " - Started DurableClient");

        // these options can actually be obtained from ClientHandler after the
        // first request is processed (they will be available after connection
        // negotiation), but it will make the code here more complex - it's
        // easier to pass these params from the client of endpoint proxy
        //
        // it also gives us some flexibility - we can show the device to the
        // kernel with different options if we need it for some reason
        ep.NbdOptions.BlockSize = request.GetBlockSize();
        ep.NbdOptions.BlocksCount = request.GetBlocksCount();

        auto handlerFactory = CreateServerHandlerFactory(
            CreateDefaultDeviceHandlerFactory(),
            Logging,
            std::make_shared<TStorageProxy>(ep.Client),
            CreateServerStatsStub(),
            ep.NbdOptions);

        ep.SockPath = request.GetUnixSocketPath() + ".proxy";
        ep.ListenAddress = std::make_unique<TNetworkAddress>(
            TUnixSocketPath{ep.SockPath});

        // TODO fix StartEndpoint signature - it's actually synchronous
        auto startResult = NbdServer->StartEndpoint(
            *ep.ListenAddress,
            std::move(handlerFactory)).GetValueSync();

        if (HasError(startResult)) {
            *response.MutableError() = std::move(startResult);
        } else {
            STORAGE_INFO(request.ShortDebugString().Quote()
                << " - Started NBD server endpoint");

            ep.NbdDevicePath = request.GetNbdDevice();
            if (ep.NbdDevicePath) {
                if (Config.Netlink) {
#ifdef NETLINK
                    ep.NbdDevice = NBD::CreateNetlinkDevice(
                        Logging,
                        *ep.ListenAddress,
                        request.GetNbdDevice(),
                        TDuration::Minutes(1),  // request timeout
                        TDuration::Days(1),     // connection timeout
                        true);                  // reconfigure device if exists
#else
                    STORAGE_ERROR("built without netlink support, falling back to ioctl");
#endif
                }
                if (ep.NbdDevice == nullptr) {
                    ep.NbdDevice = NBD::CreateDevice(
                        Logging,
                        *ep.ListenAddress,
                        request.GetNbdDevice(),
                        TDuration::Days(1));    // request timeout
                }
                ep.NbdDevice->Start();

                STORAGE_INFO(request.ShortDebugString().Quote()
                    << " - Started NBD device connection");
            } else {
                STORAGE_WARN(request.ShortDebugString().Quote()
                    << " - NbdDevice missing - no nbd connection with the"
                    << " kernel will be established");
            }

            response.SetInternalUnixSocketPath(ep.SockPath);
        }
    }

    void ProcessRequest(TStartRequestContext* requestContext)
    {
        const auto& request = requestContext->Request;
        auto& response = requestContext->Response;

        RequestReceived(request);

        auto& ep = Socket2Endpoint[request.GetUnixSocketPath()];
        if (ep) {
            if (ep->NbdDevicePath == request.GetNbdDevice()) {
                Already(request, "Endpoint already up", response);
            } else {
                *response.MutableError() = MakeError(
                    E_INVALID_STATE,
                    TStringBuilder() << "Endpoint already started with device "
                    << ep->NbdDevicePath << " != " << request.GetNbdDevice());
            }
        } else {
            ep = std::make_shared<TEndpoint>();
            try {
                DoProcessRequest(request, *ep, response);
            } catch (...) {
                ShitHappened(response);
            }
        }

        RequestCompleted(request, response);

        requestContext->Done = true;

        requestContext->Writer.Finish(
            response,
            grpc::Status::OK,
            requestContext
        );

        ResponseSent(request);
    }

    void DoProcessRequest(
        const NProto::TStopProxyEndpointRequest& request,
        TEndpoint& ep,
        NProto::TStopProxyEndpointResponse& response) const
    {
        response.SetInternalUnixSocketPath(ep.SockPath);
        response.SetNbdDevice(ep.NbdDevicePath);

        if (ep.NbdDevice) {
            ep.NbdDevice->Stop(true);
            STORAGE_INFO(request.ShortDebugString().Quote()
                << " - Stopped NBD device connection");
        }

        if (ep.ListenAddress) {
            NbdServer->StopEndpoint(*ep.ListenAddress);
            STORAGE_INFO(request.ShortDebugString().Quote()
                << " - Stopped NBD server endpoint");
        }

        if (ep.Client) {
            ep.Client->Stop();
            STORAGE_INFO(request.ShortDebugString().Quote()
                << " - Stopped NBD client endpoint");
        }
    }

    void ProcessRequest(TStopRequestContext* requestContext)
    {
        const auto& request = requestContext->Request;
        auto& response = requestContext->Response;

        RequestReceived(request);

        auto& ep = Socket2Endpoint[request.GetUnixSocketPath()];
        if (ep) {
            try {
                DoProcessRequest(request, *ep, response);
            } catch (...) {
                ShitHappened(response);
            }

            Socket2Endpoint.erase(request.GetUnixSocketPath());
        } else {
            Already(
                request,
                "Endpoint already stopped / Endpoint not found",
                response);
        }

        requestContext->Done = true;

        requestContext->Writer.Finish(
            response,
            grpc::Status::OK,
            requestContext
        );

        ResponseSent(request);
    }

    void ProcessRequest(TListRequestContext* requestContext)
    {
        const auto& request = requestContext->Request;
        auto& response = requestContext->Response;

        RequestReceived(request);

        for (const auto& x: Socket2Endpoint) {
            auto& e = *response.AddEndpoints();
            e.SetUnixSocketPath(x.first);
            e.SetInternalUnixSocketPath(x.second->SockPath);
            e.SetNbdDevice(x.second->NbdDevicePath);
            e.SetBlockSize(x.second->NbdOptions.BlockSize);
            e.SetBlocksCount(x.second->NbdOptions.BlocksCount);
        }

        SortBy(
            response.MutableEndpoints()->begin(),
            response.MutableEndpoints()->end(),
            [] (const auto& e) {
                return e.GetUnixSocketPath();
            });

        requestContext->Done = true;

        requestContext->Writer.Finish(
            response,
            grpc::Status::OK,
            requestContext
        );

        ResponseSent(request);
    }

    void Start() override
    {
        PreStart();

        Thread = SystemThreadFactory()->Run([=] () {
            Loop();
        });
    }

    void Stop() override
    {
        if (Config.UnixSocketPath) {
            EndpointPoller.Stop();
        }

        if (Server) {
            Server->Shutdown();
            CQ->Shutdown();

            if (Thread) {
                Thread->Join();
            }

            Server.reset();
        }

        if (NbdServer) {
            NbdServer->Stop();
            NbdServer.reset();
        }

        if (NbdClient) {
            NbdClient->Stop();
            NbdClient.reset();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointProxyServerPtr CreateServer(
    TEndpointProxyServerConfig config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging)
{
    return std::make_shared<TServer>(
        std::move(config),
        std::move(timer),
        std::move(scheduler),
        std::move(logging));
}

}   // namespace NCloud::NBlockStore::NServer
