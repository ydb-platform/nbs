#include "server.h"

#include "proxy_storage.h"

#include <cloud/blockstore/config/client.pb.h>

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/nbd/client_handler.h>
#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/libs/nbd/error_handler.h>
#include <cloud/blockstore/libs/nbd/netlink_device.h>
#include <cloud/blockstore/libs/nbd/server.h>
#include <cloud/blockstore/libs/nbd/server_handler.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/public/api/grpc/endpoint_proxy.grpc.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/uds/client_storage.h>
#include <cloud/storage/core/libs/uds/endpoint_poller.h>

#include <library/cpp/logger/log.h>

#include <contrib/libs/grpc/include/grpcpp/alarm.h>
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
#include <util/string/subst.h>
#include <util/system/datetime.h>
#include <util/system/fs.h>
#include <util/thread/factory.h>

#include <atomic>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto NBD_CONNECTION_TIMEOUT = TDuration::Days(1);
constexpr auto NBD_RECONFIGURE_CONNECTED = true;
constexpr auto NBD_DELETE_DEVICE = false;

constexpr auto MAX_RECONNECT_DELAY = TDuration::Minutes(10);

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& name)
{
    return TFileInput(name).ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint: std::enable_shared_from_this<TEndpoint>
{
    IBlockStorePtr Client;
    NBD::IDevicePtr NbdDevice;
    std::unique_ptr<TNetworkAddress> ListenAddress;
    IProxyRequestStatsPtr RequestStats;
    TString UnixSocketPath;
    TString InternalUnixSocketPath;
    TString NbdDevicePath;
    NBD::TStorageOptions NbdOptions;
    std::atomic<ui64> Generation = 0;
};

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

struct TResizeRequestContext: TRequestContextBase
{
    NProto::TResizeProxyDeviceRequest Request;
    NProto::TResizeProxyDeviceResponse Response;
    grpc::ServerContext ServerContext;
    grpc::ServerAsyncResponseWriter<NProto::TResizeProxyDeviceResponse> Writer;

    TResizeRequestContext(
            NProto::TBlockStoreEndpointProxy::AsyncService& service,
            grpc::ServerCompletionQueue& cq)
        : Writer(&ServerContext)
    {
        service.RequestResizeProxyDevice(
            &ServerContext,
            &Request,
            &Writer,
            &cq,
            &cq,
            this);
    }
};

struct TRestartAlarmContext: TRequestContextBase
{
    std::weak_ptr<TEndpoint> Endpoint;
    grpc::ServerCompletionQueue& CQ;
    const ui64 Generation;
    TBackoffDelayProvider Backoff;
    grpc::Alarm Alarm;

    TRestartAlarmContext(
            std::weak_ptr<TEndpoint> endpoint,
            grpc::ServerCompletionQueue& cq,
            ui64 generation,
            TDuration minReconnectDelay,
            TDuration maxReconnectDelay)
        : Endpoint(std::move(endpoint))
        , CQ(cq)
        , Generation(generation)
        , Backoff{minReconnectDelay, maxReconnectDelay}
    {
        SetAlarm();
    }

    // context will be held by grpc until CQ->Next returns it
    void SetAlarm()
    {
        Alarm.Set(
            &CQ,
            gpr_time_from_nanos(
                Backoff.GetDelayAndIncrease().NanoSeconds(),
                gpr_clock_type::GPR_TIMESPAN),
            this);
    }
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

        auto fileHandle = TFileHandle(clientSocket);
        auto dupSocket = fileHandle.Duplicate();
        fileHandle.Release();

        grpc::AddInsecureChannelFromFd(&Server, dupSocket);
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
    const TFsPath WorkingDirectory;
    NClient::TClientAppConfigPtr ClientConfig;
    TLog Log;

    NStorage::NServer::TEndpointPoller EndpointPoller;

    NProto::TBlockStoreEndpointProxy::AsyncService Service;
    std::unique_ptr<grpc::ServerCompletionQueue> CQ;
    std::unique_ptr<grpc::Server> Server;
    THolder<IThreadFactory::IThread> Thread;

    NBD::IServerPtr NbdServer;
    THashMap<TString, std::shared_ptr<TEndpoint>> Socket2Endpoint;

    struct TErrorHandler: NBD::IErrorHandler
    {
        std::weak_ptr<TEndpoint> Endpoint;
        grpc::ServerCompletionQueue& CQ;
        const ui64 Generation;
        TDuration ReconnectDelay;
        std::optional<ui32> DebugRestartEventsCount;

        TErrorHandler(
                std::weak_ptr<TEndpoint> endpoint,
                grpc::ServerCompletionQueue& cq,
                ui64 generation,
                TDuration reconnectDelay,
                std::optional<ui32> debugRestartEventsCount)
            : Endpoint(std::move(endpoint))
            , CQ(cq)
            , Generation(generation)
            , ReconnectDelay(reconnectDelay)
            , DebugRestartEventsCount(debugRestartEventsCount)
        {}

        void ProcessException(std::exception_ptr e) override
        {
            Y_UNUSED(e);

            for (ui32 i = 0; i < DebugRestartEventsCount.value_or(1); i++) {
                // context will be held by grpc until CQ->Next returns it
                new TRestartAlarmContext(
                    Endpoint,
                    CQ,
                    Generation,
                    ReconnectDelay,
                    MAX_RECONNECT_DELAY);
            }
        }
    };

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
        , WorkingDirectory(NFs::CurrentWorkingDirectory())
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

    TFsPath StoredEndpointPath(const TEndpoint& ep)
    {
        return TFsPath(Config.StoredEndpointsPath)
            / SubstGlobalCopy(ep.UnixSocketPath, "/", "_");
    }

    static NProto::TListProxyEndpointsResponse::TProxyEndpoint
        MakeProxyEndpoint(const TEndpoint& ep)
    {
        NProto::TListProxyEndpointsResponse::TProxyEndpoint e;
        e.SetUnixSocketPath(ep.UnixSocketPath);
        e.SetInternalUnixSocketPath(ep.InternalUnixSocketPath);
        e.SetNbdDevice(ep.NbdDevicePath);
        e.SetBlockSize(ep.NbdOptions.BlockSize);
        e.SetBlocksCount(ep.NbdOptions.BlocksCount);
        return e;
    }

    void StoreEndpointIfNeeded(const TEndpoint& ep)
    {
        if (!Config.StoredEndpointsPath) {
            return;
        }

        const auto p = StoredEndpointPath(ep);
        TOFStream os(p);
        const auto proxyEndpoint = MakeProxyEndpoint(ep);
        os.Write(ProtoToText(proxyEndpoint));
    }

    void RemoveStoredEndpointIfNeeded(const TEndpoint& ep)
    {
        if (!Config.StoredEndpointsPath) {
            return;
        }

        StoredEndpointPath(ep).ForceDelete();
    }

    void Wait()
    {
        Thread->Join();
    }

    TUnixSocketPath MakeUnixSocketAddress(TStringBuf unixSocketPath) const
    {
        TFsPath originalPath(unixSocketPath);
        TFsPath resultingPath;
        if (originalPath.IsSubpathOf(WorkingDirectory)) {
            resultingPath = originalPath.RelativeTo(WorkingDirectory);
        } else {
            resultingPath = std::move(originalPath);
        }
        return TUnixSocketPath(resultingPath);
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
                MakeUnixSocketAddress(Config.UnixSocketPath).Path,
                socketBacklog,
                S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR, // accessMode
                true, // multiClient
                NProto::SOURCE_FD_CONTROL_CHANNEL,
                std::make_shared<TClientStorage>(*Server));

            Y_ENSURE_EX(!HasError(error), yexception() << FormatError(error));
        }

        if (Config.StoredEndpointsPath) {
            TVector<TFsPath> files;
            TFsPath(Config.StoredEndpointsPath).List(files);

            for (const auto& f: files) {
                STORAGE_INFO("Restoring endpoint from " << f.GetPath());

                NProto::TListProxyEndpointsResponse::TProxyEndpoint e;
                try {
                    ParseProtoTextFromFile(f.GetPath(), e);
                } catch (...) {
                    STORAGE_ERROR("Couldn't load endpoint from " << f.GetPath()
                        << ", error: " << CurrentExceptionMessage());

                    continue;
                }

                auto ep = std::make_shared<TEndpoint>();
                ep->UnixSocketPath = e.GetUnixSocketPath();

                NProto::TStartProxyEndpointRequest request;
                request.SetUnixSocketPath(e.GetUnixSocketPath());
                request.SetNbdDevice(e.GetNbdDevice());
                request.SetBlockSize(e.GetBlockSize());
                request.SetBlocksCount(e.GetBlocksCount());
                NProto::TStartProxyEndpointResponse response;

                try {
                    DoProcessRequest(request, *ep, response);
                } catch (...) {
                    STORAGE_ERROR("Couldn't restore endpoint "
                        << e.GetUnixSocketPath()
                        << ", error: " << CurrentExceptionMessage());
                    continue;
                }

                Socket2Endpoint[ep->UnixSocketPath] = std::move(ep);

                STORAGE_INFO("Restored endpoint from " << f.GetPath());
            }
        }
    }

    void Loop()
    {
        STORAGE_INFO("Starting loop");

        new TStartRequestContext(Service, *CQ);
        new TStopRequestContext(Service, *CQ);
        new TListRequestContext(Service, *CQ);
        new TResizeRequestContext(Service, *CQ);

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

            auto* resizeRequestContext =
                dynamic_cast<TResizeRequestContext*>(requestContext);
            if (resizeRequestContext) {
                new TResizeRequestContext(Service, *CQ);
                ProcessRequest(resizeRequestContext);
                continue;
            }

            auto* restartAlarmContext =
                dynamic_cast<TRestartAlarmContext*>(requestContext);
            if (restartAlarmContext) {
                if (ProcessAlarm(restartAlarmContext)) {
                    // reset the alarm in case of an error, context will be
                    // held by grpc until CQ->Next returns it
                    restartAlarmContext->SetAlarm();
                } else {
                    // unlike requests, alarms don't need to wait for async
                    // writer, so can be freed immediately
                    delete restartAlarmContext;
                }
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
        STORAGE_INFO(request.ShortDebugString().Quote() << " - Received");
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
        const auto tag = TStringBuilder()
            << request.ShortDebugString().Quote() << " - ";

        if (!ValidateRequest(request, response)) {
            return;
        }
        STORAGE_INFO(tag << "Validated request");

        TNetworkAddress connectAddress(
            MakeUnixSocketAddress(request.GetUnixSocketPath()));
        ep.Client = NbdClient->CreateEndpoint(
            connectAddress,
            NBD::CreateClientHandler(Logging),
            CreateBlockStoreStub());
        ep.Client->Start();
        STORAGE_INFO(tag << "Started NBD client endpoint");

        auto retryPolicy = CreateRetryPolicy(ClientConfig, std::nullopt);
        ep.RequestStats = CreateProxyRequestStats();
        auto volumeStats = CreateVolumeStatsStub();
        ep.Client = CreateDurableClient(
            ClientConfig,
            std::move(ep.Client),
            std::move(retryPolicy),
            Logging,
            Timer,
            Scheduler,
            ep.RequestStats,
            volumeStats);
        STORAGE_INFO(tag << "Started DurableClient");

        // these options can actually be obtained from ClientHandler after the
        // first request is processed (they will be available after connection
        // negotiation), but it will make the code here more complex - it's
        // easier to pass these params from the client of endpoint proxy
        //
        // it also gives us some flexibility - we can show the device to the
        // kernel with different options if we need it for some reason
        ep.NbdOptions.BlockSize = request.GetBlockSize();
        ep.NbdOptions.BlocksCount = request.GetBlocksCount();

        ep.InternalUnixSocketPath = request.GetUnixSocketPath() + ".p";
        ep.ListenAddress = std::make_unique<TNetworkAddress>(
            MakeUnixSocketAddress(ep.InternalUnixSocketPath));

        auto status = StartServerEndpoint(ep, tag);
        if (HasError(status)) {
            *response.MutableError() = std::move(status);
            return;
        }
        response.SetInternalUnixSocketPath(ep.InternalUnixSocketPath);

        ep.NbdDevicePath = request.GetNbdDevice();

        status = StartDevice(ep, tag);
        if (HasError(status)) {
            STORAGE_ERROR(tag << "Unable to start nbd device: "
                << status.GetMessage());
            *response.MutableError() = std::move(status);
            return;
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
            ep->UnixSocketPath = request.GetUnixSocketPath();
            try {
                DoProcessRequest(request, *ep, response);
                if (HasError(response)) {
                    NProto::TStopProxyEndpointRequest stopRequest;
                    NProto::TStopProxyEndpointResponse stopResponse;
                    stopRequest.SetUnixSocketPath(request.GetUnixSocketPath());
                    DoProcessRequest(stopRequest, *ep, stopResponse);
                    Socket2Endpoint.erase(request.GetUnixSocketPath());
                } else {
                    StoreEndpointIfNeeded(*ep);
                }
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
        response.SetInternalUnixSocketPath(ep.InternalUnixSocketPath);
        response.SetNbdDevice(ep.NbdDevicePath);

        if (ep.NbdDevice) {
            auto err = ep.NbdDevice->Stop(true).GetValueSync();
            if (HasError(err)) {
                STORAGE_ERROR(
                    request.ShortDebugString().Quote()
                    << " - Failed to stop NBD device: " << err.GetCode());
            } else {
                STORAGE_INFO(
                    request.ShortDebugString().Quote()
                    << " - Stopped NBD device");
            }
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
                RemoveStoredEndpointIfNeeded(*ep);
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
            auto& pe = *response.AddEndpoints();
            pe = MakeProxyEndpoint(*x.second);

            auto& src = *x.second->RequestStats;
            auto& dst = *pe.MutableStats();
            for (ui32 i = 0; i < src.GetInternalStats().size(); ++i) {
                const auto& srcRs = src.GetInternalStats()[i];
                NProto::TListProxyEndpointsResponse::TRequestTypeStats dstRs;
                dstRs.SetCount(srcRs.Count.load(std::memory_order_relaxed));
                dstRs.SetRequestBytes(
                    srcRs.RequestBytes.load(std::memory_order_relaxed));
                dstRs.SetInflight(srcRs.Inflight.load(std::memory_order_relaxed));
                dstRs.SetInflightBytes(
                    srcRs.InflightBytes.load(std::memory_order_relaxed));

                for (ui32 j = 0; j < srcRs.ErrorKind2Count.size(); ++j) {
                    const auto& srcE = srcRs.ErrorKind2Count[j];
                    NProto::TListProxyEndpointsResponse::TErrorKindStats dstE;
                    dstE.SetCount(srcE.load(std::memory_order_relaxed));

                    if (dstE.ByteSize()) {
                        dstE.SetErrorKindName(
                            ToString(static_cast<EDiagnosticsErrorKind>(j)));
                        *dstRs.AddErrorKindStats() = std::move(dstE);
                    }
                }

                if (dstRs.ByteSize()) {
                    dstRs.SetRequestName(GetBlockStoreRequestName(
                        static_cast<EBlockStoreRequest>(i)));
                    *dst.AddRequestTypeStats() = std::move(dstRs);
                }
            }
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

    void DoProcessRequest(
        const NProto::TResizeProxyDeviceRequest& request,
        TEndpoint& ep,
        NProto::TResizeProxyDeviceResponse& response)
    {
        if (ep.NbdDevice) {
            auto err = ep.NbdDevice->Resize(request.GetDeviceSizeInBytes())
                           .GetValueSync();
            if (HasError(err)) {
                *response.MutableError() = err;
                STORAGE_ERROR(
                    request.ShortDebugString().Quote()
                    << " - Failed to resize NBD device: " << err.GetCode());
                return;
            }

            if (ep.NbdOptions.BlockSize == 0) {
                STORAGE_ERROR(
                    request.ShortDebugString().Quote()
                    << " - Failed to update NBD device options: invalid "
                       "block size");
                return;
            }

            ep.NbdOptions.BlocksCount =
                request.GetDeviceSizeInBytes() / ep.NbdOptions.BlockSize;

            StoreEndpointIfNeeded(ep);

            STORAGE_INFO(
                request.ShortDebugString().Quote()
                << " - NBD device was resized");
        }
    }

    void ProcessRequest(TResizeRequestContext* requestContext)
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
        } else {
            Already(request, "Endpoint not found", response);
        }

        requestContext->Done = true;

        requestContext->Writer.Finish(
            response,
            grpc::Status::OK,
            requestContext);

        ResponseSent(request);
    }

    NProto::TError StartServerEndpoint(TEndpoint& ep, const TString& tag)
    {
        auto handlerFactory = CreateServerHandlerFactory(
            CreateDefaultDeviceHandlerFactory(),
            Logging,
            CreateProxyStorage(
                ep.Client,
                ep.RequestStats,
                ep.NbdOptions.BlockSize),
            CreateServerStatsStub(),
            std::make_shared<TErrorHandler>(
                ep.shared_from_this(),
                *CQ,
                ep.Generation,
                Config.NbdReconnectDelay,
                Config.DebugRestartEventsCount),
            ep.NbdOptions);

        // TODO fix StartEndpoint signature - it's actually synchronous
        auto status = NbdServer->StartEndpoint(
            *ep.ListenAddress,
            std::move(handlerFactory)).GetValueSync();
        if (HasError(status)) {
            STORAGE_INFO(tag << "Unable to start NBD server endpoint: "
                << status.GetMessage());
            return status;
        }

        STORAGE_INFO(tag << "Started NBD server endpoint");
        return {};
    }

    NProto::TError StartDevice(TEndpoint& ep, const TString& tag)
    {
        if (!ep.NbdDevicePath) {
            STORAGE_WARN(tag << "NbdDevice missing - nbd connection "
                << "with the kernel won't be established");
            return {};
        }

        if (Config.Netlink) {
            ep.NbdDevice = NBD::CreateNetlinkDevice(
                Logging,
                *ep.ListenAddress,
                ep.NbdDevicePath,
                Config.NbdRequestTimeout,
                NBD_CONNECTION_TIMEOUT,
                NBD_RECONFIGURE_CONNECTED);
        } else {
            // The only case we want kernel to retry requests is when the socket
            // is dead due to nbd server restart. And since we can't configure
            // ioctl device to use a new socket, request timeout effectively
            // becomes connection timeout
            ep.NbdDevice = NBD::CreateDevice(
                Logging,
                *ep.ListenAddress,
                ep.NbdDevicePath,
                NBD_CONNECTION_TIMEOUT);
        }

        auto status = ep.NbdDevice->Start().GetValueSync();
        if (HasError(status)) {
            STORAGE_ERROR(tag << "Unable to start NBD device: "
                << status.GetMessage());
            return status;
        }

        STORAGE_INFO(tag << "Started NBD device");

        return {};
    }

    // returns true in case of an error
    bool ProcessAlarm(TRestartAlarmContext* context)
    {
        if (auto ep = context->Endpoint.lock()) {
            if (context->Generation != ep->Generation) {
                return false;
            }
            ep->Generation++;

            const auto tag = TStringBuilder()
                << "UnixSocketPath: " << ep->UnixSocketPath.Quote() << " - ";

            STORAGE_INFO(tag << "Restarting proxy endpoint");

            if (DoProcessAlarm(*ep, tag)) {
                STORAGE_ERROR(tag
                    << "Unable to restart proxy endpoint, retry in "
                    << context->Backoff.GetDelay());
                return true;
            }
        }
        return false;
    }

    // returns true in case of an error
    bool DoProcessAlarm(TEndpoint& ep, const TString& tag)
    {
        if (ep.NbdDevice) {
            auto err = ep.NbdDevice->Stop(NBD_DELETE_DEVICE).GetValueSync();
            if (HasError(err)) {
                STORAGE_ERROR(
                    tag << "Failed to stop NBD device: " << err.GetCode());
            } else {
                STORAGE_INFO(tag << "Stopped NBD device");
            }
        }

        if (ep.ListenAddress) {
            NbdServer->StopEndpoint(*ep.ListenAddress).GetValueSync();
            STORAGE_INFO(tag << "Stopped NBD server endpoint");
        }

        auto status = StartServerEndpoint(ep, tag);
        if (HasError(status)) {
            return true;
        }

        status = StartDevice(ep, tag);
        if (HasError(status)) {
            return true;
        }

        return false;
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
