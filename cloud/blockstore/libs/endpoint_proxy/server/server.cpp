#include "server.h"

#include <cloud/blockstore/public/api/grpc/endpoint_proxy.grpc.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/initializer.h>

#include <library/cpp/logger/log.h>

#include <contrib/libs/grpc/include/grpcpp/impl/codegen/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/status.h>
#include <contrib/libs/grpc/include/grpcpp/security/server_credentials.h>
#include <contrib/libs/grpc/include/grpcpp/server.h>
#include <contrib/libs/grpc/include/grpcpp/server_builder.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TServer: IEndpointProxyServer
{
    TGrpcInitializer GrpcInitializer;

    const TEndpointProxyServerConfig Config;
    TLog Log;

    NProto::TBlockStoreEndpointProxy::AsyncService Service;
    std::unique_ptr<grpc::ServerCompletionQueue> CQ;
    std::unique_ptr<grpc::Server> Server;
    THolder<IThreadFactory::IThread> Thread;

    TServer(
            TEndpointProxyServerConfig config,
            ILoggingServicePtr logging)
        : Config(std::move(config))
        , Log(logging->CreateLog("BLOCKSTORE_ENDPOINT_PROXY"))
    {
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
        grpc::ServerBuilder sb;
        const auto addr = Sprintf("0.0.0.0:%u", Config.Port);
        sb.AddListeningPort(addr, grpc::InsecureServerCredentials());
        STORAGE_INFO("Added addr " << addr);

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
    }

    void Loop()
    {
        STORAGE_INFO("Starting loop");

        new TStartRequestContext(Service, *CQ);
        new TStopRequestContext(Service, *CQ);

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
        }

        STORAGE_INFO("Exiting loop");
    }

    void ProcessRequest(TStartRequestContext* requestContext)
    {
        STORAGE_INFO("StartRequest: "
            << requestContext->Request.DebugString().Quote());

        // TODO: impl

        requestContext->Done = true;

        auto& r = requestContext->Response;
        r.SetInternalUnixSocketPath("TODO-internal-socket");
        requestContext->Writer.Finish(
            r,
            grpc::Status::OK,
            requestContext
        );
    }

    void ProcessRequest(TStopRequestContext* requestContext)
    {
        STORAGE_INFO("StopRequest: "
            << requestContext->Request.DebugString().Quote());

        // TODO: impl

        requestContext->Done = true;

        auto& r = requestContext->Response;
        r.SetInternalUnixSocketPath("TODO-internal-socket");
        r.SetNbdDevice("TODO-nbd-device");
        requestContext->Writer.Finish(
            r,
            grpc::Status::OK,
            requestContext
        );
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
        if (Server) {
            Server->Shutdown();
            CQ->Shutdown();

            if (Thread) {
                Thread->Join();
            }

            Server.reset();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointProxyServerPtr CreateServer(
    TEndpointProxyServerConfig config,
    ILoggingServicePtr logging)
{
    return std::make_shared<TServer>(std::move(config), std::move(logging));
}

}   // namespace NCloud::NBlockStore::NServer
