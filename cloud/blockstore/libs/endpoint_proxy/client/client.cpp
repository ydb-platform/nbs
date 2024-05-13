#include "client.h"

#include <cloud/blockstore/public/api/grpc/endpoint_proxy.grpc.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/stream/file.h>
#include <util/string/printf.h>
#include <util/thread/factory.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/async_unary_call.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/status.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& name)
{
    return TFileInput(name).ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestContextBase
{
    virtual ~TRequestContextBase() = default;
};

template <typename T1, typename T2>
struct TRequestContextImpl: TRequestContextBase
{
    // exposing TRequest and TResponse to the derived classes
    using TRequest = T1;
    using TResponse = T2;

    const TRequest Request;
    TResponse Response;

    grpc::CompletionQueue& CQ;

    NProto::TBlockStoreEndpointProxy::Stub Service;
    grpc::ClientContext ClientContext;

    grpc::Status Status;

    NThreading::TPromise<TResponse> Promise;

    TRequestContextImpl(
            grpc::CompletionQueue& cq,
            const TString& host,
            ui16 port,
            TRequest request,
            const std::shared_ptr<grpc::ChannelCredentials>& channelCredentials)
        : Request(std::move(request))
        , CQ(cq)
        , Service(grpc::CreateChannel(
            Sprintf("%s:%u", host.c_str(), port),
            channelCredentials
        ))
        , Promise(NThreading::NewPromise<TResponse>())
    {}
};

struct TStartRequestContext: TRequestContextImpl<
    NProto::TStartProxyEndpointRequest,
    NProto::TStartProxyEndpointResponse>
{
    using TReader = grpc::ClientAsyncResponseReader<TResponse>;

    std::unique_ptr<TReader> Reader;

    TStartRequestContext(
            grpc::CompletionQueue& cq,
            const TString& host,
            ui16 port,
            TRequest request,
            const std::shared_ptr<grpc::ChannelCredentials>& channelCredentials)
        : TRequestContextImpl(
            cq,
            host,
            port,
            std::move(request),
            channelCredentials)
    {
        Reader = Service.AsyncStartProxyEndpoint(&ClientContext, Request, &CQ);
    }
};

struct TStopRequestContext: TRequestContextImpl<
    NProto::TStopProxyEndpointRequest,
    NProto::TStopProxyEndpointResponse>
{
    using TReader = grpc::ClientAsyncResponseReader<TResponse>;

    std::unique_ptr<TReader> Reader;

    TStopRequestContext(
            grpc::CompletionQueue& cq,
            const TString& host,
            ui16 port,
            TRequest request,
            const std::shared_ptr<grpc::ChannelCredentials>& channelCredentials)
        : TRequestContextImpl(
            cq,
            host,
            port,
            std::move(request),
            channelCredentials)
    {
        Reader = Service.AsyncStopProxyEndpoint(&ClientContext, Request, &CQ);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEndpointProxyClient: IEndpointProxyClient
{
    const TEndpointProxyClientConfig Config;
    TLog Log;

    grpc::CompletionQueue CQ;
    std::shared_ptr<grpc::ChannelCredentials> ChannelCredentials;
    THolder<IThreadFactory::IThread> Thread;

    TEndpointProxyClient(
            TEndpointProxyClientConfig config,
            ILoggingServicePtr logging)
        : Config(std::move(config))
        , Log(logging->CreateLog("ENDPOINT_PROXY_CLIENT"))
    {
        if (config.SecurePort) {
            ChannelCredentials = grpc::SslCredentials({
                .pem_root_certs = ReadFile(config.RootCertsFile)
            });
        } else {
            ChannelCredentials = grpc::InsecureChannelCredentials();
        }
    }

    ui16 Port() const
    {
        return Config.SecurePort ? Config.SecurePort : Config.Port;
    }

    template <typename TContext>
    TFuture<typename TContext::TResponse> RequestImpl(
        std::shared_ptr<typename TContext::TRequest> request)
    {
        auto requestContext = std::make_unique<TContext>(
            CQ,
            Config.Host,
            Port(),
            *request,
            ChannelCredentials
        );

        auto promise = requestContext->Promise;

        requestContext->Reader->Finish(
            &requestContext->Response,
            &requestContext->Status,
            &*requestContext
        );

        requestContext.release();

        return promise;
    }

    TFuture<NProto::TStartProxyEndpointResponse> StartProxyEndpoint(
        std::shared_ptr<NProto::TStartProxyEndpointRequest> request) override
    {
        STORAGE_INFO("StartRequest: " << request->DebugString().Quote());

        return RequestImpl<TStartRequestContext>(std::move(request));
    }

    TFuture<NProto::TStopProxyEndpointResponse> StopProxyEndpoint(
        std::shared_ptr<NProto::TStopProxyEndpointRequest> request) override
    {
        STORAGE_INFO("StopRequest: " << request->DebugString().Quote());

        return RequestImpl<TStopRequestContext>(std::move(request));
    }

    void Loop()
    {
        STORAGE_INFO("Starting loop");

        void* tag;
        bool ok;
        while (CQ.Next(&tag, &ok)) {
            std::unique_ptr<TRequestContextBase> requestContext(
                static_cast<TRequestContextBase*>(tag));

            auto* startRequestContext =
                dynamic_cast<TStartRequestContext*>(&*requestContext);
            if (startRequestContext) {
                FinishRequest(*startRequestContext);
                continue;
            }

            auto* stopRequestContext =
                dynamic_cast<TStopRequestContext*>(&*requestContext);
            if (stopRequestContext) {
                FinishRequest(*stopRequestContext);
                continue;
            }
        }

        STORAGE_INFO("Exiting loop");
    }

    template <typename TRequestContext>
    void FinishRequest(TRequestContext& requestContext)
    {
        if (!requestContext.Status.ok()) {
            auto* e = requestContext.Response.MutableError();
            e->SetCode(MAKE_GRPC_ERROR(requestContext.Status.error_code()));
            e->SetMessage(requestContext.Status.error_message());
        }
        requestContext.Promise.SetValue(std::move(requestContext.Response));
    }

    void Start() override
    {
        Thread = SystemThreadFactory()->Run([=] () {
            Loop();
        });
    }

    void Stop() override
    {
        if (Thread) {
            CQ.Shutdown();
            Thread->Join();
            Thread.Reset();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointProxyClientPtr CreateClient(
    TEndpointProxyClientConfig config,
    ILoggingServicePtr logging)
{
    return std::make_shared<TEndpointProxyClient>(
        std::move(config),
        std::move(logging));
}

}   // namespace NCloud::NBlockStore::NClient
