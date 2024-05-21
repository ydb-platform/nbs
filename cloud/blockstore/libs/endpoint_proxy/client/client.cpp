#include "client.h"

#include <cloud/blockstore/public/api/grpc/endpoint_proxy.grpc.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
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

    TInstant ProcessingStartTs;

    TRequestContextImpl(
            grpc::CompletionQueue& cq,
            const TString& host,
            ui16 port,
            TRequest request,
            TInstant now,
            const std::shared_ptr<grpc::ChannelCredentials>& channelCredentials)
        : Request(std::move(request))
        , CQ(cq)
        , Service(grpc::CreateChannel(
            Sprintf("%s:%u", host.c_str(), port),
            channelCredentials))
        , Promise(NThreading::NewPromise<TResponse>())
        , ProcessingStartTs(now)
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
            TInstant now,
            const std::shared_ptr<grpc::ChannelCredentials>& channelCredentials)
        : TRequestContextImpl(
            cq,
            host,
            port,
            std::move(request),
            now,
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
            TInstant now,
            const std::shared_ptr<grpc::ChannelCredentials>& channelCredentials)
        : TRequestContextImpl(
            cq,
            host,
            port,
            std::move(request),
            now,
            channelCredentials)
    {
        Reader = Service.AsyncStopProxyEndpoint(&ClientContext, Request, &CQ);
    }
};

struct TListRequestContext: TRequestContextImpl<
    NProto::TListProxyEndpointsRequest,
    NProto::TListProxyEndpointsResponse>
{
    using TReader = grpc::ClientAsyncResponseReader<TResponse>;

    std::unique_ptr<TReader> Reader;

    TListRequestContext(
            grpc::CompletionQueue& cq,
            const TString& host,
            ui16 port,
            TRequest request,
            TInstant now,
            const std::shared_ptr<grpc::ChannelCredentials>& channelCredentials)
        : TRequestContextImpl(
            cq,
            host,
            port,
            std::move(request),
            now,
            channelCredentials)
    {
        Reader = Service.AsyncListProxyEndpoints(&ClientContext, Request, &CQ);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEndpointProxyClient
    : IEndpointProxyClient
    , std::enable_shared_from_this<TEndpointProxyClient>
{
    const TEndpointProxyClientConfig Config;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    TLog Log;

    grpc::CompletionQueue CQ;
    std::shared_ptr<grpc::ChannelCredentials> ChannelCredentials;
    THolder<IThreadFactory::IThread> Thread;

    TEndpointProxyClient(
            TEndpointProxyClientConfig config,
            ISchedulerPtr scheduler,
            ITimerPtr timer,
            ILoggingServicePtr logging)
        : Config(std::move(config))
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , Log(logging->CreateLog("ENDPOINT_PROXY_CLIENT"))
    {
        if (Config.SecurePort) {
            ChannelCredentials = grpc::SslCredentials({
                .pem_root_certs = ReadFile(Config.RootCertsFile)
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
        typename TContext::TRequest request,
        TInstant now)
    {
        auto requestContext = std::make_unique<TContext>(
            CQ,
            Config.Host,
            Port(),
            request,
            now,
            ChannelCredentials
        );

        auto promise = requestContext->Promise;

        requestContext->Reader->Finish(
            &requestContext->Response,
            &requestContext->Status,
            requestContext.release()
        );

        return promise;
    }

    TFuture<NProto::TStartProxyEndpointResponse> StartProxyEndpoint(
        std::shared_ptr<NProto::TStartProxyEndpointRequest> request) override
    {
        STORAGE_INFO("StartRequest: " << request->DebugString().Quote());

        return RequestImpl<TStartRequestContext>(
            std::move(*request),
            Timer->Now());
    }

    TFuture<NProto::TStopProxyEndpointResponse> StopProxyEndpoint(
        std::shared_ptr<NProto::TStopProxyEndpointRequest> request) override
    {
        STORAGE_INFO("StopRequest: " << request->DebugString().Quote());

        return RequestImpl<TStopRequestContext>(
            std::move(*request),
            Timer->Now());
    }

    TFuture<NProto::TListProxyEndpointsResponse> ListProxyEndpoints(
        std::shared_ptr<NProto::TListProxyEndpointsRequest> request) override
    {
        STORAGE_INFO("ListRequest: " << request->DebugString().Quote());

        return RequestImpl<TListRequestContext>(
            std::move(*request),
            Timer->Now());
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
                FinishRequest(*startRequestContext, ok);
                continue;
            }

            auto* stopRequestContext =
                dynamic_cast<TStopRequestContext*>(&*requestContext);
            if (stopRequestContext) {
                FinishRequest(*stopRequestContext, ok);
                continue;
            }

            auto* listRequestContext =
                dynamic_cast<TListRequestContext*>(&*requestContext);
            if (listRequestContext) {
                FinishRequest(*listRequestContext, ok);
                continue;
            }
        }

        STORAGE_INFO("Exiting loop");
    }

    template <typename TRequestContext>
    void FinishRequest(TRequestContext& requestContext, bool ok)
    {
        if (!ok) {
            *requestContext.Response.MutableError() =
                MakeError(E_CANCELLED, "cq shutting down");
        } else if (!requestContext.Status.ok()) {
            *requestContext.Response.MutableError() = MakeError(
                MAKE_GRPC_ERROR(requestContext.Status.error_code()),
                requestContext.Status.error_message());
        }

        const auto errorKind =
            GetErrorKind(requestContext.Response.GetError());
        const auto deadline = requestContext.ProcessingStartTs
            + Config.RetryPolicy.TotalTimeout;

        if (errorKind == EErrorKind::ErrorRetriable && Timer->Now() < deadline) {
            Retry(requestContext);
        } else {
            requestContext.Promise.SetValue(std::move(requestContext.Response));
        }
    }

    template <typename TRequestContext>
    void Retry(TRequestContext& requestContext)
    {
        auto request = std::move(requestContext.Request);
        auto p = std::move(requestContext.Promise);
        auto weakPtr = this->weak_from_this();
        Scheduler->Schedule(
            Timer->Now() + Config.RetryPolicy.Backoff,
            [
                request = std::move(request),
                now = requestContext.ProcessingStartTs,
                p = std::move(p),
                weakPtr = std::move(weakPtr)
            ] () mutable {
                auto pThis = weakPtr.lock();
                if (!pThis) {
                    typename TRequestContext::TResponse response;
                    *response.MutableError() =
                        MakeError(E_CANCELLED, "client destroyed");
                    p.SetValue(std::move(response));
                    return;
                }

                auto& Log = pThis->Log;
                STORAGE_INFO("Retry: " << request.DebugString().Quote());

                pThis->RequestImpl<TRequestContext>(std::move(request), now)
                    .Subscribe([p = std::move(p)] (auto f) mutable {
                        p.SetValue(f.ExtractValue());
                    });
            });
    }

    void Start() override
    {
        Thread = SystemThreadFactory()->Run([this] () {
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
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    ILoggingServicePtr logging)
{
    return std::make_shared<TEndpointProxyClient>(
        std::move(config),
        std::move(scheduler),
        std::move(timer),
        std::move(logging));
}

}   // namespace NCloud::NBlockStore::NClient
