#include "client.h"

#include <cloud/blockstore/public/api/grpc/endpoint_proxy.grpc.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/network/sock.h>
#include <util/stream/file.h>
#include <util/string/printf.h>
#include <util/thread/factory.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel_posix.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/async_unary_call.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/status.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>

#include <cerrno>

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
            std::shared_ptr<grpc::Channel> channel,
            TRequest request,
            TInstant now)
        : Request(std::move(request))
        , CQ(cq)
        , Service(std::move(channel))
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
            std::shared_ptr<grpc::Channel> channel,
            TRequest request,
            TInstant now)
        : TRequestContextImpl(
            cq,
            std::move(channel),
            std::move(request),
            now)
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
            std::shared_ptr<grpc::Channel> channel,
            TRequest request,
            TInstant now)
        : TRequestContextImpl(
            cq,
            std::move(channel),
            std::move(request),
            now)
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
            std::shared_ptr<grpc::Channel> channel,
            TRequest request,
            TInstant now)
        : TRequestContextImpl(
            cq,
            std::move(channel),
            std::move(request),
            now)
    {
        Reader = Service.AsyncListProxyEndpoints(&ClientContext, Request, &CQ);
    }
};

struct TResizeRequestContext
    : TRequestContextImpl<
          NProto::TResizeProxyDeviceRequest,
          NProto::TResizeProxyDeviceResponse>
{
    using TReader = grpc::ClientAsyncResponseReader<TResponse>;

    std::unique_ptr<TReader> Reader;

    TResizeRequestContext(
            grpc::CompletionQueue& cq,
            std::shared_ptr<grpc::Channel> channel,
            TRequest request,
            TInstant now)
        : TRequestContextImpl(cq, std::move(channel), std::move(request), now)
    {
        Reader = Service.AsyncResizeProxyDevice(&ClientContext, Request, &CQ);
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

    bool NeedConnect = true;
    std::shared_ptr<grpc::Channel> Channel;

    grpc::CompletionQueue CQ;
    THolder<IThreadFactory::IThread> Thread;

    TEndpointProxyClient(
            TEndpointProxyClientConfig config,
            ISchedulerPtr scheduler,
            ITimerPtr timer,
            const ILoggingServicePtr& logging)
        : Config(std::move(config))
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , Log(logging->CreateLog("ENDPOINT_PROXY_CLIENT"))
    {}

    void ReconnectIfNeeded()
    {
        if (!NeedConnect) {
            return;
        }

        if (Config.UnixSocketPath) {
            TSockAddrLocal addr(Config.UnixSocketPath.c_str());

            TLocalStreamSocket socket;

            const auto deadline =
                Timer->Now() + Config.RetryPolicy.UnixSocketConnectTimeout;
            while (true) {
                auto res = socket.Connect(&addr);
                if (res == -ENOENT || res == -ECONNREFUSED) {
                    STORAGE_WARN("Retrying uds connection, res: " << res);

                    if (Timer->Now() < deadline) {
                        Sleep(TDuration::Seconds(1));
                        continue;
                    }
                }

                Y_ENSURE_EX(res >= 0, yexception() << "failed to connect to"
                    " endpoint proxy socket, res: " << res);
                break;
            }

            Channel = grpc::CreateInsecureChannelFromFd(
                "localhost",
                socket);

            socket.Release();
        } else if (Config.SecurePort) {
            auto channelCredentials = grpc::SslCredentials({
                .pem_root_certs = ReadFile(Config.RootCertsFile)
            });

            Channel = grpc::CreateChannel(
                Sprintf("%s:%u", Config.Host.c_str(), Config.SecurePort),
                channelCredentials);
        } else {
            Y_ENSURE_EX(Config.Port, yexception() << "none of the"
                " port/secure port/unix socket path specified for endpoint proxy"
                " client");

            auto channelCredentials = grpc::InsecureChannelCredentials();

            Channel = grpc::CreateChannel(
                Sprintf("%s:%u", Config.Host.c_str(), Config.Port),
                channelCredentials);
        }

        NeedConnect = false;
    }

    template <typename TContext>
    TFuture<typename TContext::TResponse> RequestImpl(
        typename TContext::TRequest request,
        TInstant now)
    {
        try {
            ReconnectIfNeeded();
        } catch (...) {
            STORAGE_ERROR("Unexpected reconnect error: "
                << CurrentExceptionMessage());

            // Channel can't be null
            if (!Channel) {
                Channel = grpc::CreateChannel(
                    "lame://channel",
                    grpc::InsecureChannelCredentials());
            }
        }

        auto requestContext = std::make_unique<TContext>(
            CQ,
            Channel,
            request,
            now
        );

        auto future = requestContext->Promise.GetFuture();

        requestContext->Reader->Finish(
            &requestContext->Response,
            &requestContext->Status,
            requestContext.release()
        );

        // At this point, requestContext object can already be freed after
        // request completion. It is not safe to use it.
        return future;
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

    TFuture<NProto::TResizeProxyDeviceResponse> ResizeProxyDevice(
        std::shared_ptr<NProto::TResizeProxyDeviceRequest> request) override
    {
        STORAGE_INFO("ResizeRequest: " << request->DebugString().Quote());

        return RequestImpl<TResizeRequestContext>(
            std::move(*request),
            Timer->Now());
    }

    void Loop()
    {
        STORAGE_INFO("Starting loop");

        void* tag = nullptr;
        bool ok = false;
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

            auto* resizeRequestContext =
                dynamic_cast<TResizeRequestContext*>(&*requestContext);
            if (resizeRequestContext) {
                FinishRequest(*resizeRequestContext, ok);
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
            STORAGE_WARN("Will retry error: "
                << FormatError(requestContext.Response.GetError()));

            if (Config.UnixSocketPath) {
                NeedConnect = true;
            }

            Retry(requestContext);
        } else {
            if (HasError(requestContext.Response.GetError())) {
                STORAGE_WARN("Won't retry error: "
                    << FormatError(requestContext.Response.GetError()));
            }

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
