#include "client.h"

#include "config.h"
#include "probes.h"

#include <cloud/filestore/public/api/grpc/service.grpc.pb.h>

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/endpoint.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/completion.h>
#include <cloud/storage/core/libs/grpc/executor.h>
#include <cloud/storage/core/libs/grpc/channel_arguments.h>
#include <cloud/storage/core/libs/grpc/credentials.h>
#include <cloud/storage/core/libs/grpc/time_point_specialization.h>
#include <cloud/storage/core/libs/uds/uds_socket_client.h>

#include <contrib/ydb/library/actors/prof/tag.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel_posix.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>
#include <contrib/libs/grpc/include/grpcpp/support/status.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

namespace NCloud::NFileStore::NClient {

using namespace NCloud::NStorage::NClient;
using namespace NThreading;

LWTRACE_USING(FILESTORE_CLIENT_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

const char AUTH_HEADER[] = "authorization";
const char AUTH_METHOD[] = "Bearer";

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeGrpcError(const grpc::Status& status)
{
    NProto::TError error;
    if (!status.ok()) {
        error.SetCode(MAKE_GRPC_ERROR(status.error_code()));
        error.SetMessage(TString(status.error_message()));
    }
    return error;
}

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_METHOD(name, proto, method, ...)                     \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr auto RequestName = TStringBuf(#method);              \
                                                                               \
        using TRequest = NProto::T##proto##Request;                            \
        using TResponse = NProto::T##proto##Response;                          \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static auto Execute(T& service, TArgs&& ...args)                       \
        {                                                                      \
            return service.Async##method(std::forward<TArgs>(args)...);        \
        }                                                                      \
    };                                                                         \
// FILESTORE_DECLARE_METHOD

#define FILESTORE_DECLARE_METHOD_FS(name, ...) \
    FILESTORE_DECLARE_METHOD(name##Fs, name, name, __VA_ARGS__)

#define FILESTORE_DECLARE_METHOD_VHOST(name, ...) \
    FILESTORE_DECLARE_METHOD(name##Vhost, name, name, __VA_ARGS__)

#define FILESTORE_DECLARE_METHOD_STREAM(name, ...) \
    FILESTORE_DECLARE_METHOD(name##Stream, name, name##Stream, __VA_ARGS__)

FILESTORE_SERVICE(FILESTORE_DECLARE_METHOD_FS)
FILESTORE_ENDPOINT_SERVICE(FILESTORE_DECLARE_METHOD_VHOST)
FILESTORE_DECLARE_METHOD_STREAM(GetSessionEvents)

#undef FILESTORE_DECLARE_METHOD
#undef FILESTORE_DECLARE_METHOD_FS
#undef FILESTORE_DECLARE_METHOD_VHOST
#undef FILESTORE_DECLARE_METHOD_STREAM

////////////////////////////////////////////////////////////////////////////////

struct TAppContext
{
    const TClientConfigPtr Config;

    TLog Log;
    TAtomic ShouldStop = 0;

    TAppContext(TClientConfigPtr config)
        : Config(std::move(config))
    {}
};

struct TFileStoreContext : TAppContext
{
    std::shared_ptr<NProto::TFileStoreService::Stub> Service;

    TFileStoreContext(TClientConfigPtr config)
        : TAppContext(std::move(config))
    {}
};

struct TEndpointManagerContext : TAppContext
{
    std::shared_ptr<NProto::TEndpointManagerService::Stub> Service;

    TEndpointManagerContext(TClientConfigPtr config)
        : TAppContext(std::move(config))
    {}
};

////////////////////////////////////////////////////////////////////////////////

using TClientRequestsInFlight = NStorage::NGrpc::TRequestsInFlight<
    NStorage::NGrpc::TRequestHandlerBase>;

using TExecutorContext = NStorage::NGrpc::
    TExecutorContext<grpc::CompletionQueue, TClientRequestsInFlight>;
using TExecutor = NStorage::NGrpc::
    TExecutor<grpc::CompletionQueue, TClientRequestsInFlight>;

template <typename TAppContext, typename TMethod>
class TRequestHandler final
    : public NStorage::NGrpc::TRequestHandlerBase
{
    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

private:
    TAppContext& AppCtx;
    TExecutorContext& ExecutorCtx;

    grpc::ClientContext Context;
    std::unique_ptr<grpc::ClientAsyncResponseReader<TResponse>> Reader;

    TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    ui64 RequestId = 0;
    TPromise<TResponse> Promise;

    TResponse Response;
    grpc::Status Status;

    enum {
        WaitingForRequest = 0,
        SendingRequest = 1,
        RequestCompleted = 2,
    };
    TAtomic RequestState = WaitingForRequest;

public:
    TRequestHandler(
            TAppContext& appCtx,
            TExecutorContext& executorCtx,
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            const TPromise<TResponse>& promise)
        : AppCtx(appCtx)
        , ExecutorCtx(executorCtx)
        , CallContext(std::move(callContext))
        , Request(std::move(request))
        , Promise(promise)
    {}

    static void Start(
        TAppContext& appCtx,
        TExecutorContext& executorCtx,
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request,
        TPromise<TResponse>& promise)
    {
        auto handler = std::make_unique<TRequestHandler<TAppContext, TMethod>>(
            appCtx,
            executorCtx,
            std::move(callContext),
            std::move(request),
            promise);

        handler = executorCtx.EnqueueRequestHandler(std::move(handler));

        if (handler) {
            handler->Status = grpc::Status::CANCELLED;
            handler->HandleResponse();
        }
    }

    void Process(bool ok) override
    {
        Y_UNUSED(ok);

        if (AtomicGet(AppCtx.ShouldStop)) {
            AtomicSet(RequestState, RequestCompleted);

            Response.Clear();
            Status = grpc::Status::CANCELLED;
        }

        for (;;) {
            switch (AtomicGet(RequestState)) {
                case WaitingForRequest:
                    if (AtomicCas(&RequestState, SendingRequest, WaitingForRequest)) {
                        PrepareRequestContext();
                        SendRequest();

                        // request is in progress now
                        return;
                    }
                    break;

                case SendingRequest:
                    if (AtomicCas(&RequestState, RequestCompleted, SendingRequest)) {
                    }
                    break;

                case RequestCompleted:
                    HandleResponse();
                    CompleteRequest();

                    // request completed and could be safely destroyed
                    ExecutorCtx.RequestsInFlight.Unregister(this);
                    return;
            }
        }
    }

    void Cancel() override
    {
        Context.TryCancel();
    }

private:
    void PrepareRequestContext()
    {
        auto& headers = *Request->MutableHeaders();

        auto now = TInstant::Now();
        auto timestamp = TInstant::MicroSeconds(headers.GetTimestamp());
        if (!timestamp || timestamp > now || now - timestamp > TDuration::Seconds(1)) {
            // fix request timestamp
            timestamp = now;
            headers.SetTimestamp(timestamp.MicroSeconds());
        }

        auto requestTimeout = TDuration::MilliSeconds(headers.GetRequestTimeout());
        if (!requestTimeout) {
            requestTimeout = AppCtx.Config->GetRequestTimeout();
            headers.SetRequestTimeout(requestTimeout.MilliSeconds());
        }

        RequestId = CallContext->RequestId;
        if (!RequestId) {
            RequestId = CreateRequestId();
            headers.SetRequestId(RequestId);
        }

        Context.set_deadline(now + requestTimeout);

        if (const auto& authToken = AppCtx.Config->GetAuthToken()) {
            Context.AddMetadata(
                AUTH_HEADER,
                TStringBuilder() << AUTH_METHOD << " " << authToken);
        }
    }

    void SendRequest()
    {
        auto& Log = AppCtx.Log;

        STORAGE_TRACE(TMethod::RequestName
            << " #" << RequestId
            << " send request: " << DumpMessage(*Request));

        FILESTORE_TRACK(
            SendRequest,
            CallContext,
            TString(TMethod::RequestName));

        Reader = TMethod::Execute(
            *AppCtx.Service,
            &Context,
            *Request,
            ExecutorCtx.CompletionQueue.get());

        // no more need Request; try to free memory
        Request.reset();

        Reader->Finish(&Response, &Status, AcquireCompletionTag());
    }

    void HandleResponse()
    {
        auto& Log = AppCtx.Log;

        if (!Status.ok()) {
            *Response.MutableError() = MakeGrpcError(Status);
        }

        STORAGE_TRACE(TMethod::RequestName
            << " #" << RequestId
            << " response received: " << DumpMessage(Response));

        FILESTORE_TRACK(
            ResponseReceived,
            CallContext,
            TString(TMethod::RequestName));

        try {
            Promise.SetValue(std::move(Response));
        } catch (...) {
            STORAGE_ERROR(TMethod::RequestName
                << " #" << RequestId
                << " exception in callback: "
                << CurrentExceptionMessage());
        }
    }

    void CompleteRequest()
    {
        const ui64 now = GetCycleCount();
        const auto ts = CallContext->CalcRequestTime(now);

        auto& Log = AppCtx.Log;

        STORAGE_TRACE(TMethod::RequestName
            << " #" << RequestId
            << " request completed");

        FILESTORE_TRACK(
            RequestCompleted,
            CallContext,
            TString(TMethod::RequestName),
            ts.TotalTime.MicroSeconds(),
            ts.ExecutionTime.MicroSeconds());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TAppContext, typename TMethod>
class TStreamRequestHandler final
    : public NStorage::NGrpc::TRequestHandlerBase
{
    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

private:
    TAppContext& AppCtx;
    TExecutorContext& ExecutorCtx;

    grpc::ClientContext Context;
    std::unique_ptr<grpc::ClientAsyncReader<TResponse>> Reader;

    TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    IResponseHandlerPtr<TResponse> ResponseHandler;

    TResponse Response;
    grpc::Status Status;

    enum {
        WaitingForRequest = 0,
        SendingRequest = 1,
        WaitingForResponse = 2,
        WaitingForCompletion = 3,
        RequestCompleted = 4,
    };
    TAtomic RequestState = WaitingForRequest;

public:
    TStreamRequestHandler(
            TAppContext& appCtx,
            TExecutorContext& executorCtx,
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            IResponseHandlerPtr<TResponse> responseHandler)
        : AppCtx(appCtx)
        , ExecutorCtx(executorCtx)
        , CallContext(std::move(callContext))
        , Request(std::move(request))
        , ResponseHandler(std::move(responseHandler))
    {}

    static void Start(
        TAppContext& appCtx,
        TExecutorContext& executorCtx,
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request,
        IResponseHandlerPtr<TResponse> responseHandler)
    {
        auto handler = std::make_unique<TStreamRequestHandler<TAppContext, TMethod>>(
            appCtx,
            executorCtx,
            std::move(callContext),
            std::move(request),
            std::move(responseHandler));

        handler = executorCtx.EnqueueRequestHandler(std::move(handler));

        if (handler) {
            handler->Status = grpc::Status::CANCELLED;
            handler->HandleCompletion();
        }
    }

    void Process(bool ok) override
    {
        if (AtomicGet(AppCtx.ShouldStop)) {
            AtomicSet(RequestState, RequestCompleted);

            Response.Clear();
            Status = grpc::Status::CANCELLED;
        }

        for (;;) {
            switch (AtomicGet(RequestState)) {
                case WaitingForRequest:
                    if (AtomicCas(&RequestState, SendingRequest, WaitingForRequest)) {
                        SendRequest();

                        // request is in progress now
                        return;
                    }
                    break;

                case SendingRequest:
                    if (AtomicCas(&RequestState, WaitingForResponse, SendingRequest)) {
                        ReadResponse();

                        // request is in progress now
                        return;
                    }
                    break;

                case WaitingForResponse:
                    if (ok) {
                        HandleResponse();
                        ReadResponse();

                        // request is in progress now
                        return;
                    }
                    if (AtomicCas(&RequestState, WaitingForCompletion, WaitingForResponse)) {
                        ReadCompletion();

                        // request is in progress now
                        return;
                    }
                    break;

                case WaitingForCompletion:
                    if (AtomicCas(&RequestState, RequestCompleted, WaitingForCompletion)) {
                    }
                    break;

                case RequestCompleted:
                    HandleCompletion();

                    // request completed and could be safely destroyed
                    ExecutorCtx.RequestsInFlight.Unregister(this);
                    return;
            }
        }
    }

    void Cancel() override
    {
        Context.TryCancel();
    }

private:
    void SendRequest()
    {
        auto& Log = AppCtx.Log;

        STORAGE_TRACE(TMethod::RequestName
            << " send request: " << DumpMessage(*Request));

        Reader = TMethod::Execute(
            *AppCtx.Service,
            &Context,
            *Request,
            ExecutorCtx.CompletionQueue.get(),
            AcquireCompletionTag());

        // no more need Request; try to free memory
        Request.reset();
    }

    void ReadResponse()
    {
        auto& Log = AppCtx.Log;

        STORAGE_TRACE(TMethod::RequestName << " read response");

        Reader->Read(&Response, AcquireCompletionTag());
    }

    void ReadCompletion()
    {
        auto& Log = AppCtx.Log;

        STORAGE_TRACE(TMethod::RequestName << " read completion");

        Reader->Finish(&Status, AcquireCompletionTag());
    }

    void HandleResponse()
    {
        auto& Log = AppCtx.Log;

        STORAGE_TRACE(TMethod::RequestName
            << " response received: " << DumpMessage(Response));

        try {
            ResponseHandler->HandleResponse(Response);
        } catch (...) {
            STORAGE_ERROR(TMethod::RequestName
                << " exception in callback: " << CurrentExceptionMessage());
        }
    }

    void HandleCompletion()
    {
        auto& Log = AppCtx.Log;

        auto error = MakeGrpcError(Status);
        STORAGE_TRACE(TMethod::RequestName
            << " completion received: " << FormatError(error));

        try {
            ResponseHandler->HandleCompletion(error);
        } catch (...) {
            STORAGE_ERROR(TMethod::RequestName
                << " exception in callback: " << CurrentExceptionMessage());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TClient>
class TUdsClient
    : public TUdsSocketClient<TClient, TCallContextPtr>
{
public:
    using TBase = TUdsSocketClient<TClient, TCallContextPtr>;
    using TBase::TBase;

protected:
    template <typename TMethod>
    void ExecuteStreamRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request,
        IResponseHandlerPtr<typename TMethod::TResponse> responseHandler)
    {
        if (!TBase::IncInflightCounter()) {
            TBase::Connect();
            responseHandler->HandleResponse(
                TErrorResponse(E_GRPC_UNAVAILABLE, "Broken pipe"));
            return;
        }

        TBase::template ExecuteStreamRequest<TMethod>(
            std::move(callContext),
            std::move(request),
            std::move(responseHandler));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TAppContext, typename TClient>
class TClientBase
    : public TClient
{
protected:
    const ILoggingServicePtr Logging;

    TAppContext AppCtx;
    TVector<std::unique_ptr<TExecutor>> Executors;

public:
    TClientBase(TClientConfigPtr config, ILoggingServicePtr logging)
        : Logging(std::move(logging))
        , AppCtx(std::move(config))
    {}

    ~TClientBase()
    {
        Stop();
    }

    void Start() override
    {
        AppCtx.Log = Logging->CreateLog("NFS_CLIENT");
        StartClient();
    }

    void Stop() override
    {
        auto& Log = AppCtx.Log;

        if (AtomicSwap(&AppCtx.ShouldStop, 1) == 1) {
            return;
        }

        STORAGE_INFO("Shutting down");

        for (auto& executor: Executors) {
            executor->Shutdown();
        }

        AppCtx.Service.reset();
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto promise = NewPromise<typename TMethod::TResponse>();

        TRequestHandler<TAppContext, TMethod>::Start(
            AppCtx,
            PickExecutor(),
            std::move(callContext),
            std::move(request),
            promise);

        return promise.GetFuture();
    }

    template <typename TMethod>
    void ExecuteStreamRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request,
        IResponseHandlerPtr<typename TMethod::TResponse> responseHandler)
    {
        TStreamRequestHandler<TAppContext, TMethod>::Start(
            AppCtx,
            PickExecutor(),
            std::move(callContext),
            std::move(request),
            std::move(responseHandler));
    }

protected:
    virtual void InitService(std::shared_ptr<::grpc::Channel> channel) = 0;

    bool StartWithUds(const TString& unixSocketPath)
    {
        auto channel = ConfigureUnixSocketChannel(unixSocketPath);

        if (!channel) {
            return false;
        }

        InitService(std::move(channel));
        return true;
    }

    bool StartWithTcpSocket()
    {
        auto channel = ConfigureTcpSocketChannel();

        if (!channel) {
            return false;
        }

        InitService(std::move(channel));
        return true;
    }

    void StartClient()
    {
        auto& config = AppCtx.Config;

        bool res = config->GetUnixSocketPath() ?
            StartWithUds(config->GetUnixSocketPath()) :
            StartWithTcpSocket();

        if (!res) {
            ythrow TServiceError(E_FAIL)
                << "could not start gRPC client";
        }

        ui32 threadsCount = AppCtx.Config->GetThreadsCount();
        for (size_t i = 1; i <= threadsCount; ++i) {
            auto executor = std::make_unique<TExecutor>(
                TStringBuilder() << "CLI" << i,
                std::make_unique<grpc::CompletionQueue>(),
                AppCtx.Log);

            executor->Start();
            Executors.push_back(std::move(executor));
        }
    }

    TExecutor& PickExecutor()
    {
        size_t index = 0;
        if (Executors.size() > 1) {
            // pick random executor
            index = RandomNumber(Executors.size());
        }
        return *Executors[index];
    }

    grpc::ChannelArguments CreateChannelArguments()
    {
        return NStorage::NGrpc::CreateChannelArguments(*AppCtx.Config);
    }

    std::shared_ptr<grpc::Channel> ConfigureTcpSocketChannel()
    {
        auto& Log = AppCtx.Log;
        auto& config = AppCtx.Config;

        if (config->GetSecurePort() == 0 && config->GetPort() == 0) {
            ythrow TServiceError(E_ARGUMENT)
                << "gRPC client ports are not set";
        }

        bool secureEndpoint = config->GetSecurePort() != 0;
        auto address = Join(":", config->GetHost(),
            secureEndpoint ? config->GetSecurePort() : config->GetPort());

        auto credentials = CreateTcpClientChannelCredentials(
            secureEndpoint,
            *config);

        STORAGE_INFO("Connect to " << address);

        return CreateCustomChannel(
            std::move(address),
            std::move(credentials),
            CreateChannelArguments());
    }

    std::shared_ptr<grpc::Channel> ConfigureUnixSocketChannel(
        const TString& unixSocketPath)
    {
        auto& Log = AppCtx.Log;

        STORAGE_INFO("Connect to " << unixSocketPath.Quote() << " socket");

        TSockAddrLocal addr(unixSocketPath.c_str());

        TLocalStreamSocket socket;
        if (socket.Connect(&addr) < 0) {
            return nullptr;
        }

        auto channel = grpc::CreateCustomInsecureChannelFromFd(
            "localhost",
            socket,
            CreateChannelArguments());

        socket.Release();   // ownership transferred to Channel
        return channel;
    }
};

////////////////////////////////////////////////////////////////////////////////

using TUdsFileStoreClientBase = TUdsClient<
    TClientBase<TFileStoreContext, IFileStoreService>
    >;

////////////////////////////////////////////////////////////////////////////////

template <typename TBase>
class TFileStoreClient final
    : public TBase
{
public:
    using TBase::TBase;

    void InitService(std::shared_ptr<::grpc::Channel> channel) override
    {
        TBase::AppCtx.Service = NProto::TFileStoreService::NewStub(std::move(channel));
    }

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return TBase::template ExecuteRequest<T##name##Fs##Method>(           \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD

    void GetSessionEventsStream(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetSessionEventsRequest> request,
        IResponseHandlerPtr<NProto::TGetSessionEventsResponse> responseHandler) override
    {
        TBase::template ExecuteStreamRequest<TGetSessionEventsStreamMethod>(
            std::move(callContext),
            std::move(request),
            std::move(responseHandler));
    }

    TFuture<NProto::TReadDataLocalResponse> ReadDataLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataLocalRequest> request) override
    {
        return TBase::template ExecuteRequest<TReadDataFsMethod>(
            std::move(callContext),
            std::move(request)).Apply([](TFuture<NProto::TReadDataResponse> f) {
                NProto::TReadDataLocalResponse response(f.ExtractValue());
                return response;
            });
    }

    TFuture<NProto::TWriteDataLocalResponse> WriteDataLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataLocalRequest> request) override
    {
        return TBase::template ExecuteRequest<TWriteDataFsMethod>(
            std::move(callContext),
            std::move(request));
    }
};

using TUdsFileStoreClient = TFileStoreClient<TUdsFileStoreClientBase>;
using TTcpFileStoreClient = TFileStoreClient<TClientBase<TFileStoreContext, IFileStoreService>>;

////////////////////////////////////////////////////////////////////////////////

using TUdsEndpointManagerClientBase = TUdsClient<
    TClientBase<TEndpointManagerContext, IEndpointManager>
    >;

////////////////////////////////////////////////////////////////////////////////

template <typename TBase>
class TEndpointManagerClient final
    : public TBase
{
public:
    using TBase::TBase;

    void Drain() override
    {}

    NThreading::TFuture<void> RestoreEndpoints() override
    {
        return MakeFuture();
    }

    void InitService(std::shared_ptr<::grpc::Channel> channel) override
    {
        TBase::AppCtx.Service = NProto::TEndpointManagerService::NewStub(std::move(channel));
    }

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return TBase::template ExecuteRequest<T##name##Vhost##Method>(         \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD
};

using TUdsEndpointManagerClient= TEndpointManagerClient<TUdsEndpointManagerClientBase>;
using TTcpEndpointManagerClient = TEndpointManagerClient<TClientBase<TEndpointManagerContext, IEndpointManager>>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateFileStoreClient(
    TClientConfigPtr config,
    ILoggingServicePtr logging)
{
    if (config->GetUnixSocketPath()) {
        auto client = std::make_shared<TUdsFileStoreClient>(
            config->GetUnixSocketPath(),
            std::move(config),
            std::move(logging));
        client->Connect();
        return client;
    } else {
        return std::make_shared<TTcpFileStoreClient>(
            std::move(config),
            std::move(logging));
    }
}

IEndpointManagerPtr CreateEndpointManagerClient(
    TClientConfigPtr config,
    ILoggingServicePtr logging)
{
    if (config->GetUnixSocketPath()) {
        auto client = std::make_shared<TUdsEndpointManagerClient>(
            config->GetUnixSocketPath(),
            std::move(config),
            std::move(logging));
        client->Connect();
        return client;
    } else {
        return std::make_shared<TTcpEndpointManagerClient>(
            std::move(config),
            std::move(logging));
    }
}

}   // namespace NCloud::NFileStore::NClient
