#include "client.h"

#include "config.h"

#include <cloud/blockstore/public/api/grpc/service.grpc.pb.h>

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/verify.h>

#include <cloud/storage/core/libs/diagnostics/executor_counters.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <cloud/storage/core/libs/grpc/completion.h>
#include <cloud/storage/core/libs/grpc/initializer.h>
#include <cloud/storage/core/libs/grpc/time.h>

#include <cloud/storage/core/libs/requests/executor.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel_posix.h>
#include <contrib/libs/grpc/include/grpcpp/resource_quota.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>
#include <contrib/libs/grpc/include/grpcpp/security/tls_credentials_options.h>
#include <contrib/libs/grpc/include/grpcpp/support/status.h>

#include <util/datetime/cputimer.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/network/sock.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/stream/str.h>
#include <util/string/join.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

namespace NCloud::NBlockStore::NClient {

using namespace NMonitoring;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

const char AUTH_HEADER[] = "authorization";
const char AUTH_METHOD[] = "Bearer";

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& fileName)
{
    TFileInput in(fileName);
    return in.ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError CompleteReadBlocksLocalRequest(
    NProto::TReadBlocksLocalRequest& request,
    const NProto::TReadBlocksResponse& response)
{
    if (request.BlockSize == 0) {
        return TErrorResponse(E_ARGUMENT, "block size is zero");
    }

    auto sgListOrError = GetSgList(response, request.BlockSize);
    if (HasError(sgListOrError)) {
        return sgListOrError.GetError();
    }

    auto src = sgListOrError.ExtractResult();
    size_t srcSize = SgListGetSize(src);

    size_t expectedSize = request.GetBlocksCount() * request.BlockSize;
    if (srcSize != expectedSize) {
        return TErrorResponse(E_ARGUMENT, TStringBuilder()
            << "invalid response size (expected: " << expectedSize
            << ", actual: " << srcSize << ")");
    }

    auto guard = request.Sglist.Acquire();
    if (!guard) {
        return TErrorResponse(
            E_CANCELLED,
            "failed to acquire sglist in GrpcClient");
    }
    const auto& dst = guard.Get();
    auto dstSize = SgListGetSize(dst);

    if (dstSize < srcSize) {
        return TErrorResponse(E_ARGUMENT, TStringBuilder()
            << "invalid buffer size (expected: " << srcSize
            << ", actual: " << dstSize << ")");
    }

    size_t bytesRead = SgListCopy(src, dst);
    STORAGE_VERIFY(
        bytesRead == expectedSize,
        TWellKnownEntityTypes::DISK,
        GetDiskId(request));

    return {};
}

using TWriteBlocksRequestPtr = std::shared_ptr<NProto::TWriteBlocksRequest>;

TResultOrError<TWriteBlocksRequestPtr> CreateWriteBlocksRequest(
    const NProto::TWriteBlocksLocalRequest& localRequest)
{
    if (localRequest.BlockSize == 0) {
        return TErrorResponse(E_ARGUMENT, "block size is zero");
    }

    auto guard = localRequest.Sglist.Acquire();
    if (!guard) {
        return TErrorResponse(
            E_CANCELLED,
            "failed to acquire sglist in GrpcClient");
    }
    const auto& src = guard.Get();
    auto srcSize = SgListGetSize(src);

    size_t expectedSize = localRequest.BlocksCount * localRequest.BlockSize;
    if (srcSize < expectedSize) {
        return TErrorResponse(E_ARGUMENT, TStringBuilder()
            << "invalid buffer size (expected: " << expectedSize
            << ", actual: " << srcSize << ")");
    }

    auto request = std::make_shared<NProto::TWriteBlocksRequest>(localRequest);

    auto dst = ResizeIOVector(
        *request->MutableBlocks(),
        localRequest.BlocksCount,
        localRequest.BlockSize);

    size_t bytesWritten = SgListCopy(src, dst);
    STORAGE_VERIFY(
        bytesWritten == expectedSize,
        TWellKnownEntityTypes::DISK,
        GetDiskId(localRequest));

    return request;
}

////////////////////////////////////////////////////////////////////////////////

struct TClientRequestHandlerBase
    : public NStorage::NRequests::TRequestHandlerBase
{
    const EBlockStoreRequest RequestType;
    ui64 RequestId = 0;
    TString DiskId;

    enum {
        WaitingForRequest = 0,
        SendingRequest = 1,
        RequestCompleted = 2,
    };

    TAtomic RequestState = WaitingForRequest;

    NProto::TError Error;

    TClientRequestHandlerBase(EBlockStoreRequest requestType)
        : RequestType(requestType)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TRequestsInFlight
{
public:
    using TRequestHandler = TClientRequestHandlerBase;

private:
    THashSet<TRequestHandler*> Requests;
    TAdaptiveLock RequestsLock;
    bool ShouldStop = false;

public:
    bool Register(TRequestHandler* handler)
    {
        with_lock (RequestsLock) {
            if (ShouldStop) {
                return false;
            }

            auto res = Requests.emplace(handler);
            STORAGE_VERIFY(
                res.second,
                TWellKnownEntityTypes::DISK,
                handler->DiskId);
        }

        return true;
    }

    void Unregister(TRequestHandler* handler)
    {
        with_lock (RequestsLock) {
            auto it = Requests.find(handler);
            STORAGE_VERIFY(
                it != Requests.end(),
                TWellKnownEntityTypes::DISK,
                handler->DiskId);

            Requests.erase(it);
        }
    }

    void Shutdown()
    {
        with_lock (RequestsLock) {
            ShouldStop = true;
            for (auto* handler: Requests) {
                handler->Cancel();
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TAppContext
{
    TClientAppConfigPtr Config;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    TLog Log;

    IMonitoringServicePtr Monitoring;
    IServerStatsPtr ClientStats;

    TAtomic ShouldStop = 0;
};

////////////////////////////////////////////////////////////////////////////////

using TExecutorContext = NStorage::NRequests::
    TExecutorContext<grpc::CompletionQueue, TRequestsInFlight>;
using TExecutor = NStorage::NRequests::TExecutor<
    grpc::CompletionQueue,
    TRequestsInFlight,
    TExecutorCounters::TExecutorScope>;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr EBlockStoreRequest Request = EBlockStoreRequest::name;\
                                                                               \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static auto Prepare(T& service, TArgs&& ...args)                       \
        {                                                                      \
            return service.Async##name(std::forward<TArgs>(args)...);          \
        }                                                                      \
    };
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_GRPC_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
    };
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_LOCAL_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

template <typename TService, typename TMethod>
class TRequestHandler final
    : public TClientRequestHandlerBase
{
    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

private:
    TAppContext& AppCtx;
    TExecutorContext& ExecCtx;
    TService& Service;

    grpc::ClientContext Context;
    std::unique_ptr<grpc::ClientAsyncResponseReader<TResponse>> Reader;

    TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    TPromise<TResponse> Promise;

    TResponse Response;
    grpc::Status Status;

public:
    TRequestHandler(
            TAppContext& appCtx,
            TExecutorContext& execCtx,
            TService& service,
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            const TPromise<TResponse>& promise)
        : TClientRequestHandlerBase(TMethod::Request)
        , AppCtx(appCtx)
        , ExecCtx(execCtx)
        , Service(service)
        , CallContext(std::move(callContext))
        , Request(std::move(request))
        , Promise(promise)
    {}

    static void Start(
        TAppContext& appCtx,
        TExecutorContext& execCtx,
        TService& service,
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request,
        TPromise<TResponse>& promise)
    {
        auto handler = std::make_unique<TRequestHandler<TService, TMethod>>(
            appCtx,
            execCtx,
            service,
            std::move(callContext),
            std::move(request),
            promise);

        handler = execCtx.EnqueueRequestHandler(std::move(handler));

        if (handler) {
            handler->ReportError(grpc::Status::CANCELLED);
        }
    }

    void Process(bool ok) override
    {
        if (!ok || AtomicGet(AppCtx.ShouldStop)) {
            auto prevState = AtomicSwap(&RequestState, RequestCompleted);
            Y_UNUSED(prevState);

            Response.Clear();
            Status = grpc::Status::CANCELLED;

            ProcessResponse();
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
                        ProcessResponse();
                    }
                    break;

                case RequestCompleted:
                    // request completed and could be safely destroyed
                    ExecCtx.RequestsInFlight.Unregister(this);
                    return;
            }
        }
    }

    void Cancel() override
    {
        ReportError(grpc::Status::CANCELLED);
        Context.TryCancel();
    }

private:
    void PrepareRequestContext()
    {
        auto& headers = *Request->MutableHeaders();
        headers.SetClientId(AppCtx.Config->GetClientId());

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

        RequestId = headers.GetRequestId();
        if (!RequestId) {
            RequestId = CreateRequestId();
            headers.SetRequestId(RequestId);
        }

        DiskId = GetDiskId(*Request);

        Context.set_deadline(now + requestTimeout);
        if (const auto& authToken = AppCtx.Config->GetAuthToken()) {
            Context.AddMetadata(
                AUTH_HEADER,
                TStringBuilder() << AUTH_METHOD << " " << authToken);
        }
    }

    void SendRequest()
    {
        Reader = TMethod::Prepare(
            Service,
            &Context,
            *Request,
            ExecCtx.CompletionQueue.get());

        // no more need Request; try to free memory
        Request.reset();

        Reader->Finish(&Response, &Status, AcquireCompletionTag());
    }

    void ProcessResponse()
    {
        if (!Status.ok() && !Response.HasError()) {
            auto& error = *Response.MutableError();
            error.SetCode(MAKE_GRPC_ERROR(Status.error_code()));
            error.SetMessage(ToString(Status.error_message()));
        }

        if (Response.HasError()) {
            Error = Response.GetError();
        }

        OnRequestCompletion(Response);

        try {
            Promise.SetValue(std::move(Response));
        } catch (...) {
            AppCtx.ClientStats->ReportException(
                AppCtx.Log,
                RequestType,
                RequestId,
                DiskId,
                AppCtx.Config->GetClientId());
        }
    }

    void ReportError(const grpc::Status& status)
    {
        auto& error = *Response.MutableError();
        error.SetCode(MAKE_GRPC_ERROR(status.error_code()));
        error.SetMessage(ToString(status.error_message()));

        try {
            Promise.SetValue(std::move(Response));
        } catch (...) {
            AppCtx.ClientStats->ReportException(
                AppCtx.Log,
                RequestType,
                RequestId,
                DiskId,
                AppCtx.Config->GetClientId());
        }
    }

    template <typename T>
    void OnRequestCompletion(const T& response)
    {
        Y_UNUSED(response);
    }

    void OnRequestCompletion(const NProto::TMountVolumeResponse& response)
    {
        if (!HasError(response) && response.HasVolume()) {
            AppCtx.ClientStats->MountVolume(
                response.GetVolume(),
                AppCtx.Config->GetClientId(),
                AppCtx.Config->GetInstanceId());
        }
    }

    void OnRequestCompletion(const NProto::TUnmountVolumeResponse& response)
    {
        if (!HasError(response)) {
            AppCtx.ClientStats->UnmountVolume(
                DiskId,
                AppCtx.Config->GetClientId());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public TAppContext
    , public IClient
    , public std::enable_shared_from_this<TClient>
{
private:
    TGrpcInitializer GrpcInitializer;

    IBlockStorePtr ControlEndpoint;
    IBlockStorePtr DataEndpoint;

    TVector<std::unique_ptr<TExecutor>> Executors;

    TAdaptiveLock EndpointLock;

public:
    TClient(
        TClientAppConfigPtr config,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IServerStatsPtr clientStats);

    ~TClient()
    {
        Stop();
    }

    void Start() override;
    void Stop() override;

    IBlockStorePtr CreateEndpoint() override;

    IBlockStorePtr CreateDataEndpoint() override;

    IBlockStorePtr CreateDataEndpoint(
        const TString& socketPath) override;

    template <typename TMethod, typename TService>
    TFuture<typename TMethod::TResponse> ExecuteRequest(
        TService& service,
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request);

    grpc::ChannelArguments CreateChannelArguments();

    std::shared_ptr<grpc::Channel> CreateTcpSocketChannel(
        const TString& address,
        bool secureEndpoint,
        const grpc::ChannelArguments& args);

    std::shared_ptr<grpc::Channel> CreateUnixSocketChannel(
        const TString& unixSocketPath,
        const grpc::ChannelArguments& args);

private:
    bool InitControlEndpoint();
    bool InitDataEndpoint();

    void UploadStats();

    void ScheduleUploadStats();
};

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
    TClientAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr clientStats)
{
    Config = std::move(config);
    Timer = std::move(timer);
    Scheduler = std::move(scheduler);
    Log = logging->CreateLog("BLOCKSTORE_CLIENT");
    Monitoring = std::move(monitoring);
    ClientStats = std::move(clientStats);

    Y_VERIFY(Config->GetClientId());
}

void TClient::Start()
{
    ui32 threadsCount = Config->GetThreadsCount();
    for (size_t i = 1; i <= threadsCount; ++i) {
        auto executor = std::make_unique<TExecutor>(
            TStringBuilder() << "CLI" << i,
            std::make_unique<grpc::CompletionQueue>(),
            Log,
            ClientStats->StartExecutor());
        executor->Start();
        Executors.push_back(std::move(executor));
    }

    // init any service for UploadClientMetrics
    with_lock (EndpointLock) {
        bool anyEndpoint = InitDataEndpoint();
        if (!DataEndpoint) {
            anyEndpoint |= InitControlEndpoint();
        }
        STORAGE_VERIFY(
            anyEndpoint,
            TWellKnownEntityTypes::CLIENT,
            Config->GetClientId());
    }

    ScheduleUploadStats();
}

void TClient::Stop()
{
    if (AtomicSwap(&ShouldStop, 1) == 1) {
        return;
    }

    STORAGE_INFO("Shutting down");

    for (auto& executor: Executors) {
        executor->Shutdown();
    }

    with_lock (EndpointLock) {
        ControlEndpoint.reset();
        DataEndpoint.reset();
    }
}

grpc::ChannelArguments TClient::CreateChannelArguments()
{
    grpc::ChannelArguments args;

    ui32 maxMessageSize = Config->GetMaxMessageSize();
    if (maxMessageSize) {
        args.SetMaxSendMessageSize(maxMessageSize);
        args.SetMaxReceiveMessageSize(maxMessageSize);
    }

    ui32 memoryQuotaBytes = Config->GetMemoryQuotaBytes();
    if (memoryQuotaBytes) {
        grpc::ResourceQuota quota("memory_bound");
        quota.Resize(memoryQuotaBytes);

        args.SetResourceQuota(quota);
    }

    if (auto backoff = Config->GetGrpcReconnectBackoff()) {
        args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, backoff.MilliSeconds());
        args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, backoff.MilliSeconds());
        args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, backoff.MilliSeconds());
    }

    return args;
}

std::shared_ptr<grpc::Channel> TClient::CreateTcpSocketChannel(
    const TString& address,
    bool secureEndpoint,
    const grpc::ChannelArguments& args)
{
    std::shared_ptr<grpc::ChannelCredentials> credentials;
    if (!secureEndpoint) {
        credentials = grpc::InsecureChannelCredentials();
    } else if (Config->GetSkipCertVerification()) {
        grpc::experimental::TlsChannelCredentialsOptions tlsOptions;
        tlsOptions.set_verify_server_certs(false);
        credentials = grpc::experimental::TlsCredentials(tlsOptions);
    } else {
        grpc::SslCredentialsOptions sslOptions;

        if (const auto& rootCertsFile = Config->GetRootCertsFile()) {
            sslOptions.pem_root_certs = ReadFile(rootCertsFile);
        }

        if (const auto& certFile = Config->GetCertFile()) {
            sslOptions.pem_cert_chain = ReadFile(certFile);
            sslOptions.pem_private_key = ReadFile(Config->GetCertPrivateKeyFile());
        }

        credentials = grpc::SslCredentials(sslOptions);
    }

    STORAGE_INFO("Connect to " << address);

    return CreateCustomChannel(
        address,
        credentials,
        args);
}

std::shared_ptr<grpc::Channel> TClient::CreateUnixSocketChannel(
    const TString& unixSocketPath,
    const grpc::ChannelArguments& args)
{
    STORAGE_INFO("Connect to " << unixSocketPath.Quote() << " socket");

    TSockAddrLocal addr(unixSocketPath.c_str());

    TLocalStreamSocket socket;
    if (socket.Connect(&addr) < 0) {
        return nullptr;
    }

    auto channel = grpc::CreateCustomInsecureChannelFromFd(
        "localhost",
        socket,
        args);

    socket.Release();   // ownership transferred to Channel
    return channel;
}

template <typename TMethod, typename TService>
TFuture<typename TMethod::TResponse> TClient::ExecuteRequest(
    TService& service,
    TCallContextPtr callContext,
    std::shared_ptr<typename TMethod::TRequest> request)
{
    auto promise = NewPromise<typename TMethod::TResponse>();

    size_t index = 0;
    if (Executors.size() > 1) {
        // pick random executor
        index = RandomNumber(Executors.size());
    }

    TRequestHandler<TService, TMethod>::Start(
        *this,
        *Executors[index],
        service,
        std::move(callContext),
        std::move(request),
        promise);

    return promise.GetFuture();
}

void TClient::UploadStats()
{
    TStringStream out;

    auto encoder = CreateEncoder(&out, EFormat::SPACK);
    Monitoring->GetCounters()->Accept("", "", *encoder);

    auto request = std::make_shared<NProto::TUploadClientMetricsRequest>();
    request->SetMetrics(out.Str());

    auto ctx = MakeIntrusive<TCallContext>();

    IBlockStorePtr controlEndpoint;
    IBlockStorePtr dataEndpoint;

    with_lock (EndpointLock) {
        controlEndpoint = ControlEndpoint;
        dataEndpoint = DataEndpoint;
    }

    if (dataEndpoint) {
        dataEndpoint->UploadClientMetrics(std::move(ctx), std::move(request));
    } else if (controlEndpoint) {
        controlEndpoint->UploadClientMetrics(std::move(ctx), std::move(request));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TService>
class TEndpointBase
    : public IBlockStore
{
protected:
    std::shared_ptr<TClient> Client;
    std::shared_ptr<TService> Service;

public:
    TEndpointBase(
            std::shared_ptr<TClient> client,
            std::shared_ptr<TService> service)
        : Client(std::move(client))
        , Service(std::move(service))
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        const auto& type = GetBlockStoreRequestName(EBlockStoreRequest::name); \
        return MakeFuture<NProto::T##name##Response>(TErrorResponse(           \
            E_NOT_IMPLEMENTED,                                                 \
            TStringBuilder() << "Unsupported request " << type.Quote()));      \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

protected:
    template <typename TMethod>
    TFuture<typename TMethod::TResponse> ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        return Client->ExecuteRequest<TMethod>(
            *Service,
            std::move(callContext),
            std::move(request));
    }

    template <>
    TFuture<NProto::TWriteBlocksLocalResponse> ExecuteRequest<TWriteBlocksLocalMethod>(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> localRequest)
    {
        auto requestOrError = CreateWriteBlocksRequest(*localRequest);
        if (HasError(requestOrError)) {
            return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                TErrorResponse(requestOrError.GetError()));
        }
        auto request = requestOrError.ExtractResult();

        return Client->ExecuteRequest<TWriteBlocksMethod>(
            *Service,
            std::move(callContext),
            std::move(request));
    }

    template <>
    TFuture<NProto::TReadBlocksLocalResponse> ExecuteRequest<TReadBlocksLocalMethod>(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
    {
        auto future = Client->ExecuteRequest<TReadBlocksMethod>(
            *Service,
            std::move(callContext),
            request);

        return future.Apply([request = std::move(request)] (
            TFuture<NProto::TReadBlocksResponse> f)
        {
            auto response = f.ExtractValue();
            if (HasError(response)) {
                return response;
            }

            auto error = CompleteReadBlocksLocalRequest(*request, response);
            if (HasError(error)) {
                NProto::TReadBlocksLocalResponse response;
                *response.MutableError() = std::move(error);
                return response;
            }

            // no more need response's Blocks; free memory
            response.MutableBlocks()->Clear();

            return NProto::TReadBlocksLocalResponse(std::move(response));
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEndpoint final
    : public TEndpointBase<NProto::TBlockStoreService::Stub>
{
public:
    using TEndpointBase::TEndpointBase;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return ExecuteRequest<T##name##Method>(                                \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TDataEndpoint final
    : public TEndpointBase<NProto::TBlockStoreDataService::Stub>
{
public:
    using TEndpointBase::TEndpointBase;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return ExecuteRequest<T##name##Method>(                                \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_DATA_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

template <typename TService>
class TSocketEndpointBase
    : public TEndpointBase<typename TService::Stub>
    , public std::enable_shared_from_this<TSocketEndpointBase<TService>>
{
    using TBase = TEndpointBase<typename TService::Stub>;

    enum {
        Disconnected = 0,
        Connecting = 1,
        Connected = 2,
    };

    union TState
    {
        TState(ui64 value)
            : Raw(value)
        {}

        struct
        {
            ui32 InflightCounter;
            ui32 ConnectionState;
        };

        ui64 Raw;
    };

private:
    const TString SocketPath;

    TAtomic State = 0;

public:
    TSocketEndpointBase(
            std::shared_ptr<TClient> client,
            TString socketPath)
        : TBase(std::move(client), nullptr)
        , SocketPath(std::move(socketPath))
    {}

    void Connect()
    {
        TState currentState = AtomicGet(State);
        if (currentState.InflightCounter != 0) {
            return;
        }

        if (!SetConnectionState(Connecting, Disconnected)) {
            return;
        }

        auto args = TBase::Client->CreateChannelArguments();
        auto channel = TBase::Client->CreateUnixSocketChannel(SocketPath, args);

        if (channel) {
            TBase::Service = TService::NewStub(channel);
        }

        bool res = SetConnectionState(
            channel ? Connected : Disconnected,
            Connecting);
        STORAGE_VERIFY(res, "Socket", SocketPath);
    }

protected:
    template <typename TMethod>
    TFuture<typename TMethod::TResponse> TryToExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        if (!IncInflightCounter()) {
            Connect();
            return MakeFuture<typename TMethod::TResponse>(
                TErrorResponse(E_GRPC_UNAVAILABLE, "Broken pipe"));
        }

        auto future = TBase::template ExecuteRequest<TMethod>(
            std::move(callContext),
            std::move(request));

        auto weakPtr = TSocketEndpointBase<TService>::weak_from_this();
        return future.Apply([weakPtr = std::move(weakPtr)] (const auto& f) {
            if (auto p = weakPtr.lock()) {
                p->HandleResponse(f.GetValue());
            }
            return f;
        });
    }

private:
    template <typename TResponse>
    void HandleResponse(const TResponse& response)
    {
        if (response.HasError()
            && response.GetError().GetCode() == E_GRPC_UNAVAILABLE)
        {
            SetConnectionState(Disconnected, Connected);
        }

        DecInflightCounter();
    }

    bool SetConnectionState(ui32 next, ui32 prev)
    {
        while (true) {
            TState currentState = AtomicGet(State);
            if (currentState.ConnectionState != prev) {
                return false;
            }

            TState nextState = currentState;
            nextState.ConnectionState = next;

            if (AtomicCas(&State, nextState.Raw, currentState.Raw)) {
                return true;
            }
        }
    }

    bool IncInflightCounter()
    {
        while (true) {
            TState currentState = AtomicGet(State);
            if (currentState.ConnectionState != Connected) {
                return false;
            }

            TState nextState = currentState;
            ++nextState.InflightCounter;

            if (AtomicCas(&State, nextState.Raw, currentState.Raw)) {
                return true;
            }
        }
    }

    void DecInflightCounter()
    {
        // reduce InflightCounter, don't change ConnectionState
        TState oldState = AtomicGetAndDecrement(State);

        STORAGE_VERIFY(oldState.InflightCounter > 0, "Socket", SocketPath);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSocketEndpoint final
    : public TSocketEndpointBase<NProto::TBlockStoreService>
{
public:
    using TSocketEndpointBase::TSocketEndpointBase;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return TryToExecuteRequest<T##name##Method>(                           \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TSocketDataEndpoint final
    : public TSocketEndpointBase<NProto::TBlockStoreDataService>
{
public:
    using TSocketEndpointBase::TSocketEndpointBase;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return TryToExecuteRequest<T##name##Method>(                           \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_DATA_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

void TClient::ScheduleUploadStats()
{
    if (AtomicGet(ShouldStop)) {
        return;
    }

    auto weakPtr = weak_from_this();

    Scheduler->Schedule(
        Timer->Now() + UpdateCountersInterval,
        [weakPtr = std::move(weakPtr)] {
            if (auto p = weakPtr.lock()) {
                p->UploadStats();
                p->ScheduleUploadStats();
            }
        });
}

bool TClient::InitControlEndpoint()
{
    if (ControlEndpoint) {
        return true;
    }

    if (Config->GetUnixSocketPath()) {
        auto endpoint = std::make_shared<TSocketEndpoint>(
            shared_from_this(),
            Config->GetUnixSocketPath());

        endpoint->Connect();
        ControlEndpoint = std::move(endpoint);
        return true;
    }

    if (Config->GetSecurePort() != 0 || Config->GetInsecurePort() != 0) {
        auto args = CreateChannelArguments();

        bool secureEndpoint = Config->GetSecurePort() != 0;
        auto address = Join(":", Config->GetHost(),
            secureEndpoint ? Config->GetSecurePort() : Config->GetInsecurePort());

        auto channel = CreateTcpSocketChannel(address, secureEndpoint, args);
        if (!channel) {
            ythrow TServiceError(E_FAIL)
                << "could not start gRPC client";
        }

        ControlEndpoint = std::make_shared<TEndpoint>(
            shared_from_this(),
            NProto::TBlockStoreService::NewStub(std::move(channel)));
        return true;
    }

    return false;
}

bool TClient::InitDataEndpoint()
{
    if (DataEndpoint) {
        return true;
    }

    if (Config->GetPort() != 0) {
        auto args = CreateChannelArguments();

        auto address = Join(":", Config->GetHost(), Config->GetPort());
        auto secureEndpoint = false;    // auth not supported

        auto channel = CreateTcpSocketChannel(address, secureEndpoint, args);

        if (!channel) {
            ythrow TServiceError(E_FAIL)
                << "could not start gRPC client";
        }

        DataEndpoint = std::make_shared<TDataEndpoint>(
            shared_from_this(),
            NProto::TBlockStoreDataService::NewStub(std::move(channel)));
        return true;
    }

    return false;
}

IBlockStorePtr TClient::CreateEndpoint()
{
    with_lock (EndpointLock) {
        InitControlEndpoint();
        return ControlEndpoint;
    }
}

IBlockStorePtr TClient::CreateDataEndpoint()
{
    with_lock (EndpointLock) {
        InitDataEndpoint();
        return DataEndpoint;
    }
}

IBlockStorePtr TClient::CreateDataEndpoint(const TString& socketPath)
{
    auto endpoint = std::make_shared<TSocketDataEndpoint>(
        shared_from_this(),
        socketPath);

    endpoint->Connect();
    return endpoint;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IClientPtr> CreateClient(
    TClientAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr clientStats)
{
    if (!config->GetClientId()) {
        return MakeError(E_ARGUMENT, "ClientId not set");
    }

    IClientPtr client = std::make_shared<TClient>(
        std::move(config),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(clientStats));

    return client;
}

}   // namespace NCloud::NBlockStore::NClient
