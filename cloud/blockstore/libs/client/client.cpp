#include "client.h"

#include "config.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/public/api/grpc/service.grpc.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/executor_counters.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/grpc/channel_arguments.h>
#include <cloud/storage/core/libs/grpc/completion.h>
#include <cloud/storage/core/libs/grpc/credentials.h>
#include <cloud/storage/core/libs/grpc/executor.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/time_point_specialization.h>
#include <cloud/storage/core/libs/uds/uds_socket_client.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel_posix.h>
#include <contrib/libs/grpc/include/grpcpp/resource_quota.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>
#include <contrib/libs/grpc/include/grpcpp/security/tls_credentials_options.h>
#include <contrib/libs/grpc/include/grpcpp/support/status.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>

#include <util/datetime/cputimer.h>
#include <util/generic/hash.h>
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
using namespace NStorage::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

const char AUTH_HEADER[] = "authorization";
const char AUTH_METHOD[] = "Bearer";

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
concept HasInstanceId = requires (TRequest r)
{
    { r.SetInstanceId(TString()) };
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError CompleteReadBlocksLocalRequest(
    NProto::TReadBlocksLocalRequest& request,
    const NProto::TReadBlocksLocalResponse& response)
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
    : public NStorage::NGrpc::TRequestHandlerBase
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

using TExecutorContext = NStorage::NGrpc::
    TExecutorContext<grpc::CompletionQueue, TRequestsInFlight>;
using TExecutor = NStorage::NGrpc::TExecutor<
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
    {
        Context.set_wait_for_ready(true);
    }

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

        if (!headers.GetClientId()) {
            Y_ABORT_UNLESS(AppCtx.Config->GetClientId());
            headers.SetClientId(AppCtx.Config->GetClientId());
        }

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
            if (!Promise.TrySetValue(std::move(Response))) {
                AppCtx.ClientStats->ReportInfo(
                    AppCtx.Log,
                    RequestType,
                    RequestId,
                    DiskId,
                    AppCtx.Config->GetClientId(),
                    "ProcessResponse: value already set (request cancelled?)");
            }
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
            if (!Promise.TrySetValue(std::move(Response))) {
                AppCtx.ClientStats->ReportInfo(
                    AppCtx.Log,
                    RequestType,
                    RequestId,
                    DiskId,
                    AppCtx.Config->GetClientId(),
                    "ReportError: value already set (request completed?)");
            }
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

class TClientBase
    : public TAppContext
    , public IStartable
{
    TGrpcInitializer GrpcInitializer;

    TVector<std::unique_ptr<TExecutor>> Executors;

public:
    TClientBase(
        TClientAppConfigPtr config,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IServerStatsPtr clientStats);

    void Start() override;
    void Stop() override;

    template <typename TMethod, typename TService>
    TFuture<typename TMethod::TResponse> ExecuteRequest(
        TService& service,
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request);

    grpc::ChannelArguments CreateChannelArguments();

    std::shared_ptr<grpc::Channel> CreateUnixSocketChannel(
        const TString& unixSocketPath,
        const grpc::ChannelArguments& args);

    std::shared_ptr<grpc::Channel> CreateTcpSocketChannel(
        const TString& address,
        bool secureEndpoint);
};

////////////////////////////////////////////////////////////////////////////////

class TClient final
    : public TClientBase
    , public IClient
    , public std::enable_shared_from_this<TClient>
{
private:
    IBlockStorePtr ControlEndpoint;
    IBlockStorePtr DataEndpoint;

    TAdaptiveLock EndpointLock;
public:
    using TClientBase::TClientBase;

    ~TClient() override
    {
        Stop();
    }

    void Start() override;
    void Stop() override;

    IBlockStorePtr CreateEndpoint() override;

    IBlockStorePtr CreateDataEndpoint() override;

    IBlockStorePtr CreateDataEndpoint(
        const TString& socketPath) override;

private:
    bool InitControlEndpoint();
    bool InitDataEndpoint();

    void UploadStats();

    void ScheduleUploadStats();
};

////////////////////////////////////////////////////////////////////////////////

class TMultiHostClient final
    : public TClientBase
    , public IMultiHostClient
    , public std::enable_shared_from_this<TMultiHostClient>
{
private:
    TAdaptiveLock EndpointLock;

    THashMap<std::pair<TString, bool>, IBlockStorePtr> Cache;

public:
    using TClientBase::TClientBase;

    ~TMultiHostClient() override
    {
        Stop();
    }

    void Start() override {
       TClientBase::Start();
    };

    void Stop() override {
        TClientBase::Stop();
    };

    IBlockStorePtr CreateEndpoint(
        const TString& host,
        ui32 port,
        bool isSecure) override;

    IBlockStorePtr CreateDataEndpoint(
        const TString& host,
        ui32 port,
        bool isSecure) override;
};

////////////////////////////////////////////////////////////////////////////////

TClientBase::TClientBase(
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

    GrpcLoggerInit(
        logging->CreateLog("GRPC"),
        Config->GetLogConfig().GetEnableGrpcTracing());
}

void TClientBase::Start()
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
}

void TClientBase::Stop()
{
    if (AtomicSwap(&ShouldStop, 1) == 1) {
        return;
    }

    STORAGE_INFO("Shutting down");

    for (auto& executor: Executors) {
        executor->Shutdown();
    }
}

grpc::ChannelArguments TClientBase::CreateChannelArguments()
{
    return NStorage::NGrpc::CreateChannelArguments(*Config);
}

std::shared_ptr<grpc::Channel> TClientBase::CreateTcpSocketChannel(
    const TString& address,
    bool secureEndpoint)
{
    auto credentials = CreateTcpClientChannelCredentials(
        secureEndpoint,
        *Config);

    STORAGE_INFO("Connect to " << address);

    return CreateCustomChannel(
        address,
        credentials,
        CreateChannelArguments());
}

std::shared_ptr<grpc::Channel> TClientBase::CreateUnixSocketChannel(
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
TFuture<typename TMethod::TResponse> TClientBase::ExecuteRequest(
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

////////////////////////////////////////////////////////////////////////////////

void TClient::Start()
{
    TClientBase::Start();

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
    TClientBase::Stop();

    with_lock (EndpointLock) {
        ControlEndpoint.reset();
        DataEndpoint.reset();
    }
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

template <typename TService, typename TClient>
class TEndpointBase
    : public IBlockStore
{
protected:
    std::shared_ptr<TClient> Client;
    std::shared_ptr<typename TService::Stub> Service;

public:
    TEndpointBase(
            std::shared_ptr<TClient> client,
            std::shared_ptr<typename TService::Stub> service)
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
        return Client->template ExecuteRequest<TMethod>(
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

        return Client->template ExecuteRequest<TWriteBlocksMethod>(
            *Service,
            std::move(callContext),
            std::move(request));
    }

    template <>
    TFuture<NProto::TReadBlocksLocalResponse> ExecuteRequest<TReadBlocksLocalMethod>(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
    {
        auto future = Client->template ExecuteRequest<TReadBlocksMethod>(
            *Service,
            std::move(callContext),
            request);

        return future.Apply([request = std::move(request)] (
            TFuture<NProto::TReadBlocksResponse> f)
        {
            NProto::TReadBlocksLocalResponse response(f.ExtractValue());
            if (HasError(response)) {
                return response;
            }

            auto error =
                CompleteReadBlocksLocalRequest(*request, response);
            if (HasError(error)) {
                NProto::TReadBlocksLocalResponse response;
                *response.MutableError() = std::move(error);
                return response;
            }

            // no more need response's Blocks; free memory
            response.MutableBlocks()->Clear();

            return response;
        });
    }

    grpc::ChannelArguments CreateChannelArguments()
    {
        return Client->CreateChannelArguments();
    }

    std::shared_ptr<grpc::Channel> CreateUnixSocketChannel(
        const TString& unixSocketPath,
        const grpc::ChannelArguments& args)
    {
        return Client->CreateUnixSocketChannel(unixSocketPath, args);
    }

    bool StartWithUds(const TString& unixSocketPath)
    {
        auto channel = CreateUnixSocketChannel(
            unixSocketPath,
            CreateChannelArguments());

        if (!channel) {
            return false;
        }

        Service = TService::NewStub(std::move(channel));
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TClient>
class TEndpoint final
    : public TEndpointBase<NProto::TBlockStoreService, TClient>
{
    using TBase = TEndpointBase<NProto::TBlockStoreService, TClient>;

public:
    using TEndpointBase<NProto::TBlockStoreService, TClient>::TEndpointBase;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return TBase::template ExecuteRequest<T##name##Method>(                \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

template <typename TClient>
class TDataEndpoint final
    : public TEndpointBase<NProto::TBlockStoreDataService, TClient>
{
    using TBase = TEndpointBase<NProto::TBlockStoreDataService, TClient>;
public:
    using TEndpointBase<NProto::TBlockStoreDataService, TClient>::TEndpointBase;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return TBase::template ExecuteRequest<T##name##Method>(                \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_DATA_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBase>
using TNbsUdsSocketClient = TUdsSocketClient<TBase, TCallContextPtr>;

////////////////////////////////////////////////////////////////////////////////

class TSocketEndpoint final
    : public TNbsUdsSocketClient<TEndpointBase<typename NProto::TBlockStoreService, TClient>>
{
public:
    using TBase = TEndpointBase<typename NProto::TBlockStoreService, TClient>;
    using TNbsUdsSocketClient<TBase>::TNbsUdsSocketClient;

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

class TSocketDataEndpoint final
    : public TNbsUdsSocketClient<TEndpointBase<typename NProto::TBlockStoreDataService, TClient>>
{
public:
    using TBase = TEndpointBase<typename NProto::TBlockStoreDataService, TClient>;
    using TNbsUdsSocketClient<TBase>::TNbsUdsSocketClient;

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
            Config->GetUnixSocketPath(),
            shared_from_this(),
            nullptr);
        endpoint->Connect();
        ControlEndpoint = std::move(endpoint);
        return true;
    }

    if (Config->GetSecurePort() != 0 || Config->GetInsecurePort() != 0) {
        bool secureEndpoint = Config->GetSecurePort() != 0;
        auto address = Join(":", Config->GetHost(),
            secureEndpoint ? Config->GetSecurePort() : Config->GetInsecurePort());

        auto channel = CreateTcpSocketChannel(address, secureEndpoint);
        if (!channel) {
            ythrow TServiceError(E_FAIL)
                << "could not start gRPC client";
        }

        ControlEndpoint = std::make_shared<TEndpoint<TClient>>(
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
        auto address = Join(":", Config->GetHost(), Config->GetPort());
        auto secureEndpoint = false;    // auth not supported

        auto channel = CreateTcpSocketChannel(address, secureEndpoint);

        if (!channel) {
            ythrow TServiceError(E_FAIL)
                << "could not start gRPC client";
        }

        DataEndpoint = std::make_shared<TDataEndpoint<TClient>>(
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
        socketPath,
        shared_from_this(),
        nullptr);

    endpoint->Connect();
    return endpoint;
}

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr TMultiHostClient::CreateEndpoint(
    const TString& host,
    ui32 port,
    bool isSecure)
{
    with_lock (EndpointLock) {
        Y_ENSURE(port);
        auto address = Join(":", host, port);

        if (auto it = Cache.find(make_pair(address, false)); it != Cache.end()) {
            return it->second;
        }

        auto channel = CreateTcpSocketChannel(address, isSecure);
        if (!channel) {
            ythrow TServiceError(E_FAIL)
                << "could not start gRPC client";
        }

        auto endpoint = std::make_shared<TEndpoint<TMultiHostClient>>(
            shared_from_this(),
            NProto::TBlockStoreService::NewStub(std::move(channel)));
        Cache.emplace(make_pair(address, false), endpoint);
        return endpoint;
    }
}

IBlockStorePtr TMultiHostClient::CreateDataEndpoint(
    const TString& host,
    ui32 port,
    bool isSecure)
{
    with_lock (EndpointLock) {
        Y_ENSURE(port);
        auto address = Join(":", host, port);

        if (auto it = Cache.find(make_pair(address, true)); it != Cache.end()) {
            return it->second;
        }

        auto channel = CreateTcpSocketChannel(address, isSecure);
        if (!channel) {
            ythrow TServiceError(E_FAIL)
                << "could not start gRPC client";
        }

        auto endpoint = std::make_shared<TDataEndpoint<TMultiHostClient>>(
            shared_from_this(),
            NProto::TBlockStoreDataService::NewStub(std::move(channel)));

        Cache.emplace(make_pair(address, true), endpoint);
        return endpoint;
    }
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

TResultOrError<IMultiHostClientPtr> CreateMultiHostClient(
    TClientAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr clientStats)
{
    IMultiHostClientPtr client = std::make_shared<TMultiHostClient>(
        std::move(config),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(clientStats));

    return client;
}

}   // namespace NCloud::NBlockStore::NClient
