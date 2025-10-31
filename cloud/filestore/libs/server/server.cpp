#include "server.h"

#include "config.h"
#include "probes.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/endpoint.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/public/api/grpc/service.grpc.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/auth_metadata.h>
#include <cloud/storage/core/libs/grpc/completion.h>
#include <cloud/storage/core/libs/grpc/credentials.h>
#include <cloud/storage/core/libs/grpc/executor.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/keepalive.h>
#include <cloud/storage/core/libs/grpc/time_point_specialization.h>
#include <cloud/storage/core/libs/grpc/utils.h>
#include <cloud/storage/core/libs/uds/client_storage.h>
#include <cloud/storage/core/libs/uds/endpoint_poller.h>

#include <ydb/library/actors/prof/tag.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/threading/atomic/bool.h>

#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/resource_quota.h>
#include <contrib/libs/grpc/include/grpcpp/security/auth_metadata_processor.h>
#include <contrib/libs/grpc/include/grpcpp/security/server_credentials.h>
#include <contrib/libs/grpc/include/grpcpp/server.h>
#include <contrib/libs/grpc/include/grpcpp/server_builder.h>
#include <contrib/libs/grpc/include/grpcpp/server_context.h>
#include <contrib/libs/grpc/include/grpcpp/server_posix.h>
#include <contrib/libs/grpc/include/grpcpp/support/status.h>

#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/system/file.h>
#include <util/system/thread.h>

namespace NCloud::NFileStore::NServer {

using namespace NThreading;

LWTRACE_USING(FILESTORE_SERVER_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
ui64 CalculateRequestSize(const T&)
{
    return 0;
}

template <>
ui64 CalculateRequestSize(const NProto::TWriteDataRequest& request)
{
    return request.GetBuffer().size();
}

template <>
ui64 CalculateRequestSize(const NProto::TReadDataRequest& request)
{
    return request.GetLength();
}

NProto::TError MakeGrpcError(const grpc::Status& status)
{
    NProto::TError error;
    if (!status.ok()) {
        error.SetCode(MAKE_GRPC_ERROR(status.error_code()));
        error.SetMessage(TString(status.error_message()));
    }
    return error;
}

grpc::Status MakeGrpcStatus(const NProto::TError& error)
{
    if (HasError(error)) {
        return { grpc::UNAVAILABLE, error.GetMessage() };
    }
    return grpc::Status::OK;
}

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_METHOD(name, proto, method, ...)                     \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr auto RequestName = TStringBuf(#method);               \
        static constexpr auto RequestType = EFileStoreRequest::method;         \
                                                                               \
        using TRequest = NProto::T##proto##Request;                            \
        using TResponse = NProto::T##proto##Response;                          \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static auto Prepare(T& service, TArgs&& ...args)                       \
        {                                                                      \
            return service.Request##method(std::forward<TArgs>(args)...);      \
        }                                                                      \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static auto Execute(T& service, TArgs&& ...args)                       \
        {                                                                      \
            return service.method(std::forward<TArgs>(args)...);               \
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

#define FILESTORE_REQUEST_CHECK(name, ...)                                      \
    if (std::is_same_v<TRequest, NProto::T##name##Request>) {                   \
        return true;                                                            \
    }                                                                           \
// FILESTORE_REQUEST_CHECK

template <typename TRequest>
bool IsServiceControlPlaneRequest()
{
    FILESTORE_SERVICE_METHODS(FILESTORE_REQUEST_CHECK);
    FILESTORE_ENDPOINT_METHODS(FILESTORE_REQUEST_CHECK);
    return false;
}

template <typename TRequest>
ELogPriority GetRequestLogPriority()
{
    return IsServiceControlPlaneRequest<TRequest>()
        ? ELogPriority::TLOG_INFO
        : ELogPriority::TLOG_RESOURCES;
}

#undef FILESTORE_REQUEST_CHECK

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& fileName)
{
    TFileInput in(fileName);
    return in.ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

const NCloud::TRequestSourceKinds RequestSourceKinds = {
    { "INSECURE_CONTROL_CHANNEL", NProto::SOURCE_INSECURE_CONTROL_CHANNEL },
    { "SECURE_CONTROL_CHANNEL",   NProto::SOURCE_SECURE_CONTROL_CHANNEL },
};

////////////////////////////////////////////////////////////////////////////////

class TServerRequestHandlerBase
    : public NStorage::NGrpc::TRequestHandlerBase
{
protected:
    TCallContextPtr CallContext = MakeIntrusive<TCallContext>();
    NAtomic::TBool Started = false;
    NCloud::NProto::TError Error;

public:
    TMaybe<TIncompleteRequest> ToIncompleteRequest(ui64 nowCycles) const
    {
        if (!Started) {
            return Nothing();
        }

        const auto time = CallContext->CalcRequestTime(nowCycles);
        if (!time) {
            return Nothing();
        }

        return TIncompleteRequest(
            // TODO cache media kind on prepare
            NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
            CallContext->RequestType,
            time.ExecutionTime,
            time.TotalTime);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSessionStorage;

struct TAppContext
{
    TLog Log;
    TAtomic ShouldStop = 0;
    IRequestStatsPtr Stats;
    IProfileLogPtr ProfileLog;
    std::unique_ptr<grpc::Server> Server;
    std::shared_ptr<TSessionStorage> SessionStorage;

    TAppContext()
        : SessionStorage(std::make_shared<TSessionStorage>(*this))
    {}

    void ValidateRequest(
        const grpc::ServerContext& context,
        NProto::THeaders& headers);
};

struct TFileStoreContext: TAppContext
{
    NProto::TFileStoreService::AsyncService Service;
    IFileStoreServicePtr ServiceImpl;
};

struct TEndpointManagerContext : TAppContext
{
    NProto::TEndpointManagerService::AsyncService Service;
    IEndpointManagerPtr ServiceImpl;
};

////////////////////////////////////////////////////////////////////////////////

using NCloud::NStorage::NServer::IClientStorage;
using NCloud::NStorage::NServer::IClientStoragePtr;

////////////////////////////////////////////////////////////////////////////////

class TSessionStorage final
    : public std::enable_shared_from_this<TSessionStorage>
{
private:
    struct TClientInfo
    {
        ui32 Fd = 0;
        NProto::ERequestSource Source = NProto::SOURCE_FD_DATA_CHANNEL;
    };

private:
    TAppContext& AppCtx;

    TMutex Lock;
    THashMap<ui32, TClientInfo> ClientInfos;

public:
    TSessionStorage(TAppContext& appCtx)
        : AppCtx(appCtx)
    {}

    void AddClient(
        const TSocketHolder& socket,
        NProto::ERequestSource source)
    {
        if (AtomicGet(AppCtx.ShouldStop)) {
            return;
        }

        TSocketHolder dupSocket;

        with_lock (Lock) {
            auto it = FindClient(socket);
            Y_ABORT_UNLESS(it == ClientInfos.end());

            // create duplicate socket. we poll socket to know when
            // client disconnects and dupSocket is passed to GRPC to read
            // messages.
            dupSocket = SafeCreateDuplicate(socket);

            TClientInfo client {static_cast<ui32>(socket), source};
            auto res = ClientInfos.emplace((ui32)dupSocket, std::move(client));
            Y_ABORT_UNLESS(res.second);
        }

        TLog& Log = AppCtx.Log;
        STORAGE_DEBUG(
            "Accept client. Unix socket fd = " <<
            static_cast<ui32>(dupSocket));
        grpc::AddInsecureChannelFromFd(AppCtx.Server.get(), dupSocket.Release());
    }

    void RemoveClient(const TSocketHolder& socket)
    {
        if (AtomicGet(AppCtx.ShouldStop)) {
            return;
        }

        with_lock (Lock) {
            auto it = FindClient(socket);
            Y_ABORT_UNLESS(it != ClientInfos.end());
            ClientInfos.erase(it);
        }
    }

    std::optional<NProto::ERequestSource> FindSourceByFd(ui32 fd)
    {
        with_lock (Lock) {
            auto it = ClientInfos.find(fd);
            if (it != ClientInfos.end()) {
                const auto& clientInfo = it->second;
                return clientInfo.Source;
            }
        }
        return {};
    }

    IClientStoragePtr CreateClientStorage();

private:
    static TSocketHolder CreateDuplicate(const TSocketHolder& socket)
    {
        auto fileHandle = TFileHandle(socket);
        auto duplicateFd = fileHandle.Duplicate();
        fileHandle.Release();
        return TSocketHolder(duplicateFd);
    }

    // Grpc can release socket before we call RemoveClient. So it is possible
    // to observe the situation when CreateDuplicate returns fd which was not
    // yet removed from ClientInfo's. So completed RemoveClient will close fd
    // related to another connection.
    TSocketHolder SafeCreateDuplicate(const TSocketHolder& socket)
    {
        TList<TSocketHolder> holders;

        while (true) {
            TSocketHolder holder = CreateDuplicate(socket);
            if (ClientInfos.find(holder) == ClientInfos.end()) {
                return holder;
            }

            holders.push_back(std::move(holder));
        }
    }

    THashMap<ui32, TClientInfo>::iterator FindClient(const TSocketHolder& socket)
    {
        ui32 fd = socket;
        for (auto it = ClientInfos.begin(); it != ClientInfos.end(); ++it) {
            const auto& clientInfo = it->second;
            if (clientInfo.Fd == fd) {
                return it;
            }
        }
        return ClientInfos.end();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClientStorage final
    : public IClientStorage
{
    std::shared_ptr<TSessionStorage> Storage;

public:
    TClientStorage(
            std::shared_ptr<TSessionStorage> storage)
        : Storage(std::move(storage))
    {}

    void AddClient(
        const TSocketHolder& clientSocket,
        NCloud::NProto::ERequestSource source) override
    {
        Storage->AddClient(clientSocket, source);
    }

    void RemoveClient(const TSocketHolder& clientSocket) override
    {
        Storage->RemoveClient(clientSocket);
    }
};

IClientStoragePtr TSessionStorage::CreateClientStorage()
{
    return std::make_shared<TClientStorage>(
        this->shared_from_this());
}

////////////////////////////////////////////////////////////////////////////////

void TAppContext::ValidateRequest(
    const grpc::ServerContext& context,
    NProto::THeaders& headers)
{
    auto authContext = context.auth_context();
    Y_ABORT_UNLESS(authContext);

    auto source = GetRequestSource(
        *authContext,
        RequestSourceKinds);

    if (!source) {
        ui32 fd = 0;
        auto peer = context.peer();
        bool result = TryParseSourceFd(peer, &fd);

        if (!result) {
            ythrow TServiceError(E_FAIL)
                << "failed to parse request source fd: " << peer;
        }

        // requests coming from unix sockets do not need to be authorized
        // so pretend they are coming from data channel.
        auto src = SessionStorage->FindSourceByFd(fd);
        if (!src) {
            ythrow TServiceError(E_GRPC_UNAVAILABLE)
                << "endpoint has been stopped (fd = " << fd << ").";
        }

        source = *src;
    }

    if (headers.HasInternal()) {
        ythrow TServiceError(E_ARGUMENT)
            << "internal field should not be set by client";
    }

    auto& internal = *headers.MutableInternal();

    internal.Clear();
    internal.SetRequestSource(*source);

    // we will only get token from secure control channel
    if (source == NProto::SOURCE_SECURE_CONTROL_CHANNEL) {
        internal.SetAuthToken(GetAuthToken(context.client_metadata()));
    }
}

////////////////////////////////////////////////////////////////////////////////

using TRequestsInFlight =
    NStorage::NGrpc::TRequestsInFlight<TServerRequestHandlerBase>;

using TExecutorContext = NStorage::NGrpc::
    TExecutorContext<grpc::ServerCompletionQueue, TRequestsInFlight>;

using TExecutor = NStorage::NGrpc::
    TExecutor<grpc::ServerCompletionQueue, TRequestsInFlight>;

////////////////////////////////////////////////////////////////////////////////

template <typename TAppContext, typename TMethod>
class TRequestHandler final
    : public TServerRequestHandlerBase
{
    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

private:
    TAppContext& AppCtx;
    TExecutorContext& ExecCtx;

    std::unique_ptr<grpc::ServerContext> Context = std::make_unique<grpc::ServerContext>();
    grpc::ServerAsyncResponseWriter<TResponse> Writer;

    std::shared_ptr<TRequest> Request = std::make_shared<TRequest>();
    ui64 RequestId = 0;
    TFuture<TResponse> Response;

    enum {
        WaitingForRequest = 0,
        ExecutingRequest = 1,
        ExecutionCompleted = 2,
        ExecutionCancelled = 3,
        SendingResponse = 4,
        RequestCompleted = 5,
    };
    TAtomic RequestState = WaitingForRequest;

    IProfileLog::TRecord ProfileLogRecord;

public:
    TRequestHandler(TExecutorContext& execCtx, TAppContext& appCtx)
        : AppCtx(appCtx)
        , ExecCtx(execCtx)
        , Writer(Context.get())
    {}

    static void Start(TExecutorContext& execCtx, TAppContext& appCtx)
    {
        using THandler = TRequestHandler<TAppContext, TMethod>;
        execCtx.StartRequestHandler<THandler>(appCtx);
    }

    void Process(bool ok) override
    {
        if (AtomicGet(AppCtx.ShouldStop) == 0 &&
            AtomicGet(RequestState) == WaitingForRequest)
        {
            // There always should be handler waiting for request.
            // Spawn new request only when handling request from server queue.
            Start(ExecCtx, AppCtx);
        }

        if (!ok) {
            auto prevState = AtomicSwap(&RequestState, RequestCompleted);
            if (prevState != WaitingForRequest) {
                CompleteRequest();
            }
        }

        for (;;) {
            switch (AtomicGet(RequestState)) {
                case WaitingForRequest:
                    if (AtomicCas(&RequestState, ExecutingRequest, WaitingForRequest)) {
                        // fix NBS-2490
                        if (AtomicGet(AppCtx.ShouldStop)) {
                            Cancel();
                            return;
                        }

                        PrepareRequestContext();
                        ExecuteRequest();

                        // request is in progress now
                        return;
                    }
                    break;

                case ExecutionCompleted:
                    if (AtomicCas(&RequestState, SendingResponse, ExecutionCompleted)) {
                        try {
                            const auto& response = Response.GetValue();
                            SendResponse(response);
                        } catch (const TServiceError& e) {
                            SendResponse(ErrorResponse<TResponse>(
                                e.GetCode(), TString(e.GetMessage())));
                        } catch (...) {
                            SendResponse(ErrorResponse<TResponse>(
                                E_FAIL, CurrentExceptionMessage()));
                        }

                        // request is in progress now
                        return;
                    }
                    break;

                case ExecutionCancelled:
                    if (AtomicCas(&RequestState, SendingResponse, ExecutionCancelled)) {
                        // cancel inflight requests due to server shutting down
                        SendResponse(grpc::Status(
                            grpc::StatusCode::UNAVAILABLE,
                            "Server shutting down"));

                        // request is in progress now
                        return;
                    }
                    break;

                case SendingResponse:
                    if (AtomicCas(&RequestState, RequestCompleted, SendingResponse)) {
                        CompleteRequest();
                    }
                    break;

                case RequestCompleted:
                    // request completed and could be safely destroyed
                    ExecCtx.RequestsInFlight.Unregister(this);

                    // free grpc server context as soon as we completed request
                    // because request handler may be destructed later than
                    // grpc shutdown
                    Context.reset();
                    return;
            }
        }
    }

    void Cancel() override
    {
        if (AtomicCas(&RequestState, ExecutionCancelled, ExecutingRequest)) {
            // will be processed on executor thread
            EnqueueCompletion(ExecCtx.CompletionQueue.get(), AcquireCompletionTag());
            return;
        }

        if (Context->c_call()) {
            Context->TryCancel();
        }
    }

    void PrepareRequest()
    {
        TMethod::Prepare(
            AppCtx.Service,
            Context.get(),
            Request.get(),
            &Writer,
            ExecCtx.CompletionQueue.get(),
            ExecCtx.CompletionQueue.get(),
            this);
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

        RequestId = headers.GetRequestId();
        if (!RequestId) {
            RequestId = CreateRequestId();
            headers.SetRequestId(RequestId);
        }

        CallContext->FileSystemId = GetFileSystemId(*Request);
        CallContext->RequestId = RequestId;
        CallContext->RequestSize = CalculateRequestSize(*Request);
        CallContext->RequestType = TMethod::RequestType;
    }

    void ExecuteRequest()
    {
        InitProfileLogRequestInfo(ProfileLogRecord.Request, *Request);

        auto& Log = AppCtx.Log;

        STORAGE_LOG(GetRequestLogPriority<TRequest>(),
            TMethod::RequestName
            << " #" << RequestId
            << " execute request: " << DumpMessage(*Request));

        FILESTORE_TRACK(
            ExecuteRequest,
            CallContext,
            TString(TMethod::RequestName),
            CallContext->FileSystemId,
            NProto::STORAGE_MEDIA_SSD,  // TODO NBS-2954
            CallContext->RequestSize);

        AppCtx.Stats->RequestStarted(Log, *CallContext);
        Started = true;

        try {
            AppCtx.ValidateRequest(
                *Context,
                *Request->MutableHeaders());

            Response = TMethod::Execute(
                *AppCtx.ServiceImpl,
                CallContext,
                std::move(Request));
        } catch (const TServiceError& e) {
            STORAGE_WARN(
                TMethod::RequestName
                << " #" << RequestId
                << " request error: " << e);

            Response = MakeFuture(
                ErrorResponse<TResponse>(e.GetCode(), TString(e.GetMessage())));
        } catch (...) {
            STORAGE_ERROR(
                TMethod::RequestName
                << " #" << RequestId
                << " unexpected error: " << CurrentExceptionMessage());

            Response = MakeFuture(
                ErrorResponse<TResponse>(E_FAIL, CurrentExceptionMessage()));
        }

        auto* tag = AcquireCompletionTag();
        Response.Subscribe(
            [=, this] (const auto& response) {
                Y_UNUSED(response);

                if (AtomicCas(&RequestState, ExecutionCompleted, ExecutingRequest)) {
                    // will be processed on executor thread
                    EnqueueCompletion(ExecCtx.CompletionQueue.get(), tag);
                    return;
                }

                ReleaseCompletionTag();
            });
    }

    void SendResponse(const TResponse& response)
    {
        auto& Log = AppCtx.Log;

        if (HasError(response)) {
            Error = response.GetError();
        }

        STORAGE_LOG(GetRequestLogPriority<TRequest>(),
            TMethod::RequestName
            << " #" << RequestId
            << " send response: " << DumpMessage(response));

        FILESTORE_TRACK(
            SendResponse,
            CallContext,
            TString(TMethod::RequestName));

        AppCtx.Stats->ResponseSent(*CallContext);

        FinalizeProfileLogRequestInfo(ProfileLogRecord.Request, response);

        Writer.Finish(response, grpc::Status::OK, AcquireCompletionTag());
    }

    void SendResponse(const grpc::Status& status)
    {
        auto& Log = AppCtx.Log;

        Error = MakeGrpcError(status);
        STORAGE_TRACE(TMethod::RequestName
            << " #" << RequestId
            << " send response: " << FormatError(Error));

        FILESTORE_TRACK(
            SendResponse,
            CallContext,
            TString(TMethod::RequestName));

        AppCtx.Stats->ResponseSent(*CallContext);

        ProfileLogRecord.Request.SetErrorCode(Error.GetCode());

        Writer.FinishWithError(status, AcquireCompletionTag());
    }

    void CompleteRequest()
    {
        auto& Log = AppCtx.Log;

        STORAGE_TRACE(TMethod::RequestName
            << " #" << RequestId
            << " request completed");

        if (CallContext) {
            const ui64 now = GetCycleCount();
            const auto ts = CallContext->CalcRequestTime(now);

            FILESTORE_TRACK(
                RequestCompleted,
                CallContext,
                TString(TMethod::RequestName),
                ts.TotalTime.MicroSeconds(),
                ts.ExecutionTime.MicroSeconds());

            AppCtx.Stats->RequestCompleted(Log, *CallContext, Error);

            const auto startTs = TInstant::MilliSeconds(
                CallContext->GetRequestStartedCycles() /
                GetCyclesPerMillisecond());

            ProfileLogRecord.FileSystemId = CallContext->FileSystemId;

            auto request = &ProfileLogRecord.Request;
            request->SetTimestampMcs(startTs.MicroSeconds());
            request->SetDurationMcs(ts.TotalTime.MicroSeconds());
            request->SetRequestType(static_cast<ui32>(CallContext->RequestType));

            AppCtx.ProfileLog->Write(std::move(ProfileLogRecord));
        } else {
            // request processing was aborted
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TAppContext, typename TMethod>
class TStreamRequestHandler final
    : public TServerRequestHandlerBase
{
    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

    class TResponseHandler final
        : public IResponseHandler<TResponse>
    {
    private:
        TStreamRequestHandler* Self;

    public:
        TResponseHandler(TStreamRequestHandler* self)
            : Self(self)
        {}

        void HandleResponse(const TResponse& response) override
        {
            Self->HandleResponse(response);
        }

        void HandleCompletion(const NProto::TError& error) override
        {
            Self->HandleCompletion(error);

            Self->ReleaseCompletionTag();
        }
    };

private:
    TAppContext& AppCtx;
    TExecutorContext& ExecCtx;

    std::unique_ptr<grpc::ServerContext> Context = std::make_unique<grpc::ServerContext>();
    grpc::ServerAsyncWriter<TResponse> Writer;

    std::shared_ptr<TRequest> Request = std::make_shared<TRequest>();

    enum {
        WaitingForRequest = 0,
        ExecutingRequest = 1,
        ExecutionCompleted = 2,
        ExecutionCancelled = 3,
        SendingResponse = 4,
        SendingCompletion = 5,
        RequestCompleted = 6,
    };
    TAtomic RequestState = WaitingForRequest;

    TDeque<TResponse> ResponseQueue;
    TMaybe<grpc::Status> CompletionStatus;
    TAdaptiveLock ResponseLock;

    ui64 RequestId = 0;

public:
    TStreamRequestHandler(TExecutorContext& execCtx, TAppContext& appCtx)
        : AppCtx(appCtx)
        , ExecCtx(execCtx)
        , Writer(Context.get())
    {}

    static void Start(TExecutorContext& execCtx, TAppContext& appCtx)
    {
        using THandler = TStreamRequestHandler<TAppContext, TMethod>;
        execCtx.StartRequestHandler<THandler>(appCtx);
    }

    void Process(bool ok) override
    {
        if (AtomicGet(AppCtx.ShouldStop) == 0 &&
            AtomicGet(RequestState) == WaitingForRequest)
        {
            // There always should be handler waiting for request.
            // Spawn new request only when handling request from server queue.
            Start(ExecCtx, AppCtx);
        }

        if (!ok) {
            AtomicSet(RequestState, RequestCompleted);
        }

        for (;;) {
            switch (AtomicGet(RequestState)) {
                case WaitingForRequest:
                    if (AtomicCas(&RequestState, ExecutingRequest, WaitingForRequest)) {
                        PrepareRequestContext();
                        ExecuteRequest();

                        // request is in progress now
                        return;
                    }
                    break;

                case ExecutionCompleted:
                    with_lock (ResponseLock) {
                        if (ResponseQueue) {
                            if (AtomicCas(&RequestState, SendingResponse, ExecutionCompleted)) {
                                SendResponse(ResponseQueue.front());
                                ResponseQueue.pop_front();

                                // request is in progress now
                                return;
                            }
                        } else {
                            if (AtomicCas(&RequestState, SendingCompletion, ExecutionCompleted)) {
                                Y_ABORT_UNLESS(CompletionStatus);
                                SendCompletion(*CompletionStatus);

                                // request is in progress now
                                return;
                            }
                        }
                    }
                    break;

                case ExecutionCancelled:
                    if (AtomicCas(&RequestState, SendingCompletion, ExecutionCancelled)) {
                        // cancel inflight requests due to server shutting down
                        SendCompletion(grpc::Status(
                            grpc::StatusCode::UNAVAILABLE,
                            "Server shutting down"));

                        // request is in progress now
                        return;
                    }
                    break;

                case SendingResponse:
                    with_lock (ResponseLock) {
                        if (ResponseQueue) {
                            SendResponse(ResponseQueue.front());
                            ResponseQueue.pop_front();

                            // request is in progress now
                            return;
                        } else if (CompletionStatus) {
                            if (AtomicCas(&RequestState, SendingCompletion, SendingResponse)) {
                                SendCompletion(*CompletionStatus);

                                // request is in progress now
                                return;
                            }
                        } else {
                            if (AtomicCas(&RequestState, ExecutingRequest, SendingResponse)) {
                                // request is in progress now
                                return;
                            }
                        }
                    }
                    break;

                case SendingCompletion:
                    if (AtomicCas(&RequestState, RequestCompleted, SendingCompletion)) {
                    }
                    break;

                case RequestCompleted:
                    // request completed and could be safely destroyed
                    ExecCtx.RequestsInFlight.Unregister(this);

                    // free grpc server context as soon as we completed request
                    // because request handler may be destructed later than
                    // grpc shutdown
                    Context.reset();
                    return;
            }
        }
    }

    void Cancel() override
    {
        if (AtomicCas(&RequestState, ExecutionCancelled, ExecutingRequest)) {
            // will be processed on executor thread
            EnqueueCompletion(ExecCtx.CompletionQueue.get(), AcquireCompletionTag());
            return;
        }

        if (Context->c_call()) {
            Context->TryCancel();
        }
    }

    void PrepareRequest()
    {
        TMethod::Prepare(
            AppCtx.Service,
            Context.get(),
            Request.get(),
            &Writer,
            ExecCtx.CompletionQueue.get(),
            ExecCtx.CompletionQueue.get(),
            this);
    }

private:
    void PrepareRequestContext()
    {
        CallContext->FileSystemId = GetFileSystemId(*Request);
        CallContext->RequestId = RequestId;
        CallContext->RequestType = TMethod::RequestType;
        // TODO fill context
    }

    void ExecuteRequest()
    {
        AcquireCompletionTag();
        // TODO report stats

        try {
            AppCtx.ValidateRequest(
                *Context,
                *Request->MutableHeaders());

            TMethod::Execute(
                *AppCtx.ServiceImpl,
                CallContext,
                std::move(Request),
                std::make_shared<TResponseHandler>(this));

            // request is in progress now
            return;

        } catch (const TServiceError& e) {
            HandleCompletion(MakeError(e.GetCode(), TString(e.GetMessage())));
        } catch (...) {
            HandleCompletion(MakeError(E_FAIL, CurrentExceptionMessage()));
        }

        ReleaseCompletionTag();
    }

    void HandleResponse(const TResponse& response)
    {
        with_lock (ResponseLock) {
            Y_ABORT_UNLESS(!CompletionStatus);
            ResponseQueue.push_back(response);
        }

        if (AtomicCas(&RequestState, ExecutionCompleted, ExecutingRequest)) {
            // will be processed on executor thread
            EnqueueCompletion(ExecCtx.CompletionQueue.get(), AcquireCompletionTag());
        }
    }

    void HandleCompletion(const NProto::TError& error)
    {
        with_lock (ResponseLock) {
            Y_ABORT_UNLESS(!CompletionStatus);
            CompletionStatus = MakeGrpcStatus(error);
        }

        if (AtomicCas(&RequestState, ExecutionCompleted, ExecutingRequest)) {
            // will be processed on executor thread
            EnqueueCompletion(ExecCtx.CompletionQueue.get(), AcquireCompletionTag());
        }
    }

    void SendResponse(const TResponse& response)
    {
        auto& Log = AppCtx.Log;

        STORAGE_TRACE(TMethod::RequestName
            << " send response: " << DumpMessage(response));

        Writer.Write(response, AcquireCompletionTag());
    }

    void SendCompletion(const grpc::Status& status)
    {
        auto& Log = AppCtx.Log;

        STORAGE_TRACE(TMethod::RequestName
            << " send completion: " << FormatError(MakeGrpcError(status)));

        Writer.Finish(status, AcquireCompletionTag());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
using TFileStoreHandler = TRequestHandler<TFileStoreContext, T>;

template <typename T>
using TFileStoreStreamHandler = TStreamRequestHandler<TFileStoreContext, T>;

void StartRequests(TExecutorContext& execCtx, TFileStoreContext& appCtx)
{
#define FILESTORE_START_REQUEST(name, ...)                                     \
    TFileStoreHandler<T##name##Fs##Method>::Start(execCtx, appCtx);            \
// FILESTORE_START_REQUEST

    FILESTORE_SERVICE(FILESTORE_START_REQUEST)

#undef FILESTORE_START_REQUEST

    TFileStoreStreamHandler<TGetSessionEventsStreamMethod>::Start(execCtx, appCtx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
using TEndpointManagerHandler = TRequestHandler<TEndpointManagerContext, T>;

void StartRequests(TExecutorContext& execCtx, TEndpointManagerContext& appCtx)
{
#define FILESTORE_START_REQUEST(name, ...)                                     \
    TEndpointManagerHandler<T##name##Vhost##Method>::Start(execCtx, appCtx);   \
// FILESTORE_START_REQUEST

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_START_REQUEST)

#undef FILESTORE_START_REQUEST
}

////////////////////////////////////////////////////////////////////////////////

template <typename TAppContext>
class TServer final
    : public IServer
    , public std::enable_shared_from_this<TServer<TAppContext>>
{
private:
    const TGrpcInitializer GrpcInitializer;
    const TServerConfigPtr Config;
    const ILoggingServicePtr Logging;

    TAppContext AppCtx;
    std::unique_ptr<grpc::Server> Server;
    TAdaptiveLock ExecutorsLock;
    TVector<std::unique_ptr<TExecutor>> Executors;

    std::unique_ptr<NStorage::NServer::TEndpointPoller> EndpointPoller;

public:
    template <typename T>
    TServer(
            TServerConfigPtr config,
            ILoggingServicePtr logging,
            IRequestStatsPtr requestStats,
            IProfileLogPtr profileLog,
            T service)
        : Config(std::move(config))
        , Logging(std::move(logging))
    {
        AppCtx.ServiceImpl = std::move(service);
        AppCtx.Stats = std::move(requestStats);
        AppCtx.ProfileLog = std::move(profileLog);
    }

    ~TServer() override
    {
        Stop();
    }

    void Start() override
    {
        auto& Log = AppCtx.Log;
        Log = Logging->CreateLog("NFS_SERVER");

        grpc::ServerBuilder builder;
        builder.RegisterService(&AppCtx.Service);

        ui32 maxMessageSize = Config->GetMaxMessageSize();
        if (maxMessageSize) {
            builder.SetMaxSendMessageSize(maxMessageSize);
            builder.SetMaxReceiveMessageSize(maxMessageSize);
        }

        ui32 memoryQuotaBytes = Config->GetMemoryQuotaBytes();
        if (memoryQuotaBytes) {
            grpc::ResourceQuota quota("memory_bound");
            quota.Resize(memoryQuotaBytes);

            builder.SetResourceQuota(quota);
        }

        if (Config->GetKeepAliveEnabled()) {
            builder.SetOption(std::make_unique<TKeepAliveOption>(
                Config->GetKeepAliveIdleTimeout(),
                Config->GetKeepAliveProbeTimeout(),
                Config->GetKeepAliveProbesCount()));
        }

        if (auto port = Config->GetPort()) {
            auto address = Join(":", Config->GetHost(), port);

            auto credentials = CreateInsecureServerCredentials();
            credentials->SetAuthMetadataProcessor(
                std::make_shared<TAuthMetadataProcessor>(
                    RequestSourceKinds,
                    NProto::SOURCE_INSECURE_CONTROL_CHANNEL));

            builder.AddListeningPort(
                address,
                std::move(credentials));

            STORAGE_INFO("Start listening on insecure " << address);
        }

        if (auto port = Config->GetSecurePort()) {
            auto host = Config->GetSecureHost() ? Config->GetSecureHost() : Config->GetHost();
            auto address = Join(":", host, port);
            STORAGE_INFO("Listen on (secure control) " << address);

            auto sslOptions = CreateSslOptions();
            auto credentials = grpc::SslServerCredentials(sslOptions);
            credentials->SetAuthMetadataProcessor(
                std::make_shared<TAuthMetadataProcessor>(
                    RequestSourceKinds,
                    NProto::SOURCE_SECURE_CONTROL_CHANNEL));

            builder.AddListeningPort(
                address,
                std::move(credentials));
        }

        with_lock (ExecutorsLock) {
            ui32 threadsCount = Config->GetThreadsCount();
            for (size_t i = 1; i <= threadsCount; ++i) {
                auto executor = std::make_unique<TExecutor>(
                    TStringBuilder() << "SRV" << i,
                    builder.AddCompletionQueue(),
                    Log);
                executor->Start();
                Executors.push_back(std::move(executor));
            }
        }

        AppCtx.Server = builder.BuildAndStart();
        if (!AppCtx.Server) {
            ythrow TServiceError(E_FAIL)
                << "could not start gRPC server";
        }

        auto unixSocketPath = Config->GetUnixSocketPath();
        if (unixSocketPath) {
            ui32 backlog = Config->GetUnixSocketBacklog();
            StartListenUnixSocket(unixSocketPath, backlog);
        }

        with_lock (ExecutorsLock) {
            for (auto& executor: Executors) {
                for (ui32 i = 0; i < Config->GetPreparedRequestsCount(); ++i) {
                    StartRequests(*executor, AppCtx);
                }
            }
        }
    }

    void Stop() override
    {
        auto& Log = AppCtx.Log;

        if (AtomicSwap(&AppCtx.ShouldStop, 1) == 1) {
            return;
        }

        STORAGE_INFO("Shutting down");

        StopListenUnixSocket();

        AppCtx.Stats->Reset();

        auto deadline = Config->GetShutdownTimeout().ToDeadLine();
        if (AppCtx.Server) {
            AppCtx.Server->Shutdown(deadline);
        }

        for (;;) {
            size_t requestsCount = 0;
            with_lock (ExecutorsLock) {
                for (auto& executor: Executors) {
                    requestsCount += executor->RequestsInFlight.GetCount();
                }
            }

            if (!requestsCount) {
                break;
            }

            if (deadline <= TInstant::Now()) {
                STORAGE_WARN("Some requests are still active on shutdown: "
                    << requestsCount);
                break;
            }

            Sleep(TDuration::MilliSeconds(100));
        }

        with_lock (ExecutorsLock) {
            for (auto& executor: Executors) {
                executor->Shutdown();
            }

            Executors.clear();
        }
    }

    void Accept(IIncompleteRequestCollector& collector) override
    {
        TGuard g{ExecutorsLock};
        for (auto& executor: Executors) {
            const auto now = GetCycleCount();
            executor->RequestsInFlight.ForEach([&](const auto* handler) {
                if (auto request = handler->ToIncompleteRequest(now)) {
                    collector.Collect(*request);
                }
            });
        }
    }

private:

    grpc::SslServerCredentialsOptions CreateSslOptions()
    {
        grpc::SslServerCredentialsOptions sslOptions;
        sslOptions.client_certificate_request = GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;

        if (const auto& rootCertsFile = Config->GetRootCertsFile()) {
            sslOptions.pem_root_certs = ReadFile(rootCertsFile);
        }

        Y_ENSURE(Config->GetCerts().size(), "Empty Certs");

        for (const auto& cert: Config->GetCerts()) {
            grpc::SslServerCredentialsOptions::PemKeyCertPair keyCert;

            Y_ENSURE(cert.CertFile, "Empty CertFile");
            keyCert.cert_chain = ReadFile(cert.CertFile);

            Y_ENSURE(cert.CertPrivateKeyFile, "Empty CertPrivateKeyFile");
            keyCert.private_key = ReadFile(cert.CertPrivateKeyFile);

            sslOptions.pem_key_cert_pairs.push_back(keyCert);
        }

        return sslOptions;
    }

    void StartListenUnixSocket(const TString& unixSocketPath, ui32 backlog)
    {
        auto& Log = AppCtx.Log;

        STORAGE_INFO("Listen on (control) " << unixSocketPath.Quote());

        EndpointPoller = std::make_unique<NStorage::NServer::TEndpointPoller>();
        EndpointPoller->Start();

        auto error = EndpointPoller->StartListenEndpoint(
            unixSocketPath,
            backlog,
            S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR, // accessMode
            true,   // multiClient
            NProto::SOURCE_FD_CONTROL_CHANNEL,
            AppCtx.SessionStorage->CreateClientStorage());

        if (HasError(error)) {
            ReportEndpointStartingError();
            STORAGE_ERROR("Failed to start (control) endpoint: " << FormatError(error));
            StopListenUnixSocket();
        }
    }

    void StopListenUnixSocket()
    {
        if (EndpointPoller) {
            EndpointPoller->Stop();
            EndpointPoller.reset();
        }
    }
};

using TFileStoreServer = TServer<TFileStoreContext>;
using TEndpointManagerServer = TServer<TEndpointManagerContext>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    IProfileLogPtr profileLog,
    IFileStoreServicePtr service)
{
    return std::make_shared<TFileStoreServer>(
        std::move(config),
        std::move(logging),
        std::move(requestStats),
        std::move(profileLog),
        std::move(service));
}

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    IEndpointManagerPtr service)
{
    return std::make_shared<TEndpointManagerServer>(
        std::move(config),
        std::move(logging),
        std::move(requestStats),
        CreateProfileLogStub(),
        std::move(service));
}

}   // namespace NCloud::NFileStore::NServer
