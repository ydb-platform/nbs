#include "session.h"

#include "config.h"
#include "session_introspection.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/ptr.h>
#include <util/system/hostname.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_METHOD(name, ...)                                    \
    struct T##name##Method                                                     \
    {                                                                          \
        [[maybe_unused]] static constexpr auto RequestName = TStringBuf(#name);\
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static TFuture<TResponse> Execute(T& client, TArgs&& ...args)          \
        {                                                                      \
            return client.name(std::forward<TArgs>(args)...);                  \
        }                                                                      \
    };                                                                         \
// FILESTORE_DECLARE_METHOD

FILESTORE_SERVICE(FILESTORE_DECLARE_METHOD)
FILESTORE_DECLARE_METHOD(ReadDataLocal)
FILESTORE_DECLARE_METHOD(WriteDataLocal)

#undef FILESTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TRequestState
    : public TAtomicRefCount<TRequestState<T>>
{
    using TRequest = typename T::TRequest;
    using TResponse = typename T::TResponse;

    TCallContextPtr CallContext;
    std::shared_ptr<typename T::TRequest> Request;
    TPromise<typename T::TResponse> Response;

    // session context
    TString SessionId;
    ui64 MountSeqNumber = 0;

    TRequestState(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request = std::make_shared<TRequest>())
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , Response(NewPromise<TResponse>())
    {}
};

template<>
struct TRequestState<TCreateSessionMethod>
    : public TAtomicRefCount<TRequestState<TCreateSessionMethod>>
{
    using TRequest = TCreateSessionMethod::TRequest;
    using TResponse = TCreateSessionMethod::TResponse;

    // session context
    TString SessionId;

    const bool ReadOnly;
    const ui64 MountSeqNumber;
    const int Flags;

    TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    TPromise<TResponse> Response;

    TRequestState(
            bool readOnly,
            ui64 mountSeqNumber,
            int flags,
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request = std::make_shared<TRequest>())
        : ReadOnly(readOnly)
        , MountSeqNumber(mountSeqNumber)
        , Flags(flags)
        , CallContext(std::move(callContext))
        , Request(std::move(request))
        , Response(NewPromise<TResponse>())
    {
        Request->SetMountSeqNumber(MountSeqNumber);
        Request->SetReadOnly(ReadOnly);
    }
};

template <typename T>
using TRequestStatePtr = TIntrusivePtr<TRequestState<T>>;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ExtractResponse(TFuture<T>& future, T& response)
{
    try {
        response = future.ExtractValue();
    } catch (const TServiceError& e) {
        auto& error = *response.MutableError();
        error.SetCode(e.GetCode());
        error.SetMessage(e.what());
    } catch (...) {
        auto& error = *response.MutableError();
        error.SetCode(E_FAIL);
        error.SetMessage(CurrentExceptionMessage());
    }
}

template <typename T>
void GetResponse(const TFuture<T>& future, T& response)
{
    try {
        response = future.GetValue();
    } catch (const TServiceError& e) {
        auto& error = *response.MutableError();
        error.SetCode(e.GetCode());
        error.SetMessage(e.what());
    } catch (...) {
        auto& error = *response.MutableError();
        error.SetCode(E_FAIL);
        error.SetMessage(CurrentExceptionMessage());
    }
}

TString GetSessionId(const NProto::TCreateSessionResponse& response)
{
    Y_DEBUG_ABORT_UNLESS(!HasError(response));
    return response.GetSession().GetSessionId();
}

ui64 GetSessionSeqNo(const NProto::TCreateSessionResponse& response)
{
    Y_DEBUG_ABORT_UNLESS(!HasError(response));
    return response.GetSession().GetSessionSeqNo();
}

bool GetReadOnly(const NProto::TCreateSessionResponse& response)
{
    Y_DEBUG_ABORT_UNLESS(!HasError(response));
    return response.GetSession().GetReadOnly();
}

TString GetSessionState(const NProto::TCreateSessionResponse& response)
{
    Y_DEBUG_ABORT_UNLESS(!HasError(response));
    return response.GetSession().GetSessionState();
}

std::tuple<TString, ui64, bool> GetSessionParams(
    const NProto::TCreateSessionResponse& response)
{
    return {
        GetSessionId(response),
        GetSessionSeqNo(response),
        GetReadOnly(response)
    };
}

////////////////////////////////////////////////////////////////////////////////

class TSession
    : public ISession
    , public std::enable_shared_from_this<TSession>
{
    friend TSessionIntrospectedState NCloud::NFileStore::NClient::GetSessionInternalState(
        const ISessionPtr& session);

    enum ESessionState
    {
        Idle,
        SessionEstablishing,
        SessionRequested,
        SessionEstablished,
        SessionDestroying,
        SessionBroken,
    };

    enum ECreateSessionFlags
    {
        F_NONE = 0,
        F_RESTORE_SESSION = 1,
        F_RESET_SESSION_ID = 2
    };

private:
    const ILoggingServicePtr Logging;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IFileStoreServicePtr Client;
    const TSessionConfigPtr Config;

    const TString FsTag;
    TLog Log;

    TMutex SessionLock;
    ESessionState SessionState = Idle;
    TPromise<NProto::TCreateSessionResponse> CreateSessionResponse;
    TPromise<NProto::TDestroySessionResponse> DestroySessionResponse;

    TString SessionId;
    ui64 SessionSeqNo = 0;
    bool ReadOnly = 0;

    std::atomic_flag PingScheduled = false;

public:
    TSession(
            ILoggingServicePtr logging,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            IFileStoreServicePtr client,
            TSessionConfigPtr config)
        : Logging(std::move(logging))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Client(std::move(client))
        , Config(std::move(config))
        , FsTag(Sprintf("[f:%s][c:%s]",
            Config->GetFileSystemId().Quote().c_str(),
            Config->GetClientId().Quote().c_str()))
        , CreateSessionResponse(NewPromise<TCreateSessionMethod::TResponse>())
        , DestroySessionResponse(NewPromise<TDestroySessionMethod::TResponse>())
    {
        Log = Logging->CreateLog("NFS_CLIENT");
    }

    TFuture<NProto::TCreateSessionResponse> CreateSession(
        bool readOnly = false,
        ui64 mountSeqNumber = 0) override
    {
        auto callContext = MakeIntrusive<TCallContext>(Config->GetFileSystemId());
        callContext->RequestType = EFileStoreRequest::CreateSession;

        auto state = MakeIntrusive<TRequestState<TCreateSessionMethod>>(
            readOnly,
            mountSeqNumber,
            F_RESTORE_SESSION | F_RESET_SESSION_ID,
            std::move(callContext));

        ExecuteRequest(state);
        return state->Response;
    }

    TFuture<NProto::TCreateSessionResponse> AlterSession(
        bool readOnly,
        ui64 mountSeqNumber) override
    {
        auto callContext = MakeIntrusive<TCallContext>(Config->GetFileSystemId());
        callContext->RequestType = EFileStoreRequest::CreateSession;

        auto state = MakeIntrusive<TRequestState<TCreateSessionMethod>>(
            readOnly,
            mountSeqNumber,
            F_RESTORE_SESSION,
            std::move(callContext));

        ExecuteRequest(state);
        return state->Response;
    }

    TFuture<NProto::TDestroySessionResponse> DestroySession() override
    {
        auto callContext = MakeIntrusive<TCallContext>(Config->GetFileSystemId());
        callContext->RequestType = EFileStoreRequest::DestroySession;

        auto state = MakeIntrusive<TRequestState<TDestroySessionMethod>>(
            std::move(callContext));

        ExecuteRequest(state);
        return state->Response;
    }

    TFuture<NProto::TPingSessionResponse> PingSession()
    {
        auto callContext = MakeIntrusive<TCallContext>(Config->GetFileSystemId());
        callContext->RequestType = EFileStoreRequest::PingSession;

        auto state = MakeIntrusive<TRequestState<TPingSessionMethod>>(
            std::move(callContext));

        ExecuteRequest(state);
        return state->Response;
    }

    //
    // Session methods
    //

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        auto state = MakeIntrusive<TRequestState<T##name##Method>>(            \
            std::move(callContext),                                            \
            std::move(request));                                               \
                                                                               \
        ExecuteRequest(state);                                                 \
        return state->Response;                                                \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

FILESTORE_DATA_METHODS(FILESTORE_IMPLEMENT_METHOD)
FILESTORE_LOCAL_DATA_METHODS(FILESTORE_IMPLEMENT_METHOD)
FILESTORE_IMPLEMENT_METHOD(ReadDataLocal)
FILESTORE_IMPLEMENT_METHOD(WriteDataLocal)

#undef FILESTORE_IMPLEMENT_METHOD

    //
    // Forwarded methods
    //

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return ForwardRequest<T##name##Method>(                                \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \

FILESTORE_SESSION_FORWARD(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD

private:
    TString GetSafeCurrentSessionId()
    {
        if (!CreateSessionResponse.HasValue()) {
            return {};
        }

        try {
            const auto& response = CreateSessionResponse.GetValue();
            if (!HasError(response)) {
                return GetSessionId(response);
            }

            return {};
        } catch (...) {
            return {};
        }
    }

    //
    // CreateSession request
    //

    void ExecuteRequest(
        TRequestStatePtr<TCreateSessionMethod> state)
    {
        with_lock(SessionLock) {
            if (SessionState == SessionDestroying) {
                state->Response.SetValue(
                    TErrorResponse(
                        E_INVALID_STATE,
                        "Session is being destroyed"));
                return;
            }

            if (SessionState == SessionBroken) {
                state->Response.SetValue(
                    TErrorResponse(
                        E_INVALID_STATE,
                        "Session is broken"));
                return;
            }

            if (SessionState == SessionEstablishing) {
                auto postpone = [state, weakPtr = weak_from_this()] (
                    const auto& future) mutable
                {
                    Y_UNUSED(future);
                    if (auto self = weakPtr.lock()) {
                        self->ExecuteRequest(state);
                        return;
                    }
                    state->Response.SetValue(
                        TErrorResponse(
                            E_INVALID_STATE,
                            "Session is destroyed"));
                };

                CreateSessionResponse.GetFuture().Subscribe(postpone);

                return;
            }

            SessionState = SessionEstablishing;

            auto sessionId =
                (state->Flags & F_RESET_SESSION_ID) ? TString() : GetSafeCurrentSessionId();

            if (sessionId) {
                STORAGE_INFO(LogTag(sessionId, state->MountSeqNumber)
                    << " recovering session f: "
                    << static_cast<ui32>(state->Flags));
            } else {
                STORAGE_INFO(LogTag(sessionId, state->MountSeqNumber)
                    << " initiating session f: "
                    << static_cast<ui32>(state->Flags));
            }

            state->SessionId = std::move(sessionId);

            if (state->Flags & F_RESTORE_SESSION) {
                state->Request->SetRestoreClientSession(true);
            }
            CreateSessionResponse = state->Response;
        }

        ExecuteRequestWithSession(std::move(state));
    }

    void HandleResponse(
        TRequestStatePtr<TCreateSessionMethod> state,
        TFuture<NProto::TCreateSessionResponse> future)
    {
        NProto::TCreateSessionResponse response;
        GetResponse(future, response);

        if (HasError(response)) {
            const auto& error = response.GetError();
            STORAGE_WARN(LogTag(state->SessionId, state->MountSeqNumber)
                << " failed to establish session: "
                << FormatError(error));

            if (error.GetCode() == E_FS_INVALID_SESSION) {
                // reset session
                if (state->SessionId) {
                    STORAGE_INFO(LogTag(state->SessionId, state->MountSeqNumber)
                        << " retrying session f: ");

                    state->SessionId = {};
                    ExecuteRequestWithSession(std::move(state));
                    return;
                }
            }
        }

        with_lock (SessionLock) {
            if (!HasError(response)) {
                STORAGE_INFO(LogTag(GetSessionId(response), GetSessionSeqNo(response))
                    << " session established " << GetSessionState(response).size());

                SessionState = SessionEstablished;

                std::tie(SessionId, SessionSeqNo, ReadOnly) =
                    GetSessionParams(response);

                SchedulePingSession();
            } else if (!state->SessionId) {
                SessionState = SessionBroken;
            }
        }
        state->Response.SetValue(std::move(response));
    }

    //
    // DestroySession request
    //

    void ExecuteRequest(TRequestStatePtr<TDestroySessionMethod> state)
    {
        with_lock (SessionLock) {
            if (SessionState == Idle) {
                state->Response.SetValue(
                    TErrorResponse(S_ALREADY, "Session is not created"));
                return;
            }

            if (SessionState == SessionBroken) {
                state->Response.SetValue(
                    TErrorResponse(E_INVALID_STATE, "Session is not broken"));
                    return;
            }

            if (SessionState == SessionDestroying) {
                state->Response = DestroySessionResponse;
                return;
            }

            if (SessionState == SessionEstablishing ||
                !CreateSessionResponse.HasValue())
            {
                auto postpone = [state, weakPtr = weak_from_this()] (
                    const auto& future) mutable
                {
                    Y_UNUSED(future);
                    if (auto self = weakPtr.lock()) {
                        self->ExecuteRequest(state);
                        return;
                    }
                    state->Response.SetValue(
                        TErrorResponse(
                            E_INVALID_STATE,
                            "Session is destroyed"));
                };

                CreateSessionResponse.GetFuture().Subscribe(postpone);
                return;
            }

            SessionState = SessionDestroying;

            STORAGE_INFO(LogTag(SessionId, SessionSeqNo) << " destroying session");
            FillRequestFromSessionState(*state);

            DestroySessionResponse = state->Response;
        }

        ExecuteRequestWithSession<TDestroySessionMethod>(std::move(state));
    }

    void HandleResponse(
        TRequestStatePtr<TDestroySessionMethod> state,
        TFuture<NProto::TDestroySessionResponse> future)
    {
        NProto::TDestroySessionResponse response;
        GetResponse(future, response);

        if (HasError(response)) {
            const auto& error = response.GetError();
            STORAGE_WARN(LogTag(state->SessionId, state->MountSeqNumber)
                << " failed to destroy session: "
                << FormatError(error));
        } else {
            STORAGE_INFO(LogTag(state->SessionId, state->MountSeqNumber)
                << " session destroyed");
        }

        with_lock (SessionLock) {
            SessionState = Idle;
            CreateSessionResponse = {};
        }

        state->Response.SetValue(std::move(response));
    }

    //
    // Session methods
    //

    TFuture<NProto::TCreateSessionResponse> EnsureSessionExists()
    {
        TRequestStatePtr<TCreateSessionMethod> requestState;
        TFuture<NProto::TCreateSessionResponse> response;

        with_lock (SessionLock) {
            switch (SessionState) {
                case Idle: {
                    return MakeFuture<NProto::TCreateSessionResponse>(
                        TErrorResponse(
                            E_INVALID_STATE,
                            "session does not exist"));
                    break;
                }
                case SessionDestroying: {
                    return MakeFuture<NProto::TCreateSessionResponse>(
                        TErrorResponse(
                            E_INVALID_STATE,
                            "session is being destroyed"));
                    break;
                }
                case SessionBroken: {
                    return MakeFuture<NProto::TCreateSessionResponse>(
                        TErrorResponse(
                            E_INVALID_STATE,
                            "session is broken"));
                    break;
                }
                case SessionRequested: {
                    SessionState = SessionEstablishing;

                    auto callContext = MakeIntrusive<TCallContext>(Config->GetFileSystemId());
                    callContext->RequestType = EFileStoreRequest::CreateSession;

                    requestState = MakeIntrusive<TRequestState<TCreateSessionMethod>>(
                        ReadOnly,
                        SessionSeqNo,
                        F_RESTORE_SESSION,
                        std::move(callContext));

                    requestState->SessionId = SessionId;

                    CreateSessionResponse = requestState->Response;
                    response = requestState->Response;
                    break;
                }
                default: {
                    return CreateSessionResponse;
                }
            }
        }

        ExecuteRequestWithSession(std::move(requestState));
        return response;
    }

    void ForceRestoreSession()
    {
        with_lock (SessionLock) {
            if (SessionState != SessionEstablished) {
                return;
            }

            SessionState = SessionRequested;
        }
    }

    template <typename T>
    void HandleRequestAfterSessionCreate(
        TRequestStatePtr<T> state,
        const TFuture<NProto::TCreateSessionResponse>& sessionResponse)
    {
        NProto::TCreateSessionResponse sessionRes;
        GetResponse(sessionResponse, sessionRes);

        if (!HasError(sessionRes)) {
            state->SessionId = GetSessionId(sessionRes);
            state->MountSeqNumber = GetSessionSeqNo(sessionRes);
            ExecuteRequestWithSession(std::move(state));
            return;
        }

        state->Response.SetValue(TErrorResponse(sessionRes.GetError()));
    }

    // should be protected by |SessionLock|
    template <typename T>
    void FillRequestFromSessionState(TRequestState<T>& state)
    {
        state.SessionId = SessionId;
        state.MountSeqNumber = SessionSeqNo;
    }

    template <typename T>
    void ExecuteRequest(TRequestStatePtr<T> state)
    {
        NProto::TError error;

        try {
            bool shouldExecuteImmediately = false;
            with_lock (SessionLock) {
                if (SessionState == SessionEstablished) {
                    FillRequestFromSessionState(*state);
                    shouldExecuteImmediately = true;
                }
            }

            if (shouldExecuteImmediately) {
                // fast path: avoiding copying NProto::TCreateSessionResponse
                // (which is returned by EnsureSessionExists() call)
                ExecuteRequestWithSession(std::move(state));
                return;
            }

            auto sessionResponse = EnsureSessionExists();
            if (sessionResponse.HasValue()) {
                HandleRequestAfterSessionCreate<T>(
                    std::move(state),
                    sessionResponse);
            } else {
                sessionResponse.Subscribe(
                    [state, weakPtr = weak_from_this()] (
                        const auto& future) mutable
                    {
                        if (auto self = weakPtr.lock()) {
                            self->HandleRequestAfterSessionCreate<T>(
                                std::move(state),
                                future);
                        }
                    });
            }
            return;
        } catch (const TServiceError& e) {
            error.SetCode(e.GetCode());
            error.SetMessage(e.what());
        } catch (...) {
            error.SetCode(E_FAIL);
            error.SetMessage(CurrentExceptionMessage());
        }

        state->Response.SetValue(TErrorResponse(error));
   }

    template <typename T>
    void ExecuteRequestWithSession(TRequestStatePtr<T> state)
    {
        state->Request->SetFileSystemId(Config->GetFileSystemId());

        auto* headers = state->Request->MutableHeaders();
        headers->SetClientId(Config->GetClientId());
        headers->SetSessionId(state->SessionId);
        headers->SetRequestId(state->CallContext->RequestId);
        headers->SetOriginFqdn(GetFQDNHostName());
        headers->SetSessionSeqNo(state->MountSeqNumber);

        T::Execute(*Client, state->CallContext, state->Request).Subscribe(
            [state = std::move(state), weakPtr = weak_from_this()] (
                const auto& future) mutable
            {
                if (auto self = weakPtr.lock()) {
                    self->HandleResponse(std::move(state), future);
                    return;
                }
                state->Response.SetValue(
                    TErrorResponse(E_INVALID_STATE, "request was cancelled"));
            });
    }

    template <typename T>
    void HandleResponse(
        TRequestStatePtr<T> state,
        TFuture<typename T::TResponse> future)
    {
        typename T::TResponse response;
        ExtractResponse(future, response);

        if (HasError(response)) {
            const auto& error = response.GetError();
            if (error.GetCode() == E_FS_INVALID_SESSION) {
                ForceRestoreSession();
                ExecuteRequest(std::move(state));
                return;
            }
        }

        state->Response.SetValue(std::move(response));
    }

    //
    // Forwarded methods
    //

    template <typename T>
    TFuture<typename T::TResponse> ForwardRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request)
    {
        request->SetFileSystemId(Config->GetFileSystemId());

        auto* headers = request->MutableHeaders();
        headers->SetClientId(Config->GetClientId());

        return T::Execute(*Client, std::move(callContext), std::move(request));
    }

    //
    // Session timer
    //

    void SchedulePingSession()
    {
        if (PingScheduled.test_and_set()) {
            return;
        }

        Scheduler->Schedule(
            Timer->Now() + Config->GetSessionPingTimeout(),
            [weakPtr = weak_from_this()] {
                if (auto self = weakPtr.lock()) {
                    self->DoPing();
                }
            });
    }

    void DoPing()
    {
        auto ping = PingSession();

        ping.Subscribe(
            [weakPtr = weak_from_this()] (
                const TFuture<NProto::TPingSessionResponse>& future)
            {
                Y_UNUSED(future);
                if (auto self = weakPtr.lock()) {
                    self->PingScheduled.clear();
                    self->SchedulePingSession();
                }
            });
    }

    TString LogTag(const TString& sessionId, ui64 seqNo)
    {
        return Sprintf(
            "%s[s:%s][n:%lu]",
            FsTag.c_str(),
            sessionId.Quote().c_str(),
            seqNo);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISessionPtr CreateSession(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IFileStoreServicePtr client,
    TSessionConfigPtr config)
{
    return std::make_shared<TSession>(
        std::move(logging),
        std::move(timer),
        std::move(scheduler),
        std::move(client),
        std::move(config));
}

TSessionIntrospectedState GetSessionInternalState(const ISessionPtr& session)
{
    const TSession* p = dynamic_cast<const TSession*>(session.get());

    if (!p) {
        return {};
    }

    TSessionIntrospectedState ans;

    if (p->CreateSessionResponse.Initialized()) {
        ans.CreateSessionResponse = p->CreateSessionResponse.GetFuture();
        if (p->CreateSessionResponse.HasValue()) {
            const auto& response = p->CreateSessionResponse.GetValue();
            ans.MountSeqNumber = GetSessionSeqNo(response);
            ans.ReadOnly = GetReadOnly(response);
        }
    }

    if (p->DestroySessionResponse.Initialized()) {
        ans.DestroySessionResponse = p->DestroySessionResponse.GetFuture();
    }

    return ans;
}

}   // namespace NCloud::NFileStore::NClient
