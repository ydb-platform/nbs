#include "service.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>

#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud::NFileStore {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NFileStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_METHOD(name, ...)                                    \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr auto RequestName = TStringBuf(#name);                 \
                                                                               \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        using TRequestEvent = TEvService::TEv##name##Request;                  \
        using TResponseEvent = TEvService::TEv##name##Response;                \
    };                                                                         \
// FILESTORE_DECLARE_METHOD

FILESTORE_REMOTE_SERVICE(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD

#define FILESTORE_DECLARE_METHOD(name, ...)                                    \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr auto RequestName = TStringBuf(#name);                 \
                                                                               \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
    };                                                                         \
// FILESTORE_DECLARE_METHOD

FILESTORE_LOCAL_DATA_METHODS(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////
// Method structs for the private tablet API (NProtoPrivate / TEvIndexTablet).

struct TUnsafeCreateNodeMethod
{
    static constexpr auto RequestName = TStringBuf("UnsafeCreateNode");

    using TRequest = NProtoPrivate::TUnsafeCreateNodeRequest;
    using TResponse = NProtoPrivate::TUnsafeCreateNodeResponse;

    using TRequestEvent = TEvIndexTablet::TEvUnsafeCreateNodeRequest;
    using TResponseEvent = TEvIndexTablet::TEvUnsafeCreateNodeResponse;
};

struct TUnsafeDeleteNodeMethod
{
    static constexpr auto RequestName = TStringBuf("UnsafeDeleteNode");

    using TRequest = NProtoPrivate::TUnsafeDeleteNodeRequest;
    using TResponse = NProtoPrivate::TUnsafeDeleteNodeResponse;

    using TRequestEvent = TEvIndexTablet::TEvUnsafeDeleteNodeRequest;
    using TResponseEvent = TEvIndexTablet::TEvUnsafeDeleteNodeResponse;
};

struct TUnsafeCreateNodeRefMethod
{
    static constexpr auto RequestName = TStringBuf("UnsafeCreateNodeRef");

    using TRequest = NProtoPrivate::TUnsafeCreateNodeRefRequest;
    using TResponse = NProtoPrivate::TUnsafeCreateNodeRefResponse;

    using TRequestEvent = TEvIndexTablet::TEvUnsafeCreateNodeRefRequest;
    using TResponseEvent = TEvIndexTablet::TEvUnsafeCreateNodeRefResponse;
};

struct TUnsafeDeleteNodeRefMethod
{
    static constexpr auto RequestName = TStringBuf("UnsafeDeleteNodeRef");

    using TRequest = NProtoPrivate::TUnsafeDeleteNodeRefRequest;
    using TResponse = NProtoPrivate::TUnsafeDeleteNodeRefResponse;

    using TRequestEvent = TEvIndexTablet::TEvUnsafeDeleteNodeRefRequest;
    using TResponseEvent = TEvIndexTablet::TEvUnsafeDeleteNodeRefResponse;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TRequestActor final
    : public TActorBootstrapped<TRequestActor<TMethod>>
{
    using TThis = TRequestActor<TMethod>;
    using TBase = TActorBootstrapped<TThis>;

    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

    using TRequestEvent = typename TMethod::TRequestEvent;
    using TResponseEvent = typename TMethod::TResponseEvent;

private:
    TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    TPromise<TResponse> Response;

public:
    static constexpr const char ActorName[] = "NCloud::NFileStore::TRequestActor<T>";

public:
    TRequestActor(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            TPromise<TResponse> response)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , Response(std::move(response))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        TThis::Become(&TThis::StateWork);

        SendRequest(ctx);
    }

private:
    void SendRequest(const TActorContext& ctx)
    {
        LOG_TRACE_S(ctx, TFileStoreComponents::SERVICE_PROXY,
            TMethod::RequestName << " send request");

        auto request = std::make_unique<TRequestEvent>(
            CallContext,
            *Request
        );

        NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
    }

    void HandleResponse(
        const typename TResponseEvent::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_TRACE_S(ctx, TFileStoreComponents::SERVICE_PROXY,
            TMethod::RequestName << " response received");

        try {
            Response.SetValue(std::move(msg->Record));
        } catch (...) {
            LOG_ERROR_S(ctx, TFileStoreComponents::SERVICE_PROXY,
                TMethod::RequestName << " exception in callback: "
                << CurrentExceptionMessage());
        }

        TThis::Die(ctx);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponseEvent, HandleResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE_PROXY,
                    __PRETTY_FUNCTION__);
                break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TMethodHandler final
{
    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

    using TRequestEvent = typename TMethod::TRequestEvent;
    using TResponseEvent = typename TMethod::TResponseEvent;

private:
    THashMap<ui64, TPromise<TResponse>> Responses;
    ui64 Cookie = 0;
    TAdaptiveLock Lock;

public:
    void SendRequest(
        IActorSystem& actorSystem,
        TCallContextPtr callContext,
        const TRequest& request,
        TPromise<TResponse> promise,
        TActorId actorId)
    {
        ui64 cookie = 0;
        with_lock (Lock) {
            cookie = ++Cookie;
            Responses[cookie] = std::move(promise);
        }

        auto event = std::make_unique<IEventHandle>(
            MakeStorageServiceId(),
            actorId,
            std::make_unique<TRequestEvent>(
                std::move(callContext),
                request).release(),
            0 /* flags */,
            cookie,
            nullptr /* forwardOnNondelivery */);

        actorSystem.Send(std::move(event));
    }

    void HandleResponse(
        const typename TResponseEvent::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_TRACE_S(ctx, TFileStoreComponents::SERVICE_PROXY,
            TMethod::RequestName << " response received");

        TPromise<TResponse> promise;
        with_lock (Lock) {
            auto it = Responses.find(ev->Cookie);
            if (it != Responses.end()) {
                promise = std::move(it->second);
                Responses.erase(it);
            }
        }

        if (!promise.Initialized()) {
            LOG_ERROR_S(ctx, TFileStoreComponents::SERVICE_PROXY,
                TMethod::RequestName << " unknown cookie: " << ev->Cookie);
            return;
        }

        try {
            promise.SetValue(std::move(msg->Record));
        } catch (...) {
            LOG_ERROR_S(ctx, TFileStoreComponents::SERVICE_PROXY,
                TMethod::RequestName << " exception in callback: "
                << CurrentExceptionMessage());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct THandler
{
    TActorId SelfId;

public:
#define FILESTORE_SEND_REQUEST(name, ...)                                      \
    TMethodHandler<T##name##Method> name##Handler;                             \
    void SendRequest(                                                          \
        IActorSystem& actorSystem,                                             \
        TCallContextPtr callContext,                                           \
        NProto::T##name##Request request,                                      \
        TPromise<T##name##Method::TResponse> promise)                          \
    {                                                                          \
        name##Handler.SendRequest(                                             \
            actorSystem,                                                       \
            std::move(callContext),                                            \
            std::move(request),                                                \
            std::move(promise),                                                \
            SelfId);                                                           \
    }                                                                          \
// FILESTORE_SEND_REQUEST

FILESTORE_REMOTE_SERVICE(FILESTORE_SEND_REQUEST)

#undef FILESTORE_SEND_REQUEST

#define FILESTORE_SEND_PRIVATE_REQUEST(name)                                   \
    TMethodHandler<T##name##Method> name##Handler;                             \
    void SendRequest(                                                          \
        IActorSystem& actorSystem,                                             \
        TCallContextPtr callContext,                                           \
        NProtoPrivate::T##name##Request request,                               \
        TPromise<NProtoPrivate::T##name##Response> promise)                    \
    {                                                                          \
        name##Handler.SendRequest(                                             \
            actorSystem,                                                       \
            std::move(callContext),                                            \
            std::move(request),                                                \
            std::move(promise),                                                \
            SelfId);                                                           \
    }                                                                          \
// FILESTORE_SEND_PRIVATE_REQUEST

FILESTORE_SEND_PRIVATE_REQUEST(UnsafeCreateNode)
FILESTORE_SEND_PRIVATE_REQUEST(UnsafeDeleteNode)
FILESTORE_SEND_PRIVATE_REQUEST(UnsafeCreateNodeRef)
FILESTORE_SEND_PRIVATE_REQUEST(UnsafeDeleteNodeRef)

#undef FILESTORE_SEND_PRIVATE_REQUEST
};

////////////////////////////////////////////////////////////////////////////////

class THandlerActor final
    : public TActor<THandlerActor>
{
private:
    std::shared_ptr<THandler> Impl;

public:
    explicit THandlerActor(std::shared_ptr<THandler> impl)
        : TActor<THandlerActor>(&THandlerActor::StateWork)
        , Impl(std::move(impl))
    {}

public:
    static constexpr const char ActorName[] =
        "NCloud::NFileStore::THandlerActor";

public:
#define FILESTORE_HANDLE_RESPONSE_IMPL(name, ...)                              \
    HFunc(T##name##Method::TResponseEvent, Impl->name##Handler.HandleResponse);\
// FILESTORE_HANDLE_RESPONSE_IMPL

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            FILESTORE_REMOTE_SERVICE(FILESTORE_HANDLE_RESPONSE_IMPL)

            HFunc(TUnsafeCreateNodeMethod::TResponseEvent,
                Impl->UnsafeCreateNodeHandler.HandleResponse);
            HFunc(TUnsafeDeleteNodeMethod::TResponseEvent,
                Impl->UnsafeDeleteNodeHandler.HandleResponse);
            HFunc(TUnsafeCreateNodeRefMethod::TResponseEvent,
                Impl->UnsafeCreateNodeRefHandler.HandleResponse);
            HFunc(TUnsafeDeleteNodeRefMethod::TResponseEvent,
                Impl->UnsafeDeleteNodeRefHandler.HandleResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE_PROXY,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

#undef FILESTORE_HANDLE_RESPONSE_IMPL
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TStreamRequestActor final
    : public TActorBootstrapped<TStreamRequestActor<TMethod>>
{
    using TThis = TStreamRequestActor<TMethod>;
    using TBase = TActorBootstrapped<TThis>;

    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

    using TRequestEvent = typename TMethod::TRequestEvent;
    using TResponseEvent = typename TMethod::TResponseEvent;

private:
    TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    IResponseHandlerPtr<TResponse> ResponseHandler;

public:
    TStreamRequestActor(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            IResponseHandlerPtr<TResponse> responseHandler)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , ResponseHandler(std::move(responseHandler))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        TThis::Become(&TThis::StateWork);
        SendRequest(ctx);
    }

private:
    void SendRequest(const TActorContext& ctx)
    {
        LOG_TRACE_S(ctx, TFileStoreComponents::SERVICE_PROXY,
            TMethod::RequestName << " send request");

        auto request = std::make_unique<TRequestEvent>(
            CallContext,
            *Request
        );

        // HACK: we use cookie as a marker for streaming requests
        NCloud::Send(
            ctx,
            MakeStorageServiceId(),
            std::move(request),
            TEvService::StreamCookie);
    }

    void HandleResponse(
        const typename TResponseEvent::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_TRACE_S(ctx, TFileStoreComponents::SERVICE_PROXY,
            TMethod::RequestName << " response received");

        try {
            ResponseHandler->HandleResponse(msg->Record);
        } catch (...) {
            LOG_ERROR_S(ctx, TFileStoreComponents::SERVICE_PROXY,
                TMethod::RequestName << " exception in callback: "
                << CurrentExceptionMessage());
        }
    }

    void HandleCompleted(
        const TEvents::TEvCompleted::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_TRACE_S(ctx, TFileStoreComponents::SERVICE_PROXY,
            TMethod::RequestName << " request completed");

        try {
            ResponseHandler->HandleCompletion(MakeError(msg->Status));
        } catch (...) {
            LOG_ERROR_S(ctx, TFileStoreComponents::SERVICE_PROXY,
                TMethod::RequestName << " exception in callback: "
                << CurrentExceptionMessage());
        }

        TThis::Die(ctx);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponseEvent, HandleResponse);
            HFunc(TEvents::TEvCompleted, HandleCompleted)

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE_PROXY,
                    __PRETTY_FUNCTION__);
                break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TKikimrFileStore final
    : public IFileStoreService
{
private:
    const IActorSystemPtr ActorSystem;
    TLog Log;

    using THandlerPtr = std::shared_ptr<THandler>;
    TVector<THandlerPtr> Handlers;
    std::atomic<ui32> Selector{0};

public:
    TKikimrFileStore(
            IActorSystemPtr actorSystem,
            ui32 permanentActorCount)
        : ActorSystem(std::move(actorSystem))
        , Log(ActorSystem->CreateLog("KIKIMR_SERVICE"))
    {
        Handlers.resize(permanentActorCount);
    }

    void Start() override
    {
        for (auto& handler: Handlers) {
            handler = std::make_shared<THandler>();
            auto actorId = ActorSystem->Register(
                std::make_unique<THandlerActor>(handler));
            handler->SelfId = actorId;

            // we'll switch prio to INFO after the scheme with permanent actors
            // gets widely adopted
            STORAGE_WARN("created THandlerActor: " << actorId);
        }
    }

    void Stop() override
    {}

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        auto response = NewPromise<NProto::T##name##Response>();               \
        ExecuteRequest<T##name##Method>(                                       \
            std::move(callContext),                                            \
            std::move(request),                                                \
            response);                                                         \
        return response.GetFuture();                                           \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD

    void GetSessionEventsStream(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetSessionEventsRequest> request,
        IResponseHandlerPtr<NProto::TGetSessionEventsResponse> responseHandler) override
    {
        ExecuteStreamRequest<TGetSessionEventsMethod>(
            std::move(callContext),
            std::move(request),
            std::move(responseHandler));
    }

    TFuture<NProtoPrivate::TUnsafeCreateNodeResponse> UnsafeCreateNode(
        TCallContextPtr callContext,
        std::shared_ptr<NProtoPrivate::TUnsafeCreateNodeRequest> request) override
    {
        auto response = NewPromise<NProtoPrivate::TUnsafeCreateNodeResponse>();
        ExecuteRequest<TUnsafeCreateNodeMethod>(
            std::move(callContext),
            std::move(request),
            response);
        return response.GetFuture();
    }

    TFuture<NProtoPrivate::TUnsafeDeleteNodeResponse> UnsafeDeleteNode(
        TCallContextPtr callContext,
        std::shared_ptr<NProtoPrivate::TUnsafeDeleteNodeRequest> request) override
    {
        auto response = NewPromise<NProtoPrivate::TUnsafeDeleteNodeResponse>();
        ExecuteRequest<TUnsafeDeleteNodeMethod>(
            std::move(callContext),
            std::move(request),
            response);
        return response.GetFuture();
    }

    TFuture<NProtoPrivate::TUnsafeCreateNodeRefResponse> UnsafeCreateNodeRef(
        TCallContextPtr callContext,
        std::shared_ptr<NProtoPrivate::TUnsafeCreateNodeRefRequest> request) override
    {
        auto response = NewPromise<NProtoPrivate::TUnsafeCreateNodeRefResponse>();
        ExecuteRequest<TUnsafeCreateNodeRefMethod>(
            std::move(callContext),
            std::move(request),
            response);
        return response.GetFuture();
    }

    TFuture<NProtoPrivate::TUnsafeDeleteNodeRefResponse> UnsafeDeleteNodeRef(
        TCallContextPtr callContext,
        std::shared_ptr<NProtoPrivate::TUnsafeDeleteNodeRefRequest> request) override
    {
        auto response = NewPromise<NProtoPrivate::TUnsafeDeleteNodeRefResponse>();
        ExecuteRequest<TUnsafeDeleteNodeRefMethod>(
            std::move(callContext),
            std::move(request),
            response);
        return response.GetFuture();
    }

    TFuture<NProto::TReadDataLocalResponse> ReadDataLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataLocalRequest> request) override
    {
        auto response = NewPromise<NProto::TReadDataResponse>();
        ExecuteRequest<TReadDataMethod>(
            std::move(callContext),
            std::move(request),
            response);
        return response.GetFuture().Apply(
            [](TFuture<NProto::TReadDataResponse> f)
            {
                NProto::TReadDataLocalResponse response(f.ExtractValue());
                return response;
            });
    }

    TFuture<NProto::TWriteDataLocalResponse> WriteDataLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataLocalRequest> request) override
    {
        auto response = NewPromise<NProto::TWriteDataResponse>();
        ExecuteRequest<TWriteDataMethod>(
            std::move(callContext),
            std::move(request),
            response);
        return response.GetFuture();
    }

private:
    template <typename T>
    void ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request,
        TPromise<typename T::TResponse> response)
    {
        if (Handlers.size()) {
            const ui32 handlerIdx =
                Selector.fetch_add(1, std::memory_order_relaxed);
            auto& handler = Handlers[handlerIdx % Handlers.size()];
            handler->SendRequest(
                *ActorSystem,
                std::move(callContext),
                *request,
                std::move(response));
            return;
        }

        ActorSystem->Register(std::make_unique<TRequestActor<T>>(
            std::move(callContext),
            std::move(request),
            std::move(response)));
    }

    template<>
    void ExecuteRequest<TFsyncMethod>(
        TCallContextPtr callContext,
        std::shared_ptr<TFsyncMethod::TRequest> request,
        TPromise<TFsyncMethod::TResponse> response)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        Y_UNUSED(TFsyncMethod::RequestName);

        response.SetValue(TFsyncMethod::TResponse());
    }

    template<>
    void ExecuteRequest<TFsyncDirMethod>(
        TCallContextPtr callContext,
        std::shared_ptr<TFsyncDirMethod::TRequest> request,
        TPromise<TFsyncDirMethod::TResponse> response)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        Y_UNUSED(TFsyncDirMethod::RequestName);

        response.SetValue(TFsyncDirMethod::TResponse());
    }

    template <typename T>
    void ExecuteStreamRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request,
        IResponseHandlerPtr<typename T::TResponse> responseHandler)
    {
        ActorSystem->Register(std::make_unique<TStreamRequestActor<T>>(
            std::move(callContext),
            std::move(request),
            std::move(responseHandler)));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateKikimrFileStore(
    IActorSystemPtr actorSystem,
    ui32 permanentActorCount)
{
    return std::make_shared<TKikimrFileStore>(
        std::move(actorSystem),
        permanentActorCount);
}

}   // namespace NCloud::NFileStore
