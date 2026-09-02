#include "service_kikimr.h"

#include "method_handler.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

#include <atomic>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NBlockStore::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

// Requests are handled in one of two ways. Without permanent actors, every
// request gets its own TRequestActor. Otherwise, requests are distributed among
// a fixed pool of THandlerActor instances. In the latter case request state is
// registered and the request is sent on the calling thread, while responses and
// timeouts are processed by the permanent actors on actor-system threads.

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr EBlockStoreRequest Request =                          \
            EBlockStoreRequest::name;                                          \
                                                                               \
        using TRequest = TEvService::TEv##name##Request;                       \
        using TRequestProto = NProto::T##name##Request;                        \
                                                                               \
        using TResponse = TEvService::TEv##name##Response;                     \
        using TResponseProto = NProto::T##name##Response;                      \
    };                                                                         \
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TRequestActor final: public TActorBootstrapped<TRequestActor<T>>
{
    using TThis = TRequestActor<T>;
    using TBase = TActorBootstrapped<TThis>;

    using TRequest = typename T::TRequest;
    using TRequestProto = typename T::TRequestProto;

    using TResponse = typename T::TResponse;
    using TResponseProto = typename T::TResponseProto;

private:
    std::shared_ptr<TRequestProto> Request;
    TPromise<TResponseProto> Response;
    TCallContextPtr CallContext;

    const TDuration RequestTimeout;
    const TString DiskId;

    bool RequestCompleted = false;

public:
    static constexpr const char ActorName[] =
        "NCloud::NBlockStore::NServer::TRequestActor<T>";

public:
    TRequestActor(
            std::shared_ptr<TRequestProto> request,
            TPromise<TResponseProto> response,
            TCallContextPtr callContext,
            TDuration requestTimeout)
        : Request(std::move(request))
        , Response(std::move(response))
        , CallContext(std::move(callContext))
        , RequestTimeout(requestTimeout)
        , DiskId(GetDiskId(*Request))
    {}

    ~TRequestActor() override
    {
        if (!RequestCompleted) {
            TResponseProto response;

            auto& error = *response.MutableError();
            error.SetCode(E_REJECTED);

            try {
                Response.SetValue(std::move(response));
            } catch (...) {
                // no way to log error message
            }

            RequestCompleted = true;
        }
    }

    void Bootstrap(const TActorContext& ctx)
    {
        TThis::Become(&TThis::StateWork);

        SendRequest(ctx);
    }

private:
    void SendRequest(const TActorContext& ctx)
    {
        LOG_TRACE_S(
            ctx,
            TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, CallContext->RequestId, DiskId)
                << " sending request");

        auto request =
            std::make_unique<TRequest>(CallContext, std::move(*Request));

        LWTRACK(
            RequestSent_Proxy,
            CallContext->LWOrbit,
            GetBlockStoreRequestName(T::Request),
            CallContext->RequestId);

        NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));

        if (RequestTimeout && RequestTimeout != TDuration::Max()) {
            ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());
        }
    }

    void CompleteRequest(const TActorContext& ctx, TResponseProto&& response)
    {
        try {
            Response.SetValue(std::move(response));
        } catch (...) {
            LOG_ERROR_S(
                ctx,
                TBlockStoreComponents::SERVICE_PROXY,
                TRequestInfo(T::Request, CallContext->RequestId, DiskId)
                    << " exception in callback: " << CurrentExceptionMessage());
        }

        RequestCompleted = true;
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);
            HFunc(TEvents::TEvWakeup, HandleTimeout);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::SERVICE_PROXY,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleResponse(
        const typename TResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LWTRACK(
            ResponseReceived_Proxy,
            CallContext->LWOrbit,
            GetBlockStoreRequestName(T::Request),
            CallContext->RequestId);

        LOG_TRACE_S(
            ctx,
            TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, CallContext->RequestId, DiskId)
                << " response received");

        CompleteRequest(ctx, std::move(msg->Record));

        TThis::Die(ctx);
    }

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, CallContext->RequestId, DiskId)
                << " request wakeup timer hit");

        if constexpr (IsWriteRequest(T::Request)) {
            // Write requests are a special case: do not time them out because
            // TVolumeActor already protects against overlapping requests.
            ReportServiceProxyWakeupTimerHit(
                {{"disk", DiskId}, {"RequestId", CallContext->RequestId}});
            return;
        }

        TResponseProto response;

        auto& error = *response.MutableError();
        error.SetCode(E_TIMEOUT);
        error.SetMessage("Timeout");

        CompleteRequest(ctx, std::move(response));

        TThis::Die(ctx);
    }
};

// Bridges calls arriving on service threads with replies handled by an actor.
// It is separate from THandlerActor because the actor system owns and may
// destroy the actor independently, while TKikimrService may still be entered by
// a request thread. Shared ownership keeps the request-tracking state alive for
// both sides.
struct THandler
{
    TActorId SelfId;
    TLog Log;
    // A zero cookie usually means that a responder did not copy the request
    // cookie. Should not issue it, so such a response cannot complete an
    // unrelated request by accident.
    std::atomic<ui64> Cookie{1};

    explicit THandler(TLog log)
        : Log(std::move(log))
    {}

#define BLOCKSTORE_SEND_REQUEST(name, ...)                                     \
    TMethodHandler<T##name##Method> name##Handler;                             \
    void SendRequest(                                                          \
        IActorSystem& actorSystem,                                             \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request,                     \
        TPromise<NProto::T##name##Response> response,                          \
        TDuration requestTimeout)                                              \
    {                                                                          \
        const ui64 cookie = Cookie.fetch_add(1, std::memory_order_relaxed);    \
        name##Handler.SendRequest(                                             \
            actorSystem,                                                       \
            Log,                                                               \
            std::move(callContext),                                            \
            std::move(request),                                                \
            std::move(response),                                               \
            requestTimeout,                                                    \
            SelfId,                                                            \
            cookie);                                                           \
    }                                                                          \
// BLOCKSTORE_SEND_REQUEST

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_SEND_REQUEST)

#undef BLOCKSTORE_SEND_REQUEST

    void HandleTimeout(ui64 tag, ui64 cookie, const TActorContext& ctx)
    {
#define BLOCKSTORE_HANDLE_TIMEOUT(name, ...)                                   \
    case TMethodHandler<T##name##Method>::TimeoutTag:                          \
        name##Handler.HandleTimeout(cookie, ctx);                              \
        return;                                                                \
// BLOCKSTORE_HANDLE_TIMEOUT

        switch (tag) {
            BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_HANDLE_TIMEOUT)

            default:
                return;
        }

#undef BLOCKSTORE_HANDLE_TIMEOUT
    }

    void RejectRequests()
    {
#define BLOCKSTORE_REJECT_REQUESTS(name, ...)                                  \
    name##Handler.RejectRequests(Log);                                         \
// BLOCKSTORE_REJECT_REQUESTS

        BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_REJECT_REQUESTS)

#undef BLOCKSTORE_REJECT_REQUESTS
    }
};

////////////////////////////////////////////////////////////////////////////////

class THandlerActor final: public TActor<THandlerActor>
{
    using TThis = THandlerActor;

private:
    // Shared with TKikimrService because the actor and service have independent
    // lifetimes and are owned by different subsystems.
    std::shared_ptr<THandler> Impl;

public:
    explicit THandlerActor(std::shared_ptr<THandler> impl)
        : TActor<THandlerActor>(&THandlerActor::StateWork)
        , Impl(std::move(impl))
    {}

    ~THandlerActor() override
    {
        Impl->RejectRequests();
    }

    static constexpr const char ActorName[] =
        "NCloud::NBlockStore::NServer::THandlerActor";

private:
#define BLOCKSTORE_HANDLE_RESPONSE_IMPL(name, ...)                             \
    HFunc(T##name##Method::TResponse, Impl->name##Handler.HandleResponse);

// BLOCKSTORE_HANDLE_RESPONSE_IMPL

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_HANDLE_RESPONSE_IMPL)
            HFunc(TEvents::TEvWakeup, HandleTimeout);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::SERVICE_PROXY,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

#undef BLOCKSTORE_HANDLE_RESPONSE_IMPL

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Impl->HandleTimeout(ev->Get()->Tag, ev->Cookie, ctx);
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Impl->RejectRequests();
        TThis::Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TKikimrService final: public IBlockStore
{
private:
    const IActorSystemPtr ActorSystem;
    const NProto::TKikimrServiceConfig Config;
    TLog Log;

    using THandlerPtr = std::shared_ptr<THandler>;

    // The actors are independent and each can handle every storage request
    // type. Concurrent callers are assigned to them round-robin.
    TVector<THandlerPtr> Handlers;

    std::atomic<ui32> HandlerSelector{0};

public:
    TKikimrService(
        IActorSystemPtr actorSystem,
        const NProto::TKikimrServiceConfig& config)
        : ActorSystem(std::move(actorSystem))
        , Config(config)
        , Log(ActorSystem->CreateLog("KIKIMR_SERVICE"))
    {}

    void Start() override
    {
        Y_ABORT_UNLESS(Handlers.empty(), "KikimrService already started");
        Handlers.resize(Config.GetPermanentActorCount());
        for (auto& handler: Handlers) {
            handler = std::make_shared<THandler>(Log);
            auto actorId =
                ActorSystem->Register(std::make_unique<THandlerActor>(handler));
            handler->SelfId = actorId;
        }
    }

    void Stop() override
    {
        for (const auto& handler: Handlers) {
            if (handler) {
                handler->RejectRequests();
            }
        }
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        auto response = NewPromise<NProto::T##name##Response>();               \
        ExecuteRequest<T##name##Method>(                                       \
            std::move(ctx),                                                    \
            std::move(request),                                                \
            response);                                                         \
        return response.GetFuture();                                           \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(ctx);                                                         \
        Y_UNUSED(request);                                                     \
        return MakeFuture<NProto::T##name##Response>(TErrorResponse(           \
            E_NOT_IMPLEMENTED,                                                 \
            "Method " #name " not implemeted"));                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_ENDPOINT_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)
    BLOCKSTORE_LOCAL_NVME_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename T>
    void ExecuteRequest(
        TCallContextPtr ctx,
        std::shared_ptr<typename T::TRequestProto> request,
        TPromise<typename T::TResponseProto> response)
    {
        const auto& headers = request->GetHeaders();
        auto timeout = TDuration::MilliSeconds(headers.GetRequestTimeout());

        if (!Handlers.empty()) {
            const ui32 handlerIdx =
                HandlerSelector.fetch_add(1, std::memory_order_relaxed);
            auto& handler = Handlers[handlerIdx % Handlers.size()];

            // The request is not sent to THandlerActor in the usual actor
            // fashion. Instead, the current service thread calls the shared
            // handler directly; it records the request and sends it to the
            // storage service using THandlerActor as the reply recipient.
            handler->SendRequest(
                *ActorSystem,
                std::move(ctx),
                std::move(request),
                std::move(response),
                timeout);
            return;
        }

        ActorSystem->Register(std::make_unique<TRequestActor<T>>(
            std::move(request),
            std::move(response),
            std::move(ctx),
            timeout));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateKikimrService(
    IActorSystemPtr actorSystem,
    const NProto::TKikimrServiceConfig& config)
{
    return std::make_shared<TKikimrService>(std::move(actorSystem), config);
}

}   // namespace NCloud::NBlockStore::NServer
