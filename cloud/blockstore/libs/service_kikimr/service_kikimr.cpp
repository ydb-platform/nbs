#include "service_kikimr.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/actors/actor_pool.h>
#include <cloud/storage/core/libs/actors/pooled_actor.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/scheduler_cookie.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NBlockStore::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr EBlockStoreRequest Request = EBlockStoreRequest::name;\
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

// Per-request actor that sends a request into actor system and fulfills a
// promise with the response. The actor can be reused for multiple requests.
//
// Warning: this object mostly behaves like a regular
// actor, but c-tor and SendRequest() are called from vhost thread.
template <typename T>
class TRequestActor final : public IPooledActor<TRequestActor<T>>
{
    using TThis = TRequestActor<T>;

    using TRequest = typename T::TRequest;
    using TRequestProto = typename T::TRequestProto;

    using TResponse = typename T::TResponse;
    using TResponseProto = typename T::TResponseProto;

private:
    std::shared_ptr<TRequestProto> Request;
    TPromise<TResponseProto> Response;
    TCallContextPtr CallContext;
    TDuration RequestTimeout;
    TString DiskId;

    ui64 SeqNumber = 0;
    NActors::TSchedulerCookieHolder TimeoutCookie;

public:
    static constexpr const char ActorName[] =
        "NCloud::NBlockStore::NServer::TRequestActor<T>";

public:
    TRequestActor()
        : IPooledActor<TRequestActor<T>>(&TThis::StateWork)
    {}

    ~TRequestActor() override
    {
        if (this->CurrentStateFunc() == &TThis::StateWork) {
            TResponseProto response;

            auto& error = *response.MutableError();
            error.SetCode(E_REJECTED);
            error.SetMessage("Request actor destroyed");

            try {
                Response.SetValue(std::move(response));
            } catch (...) {
                LOG_WARN_S(
                    *this->GetActorSystem(),
                    TBlockStoreComponents::SERVICE_PROXY,
                    TRequestInfo(T::Request, CallContext->RequestId, DiskId)
                        << " Failed to set response value: "
                        << CurrentExceptionMessage());
            }
        }
    }

    void Reset() override {
        this->Become(&TThis::StateSleep);
        Request.reset();
        Response = {};
        CallContext.Reset();
        RequestTimeout = TDuration::Zero();
        DiskId.clear();
        SeqNumber++;
        TimeoutCookie.Detach();
    }

    void SendRequest(
        std::shared_ptr<TRequestProto> requestProto,
        TPromise<TResponseProto> response,
        TCallContextPtr callContext,
        TDuration requestTimeout)
    {
        this->Become(&TThis::StateWork);

        Response = std::move(response);
        CallContext = std::move(callContext);
        RequestTimeout = requestTimeout;
        DiskId = GetDiskId(*requestProto);

        LOG_TRACE_S(
            *this->GetActorSystem(),
            TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, CallContext->RequestId, DiskId)
                << " sending request");

        auto request =
            std::make_unique<TRequest>(CallContext, std::move(*requestProto));

        LWTRACK(
            RequestSent_Proxy,
            CallContext->LWOrbit,
            GetBlockStoreRequestName(T::Request),
            CallContext->RequestId);

        this->GetActorSystem()->Send(
            std::make_unique<IEventHandle>(
                MakeStorageServiceId(),   // recipient
                this->GetSelfId(),        // sender
                request.release(),
                0,   // flags
                SeqNumber));

        if (RequestTimeout && RequestTimeout != TDuration::Max()) {
            TimeoutCookie.Reset(ISchedulerCookie::Make2Way());
            this->GetActorSystem()->Schedule(
                RequestTimeout,
                std::make_unique<IEventHandle>(
                    this->GetSelfId(),   // recipient
                    this->GetSelfId(),   // sender
                    new TEvents::TEvWakeup(),
                    0,   // flags
                    SeqNumber),
                TimeoutCookie.Get());
        }
    }

private:
    void CompleteRequest(const TActorContext& ctx, TResponseProto&& response)
    {
        try {
            Response.SetValue(std::move(response));
        } catch (...) {
            LOG_ERROR_S(ctx, TBlockStoreComponents::SERVICE_PROXY,
                TRequestInfo(T::Request, CallContext->RequestId, DiskId)
                << " exception in callback: " << CurrentExceptionMessage());
        }

        this->Become(&TThis::StateSleep);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);
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

    STFUNC(StateSleep)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, HandleTimeout);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            IgnoreFunc(TResponse);

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
        if (ev->Cookie != SeqNumber) {
            LOG_WARN_S(
                *this->GetActorSystem(),
                TBlockStoreComponents::SERVICE_PROXY,
                TRequestInfo(T::Request, CallContext->RequestId, DiskId)
                    << " response received for wrong sequence number: "
                    << ev->Cookie << ", expected: " << SeqNumber);
            return;
        }

        auto* msg = ev->Get();

        LWTRACK(
            ResponseReceived_Proxy,
            CallContext->LWOrbit,
            GetBlockStoreRequestName(T::Request),
            CallContext->RequestId);

        LOG_TRACE_S(ctx, TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, CallContext->RequestId, DiskId)
            << " response received");

        CompleteRequest(ctx, std::move(msg->Record));

        this->WorkFinished(ctx);
    }

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        // TimeoutCookie should guarantee that the wakeup event is for the
        // correct sequence number
        Y_DEBUG_ABORT_UNLESS(ev->Cookie == SeqNumber);
        if (ev->Cookie != SeqNumber) {
            LOG_WARN_S(
                *this->GetActorSystem(),
                TBlockStoreComponents::SERVICE_PROXY,
                TRequestInfo(T::Request, CallContext->RequestId, DiskId)
                    << " wakeup received for wrong sequence number: "
                    << ev->Get()->Tag << ", expected: " << SeqNumber);
            return;
        }

        LOG_WARN_S(ctx, TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, CallContext->RequestId, DiskId)
            << " request wakeup timer hit");

        if constexpr (IsWriteRequest(T::Request)) {
            ReportServiceProxyWakeupTimerHit(
                {{"disk", DiskId}, {"RequestId", CallContext->RequestId}});
            return;
        }

        TResponseProto response;

        auto& error = *response.MutableError();
        error.SetCode(E_TIMEOUT);
        error.SetMessage("Timeout");

        CompleteRequest(ctx, std::move(response));

        this->WorkFinished(ctx);
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        TThis::Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TKikimrService final
    : public IBlockStore
{
private:
    const IActorSystemPtr ActorSystem;
    const NProto::TKikimrServiceConfig Config;

#define BLOCKSTORE_DECLARE_ACTOR_POOL(name, ...)                               \
    TIntrusivePtr<TActorPool<TRequestActor<T##name##Method>>> name##ActorPool;

    // BLOCKSTORE_DECLARE_ACTOR_POOL

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_DECLARE_ACTOR_POOL)

#undef BLOCKSTORE_DECLARE_ACTOR_POOL

    ui32 GetPoolSize(EBlockStoreRequest request) const
    {
        switch (request) {
            case EBlockStoreRequest::ReadBlocks:
            case EBlockStoreRequest::WriteBlocks:
            case EBlockStoreRequest::ZeroBlocks:
            case EBlockStoreRequest::ReadBlocksLocal:
            case EBlockStoreRequest::WriteBlocksLocal:
                return Config.GetHotPoolSize();
            default:
                return Config.GetColdPoolSize();
        }
    }

public:
    TKikimrService(
        IActorSystemPtr actorSystem,
        const NProto::TKikimrServiceConfig& config)
        : ActorSystem(std::move(actorSystem))
        , Config(config)
    {
#define BLOCKSTORE_MAKE_ACTOR_POOL(name, ...)                                  \
    name##ActorPool =                                                          \
        MakeIntrusive<TActorPool<TRequestActor<T##name##Method>>>(             \
            ActorSystem,                                                       \
            GetPoolSize(T##name##Method::Request));

        // BLOCKSTORE_MAKE_ACTOR_POOL

        BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_MAKE_ACTOR_POOL)

#undef BLOCKSTORE_MAKE_ACTOR_POOL
    }

    void Start() override {}

    void Stop() override
    {
#define BLOCKSTORE_STOP_ACTOR_POOL(name, ...)                                  \
        name##ActorPool->OnBeforeDestroy();

        // BLOCKSTORE_STOP_ACTOR_POOL

        BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_STOP_ACTOR_POOL)

#undef BLOCKSTORE_STOP_ACTOR_POOL
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
            std::move(ctx), std::move(request), response,                      \
            name##ActorPool.Get());                                            \
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

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename TMethod>
    void ExecuteRequest(
        TCallContextPtr ctx,
        std::shared_ptr<typename TMethod::TRequestProto> request,
        TPromise<typename TMethod::TResponseProto> response,
        TActorPool<TRequestActor<TMethod>>* pool)
    {
        const auto& headers = request->GetHeaders();
        auto timeout = TDuration::MilliSeconds(headers.GetRequestTimeout());

        auto* actor = pool->template GetPooledActor<TRequestActor<TMethod>>();
        if (!actor) {
            response.SetValue(
                TErrorResponse(E_REJECTED, "Failed to get actor from pool"));
            return;
        }
        actor->SendRequest(
            std::move(request),
            std::move(response),
            std::move(ctx),
            timeout);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateKikimrService(
    IActorSystemPtr actorSystem,
    const NProto::TKikimrServiceConfig& config)
{
    return std::make_shared<TKikimrService>(
        std::move(actorSystem),
        config);
}

}   // namespace NCloud::NBlockStore::NServer
