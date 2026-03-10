#include "service_kikimr.h"

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
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/thread/lfstack.h>

#include <variant>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NBlockStore::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DequeuePendingRequestsTag = 1;

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

template <typename TMethod>
struct TRequestContext
{
    using TRequestProto = typename TMethod::TRequestProto;
    using TResponseProto = typename TMethod::TResponseProto;

    std::shared_ptr<TRequestProto> Request;
    TPromise<TResponseProto> Response;
    TCallContextPtr CallContext;
    TDuration RequestTimeout;
    TString DiskId;
};

#define BLOCKSTORE_DECLARE_ITEM(name, ...)                                     \
    , TRequestContext<T##name##Method>                                         \
// BLOCKSTORE_DECLARE_ITEM

// Each BLOCKSTORE_DECLARE_ITEM expands with a leading comma,
// so we pass a dummy `void` as the first argument and discard it here.
template <typename, typename ... Ts>
using TRequestImpl = std::variant<Ts...>;

using TRequest = TRequestImpl<
    void
    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_DECLARE_ITEM)
>;

#undef BLOCKSTORE_DECLARE_ITEM

////////////////////////////////////////////////////////////////////////////////

class TPendingRequests
{
private:
    const IActorSystemPtr ActorSystem;

    TActorId TargetActorId;
    TLockFreeStack<TRequest> Requests;

    std::atomic_bool WakeupInProgress = false;

public:
    explicit TPendingRequests(IActorSystemPtr actorSystem)
        : ActorSystem(std::move(actorSystem))
    {}

    ~TPendingRequests()
    {
        Y_DEBUG_ABORT_UNLESS(Requests.IsEmpty());
    }

    void SetTarget(const TActorId& actorId)
    {
        TargetActorId = actorId;
    }

    void Shutdown()
    {
        ActorSystem->Send(
            TargetActorId,
            std::make_unique<TEvents::TEvPoisonPill>());
    }

    TVector<TRequest> DequeueAllSingleConsumer()
    {
        WakeupInProgress.store(false);

        TVector<TRequest> requests;
        Requests.DequeueAllSingleConsumer(&requests);

        return requests;
    }

    template <typename T>
    void PostRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequestProto> request,
        TPromise<typename T::TResponseProto> response)
    {
        Y_ABORT_UNLESS(TargetActorId);

        auto diskId = GetDiskId(*request);

        const auto& headers = request->GetHeaders();
        auto timeout = TDuration::MilliSeconds(headers.GetRequestTimeout());

        Requests.Enqueue(
            TRequestContext<T>{
                .Request = std::move(request),
                .Response = std::move(response),
                .CallContext = std::move(callContext),
                .RequestTimeout = timeout,
                .DiskId = std::move(diskId),
            });

        if (!WakeupInProgress.exchange(true)) {
            ActorSystem->Send(
                TargetActorId,
                std::make_unique<TEvents::TEvWakeup>(
                    DequeuePendingRequestsTag));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestActor final
    : public TActor<TRequestActor>
{
    using TRequestsInFlight = THashMap<ui64, TRequest>;

private:
    ui64 NextRequestId = 1;

    std::shared_ptr<TPendingRequests> PendingRequests;
    TRequestsInFlight RequestsInFlight;

public:
    static constexpr const char ActorName[] =
        "NCloud::NBlockStore::NServer::TRequestActor";

public:
    explicit TRequestActor(std::shared_ptr<TPendingRequests> pendingRequests)
        : TActor(&TThis::StateWork)
        , PendingRequests(std::move(pendingRequests))
    {}

    ~TRequestActor()
    {
        Y_DEBUG_ABORT_UNLESS(RequestsInFlight.empty());
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {

#define BLOCKSTORE_DECLARE_HFUNC(name, ...)                                    \
            HFunc(TEvService::TEv##name##Response, HandleResponse<T##name##Method>);\
// BLOCKSTORE_DECLARE_ITEM

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_DECLARE_HFUNC)

#undef BLOCKSTORE_DECLARE_HFUNC

            HFunc(TEvents::TEvWakeup, HandleWakeup);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::SERVICE_PROXY,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        for (auto& [_, req]: RequestsInFlight) {
            std::visit(
                [&]<typename T>(T&& req)
                {
                    typename T::TResponseProto response;

                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);

                    CompleteRequest(ctx, std::move(req), std::move(response));
                },
                std::move(req));
        }

        RequestsInFlight.clear();

        Die(ctx);
    }

    template <typename T>
    void CompleteRequest(
        const TActorContext& ctx,
        TRequestContext<T>&& req,
        typename T::TResponseProto&& response)
    {
        try {
            req.Response.SetValue(std::move(response));
        } catch (...) {
            LOG_ERROR_S(
                ctx,
                TBlockStoreComponents::SERVICE_PROXY,
                TRequestInfo(T::Request, req.CallContext->RequestId, req.DiskId)
                    << " exception in callback: " << CurrentExceptionMessage());
        }
    }

    template <typename T>
    void HandleResponse(
        const typename T::TResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();
        const ui64 requestId = ev->Cookie;
        auto it = RequestsInFlight.find(requestId);
        if (it == RequestsInFlight.end()) {
            return;
        }

        auto* req = std::get_if<TRequestContext<T>>(&it->second);
        Y_DEBUG_ABORT_UNLESS(req);

        if (req) {
            LWTRACK(
                ResponseReceived_Proxy,
                req->CallContext->LWOrbit,
                GetBlockStoreRequestName(T::Request),
                req->CallContext->RequestId);

            LOG_TRACE_S(
                ctx,
                TBlockStoreComponents::SERVICE_PROXY,
                TRequestInfo(
                    T::Request,
                    req->CallContext->RequestId,
                    req->DiskId)
                    << " response received");

            CompleteRequest(ctx, std::move(*req), std::move(msg->Record));
        }

        RequestsInFlight.erase(it);
    }

    template <typename T>
    void HandleTimeout(const TActorContext& ctx, TRequestContext<T>&& req)
    {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, req.CallContext->RequestId, req.DiskId)
                << " request wakeup timer hit");

        if constexpr (IsWriteRequest(T::Request)) {
            ReportServiceProxyWakeupTimerHit(
                {{"disk", req.DiskId},
                 {"RequestId", req.CallContext->RequestId}});
            return;
        }

        typename T::TResponseProto response;

        auto& error = *response.MutableError();
        error.SetCode(E_TIMEOUT);
        error.SetMessage("Timeout");

        CompleteRequest(ctx, std::move(req), std::move(response));
    }

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        if (ev->Get()->Tag == DequeuePendingRequestsTag) {
            SendPendingRequests(ctx);
            return;
        }

        // Timeout

        const ui64 requestId = ev->Cookie;

        auto it = RequestsInFlight.find(requestId);
        if (it == RequestsInFlight.end()) {
            return;
        }

        TRequest req = std::move(it->second);
        RequestsInFlight.erase(it);

        std::visit(
            [&](auto&& req) { HandleTimeout(ctx, std::move(req)); },
            std::move(req));
    }

    template <typename T>
    void SendRequest(const TActorContext& ctx, TRequestContext<T>&& req)
    {
        const ui64 requestId = NextRequestId++;

        LOG_TRACE_S(ctx, TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, req.CallContext->RequestId, req.DiskId)
            << " sending request");

        auto request = std::make_unique<typename T::TRequest>(
            req.CallContext,
            std::move(*req.Request));

        LWTRACK(
            RequestSent_Proxy,
            req.CallContext->LWOrbit,
            GetBlockStoreRequestName(T::Request),
            req.CallContext->RequestId);

        NCloud::Send(
            ctx,
            MakeStorageServiceId(),
            std::move(request),
            requestId);

        if (req.RequestTimeout && req.RequestTimeout != TDuration::Max()) {
            ctx.Schedule(
                req.RequestTimeout,
                std::make_unique<IEventHandle>(
                    ctx.SelfID,
                    ctx.SelfID,
                    new TEvents::TEvWakeup(),
                    0,  // flags
                    requestId));
        }

        RequestsInFlight[requestId] = std::move(req);
    }

    void SendPendingRequests(const TActorContext& ctx)
    {
        for (auto& req: PendingRequests->DequeueAllSingleConsumer()) {
            std::visit(
                [&](auto&& req) { SendRequest(ctx, std::move(req)); },
                std::move(req));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TKikimrService final
    : public IBlockStore
{
private:
    const IActorSystemPtr ActorSystem;
    const NProto::TKikimrServiceConfig Config;
    TVector<std::shared_ptr<TPendingRequests>> PendingRequests;

public:
    TKikimrService(
            IActorSystemPtr actorSystem,
            const NProto::TKikimrServiceConfig& config)
        : ActorSystem(std::move(actorSystem))
        , Config(config)
    {}

    void Start() override
    {
        // XXX
        const ui32 count = 10; // Max<ui32>(1, Config.GetLongLiveRequestActorsCount());

        PendingRequests.reserve(count);

        for (ui32 i = 0; i != count; ++i) {
            auto& pr = PendingRequests.emplace_back(
                std::make_shared<TPendingRequests>(ActorSystem));
            auto actorId =
                ActorSystem->Register(std::make_unique<TRequestActor>(pr));
            pr->SetTarget(actorId);
        }
    }

    void Stop() override
    {
        for (auto& pr: PendingRequests) {
            pr->Shutdown();
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
            std::move(ctx), std::move(request), response);                     \
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
        Y_DEBUG_ABORT_UNLESS(!PendingRequests.empty());

        const auto i = ctx->RequestId % PendingRequests.size();

        PendingRequests[i]->template PostRequest<T>(
            std::move(ctx),
            std::move(request),
            std::move(response));
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
