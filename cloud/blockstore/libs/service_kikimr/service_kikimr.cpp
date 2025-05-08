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

template <typename T>
class TRequestActor final
    : public TActorBootstrapped<TRequestActor<T>>
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
        LOG_TRACE_S(ctx, TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, CallContext->RequestId, DiskId)
            << " sending request");

        auto request = std::make_unique<TRequest>(
            CallContext,
            std::move(*Request));

        LWTRACK(
            RequestSent_Proxy,
            CallContext->LWOrbit,
            GetBlockStoreRequestName(T::Request),
            CallContext->RequestId);

        NCloud::Send(
            ctx,
            MakeStorageServiceId(),
            std::move(request));

        if (RequestTimeout && RequestTimeout != TDuration::Max()) {
            ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());
        }
    }

    void CompleteRequest(const TActorContext& ctx, TResponseProto&& response)
    {
        try {
            Response.SetValue(std::move(response));
        } catch (...) {
            LOG_ERROR_S(ctx, TBlockStoreComponents::SERVICE_PROXY,
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

        LOG_TRACE_S(ctx, TBlockStoreComponents::SERVICE_PROXY,
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

        LOG_WARN_S(ctx, TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(T::Request, CallContext->RequestId, DiskId)
            << " request wakeup timer hit");

        if constexpr (IsWriteRequest(T::Request)) {
            ReportServiceProxyWakeupTimerHit();
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

////////////////////////////////////////////////////////////////////////////////

class TKikimrService final
    : public IBlockStore
{
private:
    const IActorSystemPtr ActorSystem;
    const NProto::TKikimrServiceConfig Config;

public:
    TKikimrService(
            IActorSystemPtr actorSystem,
            const NProto::TKikimrServiceConfig& config)
        : ActorSystem(std::move(actorSystem))
        , Config(config)
    {}

    void Start() override {}
    void Stop() override {}

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
    return std::make_shared<TKikimrService>(
        std::move(actorSystem),
        config);
}

}   // namespace NCloud::NBlockStore::NServer
