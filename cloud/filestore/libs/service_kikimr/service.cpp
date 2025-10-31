#include "service.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

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

public:
    TKikimrFileStore(IActorSystemPtr actorSystem)
        : ActorSystem(std::move(actorSystem))
    {}

    void Start() override
    {}

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

IFileStoreServicePtr CreateKikimrFileStore(IActorSystemPtr actorSystem)
{
    return std::make_shared<TKikimrFileStore>(std::move(actorSystem));
}

}   // namespace NCloud::NFileStore
