#include "service.h"

#include "handler_actor.h"
#include "methods.h"
#include "request_actor.h"
#include "side_channel.h"
#include "stream_request_actor.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/kikimr/actorsystem.h>

namespace NCloud::NFileStore {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NFileStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TKikimrFileStore final
    : public IFileStoreService
{
private:
    const IActorSystemPtr ActorSystem;
    TLog Log;

    ISideChannelPtr SideChannel;

    using THandlerPtr = std::shared_ptr<THandler>;
    TVector<THandlerPtr> Handlers;
    std::atomic<ui32> Selector{0};

public:
    TKikimrFileStore(
            IActorSystemPtr actorSystem,
            ISideChannelPtr sideChannel,
            ui32 permanentActorCount)
        : ActorSystem(std::move(actorSystem))
        , Log(ActorSystem->CreateLog("KIKIMR_SERVICE"))
        , SideChannel(std::move(sideChannel))
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
        ExecuteRequest<T##name##ServiceMethod>(                                \
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
        IResponseHandlerPtr<NProto::TGetSessionEventsResponse>
        responseHandler) override
    {
        ExecuteStreamRequest<TGetSessionEventsServiceMethod>(
            std::move(callContext),
            std::move(request),
            std::move(responseHandler));
    }

    TFuture<NProto::TReadDataLocalResponse> ReadDataLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataLocalRequest> request) override
    {
        auto response = NewPromise<NProto::TReadDataResponse>();
        ExecuteRequest<TReadDataServiceMethod>(
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
        ExecuteRequest<TWriteDataServiceMethod>(
            std::move(callContext),
            std::move(request),
            response);
        return response.GetFuture();
    }

private:
    template <typename T>
    void ExecuteRequestImpl(
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

    template <typename T>
    void ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request,
        TPromise<typename T::TResponse> response)
    {
        ExecuteRequestImpl<T>(
            std::move(callContext),
            std::move(request),
            std::move(response));
    }

    template<>
    void ExecuteRequest<TFsyncServiceMethod>(
        TCallContextPtr callContext,
        std::shared_ptr<TFsyncServiceMethod::TRequest> request,
        TPromise<TFsyncServiceMethod::TResponse> response)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        Y_UNUSED(TFsyncServiceMethod::RequestName);

        response.SetValue(TFsyncServiceMethod::TResponse());
    }

    template<>
    void ExecuteRequest<TFsyncDirServiceMethod>(
        TCallContextPtr callContext,
        std::shared_ptr<TFsyncDirServiceMethod::TRequest> request,
        TPromise<TFsyncDirServiceMethod::TResponse> response)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        Y_UNUSED(TFsyncDirServiceMethod::RequestName);

        response.SetValue(TFsyncDirServiceMethod::TResponse());
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

    template <typename T>
    void ExecuteRequestWithSideChannel(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request,
        TPromise<typename T::TResponse> response)
    {
        if (SideChannel) {
            if (SideChannel->ExecuteRequest(callContext, request, response)) {
                return;
            }

            response.GetFuture().Subscribe(
                [sc = SideChannel] (TFuture<typename T::TResponse> f) {
                    const auto& i = f.GetValue().GetHeaders().GetBackendInfo();
                    sc->Update(i);
                });
        }

        ExecuteRequestImpl<T>(
            std::move(callContext),
            std::move(request),
            std::move(response));
    }

    template <>
    void ExecuteRequest<TReadDataServiceMethod>(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request,
        TPromise<NProto::TReadDataResponse> response)
    {
        ExecuteRequestWithSideChannel<TReadDataServiceMethod>(
            std::move(callContext),
            std::move(request),
            std::move(response));
    }

    template <>
    void ExecuteRequest<TWriteDataServiceMethod>(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request,
        TPromise<NProto::TWriteDataResponse> response)
    {
        ExecuteRequestWithSideChannel<TWriteDataServiceMethod>(
            std::move(callContext),
            std::move(request),
            std::move(response));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateKikimrFileStore(
    IActorSystemPtr actorSystem,
    ISideChannelPtr sideChannel,
    ui32 permanentActorCount)
{
    return std::make_shared<TKikimrFileStore>(
        std::move(actorSystem),
        std::move(sideChannel),
        permanentActorCount);
}

}   // namespace NCloud::NFileStore
