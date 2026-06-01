#pragma once

#include "methods.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TMethodHandler final
{
    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

    using TRequestEvent = typename TMethod::TRequestEvent;
    using TResponseEvent = typename TMethod::TResponseEvent;

private:
    THashMap<ui64, NThreading::TPromise<TResponse>> Responses;
    ui64 Cookie = 0;
    TAdaptiveLock Lock;

public:
    void SendRequest(
        IActorSystem& actorSystem,
        TCallContextPtr callContext,
        const TRequest& request,
        NThreading::TPromise<TResponse> promise,
        NActors::TActorId actorId)
    {
        ui64 cookie = 0;
        with_lock (Lock) {
            cookie = ++Cookie;
            Responses[cookie] = std::move(promise);
        }

        auto event = std::make_unique<NActors::IEventHandle>(
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
        const NActors::TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_TRACE_S(ctx, TFileStoreComponents::SERVICE_PROXY,
            TMethod::RequestName << " response received");

        NThreading::TPromise<TResponse> promise;
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
    NActors::TActorId SelfId;

public:
#define FILESTORE_SEND_REQUEST(name, ...)                                      \
    TMethodHandler<T##name##ServiceMethod> name##Handler;                      \
    void SendRequest(                                                          \
        IActorSystem& actorSystem,                                             \
        TCallContextPtr callContext,                                           \
        NProto::T##name##Request request,                                      \
        NThreading::TPromise<T##name##ServiceMethod::TResponse> promise)       \
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
};

////////////////////////////////////////////////////////////////////////////////

class THandlerActor final
    : public NActors::TActor<THandlerActor>
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
    HFunc(                                                                     \
        T##name##ServiceMethod::TResponseEvent,                                \
        Impl->name##Handler.HandleResponse);                                   \
// FILESTORE_HANDLE_RESPONSE_IMPL

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            FILESTORE_REMOTE_SERVICE(FILESTORE_HANDLE_RESPONSE_IMPL)

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

}   // namespace NCloud::NFileStore::NStorage
