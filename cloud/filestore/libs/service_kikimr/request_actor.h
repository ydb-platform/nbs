#pragma once

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TRequestActor final
    : public NActors::TActorBootstrapped<TRequestActor<TMethod>>
{
    using TThis = TRequestActor<TMethod>;
    using TBase = NActors::TActorBootstrapped<TThis>;

    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

    using TRequestEvent = typename TMethod::TRequestEvent;
    using TResponseEvent = typename TMethod::TResponseEvent;

private:
    TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    NThreading::TPromise<TResponse> Response;

public:
    static constexpr const char ActorName[] =
        "NCloud::NFileStore::TRequestActor<T>";

public:
    TRequestActor(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            NThreading::TPromise<TResponse> response)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , Response(std::move(response))
    {}

    void Bootstrap(const NActors::TActorContext& ctx)
    {
        TThis::Become(&TThis::StateWork);

        SendRequest(ctx);
    }

private:
    void SendRequest(const NActors::TActorContext& ctx)
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
        const NActors::TActorContext& ctx)
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

}   // namespace NCloud::NFileStore::NStorage
