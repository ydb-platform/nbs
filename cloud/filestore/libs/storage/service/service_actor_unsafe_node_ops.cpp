#include "service_actor.h"

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUnsafeCreateNodeActor final
    : public TActorBootstrapped<TUnsafeCreateNodeActor>
{
private:
    using TThis = TUnsafeCreateNodeActor;
    using TBase = TActorBootstrapped<TThis>;

    const TRequestInfoPtr RequestInfo;
    NProto::TUnsafeCreateNodeRequest Request;

public:
    TUnsafeCreateNodeActor(
            TRequestInfoPtr requestInfo,
            NProto::TUnsafeCreateNodeRequest request)
        : RequestInfo(std::move(requestInfo))
        , Request(std::move(request))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        if (!Request.GetFileSystemId()) {
            ReplyAndDie(
                ctx,
                MakeError(E_ARGUMENT, "FileSystem id should be supplied"));
            return;
        }

        auto request =
            std::make_unique<TEvIndexTablet::TEvUnsafeCreateNodeRequest>(
                RequestInfo->CallContext);
        request->Record = std::move(Request);
        NCloud::Send(
            ctx,
            MakeIndexTabletProxyServiceId(),
            std::move(request));

        TBase::Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvIndexTablet::TEvUnsafeCreateNodeResponse, HandleResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleResponse(
        const TEvIndexTablet::TEvUnsafeCreateNodeResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto response =
            std::make_unique<TEvService::TEvUnsafeCreateNodeResponse>();
        response->Record = std::move(ev->Get()->Record);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        TBase::Die(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error)
    {
        auto response =
            std::make_unique<TEvService::TEvUnsafeCreateNodeResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        TBase::Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnsafeCreateNodeRefActor final
    : public TActorBootstrapped<TUnsafeCreateNodeRefActor>
{
private:
    using TThis = TUnsafeCreateNodeRefActor;
    using TBase = TActorBootstrapped<TThis>;

    const TRequestInfoPtr RequestInfo;
    NProto::TUnsafeCreateNodeRefRequest Request;

public:
    TUnsafeCreateNodeRefActor(
            TRequestInfoPtr requestInfo,
            NProto::TUnsafeCreateNodeRefRequest request)
        : RequestInfo(std::move(requestInfo))
        , Request(std::move(request))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        if (!Request.GetFileSystemId()) {
            ReplyAndDie(
                ctx,
                MakeError(E_ARGUMENT, "FileSystem id should be supplied"));
            return;
        }

        auto request =
            std::make_unique<TEvIndexTablet::TEvUnsafeCreateNodeRefRequest>(
                RequestInfo->CallContext);
        request->Record = std::move(Request);
        NCloud::Send(
            ctx,
            MakeIndexTabletProxyServiceId(),
            std::move(request));

        TBase::Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvIndexTablet::TEvUnsafeCreateNodeRefResponse,
                HandleResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleResponse(
        const TEvIndexTablet::TEvUnsafeCreateNodeRefResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto response =
            std::make_unique<TEvService::TEvUnsafeCreateNodeRefResponse>();
        response->Record = std::move(ev->Get()->Record);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        TBase::Die(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error)
    {
        auto response =
            std::make_unique<TEvService::TEvUnsafeCreateNodeRefResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        TBase::Die(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleUnsafeCreateNode(
    const TEvService::TEvUnsafeCreateNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ctx.Register(new TUnsafeCreateNodeActor(
        std::move(requestInfo),
        std::move(msg->Record)));
}

void TStorageServiceActor::HandleUnsafeCreateNodeRef(
    const TEvService::TEvUnsafeCreateNodeRefRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ctx.Register(new TUnsafeCreateNodeRefActor(
        std::move(requestInfo),
        std::move(msg->Record)));
}
}   // namespace NCloud::NFileStore::NStorage
