#include "service_actor.h"

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TServiceRequest, typename TServiceResponse, typename TTabletRequest, typename TTabletResponse>
class TUnsafeNodeActionActor final
    : public TActorBootstrapped<TUnsafeNodeActionActor<
          TServiceRequest,
          TServiceResponse,
          TTabletRequest,
          TTabletResponse>>
{
private:
    using TThis = TUnsafeNodeActionActor<
        TServiceRequest,
        TServiceResponse,
        TTabletRequest,
        TTabletResponse>;
    using TBase = TActorBootstrapped<TThis>;

    const TRequestInfoPtr RequestInfo;
    typename TServiceRequest::ProtoRecordType Request;

public:
    TUnsafeNodeActionActor(
            TRequestInfoPtr requestInfo,
            typename TServiceRequest::ProtoRecordType request)
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
            std::make_unique<TTabletRequest>(RequestInfo->CallContext);
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
            HFunc(TTabletResponse, HandleResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleResponse(
        const typename TTabletResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto response = std::make_unique<TServiceResponse>();
        response->Record = std::move(ev->Get()->Record);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        TBase::Die(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error)
    {
        auto response = std::make_unique<TServiceResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        TBase::Die(ctx);
    }
};

template <typename TServiceRequest, typename TServiceResponse, typename TTabletRequest, typename TTabletResponse>
void ForwardUnsafeNodeRequest(
    const TActorContext& ctx,
    const typename TServiceRequest::TPtr& ev)
{
    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    using TActor = TUnsafeNodeActionActor<
        TServiceRequest,
        TServiceResponse,
        TTabletRequest,
        TTabletResponse>;

    ctx.Register(new TActor(std::move(requestInfo), std::move(msg->Record)));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleUnsafeCreateNode(
    const TEvService::TEvUnsafeCreateNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardUnsafeNodeRequest<
        TEvService::TEvUnsafeCreateNodeRequest,
        TEvService::TEvUnsafeCreateNodeResponse,
        TEvIndexTablet::TEvUnsafeCreateNodeRequest,
        TEvIndexTablet::TEvUnsafeCreateNodeResponse>(ctx, ev);
}

void TStorageServiceActor::HandleUnsafeCreateNodeRef(
    const TEvService::TEvUnsafeCreateNodeRefRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardUnsafeNodeRequest<
        TEvService::TEvUnsafeCreateNodeRefRequest,
        TEvService::TEvUnsafeCreateNodeRefResponse,
        TEvIndexTablet::TEvUnsafeCreateNodeRefRequest,
        TEvIndexTablet::TEvUnsafeCreateNodeRefResponse>(ctx, ev);
}
}   // namespace NCloud::NFileStore::NStorage
