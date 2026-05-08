#include "service_actor.h"

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProtoPrivate::TUnsafeCreateNodeRequest ToTabletRequest(
    const NProto::TUnsafeCreateNodeRequest& request)
{
    NProtoPrivate::TUnsafeCreateNodeRequest result;
    result.SetFileSystemId(request.GetFileSystemId());
    result.MutableNode()->CopyFrom(request.GetNode());
    return result;
}

NProtoPrivate::TUnsafeDeleteNodeRequest ToTabletRequest(
    const NProto::TUnsafeDeleteNodeRequest& request)
{
    NProtoPrivate::TUnsafeDeleteNodeRequest result;
    result.SetFileSystemId(request.GetFileSystemId());
    result.SetId(request.GetId());
    return result;
}

NProtoPrivate::TUnsafeCreateNodeRefRequest ToTabletRequest(
    const NProto::TUnsafeCreateNodeRefRequest& request)
{
    NProtoPrivate::TUnsafeCreateNodeRefRequest result;
    result.SetFileSystemId(request.GetFileSystemId());
    result.SetParentId(request.GetParentId());
    result.SetName(request.GetName());
    result.SetChildId(request.GetChildId());
    result.SetShardId(request.GetShardId());
    result.SetShardNodeName(request.GetShardNodeName());
    return result;
}

NProtoPrivate::TUnsafeDeleteNodeRefRequest ToTabletRequest(
    const NProto::TUnsafeDeleteNodeRefRequest& request)
{
    NProtoPrivate::TUnsafeDeleteNodeRefRequest result;
    result.SetFileSystemId(request.GetFileSystemId());
    result.SetParentId(request.GetParentId());
    result.SetName(request.GetName());
    return result;
}

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
    const typename TServiceRequest::ProtoRecordType Request;

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

        auto request = std::make_unique<TTabletRequest>();
        request->Record = ToTabletRequest(Request);
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
        ReplyAndDie(ctx, ev->Get()->Record.GetError());
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

void TStorageServiceActor::HandleUnsafeDeleteNode(
    const TEvService::TEvUnsafeDeleteNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardUnsafeNodeRequest<
        TEvService::TEvUnsafeDeleteNodeRequest,
        TEvService::TEvUnsafeDeleteNodeResponse,
        TEvIndexTablet::TEvUnsafeDeleteNodeRequest,
        TEvIndexTablet::TEvUnsafeDeleteNodeResponse>(ctx, ev);
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

void TStorageServiceActor::HandleUnsafeDeleteNodeRef(
    const TEvService::TEvUnsafeDeleteNodeRefRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardUnsafeNodeRequest<
        TEvService::TEvUnsafeDeleteNodeRefRequest,
        TEvService::TEvUnsafeDeleteNodeRefResponse,
        TEvIndexTablet::TEvUnsafeDeleteNodeRefRequest,
        TEvIndexTablet::TEvUnsafeDeleteNodeRefResponse>(ctx, ev);
}

}   // namespace NCloud::NFileStore::NStorage
