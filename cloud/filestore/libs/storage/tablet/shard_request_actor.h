#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <util/string/join.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
class TShardRequestActor final
    : public NActors::TActorBootstrapped<TShardRequestActor<TRequest, TResponse>>
{
private:
    const TString LogTag;
    const NActors::TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const TRequest::ProtoRecordType Request;
    const TVector<TString> ShardIds;
    std::unique_ptr<TResponse> Response;
    ui32 ProcessedShards = 0;

    NProto::TProfileLogRequestInfo ProfileLogRequest;

    using TBase =
        NActors::TActorBootstrapped<TShardRequestActor<TRequest, TResponse>>;

public:
    TShardRequestActor(
        TString logTag,
        NActors::TActorId tablet,
        TRequestInfoPtr requestInfo,
        TRequest::ProtoRecordType request,
        TVector<TString> shardIds,
        std::unique_ptr<TResponse> response);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(TResponse, HandleResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::TABLET_WORKER,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void SendRequests(const NActors::TActorContext& ctx);

    void HandleResponse(
        const TResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
TShardRequestActor<TRequest, TResponse>::TShardRequestActor(
        TString logTag,
        NActors::TActorId tablet,
        TRequestInfoPtr requestInfo,
        TRequest::ProtoRecordType request,
        TVector<TString> shardIds,
        std::unique_ptr<TResponse> response)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , ShardIds(std::move(shardIds))
    , Response(std::move(response))
{}

template <typename TRequest, typename TResponse>
void TShardRequestActor<TRequest, TResponse>::Bootstrap(
    const NActors::TActorContext& ctx)
{
    SendRequests(ctx);
    TBase::Become(&TShardRequestActor<TRequest, TResponse>::StateWork);
}

template <typename TRequest, typename TResponse>
void TShardRequestActor<TRequest, TResponse>::SendRequests(
    const NActors::TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& shardId: ShardIds) {
        auto request = std::make_unique<TRequest>();
        request->Record = Request;
        request->Record.SetFileSystemId(shardId);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Sending %s to shard %s",
            LogTag.c_str(),
            Request.GetTypeName().Quote().c_str(),
            shardId.c_str());

        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            {}, // flags
            cookie++);
    }
}

template <typename TRequest, typename TResponse>
void TShardRequestActor<TRequest, TResponse>::HandleResponse(
    const TResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s %s failed for shard %s with error %s",
            LogTag.c_str(),
            Request.GetTypeName().Quote().c_str(),
            ShardIds[ev->Cookie].c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s %s succeeded for shard %s",
        LogTag.c_str(),
        Request.GetTypeName().Quote().c_str(),
        ShardIds[ev->Cookie].c_str());

    if (++ProcessedShards == ShardIds.size()) {
        ReplyAndDie(ctx, {});
    }
}

template <typename TRequest, typename TResponse>
void TShardRequestActor<TRequest, TResponse>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

template <typename TRequest, typename TResponse>
void TShardRequestActor<TRequest, TResponse>::ReplyAndDie(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    if (RequestInfo) {
        if (HasError(error)) {
            Response = std::make_unique<TResponse>(error);
        }

        if (Response) {
            NCloud::Reply(ctx, *RequestInfo, std::move(Response));
        } else {
            Y_DEBUG_ABORT_UNLESS(0);
        }
    }

    using TCompletion = TEvIndexTabletPrivate::TEvShardRequestCompleted;
    auto completion = std::make_unique<TCompletion>(error);
    NCloud::Send(ctx, Tablet, std::move(completion));

    TBase::Die(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
