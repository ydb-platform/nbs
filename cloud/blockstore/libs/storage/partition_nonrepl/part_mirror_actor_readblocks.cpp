#include "part_mirror_actor.h"

#include "mirror_request_actor.h"

#include <cloud/blockstore/libs/storage/api/undelivered.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TRequestActor final
    : public NActors::TActorBootstrapped<TRequestActor<TMethod>>
{
private:
    const TRequestInfoPtr RequestInfo;
    const NActors::TActorId Partition;
    typename TMethod::TRequest::ProtoRecordType Request;
    const TString DiskId;

    using TResponseProto = typename TMethod::TResponse::ProtoRecordType;
    using TBase = NActors::TActorBootstrapped<TRequestActor<TMethod>>;

public:
    TRequestActor(
        TRequestInfoPtr requestInfo,
        const NActors::TActorId& partition,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendRequest(const NActors::TActorContext& ctx);
    bool HandleError(const NActors::TActorContext& ctx, NProto::TError error);
    void Done(const NActors::TActorContext& ctx, TResponseProto record);

private:
    STFUNC(StateWork);

    void HandleResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TRequestActor<TMethod>::TRequestActor(
        TRequestInfoPtr requestInfo,
        const NActors::TActorId& partition,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId)
    : RequestInfo(std::move(requestInfo))
    , Partition(partition)
    , Request(std::move(request))
    , DiskId(std::move(diskId))
{}

template <typename TMethod>
void TRequestActor<TMethod>::Bootstrap(const NActors::TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    SendRequest(ctx);
}

template <typename TMethod>
void TRequestActor<TMethod>::SendRequest(const NActors::TActorContext& ctx)
{
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->CallContext = RequestInfo->CallContext;
    request->Record = std::move(Request);

    auto event = std::make_unique<IEventHandle>(
        Partition,
        ctx.SelfID,
        request.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,
        RequestInfo->Cookie,
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());
}

template <typename TMethod>
void TRequestActor<TMethod>::Done(
    const NActors::TActorContext& ctx,
    TResponseProto record)
{
    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(record);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TRequestActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[%s] %s request undelivered to some nonrepl partitions",
        DiskId.c_str(),
        TMethod::Name);

    TResponseProto record;
    record.MutableError()->CopyFrom(MakeError(E_REJECTED, TStringBuilder()
        << TMethod::Name << " request undelivered to some nonrepl partitions"));

    Done(ctx, std::move(record));
}

template <typename TMethod>
void TRequestActor<TMethod>::HandleResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto& record = msg->Record;
    if (HasError(record)) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "[%s] %s got error from nonreplicated partition: %s",
            DiskId.c_str(),
            TMethod::Name,
            FormatError(record.GetError()).c_str());
    }

    ProcessMirrorActorError(*record.MutableError());

    Done(ctx, std::move(record));
}

template <typename TMethod>
void TRequestActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    TResponseProto record;
    record.MutableError()->CopyFrom(MakeError(E_REJECTED, "Dead"));
    Done(ctx, std::move(record));
}

template <typename TMethod>
STFUNC(TRequestActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMirrorPartitionActor::ReadBlocks(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);

    if (HasError(Status)) {
        Reply(
            ctx,
            *requestInfo,
            std::make_unique<typename TMethod::TResponse>(Status));

        return;
    }

    auto& record = ev->Get()->Record;

    const auto blockRange = TBlockRange64::WithLength(
        record.GetStartIndex(),
        record.GetBlocksCount());


    if (ResyncRangeStarted && GetScrubbingRange().Overlaps(blockRange)) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Request " << TMethod::Name
                    << " intersects with currently resyncing range"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    TActorId replicaActorId;
    const auto error = State.NextReadReplica(blockRange, &replicaActorId);
    if (HasError(error)) {
        Reply(
            ctx,
            *requestInfo,
            std::make_unique<typename TMethod::TResponse>(error));

        return;
    }

    NCloud::Register<TRequestActor<TMethod>>(
        ctx,
        std::move(requestInfo),
        replicaActorId,
        std::move(record),
        State.GetReplicaInfos()[0].Config->GetName());
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadBlocks<TEvService::TReadBlocksMethod>(ev, ctx);
}

void TMirrorPartitionActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadBlocks<TEvService::TReadBlocksLocalMethod>(ev, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
