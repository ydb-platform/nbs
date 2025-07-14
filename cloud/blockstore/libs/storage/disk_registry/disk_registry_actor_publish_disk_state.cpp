#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TDiskState OverrideDiskState(NProto::TDiskState state)
{
    switch (state.GetState()) {
        case NProto::DISK_STATE_WARNING:
            state.SetState(NProto::DISK_STATE_ONLINE);
            break;
        case NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE:
            state.SetState(NProto::DISK_STATE_ERROR);
            break;
        default:
            break;
    }

    return state;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::PublishDiskStates(const TActorContext& ctx)
{
    if (DiskStatesPublicationInProgress || State->GetDiskStateUpdates().empty()) {
        return;
    }

    DiskStatesPublicationInProgress = true;

    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvPublishDiskStatesRequest>();

    auto deadline = Min(DiskStatesPublicationStartTs, ctx.Now()) + TDuration::Seconds(5);
    if (deadline > ctx.Now()) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Scheduled disk state updates publication, now: %lu, deadline: %lu",
            TabletID(),
            ctx.Now().MicroSeconds(),
            deadline.MicroSeconds());

        ctx.Schedule(
            deadline,
            std::make_unique<IEventHandle>(ctx.SelfID, ctx.SelfID, request.get()));
        request.release();
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Sending disk state updates request",
            TabletID());

        NCloud::Send(ctx, ctx.SelfID, std::move(request));
    }
}

void TDiskRegistryActor::HandlePublishDiskStates(
    const TEvDiskRegistryPrivate::TEvPublishDiskStatesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(PublishDiskStates);

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Disk state updates request. Updates=%d",
        TabletID(),
        State->GetDiskStateUpdates().size());

    DiskStatesPublicationStartTs = ctx.Now();

    if (State->GetDiskStateUpdates().empty()) {
        auto response = std::make_unique<TEvDiskRegistryPrivate::TEvPublishDiskStatesResponse>(
            ev->Get()->CallContext,
            0);
        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext
    );

    auto* actorSystem = ctx.ActorSystem();
    auto replyFrom = ctx.SelfID;

    TVector<NLogbroker::TMessage> messages;
    messages.reserve(State->GetDiskStateUpdates().size());

    for (const auto& u: State->GetDiskStateUpdates()) {
        const auto& state = OverrideDiskState(u.State);

        TString s;
        const bool ok = state.SerializeToString(&s);
        Y_DEBUG_ABORT_UNLESS(ok);
        messages.push_back({ std::move(s), u.SeqNo });
    }

    const ui64 maxSeqNo = messages.back().SeqNo;

    LogbrokerService->Write(std::move(messages), DiskStatesPublicationStartTs)
        .Subscribe([=] (const auto& future) {
            auto response = std::make_unique<TEvDiskRegistryPrivate::TEvPublishDiskStatesResponse>(
                future.GetValue(),
                requestInfo->CallContext,
                maxSeqNo);

            actorSystem->Send(
                new IEventHandle(
                    requestInfo->Sender,
                    replyFrom,
                    response.release(),
                    0,          // flags
                    requestInfo->Cookie));
        });
}

void TDiskRegistryActor::HandlePublishDiskStatesResponse(
    const TEvDiskRegistryPrivate::TEvPublishDiskStatesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReportPublishDiskStateError(
            TStringBuilder()
            << TabletID() << " Failed to publish disk state. Error="
            << FormatError(msg->GetError()));

        DiskStatesPublicationInProgress = false;
        PublishDiskStates(ctx);

        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Publish disk state completed",
        TabletID());

    ExecuteTx<TDeleteDiskStateUpdates>(
        ctx,
        CreateRequestInfo<TEvDiskRegistryPrivate::TPublishDiskStatesMethod>(
            ev->Sender,
            ev->Cookie,
            msg->CallContext
        ),
        msg->MaxSeqNo
    );
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareDeleteDiskStateUpdates(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteDiskStateUpdates& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteDeleteDiskStateUpdates(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteDiskStateUpdates& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    State->DeleteDiskStateUpdate(db, args.MaxSeqNo);
}

void TDiskRegistryActor::CompleteDeleteDiskStateUpdates(
    const TActorContext& ctx,
    TTxDiskRegistry::TDeleteDiskStateUpdates& args)
{
    Y_UNUSED(args);

    DiskStatesPublicationInProgress = false;
    PublishDiskStates(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
