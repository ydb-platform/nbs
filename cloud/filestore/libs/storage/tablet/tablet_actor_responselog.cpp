#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDeleteResponseLogEntry(
    const TEvIndexTablet::TEvDeleteResponseLogEntryRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddInFlightRequest<TEvIndexTablet::TDeleteResponseLogEntryMethod>(
        *requestInfo);

    ExecuteTx<TDeleteResponseLogEntries>(
        ctx,
        std::move(requestInfo),
        TVector<TInternalRequestId>(
           {{msg->Record.GetClientTabletId(), msg->Record.GetRequestId()}}));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DeleteResponseLogEntries(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteResponseLogEntries& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_DeleteResponseLogEntries(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteResponseLogEntries& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    for (const auto& x: args.InternalRequestIds) {
        DeleteResponseLogEntry(db, x.ClientTabletId, x.RequestId);
    }
}

void TIndexTabletActor::CompleteTx_DeleteResponseLogEntries(
    const TActorContext& ctx,
    TTxIndexTablet::TDeleteResponseLogEntries& args)
{
    for (const auto& x: args.InternalRequestIds) {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s DeleteResponseLogEntries completed (%lu, %lu)",
            LogTag.c_str(),
            x.ClientTabletId,
            x.RequestId);
    }

    if (args.RequestInfo) {
        RemoveInFlightRequest(*args.RequestInfo);
        using TResponse = TEvIndexTablet::TEvDeleteResponseLogEntryResponse;
        auto response = std::make_unique<TResponse>();
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGetResponseLogEntry(
    const TEvIndexTablet::TEvGetResponseLogEntryRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddInFlightRequest<TEvIndexTablet::TGetResponseLogEntryMethod>(
        *requestInfo);

    ExecuteTx<TGetResponseLogEntry>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetClientTabletId(),
        msg->Record.GetRequestId());
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_GetResponseLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TGetResponseLogEntry& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    return db.ReadResponseLogEntry(
        args.ClientTabletId,
        args.RequestId,
        args.Entry);
}

void TIndexTabletActor::ExecuteTx_GetResponseLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TGetResponseLogEntry& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TIndexTabletActor::CompleteTx_GetResponseLogEntry(
    const TActorContext& ctx,
    TTxIndexTablet::TGetResponseLogEntry& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s GetResponseLogEntry completed (%lu, %lu): %s",
        LogTag.c_str(),
        args.ClientTabletId,
        args.RequestId,
        args.Entry
            ? args.Entry->ShortUtf8DebugString().Quote().c_str()
            : "<null>");

    using TResponse = TEvIndexTablet::TEvGetResponseLogEntryResponse;
    auto response = std::make_unique<TResponse>();
    if (args.Entry) {
        *response->Record.MutableEntry() = *args.Entry;
    }
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleWriteResponseLogEntry(
    const TEvIndexTablet::TEvWriteResponseLogEntryRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddInFlightRequest<TEvIndexTablet::TWriteResponseLogEntryMethod>(
        *requestInfo);

    ExecuteTx<TWriteResponseLogEntry>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record.GetEntry()));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_WriteResponseLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteResponseLogEntry& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_WriteResponseLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteResponseLogEntry& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    WriteResponseLogEntry(db, args.Entry);
}

void TIndexTabletActor::CompleteTx_WriteResponseLogEntry(
    const TActorContext& ctx,
    TTxIndexTablet::TWriteResponseLogEntry& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    TABLET_VERIFY(!HasError(args.Error));
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s WriteResponseLogEntry completed: %s",
        LogTag.c_str(),
        args.Entry.ShortUtf8DebugString().Quote().c_str());

    CommitResponseLogEntry(std::move(args.Entry));

    using TResponse = TEvIndexTablet::TEvWriteResponseLogEntryResponse;
    auto response = std::make_unique<TResponse>(std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::DeleteOldResponseLogEntries(
    const NActors::TActorContext& ctx)
{
    const auto minTimestamp = ctx.Now() - Config->GetResponseLogEntryTTL();
    auto reqIds = ListOldResponseLogEntries(minTimestamp);

    if (reqIds.empty()) {
        return;
    }

    ExecuteTx<TDeleteResponseLogEntries>(
        ctx,
        nullptr /* requestInfo */,
        std::move(reqIds));
}

}   // namespace NCloud::NFileStore::NStorage
