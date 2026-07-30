#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCreateQuota(
    const TEvIndexTablet::TEvCreateQuotaRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s CreateQuota started (maxBytes: %lu, maxNodes: %lu)",
        LogTag.c_str(),
        msg->Record.GetMaxBytes(),
        msg->Record.GetMaxNodes());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());
    requestInfo->StartedTs = ctx.Now();

    if (!IsMainTablet()) {
        // TODO(6608): propagate knowledge of the quota to all shards
        auto response =
            std::make_unique<TEvIndexTablet::TEvCreateQuotaResponse>(MakeError(
                E_ARGUMENT,
                "quotas can only be created on the main tablet"));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    AddInFlightRequest<TEvIndexTablet::TCreateQuotaMethod>(*requestInfo);

    ExecuteTx<TCreateQuota>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetMaxBytes(),
        msg->Record.GetMaxNodes());
}

bool TIndexTabletActor::PrepareTx_CreateQuota(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateQuota& args)
{
    Y_UNUSED(ctx, tx, args);

    return true;
}

void TIndexTabletActor::ExecuteTx_CreateQuota(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateQuota& args)
{
    auto db = CreateIndexTabletDatabase(tx.DB);
    args.Quota = CreateQuota(*db, args.MaxBytes, args.MaxNodes, ctx.Now());
}

void TIndexTabletActor::CompleteTx_CreateQuota(
    const TActorContext& ctx,
    TTxIndexTablet::TCreateQuota& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s CreateQuota completed (%s)",
        LogTag.c_str(),
        FormatError(args.Error).c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvCreateQuotaResponse>(args.Error);
    if (!HasError(args.Error)) {
        *response->Record.MutableQuota() = args.Quota;
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDeleteQuota(
    const TEvIndexTablet::TEvDeleteQuotaRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s DeleteQuota started (quotaId: %u)",
        LogTag.c_str(),
        msg->Record.GetQuotaId());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());
    requestInfo->StartedTs = ctx.Now();

    if (!IsMainTablet()) {
        // TODO(6608): propagate deletion of the quota to all shards
        auto response =
            std::make_unique<TEvIndexTablet::TEvDeleteQuotaResponse>(MakeError(
                E_ARGUMENT,
                "quotas can only be deleted on the main tablet"));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    AddInFlightRequest<TEvIndexTablet::TDeleteQuotaMethod>(*requestInfo);

    ExecuteTx<TDeleteQuota>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetQuotaId());
}

bool TIndexTabletActor::PrepareTx_DeleteQuota(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteQuota& args)
{
    Y_UNUSED(ctx, tx, args);

    return true;
}

void TIndexTabletActor::ExecuteTx_DeleteQuota(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteQuota& args)
{
    Y_UNUSED(ctx);

    auto db = CreateIndexTabletDatabase(tx.DB);
    DeleteQuota(*db, args.QuotaId);
}

void TIndexTabletActor::CompleteTx_DeleteQuota(
    const TActorContext& ctx,
    TTxIndexTablet::TDeleteQuota& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s DeleteQuota completed (%s)",
        LogTag.c_str(),
        FormatError(args.Error).c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvDeleteQuotaResponse>(args.Error);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleListQuotas(
    const TEvIndexTablet::TEvListQuotasRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvIndexTablet::TEvListQuotasResponse>();

    auto quotas = GetQuotas();
    for (auto& quota: quotas) {
        *response->Record.AddQuotas() = std::move(quota);
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ListQuotas completed (count: %lu)",
        LogTag.c_str(),
        quotas.size());

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
