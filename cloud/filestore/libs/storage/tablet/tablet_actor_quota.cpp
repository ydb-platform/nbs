#include "tablet_actor.h"

#include "shard_request_actor.h"

#include <util/string/join.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleSetQuota(
    const TEvIndexTablet::TEvSetQuotaRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s SetQuota started (quotaId: %u, maxBytes: %lu, maxNodes: %lu)",
        LogTag.c_str(),
        msg->Record.GetQuotaId(),
        msg->Record.GetMaxBytes(),
        msg->Record.GetMaxNodes());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());
    requestInfo->StartedTs = ctx.Now();

    if (!msg->Record.GetQuotaId()) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvSetQuotaResponse>(MakeError(
                E_ARGUMENT,
                "quotaId must be non-zero"));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    // TODO(6608): add limit for the number of quotas per file system

    AddInFlightRequest<TEvIndexTablet::TSetQuotaMethod>(*requestInfo);

    ExecuteTx<TSetQuota>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetQuotaId(),
        msg->Record.GetMaxBytes(),
        msg->Record.GetMaxNodes());
}

bool TIndexTabletActor::PrepareTx_SetQuota(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetQuota& args)
{
    Y_UNUSED(ctx, tx, args);

    return true;
}

void TIndexTabletActor::ExecuteTx_SetQuota(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetQuota& args)
{
    auto db = CreateIndexTabletDatabase(tx.DB);
    args.Quota = SetQuota(
        *db,
        args.QuotaId,
        args.MaxBytes,
        args.MaxNodes,
        ctx.Now());
}

void TIndexTabletActor::CompleteTx_SetQuota(
    const TActorContext& ctx,
    TTxIndexTablet::TSetQuota& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s SetQuota completed (%s)",
        LogTag.c_str(),
        FormatError(args.Error).c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvSetQuotaResponse>(args.Error);
    if (!HasError(args.Error)) {
        *response->Record.MutableQuota() = args.Quota;
    }

    if (HasError(args.Error) ||
        GetFileSystem().GetShardFileSystemIds().empty() || !IsMainTablet())
    {
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    TVector<TString> shardIds;
    for (const auto& shardId: GetFileSystem().GetShardFileSystemIds()) {
        shardIds.push_back(shardId);
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s Propagating SetQuota to shards (%s)",
        LogTag.c_str(),
        JoinSeq(",", shardIds).c_str());

    NProtoPrivate::TSetQuotaRequest request;
    request.SetQuotaId(args.QuotaId);
    request.SetMaxBytes(args.MaxBytes);
    request.SetMaxNodes(args.MaxNodes);

    auto actor = std::make_unique<TShardRequestActor<
        TEvIndexTablet::TEvSetQuotaRequest,
        TEvIndexTablet::TEvSetQuotaResponse>>(
        LogTag,
        SelfId(),
        std::move(args.RequestInfo),
        std::move(request),
        std::move(shardIds),
        std::move(response));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
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

    if (HasError(args.Error) ||
        GetFileSystem().GetShardFileSystemIds().empty() || !IsMainTablet())
    {
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    TVector<TString> shardIds;
    for (const auto& shardId: GetFileSystem().GetShardFileSystemIds()) {
        shardIds.push_back(shardId);
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s Propagating DeleteQuota to shards (%s)",
        LogTag.c_str(),
        JoinSeq(",", shardIds).c_str());

    NProtoPrivate::TDeleteQuotaRequest request;
    request.SetQuotaId(args.QuotaId);

    auto actor = std::make_unique<TShardRequestActor<
        TEvIndexTablet::TEvDeleteQuotaRequest,
        TEvIndexTablet::TEvDeleteQuotaResponse>>(
        LogTag,
        SelfId(),
        std::move(args.RequestInfo),
        std::move(request),
        std::move(shardIds),
        std::move(response));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
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

    for (const auto& usage: GetQuotaUsages()) {
        auto* proto = response->Record.AddUsages();
        proto->SetQuotaId(usage.QuotaId);
        proto->SetUsedBytes(usage.UsedBytes);
        proto->SetUsedNodes(usage.UsedNodes);
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
