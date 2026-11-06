#include "tablet_actor.h"

#include "shard_request_actor.h"

#include <util/string/join.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateSetQuotaRequest(const NProto::TSetQuotaRequest& request)
{
    if (!request.GetQuotaId()) {
        return MakeError(E_ARGUMENT, "quotaId must be non-zero");
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleSetQuota(
    const TEvService::TEvSetQuotaRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequestNoSession<TEvService::TSetQuotaMethod>(
            ev,
            ctx,
            ValidateSetQuotaRequest))
    {
        return;
    }

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    if (!FindQuota(msg->Record.GetQuotaId()) && GetQuotaCount() >= MaxQuotas)
    {
        auto response =
            std::make_unique<TEvService::TEvSetQuotaResponse>(MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "too many quotas, limit is " << MaxQuotas));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    AddInFlightRequest<TEvService::TSetQuotaMethod>(*requestInfo);

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

    auto response =
        std::make_unique<TEvService::TEvSetQuotaResponse>(args.Error);
    if (!HasError(args.Error)) {
        *response->Record.MutableQuota() = args.Quota;
    }

    CompleteResponse<TEvService::TSetQuotaMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

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

    NProto::TSetQuotaRequest request;
    request.SetQuotaId(args.QuotaId);
    request.SetMaxBytes(args.MaxBytes);
    request.SetMaxNodes(args.MaxNodes);

    auto actor = std::make_unique<TShardRequestActor<
        TEvService::TEvSetQuotaRequest,
        TEvService::TEvSetQuotaResponse>>(
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
    const TEvService::TEvDeleteQuotaRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequestNoSession<TEvService::TDeleteQuotaMethod>(
            ev,
            ctx,
            {} /* validator */))
    {
        return;
    }

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TEvService::TDeleteQuotaMethod>(*requestInfo);

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

    auto response =
        std::make_unique<TEvService::TEvDeleteQuotaResponse>(args.Error);

    CompleteResponse<TEvService::TDeleteQuotaMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

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

    NProto::TDeleteQuotaRequest request;
    request.SetQuotaId(args.QuotaId);

    auto actor = std::make_unique<TShardRequestActor<
        TEvService::TEvDeleteQuotaRequest,
        TEvService::TEvDeleteQuotaResponse>>(
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
    const TEvService::TEvListQuotasRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequestNoSession<TEvService::TListQuotasMethod>(
            ev,
            ctx,
            {} /* validator */))
    {
        return;
    }

    auto* msg = ev->Get();

    auto response = std::make_unique<TEvService::TEvListQuotasResponse>();

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

    CompleteResponse<TEvService::TListQuotasMethod>(
        response->Record,
        msg->CallContext,
        ctx);

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
