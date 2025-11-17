#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString CreateInputDescription(
    const TString& masterDiskId,
    ui32 replicaCount)
{
    return TStringBuilder()
        << "MasterDiskId=" << masterDiskId.c_str()
        << ", ReplicaCount=" << replicaCount;
}

TString CreateInputDescription(
    const TTxDiskRegistry::TUpdateDiskReplicaCount& args)
{
    return CreateInputDescription(args.MasterDiskId, args.ReplicaCount);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdateDiskReplicaCount(
    const TEvDiskRegistry::TEvUpdateDiskReplicaCountRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdateDiskReplicaCount);

    const auto& record = ev->Get()->Record;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received UpdateDiskReplicaCount request: %s %s",
        LogTitle.GetWithTime().c_str(),
        record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TUpdateDiskReplicaCount>(
        ctx,
        CreateRequestInfo<TEvDiskRegistry::TUpdateDiskReplicaCountMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ),
        record.GetMasterDiskId(),
        record.GetReplicaCount());
}

bool TDiskRegistryActor::PrepareUpdateDiskReplicaCount(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateDiskReplicaCount& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdateDiskReplicaCount(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateDiskReplicaCount& args)
{
    TDiskRegistryDatabase db(tx.DB);

    args.Error = State->UpdateDiskReplicaCount(db, args.MasterDiskId,
        args.ReplicaCount);

    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s UpdateDiskReplicaCount execution errored: %s. %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str(),
            CreateInputDescription(args).c_str());
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s UpdateDiskReplicaCount execution succeeded. %s",
        LogTitle.GetWithTime().c_str(),
        CreateInputDescription(args).c_str());
}

void TDiskRegistryActor::CompleteUpdateDiskReplicaCount(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateDiskReplicaCount& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s UpdateDiskReplicaCount complete. %s",
        LogTitle.GetWithTime().c_str(),
        CreateInputDescription(args).c_str());

    ReallocateDisks(ctx);

    auto response =
        std::make_unique<TEvDiskRegistry::TEvUpdateDiskReplicaCountResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
