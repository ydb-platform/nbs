#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString
CreateInputDescription(const TString& diskId, ui32 blockSize, bool force)
{
    return TStringBuilder()
                   << "DiskId=" << diskId.c_str() << ", BlockSize=" << blockSize
                   << ", Force=" << force
               ? "true"
               : "false";
}

TString CreateInputDescription(
    const TTxDiskRegistry::TUpdateDiskBlockSize& args)
{
    return CreateInputDescription(args.DiskId, args.BlockSize, args.Force);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdateDiskBlockSize(
    const TEvDiskRegistry::TEvUpdateDiskBlockSizeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdateDiskBlockSize);

    const auto& record = ev->Get()->Record;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received UpdateDiskBlockSize request: %s %s",
        LogTitle.GetWithTime().c_str(),
        record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TUpdateDiskBlockSize>(
        ctx,
        CreateRequestInfo<TEvDiskRegistry::TUpdateDiskBlockSizeMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext),
        record.GetDiskId(),
        record.GetBlockSize(),
        record.GetForce());
}

bool TDiskRegistryActor::PrepareUpdateDiskBlockSize(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateDiskBlockSize& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdateDiskBlockSize(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateDiskBlockSize& args)
{
    TDiskRegistryDatabase db(tx.DB);

    args.Error = State->UpdateDiskBlockSize(
        ctx.Now(),
        db,
        args.DiskId,
        args.BlockSize,
        args.Force);

    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s UpdateDiskBlockSize execution errored: %s. %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str(),
            CreateInputDescription(args).c_str());
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s UpdateDiskBlockSize execution succeeded. %s",
        LogTitle.GetWithTime().c_str(),
        CreateInputDescription(args).c_str());
}

void TDiskRegistryActor::CompleteUpdateDiskBlockSize(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateDiskBlockSize& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s UpdateDiskBlockSize complete. %s",
        LogTitle.GetWithTime().c_str(),
        CreateInputDescription(args).c_str());

    ReallocateDisks(ctx);

    auto response =
        std::make_unique<TEvDiskRegistry::TEvUpdateDiskBlockSizeResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
