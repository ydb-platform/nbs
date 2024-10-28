#include "disk_registry_actor.h"
#include "disk_registry_database.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandlePurgeHostCms(
    const TEvDiskRegistryPrivate::TEvPurgeHostCmsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(PurgeHostCms);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received PurgeHostCms request: Host=%s",
        TabletID(),
        msg->Host.c_str());

    ExecuteTx<TPurgeHostCms>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Host),
        msg->DryRun);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PreparePurgeHostCms(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TPurgeHostCms& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecutePurgeHostCms(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TPurgeHostCms& args)
{
    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->PurgeHost(
        db,
        args.Host,
        ctx.Now(),
        args.DryRun,
        args.AffectedDisks);
}

void TDiskRegistryActor::CompletePurgeHostCms(
    const TActorContext& ctx,
    TTxDiskRegistry::TPurgeHostCms& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "PurgeHostCms result: Host=%s Error=%s AffectedDisks=%s",
        args.Host.c_str(),
        FormatError(args.Error).c_str(),
        [&]
        {
            TStringStream out;
            out << "[";
            for (const auto& diskId: args.AffectedDisks) {
                out << " " << diskId << ":"
                    << NProto::EDiskState_Name(State->GetDiskState(diskId));
            }
            out << " ]";
            return out.Str();
        }().c_str());

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);

    SecureErase(ctx);
    StartMigration(ctx);

    auto response =
        std::make_unique<TEvDiskRegistryPrivate::TEvPurgeHostCmsResponse>(
            std::move(args.Error));
    response->Timeout = TDuration();
    response->DependentDiskIds = std::move(args.AffectedDisks);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
