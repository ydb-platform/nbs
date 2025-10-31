#include "disk_registry_actor.h"
#include "disk_registry_database.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdateCmsHostState(
    const TEvDiskRegistryPrivate::TEvUpdateCmsHostStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdateCmsHostState);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received UpdateCmsHostState request: Host=%s, State=%u",
        TabletID(),
        msg->Host.c_str(),
        static_cast<ui32>(msg->State));

    ExecuteTx<TUpdateCmsHostState>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Host),
        msg->State,
        msg->DryRun);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdateCmsHostState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateCmsHostState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdateCmsHostState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateCmsHostState& args)
{
    TDiskRegistryDatabase db(tx.DB);

    args.TxTs = ctx.Now();

    args.Error = State->UpdateCmsHostState(
        db,
        args.Host,
        args.State,
        args.TxTs,
        args.DryRun,
        args.AffectedDisks,
        args.Timeout);

    // Round up to seconds because TActionResult::Timeout is specified in
    // seconds
    if (args.Timeout) {
        args.Timeout = Max(args.Timeout, TDuration::Seconds(1));
    }
}

void TDiskRegistryActor::CompleteUpdateCmsHostState(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateCmsHostState& args)
{
    const auto* agent = State->FindAgent(args.Host);
    const bool needToDetachPaths =
        Config->GetAttachDetachPathsEnabled() &&
        args.State == NProto::AGENT_STATE_WARNING && !HasError(args.Error) &&
        agent && agent->GetState() != NProto::AGENT_STATE_UNAVAILABLE;

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "UpdateCmsHost result: Host=%s Error=%s Timeout=%u AffectedDisks=%s",
        args.Host.c_str(),
        FormatError(args.Error).c_str(),
        args.Timeout.Seconds(),
        [&] {
            TStringStream out;
            out << "[";
            for (const auto& diskId: args.AffectedDisks) {
                out << " " << diskId
                    << ":" << NProto::EDiskState_Name(State->GetDiskState(diskId));
            }
            out << " ]";
            return out.Str();
        }().c_str());

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);

    SecureErase(ctx);
    StartMigration(ctx);

    ProcessPathsToAttach(ctx);

    if (!needToDetachPaths) {
        using TResponse = TEvDiskRegistryPrivate::TEvUpdateCmsHostStateResponse;

        auto response = std::make_unique<TResponse>(std::move(args.Error));
        response->Timeout = args.Timeout;
        response->DependentDiskIds = std::move(args.AffectedDisks);

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    TVector<TString> paths;
    for (const auto& [path, state]: agent->GetPathAttachStates()) {
        if (state == NProto::PATH_ATTACH_STATE_DETACHED) {
            paths.emplace_back(path);
        }
    }

    for (const auto& device: agent->GetUnknownDevices()) {
        paths.emplace_back(device.GetDeviceName());
    }

    SortUnique(paths);

    TryToDetachPaths(
        ctx,
        args.Host,
        std::move(paths),
        args.RequestInfo,
        NProto::TAction::REMOVE_HOST);
}

}   // namespace NCloud::NBlockStore::NStorage
