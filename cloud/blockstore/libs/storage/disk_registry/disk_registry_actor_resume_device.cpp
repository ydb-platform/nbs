#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleResumeDevice(
    const TEvService::TEvResumeDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const TString& agentId = msg->Record.GetAgentId();
    const TString& path = msg->Record.GetPath();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received ResumeDevice request: AgentId=%s Path=%s",
        TabletID(),
        agentId.c_str(),
        path.c_str());

    auto deviceIds = State->GetDeviceIds(agentId, path);

    if (deviceIds.empty()) {
        auto response = std::make_unique<TEvService::TEvResumeDeviceResponse>(
            MakeError(E_NOT_FOUND, TStringBuilder() <<
                "devices not found: " << agentId << ":" << path));

        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TResumeDevices>(
        ctx,
        std::move(requestInfo),
        std::move(deviceIds));
}

bool TDiskRegistryActor::PrepareResumeDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TResumeDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteResumeDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TResumeDevices& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);

    State->ResumeDevices(ctx.Now(), db, args.DeviceIds);
}

void TDiskRegistryActor::CompleteResumeDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TResumeDevices& args)
{
    auto response = std::make_unique<TEvService::TEvResumeDeviceResponse>();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    SecureErase(ctx);

    StartMigration(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
