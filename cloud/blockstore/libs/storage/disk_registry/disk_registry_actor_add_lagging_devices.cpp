#include "disk_registry_actor.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleAddLaggingDevices(
    const TEvDiskRegistry::TEvAddLaggingDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AddLaggingDevices);

    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received AddLaggingDevices request: DiskId=%s, "
        "LaggingDevices=[%s]",
        TabletID(),
        msg->Record.GetDiskId().c_str(),
        [&laggingDevices = msg->Record.GetLaggingDevices()]()
        {
            TStringBuilder str;
            for (const auto& laggingDevice: laggingDevices) {
                if (!str.empty()) {
                    str << ", ";
                }
                str << laggingDevice.GetDeviceUUID();
            }
            return str;
        }()
            .c_str());

    TVector<NProto::TLaggingDevice> laggingDevices(
        std::make_move_iterator(msg->Record.MutableLaggingDevices()->begin()),
        std::make_move_iterator(msg->Record.MutableLaggingDevices()->end()));
    ExecuteTx<TAddLaggingDevices>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        std::move(laggingDevices));
}

void TDiskRegistryActor::HandleGetLaggingDevicesAllowed(
    const TEvDiskRegistry::TEvGetLaggingDevicesAllowedRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
    BLOCKSTORE_DISK_REGISTRY_COUNTER(GetLaggingDevicesAllowed);

    // State->GetAgents()

    auto response = std::make_unique<
        TEvDiskRegistry::TEvGetLaggingDevicesAllowedResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareAddLaggingDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddLaggingDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteAddLaggingDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddLaggingDevices& args)
{
    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->AddLaggingDevices(
        ctx.Now(),
        db,
        args.DiskId,
        std::move(args.VolumeLaggingDevices));
}

void TDiskRegistryActor::CompleteAddLaggingDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TAddLaggingDevices& args)
{
    LOG_LOG(
        ctx,
        HasError(args.Error) ? NLog::PRI_ERROR : NLog::PRI_INFO,
        TBlockStoreComponents::DISK_REGISTRY,
        "AddLaggingDevices result: DiskId=%s Error=%s",
        args.DiskId.c_str(),
        FormatError(args.Error).c_str());

    auto response = std::make_unique<
        TEvDiskRegistry::TEvAddLaggingDevicesResponse>(args.Error);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);
    SecureErase(ctx);
    StartMigration(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
