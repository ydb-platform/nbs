#include "disk_registry_actor.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleAddOutdatedLaggingDevices(
    const TEvDiskRegistry::TEvAddOutdatedLaggingDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AddOutdatedLaggingDevices);

    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received AddOutdatedLaggingDevices request: DiskId=%s, "
        "LaggingDevices=[%s]",
        TabletID(),
        msg->Record.GetDiskId().c_str(),
        [&outdatedDevices = msg->Record.GetOutdatedLaggingDevices()]()
        {
            TStringBuilder str;
            for (const auto& outdatedDevice: outdatedDevices) {
                if (!str.empty()) {
                    str << ", ";
                }
                str << outdatedDevice.GetDeviceUUID();
            }
            return str;
        }()
            .c_str());

    TVector<NProto::TLaggingDevice> outdatedDevices(
        std::make_move_iterator(
            msg->Record.MutableOutdatedLaggingDevices()->begin()),
        std::make_move_iterator(
            msg->Record.MutableOutdatedLaggingDevices()->end()));
    ExecuteTx<TAddOutdatedLaggingDevices>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        std::move(outdatedDevices));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareAddOutdatedLaggingDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddOutdatedLaggingDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteAddOutdatedLaggingDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddOutdatedLaggingDevices& args)
{
    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->AddOutdatedLaggingDevices(
        ctx.Now(),
        db,
        args.DiskId,
        std::move(args.VolumeOutdatedDevices));
}

void TDiskRegistryActor::CompleteAddOutdatedLaggingDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TAddOutdatedLaggingDevices& args)
{
    LOG_LOG(
        ctx,
        HasError(args.Error) ? NLog::PRI_ERROR : NLog::PRI_INFO,
        TBlockStoreComponents::DISK_REGISTRY,
        "AddOutdatedLaggingDevices result: DiskId=%s Error=%s",
        args.DiskId.c_str(),
        FormatError(args.Error).c_str());

    auto response = std::make_unique<
        TEvDiskRegistry::TEvAddOutdatedLaggingDevicesResponse>(args.Error);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);
    SecureErase(ctx);
    StartMigration(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
