#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCreateDiskFromDevices(
    const TEvDiskRegistry::TEvCreateDiskFromDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(CreateDiskFromDevices);

    const auto& record = ev->Get()->Record;
    const auto& volume = record.GetVolumeConfig();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received CreateDiskFromDevices %s, %u, %u, [ %s] %s",
        TabletID(),
        volume.GetDiskId().c_str(),
        volume.GetBlockSize(),
        volume.GetStorageMediaKind(),
        [&] {
            TStringStream out;
            for (auto& d: record.GetDevices()) {
                out << "(" << d.GetAgentId() << " " << d.GetDeviceName() << ") ";
            }
            return out.Str();
        }().c_str(),
        record.GetForce() ? "force" : "");

    if (!NProto::EStorageMediaKind_IsValid(volume.GetStorageMediaKind())) {
        auto response =
            std::make_unique<TEvDiskRegistry::TEvCreateDiskFromDevicesResponse>(
                MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "Storage Media kind %d"
                        << volume.GetStorageMediaKind() << " is not valid."));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    ExecuteTx<TCreateDiskFromDevices>(
        ctx,
        CreateRequestInfo<TEvDiskRegistry::TCreateDiskFromDevicesMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ),
        record.GetForce(),
        volume.GetDiskId(),
        volume.GetBlockSize(),
        static_cast<NProto::EStorageMediaKind>(volume.GetStorageMediaKind()),
        TVector<NProto::TDeviceConfig> (
            record.GetDevices().begin(),
            record.GetDevices().end())
        );
}

bool TDiskRegistryActor::PrepareCreateDiskFromDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCreateDiskFromDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCreateDiskFromDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCreateDiskFromDevices& args)
{
    TDiskRegistryDatabase db(tx.DB);

    TDiskRegistryState::TAllocateDiskResult result;
    args.Error = State->CreateDiskFromDevices(
        ctx.Now(),
        db,
        args.Force,
        args.DiskId,
        args.BlockSize,
        args.MediaKind,
        args.Devices,
        &result);

    if (HasError(args.Error)) {
        return;
    }

    for (auto& d: result.Devices) {
        if (!ToLogicalBlocks(d, args.BlockSize)) {
            args.Error = MakeError(E_FAIL, TStringBuilder()
                << "CreateDiskFromDevices: ToLogicalBlocks failed, device: "
                << d.GetDeviceUUID().Quote().c_str()
            );

            return;
        }

        args.LogicalBlockCount += d.GetBlocksCount();
    }
}

void TDiskRegistryActor::CompleteCreateDiskFromDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TCreateDiskFromDevices& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "CreateDiskFromDevices %s failed. Error: %s",
            args.ToString().c_str(),
            FormatError(args.Error).c_str());
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "CreateDiskFromDevices %s succeeded. Error: %s",
            args.ToString().c_str(),
            FormatError(args.Error).c_str());
    }

    auto response =
        std::make_unique<TEvDiskRegistry::TEvCreateDiskFromDevicesResponse>(
            std::move(args.Error));

    response->Record.SetBlockCount(args.LogicalBlockCount);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
