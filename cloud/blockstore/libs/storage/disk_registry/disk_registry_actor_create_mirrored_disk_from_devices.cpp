#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/common/safe_debug_print.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCreateMirroredDiskFromDevices(
    const TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(CreateMirroredDiskFromDevices);

    const auto& record = ev->Get()->Record;
    const auto& volume = ev->Get()->Record.GetVolumeConfig();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received CreateMirroredDiskFromDevices request: %s %s",
        LogTitle.GetWithTime().c_str(),
        SafeDebugPrint(record).c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    TVector<TVector<NProto::TDeviceConfig>> replicas;
    for (const auto& replica: record.GetReplicas()) {
        replicas.push_back(
            TVector<NProto::TDeviceConfig>(
                replica.GetDevices().begin(),
                replica.GetDevices().end()));
    }

    ExecuteTx<TCreateMirroredDiskFromDevices>(
        ctx,
        CreateRequestInfo<
            TEvDiskRegistry::TCreateMirroredDiskFromDevicesMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext),
        volume.GetDiskId(),
        volume.GetBlockSize(),
        static_cast<NProto::EStorageMediaKind>(volume.GetStorageMediaKind()),
        std::move(replicas),
        TVector<TString>(
            record.GetFreshDeviceUUIDs().begin(),
            record.GetFreshDeviceUUIDs().end()),
        record.GetForce(),
        record.GetKeepBlockCount());
}

bool TDiskRegistryActor::PrepareCreateMirroredDiskFromDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCreateMirroredDiskFromDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCreateMirroredDiskFromDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCreateMirroredDiskFromDevices& args)
{
    TDiskRegistryDatabase db(tx.DB);

    TDiskRegistryState::TAllocateDiskResult result;
    args.Error = State->CreateMirroredDiskFromDevices(
        ctx.Now(),
        db,
        TDiskRegistryState::TCreateDiskFromDevicesParams(
            args.MasterDiskId,
            args.BlockSize,
            args.MediaKind,
            args.Force,
            args.KeepBlockCount),
        args.Replicas,
        args.FreshDeviceIds,
        &result);

    if (HasError(args.Error)) {
        return;
    }

    for (auto& d: result.Devices) {
        if (!ToLogicalBlocks(d, args.BlockSize)) {
            args.Error = MakeError(
                E_FAIL,
                TStringBuilder() << "CreateMirroredDiskFromDevices: "
                                    "ToLogicalBlocks failed, device: "
                                 << d.GetDeviceUUID().Quote().c_str());

            return;
        }

        args.LogicalBlockCount += d.GetBlocksCount();
    }
}

void TDiskRegistryActor::CompleteCreateMirroredDiskFromDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TCreateMirroredDiskFromDevices& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s CreateMirroredDiskFromDevices result: %s",
        LogTitle.GetWithTime().c_str(),
        FormatError(args.Error).c_str());

    auto response = std::make_unique<
        TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesResponse>(
        std::move(args.Error));

    response->Record.SetBlockCount(args.LogicalBlockCount);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
