#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::ReleaseDisk(
    const TActorContext& ctx,
    const TString& clientId,
    const TVector<NProto::TDeviceConfig>& devicesToRelease)
{
    if (Config->GetNonReplicatedVolumeDirectAcquireEnabled()) {
        SendReleaseDevicesToAgents(clientId, ctx, devicesToRelease);
        return;
    }

    // Only direct acquire protocol allows to release specific devices.
    if (!devicesToRelease.empty()) {
        Y_DEBUG_ABORT_UNLESS(
            !AcquireReleaseDiskRequests.empty() &&
            !AcquireReleaseDiskRequests.front().ClientRequest);
        HandleDevicesReleasedFinishedImpl(
            MakeError(
                E_NOT_IMPLEMENTED,
                "Can't release specific devices through Disk Registry."),
            ctx);
        return;
    }

    auto request = std::make_unique<TEvDiskRegistry::TEvReleaseDiskRequest>();
    request->Record.SetDiskId(State->GetDiskId());
    request->Record.MutableHeaders()->SetClientId(clientId);
    request->Record.SetVolumeGeneration(Executor()->Generation());
    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request));
}

void TVolumeActor::HandleReleaseDiskResponse(
    const TEvDiskRegistry::TEvReleaseDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;

    HandleDevicesReleasedFinishedImpl(record.GetError(), ctx);
}

void TVolumeActor::HandleDevicesReleasedFinishedImpl(
    const NProto::TError& error,
    const NActors::TActorContext& ctx)
{
    if (AcquireReleaseDiskRequests.empty()) {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Unexpected TEvReleaseDiskResponse for disk " << State->GetDiskId()
        );

        return;
    }

    auto& request = AcquireReleaseDiskRequests.front();
    auto& cr = request.ClientRequest;

    if (HasError(error) && (error.GetCode() != E_NOT_FOUND)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Can't release disk " << State->GetDiskId()
                                  << " due to error: " << FormatError(error));

        if (cr) {
            auto response = std::make_unique<TEvVolume::TEvRemoveClientResponse>(
                error);
            response->Record.SetDiskId(cr->DiskId);
            response->Record.SetClientId(cr->GetClientId());
            response->Record.SetTabletId(TabletID());

            NCloud::Reply(ctx, *cr->RequestInfo, std::move(response));

            PendingClientRequests.pop_front();
            ProcessNextPendingClientRequest(ctx);
        }
    } else if (cr) {
        // This shouldn't be release of replaced devices.
        Y_DEBUG_ABORT_UNLESS(request.DevicesToRelease.empty());
        ExecuteTx<TRemoveClient>(
            ctx,
            std::move(cr->RequestInfo),
            std::move(cr->DiskId),
            cr->PipeServerActorId,
            std::move(cr->RemovedClientId),
            cr->IsMonRequest);
    }

    AcquireReleaseDiskRequests.pop_front();
    ProcessNextAcquireReleaseDiskRequest(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleRemoveClient(
    const TEvVolume::TEvRemoveClientRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_VOLUME_COUNTER(RemoveClient);

    const auto* msg = ev->Get();
    const auto& diskId = msg->Record.GetDiskId();
    const auto& clientId = GetClientId(*msg);
    const bool isMonRequest = msg->Record.GetIsMonRequest();

    // If event was forwarded through pipe, its recipient and recipient
    // rewrite would be different
    TActorId pipeServerActorId;
    if (ev->Recipient != ev->GetRecipientRewrite()) {
        pipeServerActorId = ev->Recipient;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto request = std::make_shared<TClientRequest>(
        std::move(requestInfo),
        diskId,
        pipeServerActorId,
        clientId,
        isMonRequest);
    PendingClientRequests.emplace_back(std::move(request));

    if (PendingClientRequests.size() == 1) {
        ProcessNextPendingClientRequest(ctx);
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%lu] Postponing RemoveClientRequest[%s] for volume %s: another "
            "request in flight",
            TabletID(),
            clientId.Quote().data(),
            diskId.Quote().data());
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareRemoveClient(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TRemoveClient& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteRemoveClient(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TRemoveClient& args)
{
    Y_ABORT_UNLESS(State);

    auto now = ctx.Now();

    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Volume %s received remove client %s request; pipe server %s, "
        "pipe generation %u",
        TabletID(),
        args.DiskId.Quote().data(),
        args.ClientId.Quote().data(),
        ToString(args.PipeServerActorId).data());

    args.Error = State->RemoveClient(
        args.ClientId,
        args.IsMonRequest ? TActorId() : args.PipeServerActorId);

    TVolumeDatabase db(tx.DB);
    db.WriteHistory(
        State->LogRemoveClient(
            now,
            args.ClientId,
            "Removed by request",
            args.Error));

    if (FAILED(args.Error.GetCode())) {
        return;
    }

    db.RemoveClient(args.ClientId);
}

void TVolumeActor::CompleteRemoveClient(
    const TActorContext& ctx,
    TTxVolume::TRemoveClient& args)
{
    Y_DEFER {
        PendingClientRequests.pop_front();
        ProcessNextPendingClientRequest(ctx);
    };

    const auto& clientId = args.ClientId;
    const auto& diskId = args.DiskId;

    if (FAILED(args.Error.GetCode())) {
        auto response = std::make_unique<TEvVolume::TEvRemoveClientResponse>(
            std::move(args.Error));
        response->Record.SetDiskId(diskId);
        response->Record.SetClientId(clientId);
        response->Record.SetTabletId(TabletID());

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Removed client %s from volume %s",
        TabletID(),
        clientId.Quote().data(),
        diskId.Quote().data());

    auto response = std::make_unique<TEvVolume::TEvRemoveClientResponse>();
    *response->Record.MutableError() = std::move(args.Error);
    response->Record.SetDiskId(diskId);
    response->Record.SetClientId(clientId);
    response->Record.SetTabletId(TabletID());

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    OnClientListUpdate(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
