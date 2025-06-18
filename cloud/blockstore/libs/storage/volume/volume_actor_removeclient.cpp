#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/common/media.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvVolume::TEvRemoveClientResponse> CreateReleaseResponse(
    const NProto::TError& error,
    TString diskId,
    TString clientId,
    ui64 tabletId)
{
    auto response = std::make_unique<TEvVolume::TEvRemoveClientResponse>(error);

    response->Record.SetDiskId(std::move(diskId));
    response->Record.SetClientId(std::move(clientId));
    response->Record.SetTabletId(tabletId);

    return response;
}

}   // namespace

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
    auto clientRequest = request.ClientRequest;

    if (HasError(error) && (error.GetCode() != E_NOT_FOUND)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Can't release disk " << State->GetDiskId()
                                  << " due to error: " << FormatError(error));
    }
    // This shouldn't be release of replaced devices.
    Y_DEBUG_ABORT_UNLESS(!clientRequest || request.DevicesToRelease.empty());

    if (clientRequest) {
        NCloud::Reply(
            ctx,
            *clientRequest->RequestInfo,
            CreateReleaseResponse(
                error.GetCode() == E_NOT_FOUND ? NProto::TError() : error,
                clientRequest->DiskId,
                clientRequest->GetClientId(),
                TabletID()));

        PendingClientRequests.pop_front();
        ProcessNextPendingClientRequest(ctx);
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
            "%s Postponing RemoveClientRequest[%s] for volume: another "
            "request in flight",
            LogTitle.GetWithTime().c_str(),
            clientId.Quote().c_str());
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

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Volume received remove client %s request; pipe server %s",
        LogTitle.GetWithTime().c_str(),
        args.ClientId.Quote().c_str(),
        ToString(args.PipeServerActorId).c_str());

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
    const bool needToAcquireOrReleaseDevices =
        State->IsDiskRegistryMediaKind() &&
        Config->GetAcquireNonReplicatedDevices() && !HasError(args.Error) &&
        args.Error.GetCode() != S_ALREADY;
    Y_DEFER {
        if (!needToAcquireOrReleaseDevices) {
            PendingClientRequests.pop_front();
            ProcessNextPendingClientRequest(ctx);
        }
    };

    const auto& clientId = args.ClientId;
    const auto& diskId = args.DiskId;

    if (FAILED(args.Error.GetCode())) {
        NCloud::Reply(
            ctx,
            *args.RequestInfo,
            CreateReleaseResponse(args.Error, diskId, clientId, TabletID()));
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Removed client %s from volume",
        LogTitle.GetWithTime().c_str(),
        clientId.Quote().c_str());

    if (needToAcquireOrReleaseDevices) {
        // Release all devices for client.
        AddAcquireReleaseDiskRequest(
            ctx,
            {args.ClientId,
             std::make_shared<TClientRequest>(
                 args.RequestInfo,
                 args.DiskId,
                 args.PipeServerActorId,
                 args.ClientId,
                 args.IsMonRequest),
             TVector<NProto::TDeviceConfig>{}});
    } else {
        NCloud::Reply(
            ctx,
            *args.RequestInfo,
            CreateReleaseResponse(args.Error, diskId, clientId, TabletID()));
    }

    OnClientListUpdate(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
