#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/common/media.h>

#include <util/generic/guid.h>
#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

std::unique_ptr<TEvVolume::TEvAddClientResponse> CreateAddClientResponse(
    NProto::TError error,
    ui64 tabletId,
    TString clientId,
    bool forceTabletRestart,
    TString instanceId,
    const TVolumeState& state)
{
    auto response = std::make_unique<TEvVolume::TEvAddClientResponse>();
    *response->Record.MutableError() = std::move(error);
    response->Record.SetTabletId(tabletId);
    response->Record.SetClientId(std::move(clientId));
    response->Record.SetForceTabletRestart(forceTabletRestart);

    auto& volumeConfig = state.GetMeta().GetVolumeConfig();
    auto* volumeInfo = response->Record.MutableVolume();
    VolumeConfigToVolume(volumeConfig, *volumeInfo);
    volumeInfo->SetInstanceId(std::move(instanceId));
    state.FillDeviceInfo(*volumeInfo);

    return response;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::AcquireDisk(
    const TActorContext& ctx,
    TString clientId,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber)
{
    Y_ABORT_UNLESS(State);

    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Acquiring disk " << State->GetDiskId()
    );

    if (Config->GetNonReplicatedVolumeDirectAcquireEnabled()) {
        SendAcquireDevicesToAgents(
            std::move(clientId),
            accessMode,
            mountSeqNumber,
            ctx);
        return;
    }

    auto request = std::make_unique<TEvDiskRegistry::TEvAcquireDiskRequest>();

    request->Record.SetDiskId(State->GetDiskId());
    request->Record.MutableHeaders()->SetClientId(clientId);
    request->Record.SetAccessMode(accessMode);
    request->Record.SetMountSeqNumber(mountSeqNumber);
    request->Record.SetVolumeGeneration(Executor()->Generation());

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request));
}

void TVolumeActor::AddAcquireReleaseDiskRequest(
    const NActors::TActorContext& ctx,
    TAcquireReleaseDiskRequest request)
{
    AcquireReleaseDiskRequests.emplace_back(std::move(request));

    if (AcquireReleaseDiskRequests.size() == 1) {
        ProcessNextAcquireReleaseDiskRequest(ctx);
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Postponing AcquireReleaseRequest[%s] for volume %s: "
            "another request in flight",
            LogTitle.GetWithTime().c_str(),
            AcquireReleaseDiskRequests.back().ClientId.Quote().data(),
            State->GetDiskId().Quote().data());
    }
}

void TVolumeActor::ProcessNextAcquireReleaseDiskRequest(const TActorContext& ctx)
{
    if (AcquireReleaseDiskRequests) {
        auto& request = AcquireReleaseDiskRequests.front();

        if (request.IsAcquire) {
            AcquireDisk(
                ctx,
                request.ClientId,
                request.AccessMode,
                request.MountSeqNumber
            );
        } else {
            ReleaseDisk(ctx, request.ClientId, request.DevicesToRelease);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::AcquireDiskIfNeeded(const TActorContext& ctx)
{
    if (!State->GetClients()) {
        return;
    }

    bool queueEmpty = AcquireReleaseDiskRequests.empty();

    for (const auto& x: State->GetClients()) {
        bool skip = false;
        for (const auto& clientRequest: PendingClientRequests) {
            if (x.first == clientRequest->GetClientId()) {
                // inflight client request will cause the right acquire/release
                // op anyway, so we should not interfere
                skip = true;
                break;
            }
        }

        if (skip) {
            continue;
        }

        TAcquireReleaseDiskRequest request(
            x.first,
            x.second.GetVolumeClientInfo().GetVolumeAccessMode(),
            x.second.GetVolumeClientInfo().GetMountSeqNumber(),
            nullptr,
            false);

        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Reacquiring disk " << State->GetDiskId()
                << " for client " << request.ClientId
                << " with access mode " << static_cast<int>(request.AccessMode)
        );

        AcquireReleaseDiskRequests.push_back(std::move(request));
    }

    if (queueEmpty) {
        ProcessNextAcquireReleaseDiskRequest(ctx);
    }
}

void TVolumeActor::ScheduleAcquireDiskIfNeeded(const TActorContext& ctx)
{
    if (AcquireDiskScheduled) {
        return;
    }

    AcquireDiskScheduled = true;

    ctx.Schedule(
        Config->GetClientRemountPeriod(),
        new TEvVolumePrivate::TEvAcquireDiskIfNeeded()
    );
}

void TVolumeActor::HandleAcquireDiskIfNeeded(
    const TEvVolumePrivate::TEvAcquireDiskIfNeeded::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    AcquireDiskIfNeeded(ctx);

    AcquireDiskScheduled = false;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleReacquireDisk(
    const TEvVolume::TEvReacquireDisk::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (!State->GetReadWriteAccessClientId()
            && ctx.Now() > StateLoadTimestamp + TDuration::Seconds(5))
    {
        AcquireReleaseDiskRequests.emplace_back(
            TString(AnyWriterClientId),
            nullptr,
            TVector<NProto::TDeviceConfig>{});
        if (AcquireReleaseDiskRequests.size() == 1) {
            ProcessNextAcquireReleaseDiskRequest(ctx);
        }
    }

    AcquireDiskIfNeeded(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleAcquireDiskResponse(
    const TEvDiskRegistry::TEvAcquireDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    // NOTE: record.GetDevices() contains only the devices located at available
    // agents
    auto& record = msg->Record;

    HandleDevicesAcquireFinishedImpl(record.GetError(), ctx);
}

void TVolumeActor::HandleDevicesAcquireFinishedImpl(
    const NProto::TError& error,
    const NActors::TActorContext& ctx)
{
    ScheduleAcquireDiskIfNeeded(ctx);

    if (AcquireReleaseDiskRequests.empty()) {
        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Unexpected TEvAcquireDiskResponse for disk " << State->GetDiskId()
        );

        return;
    }

    auto& request = AcquireReleaseDiskRequests.front();
    auto clientRequest = request.ClientRequest;

    if (HasError(error)) {
        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Can't acquire disk " << State->GetDiskId());
    }

    if (clientRequest) {
        auto response = CreateAddClientResponse(
            error,
            TabletID(),
            clientRequest->GetClientId(),
            request.ForceTabletRestart,
            request.ClientRequest->AddedClientInfo.GetInstanceId(),
            *State);

        NCloud::Reply(ctx, *clientRequest->RequestInfo, std::move(response));

        PendingClientRequests.pop_front();
        ProcessNextPendingClientRequest(ctx);
    }

    AcquireReleaseDiskRequests.pop_front();
    ProcessNextAcquireReleaseDiskRequest(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleAddClient(
    const TEvVolume::TEvAddClientRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_VOLUME_COUNTER(AddClient);

    const auto* msg = ev->Get();
    const auto& diskId = msg->Record.GetDiskId();
    const auto& clientId = GetClientId(*msg);
    const auto& instanceId = msg->Record.GetInstanceId();
    const auto accessMode = msg->Record.GetVolumeAccessMode();
    const auto mountMode = msg->Record.GetVolumeMountMode();
    const auto mountFlags = msg->Record.GetMountFlags();
    const auto mountSeqNumber = msg->Record.GetMountSeqNumber();
    const auto fillSeqNumber = msg->Record.GetFillSeqNumber();
    const auto fillGeneration = msg->Record.GetFillGeneration();
    const auto& host = msg->Record.GetHost();

    // If event was forwarded through pipe, its recipient and recipient rewrite
    // would be different
    TActorId pipeServerActorId;
    if (ev->Recipient != ev->GetRecipientRewrite()) {
        pipeServerActorId = ev->Recipient;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    NProto::TVolumeClientInfo clientInfo;
    clientInfo.SetClientId(clientId);
    clientInfo.SetInstanceId(instanceId);
    clientInfo.SetVolumeAccessMode(accessMode);
    clientInfo.SetVolumeMountMode(mountMode);
    clientInfo.SetMountFlags(mountFlags);
    clientInfo.SetFillSeqNumber(fillSeqNumber);
    clientInfo.SetMountSeqNumber(mountSeqNumber);
    clientInfo.SetFillGeneration(fillGeneration);
    clientInfo.SetHost(host);
    clientInfo.SetLastActivityTimestamp(ctx.Now().MicroSeconds());

    auto request = std::make_shared<TClientRequest>(
        std::move(requestInfo),
        diskId,
        pipeServerActorId,
        std::move(clientInfo));
    PendingClientRequests.emplace_back(std::move(request));

    if (PendingClientRequests.size() == 1) {
        ProcessNextPendingClientRequest(ctx);
    } else {
        LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Postponing AddClientRequest[%s] for volume %s: another request in flight",
            TabletID(),
            clientId.Quote().data(),
            diskId.Quote().data());
    }
}

void TVolumeActor::ProcessNextPendingClientRequest(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(State);

    if (PendingClientRequests) {
        auto& request = PendingClientRequests.front();

        if (request->RemovedClientId) {
            ExecuteTx<TRemoveClient>(
                ctx,
                request->RequestInfo,
                request->DiskId,
                request->PipeServerActorId,
                request->RemovedClientId,
                request->IsMonRequest);
        } else {
            ExecuteTx<TAddClient>(
                ctx,
                request->RequestInfo,
                request->DiskId,
                request->PipeServerActorId,
                request->AddedClientInfo);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareAddClient(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TAddClient& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteAddClient(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TAddClient& args)
{
    Y_ABORT_UNLESS(State);

    auto now = ctx.Now();
    TString prevWriter = State->GetReadWriteAccessClientId();
    args.WriterLastActivityTimestamp = State->GetLastActivityTimestamp(prevWriter);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Received add client %s:%s request; pipe server %s, sender %s",
        LogTitle.GetWithTime().c_str(),
        args.Info.GetClientId().Quote().c_str(),
        args.Info.GetInstanceId().Quote().c_str(),
        ToString(args.PipeServerActorId).c_str(),
        ToString(args.RequestInfo->Sender).c_str());

    auto res = State->AddClient(
        args.Info,
        args.PipeServerActorId,
        args.RequestInfo->Sender,
        now);
    args.Error = std::move(res.Error);
    args.ForceTabletRestart = res.ForceTabletRestart;

    TVolumeDatabase db(tx.DB);
    db.WriteHistory(
        State->LogAddClient(
            ctx.Now(),
            args.Info,
            args.Error,
            args.PipeServerActorId,
            args.RequestInfo->Sender));

    if (SUCCEEDED(args.Error.GetCode())) {
        // Set flag only with first client
        if (State->GetClients().size() == 1 &&
            !Config->GetDisableStartPartitionsForGc())
        {
            State->SetStartPartitionsNeeded(true);
            db.WriteStartPartitionsNeeded(true);
        }
        if (prevWriter != State->GetReadWriteAccessClientId()) {
            args.WriterChanged = true;

            if (prevWriter) {
                const auto& clients = State->GetClients();
                auto it = clients.find(prevWriter);
                if (it != clients.end()) {
                    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
                        "[%lu] Replace %s writer with %s",
                        TabletID(),
                        prevWriter.Quote().data(),
                        State->GetReadWriteAccessClientId().Quote().data());
                }
            }
        }

        for (const auto& clientId: res.RemovedClientIds) {
            db.RemoveClient(clientId);
            State->RemoveClient(clientId, TActorId());

            auto builder = TStringBuilder() << "Preempted by " << args.Info.GetClientId();
            db.WriteHistory(
                State->LogRemoveClient(ctx.Now(), clientId, builder, {}));

            args.RemovedClientIds.emplace_back(clientId);
        }

        TVector<TString> staleClientIds;
        for (const auto& pair: State->GetClients()) {
            const auto& clientId = pair.first;
            const auto& clientInfo = pair.second;
            if (State->IsClientStale(clientInfo, now)) {
                staleClientIds.push_back(clientId);
            }
        }

        for (const auto& clientId: staleClientIds) {
            db.RemoveClient(clientId);
            State->RemoveClient(clientId, TActorId());
            db.WriteHistory(
                State->LogRemoveClient(ctx.Now(), clientId, "Stale", {}));

            args.RemovedClientIds.emplace_back(clientId);
        }

        db.WriteClient(args.Info);

        if (IsReadWriteMode(args.Info.GetVolumeAccessMode()) &&
            (args.Info.GetFillGeneration() > 0 ||
             State->GetMeta().GetVolumeConfig().GetIsFillFinished()))
        {
            State->UpdateFillSeqNumberInMeta(args.Info.GetFillSeqNumber());
            db.WriteMeta(State->GetMeta());
        }
    }
}

void TVolumeActor::CompleteAddClient(
    const TActorContext& ctx,
    TTxVolume::TAddClient& args)
{
    const bool needToAcquireOrReleaseDevices =
        State->IsDiskRegistryMediaKind() &&
        Config->GetAcquireNonReplicatedDevices() && !HasError(args.Error);

    Y_DEFER
    {
        if (!needToAcquireOrReleaseDevices) {
            PendingClientRequests.pop_front();
            ProcessNextPendingClientRequest(ctx);
        }
    };

    const auto& clientId = args.Info.GetClientId();
    const auto& diskId = args.DiskId;

    if (FAILED(args.Error.GetCode())) {
        auto response = std::make_unique<TEvVolume::TEvAddClientResponse>(
            std::move(args.Error));
        response->Record.MutableVolume()->SetDiskId(diskId);
        response->Record.SetClientId(clientId);
        response->Record.SetTabletId(TabletID());

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Added client %s to volume %s",
        TabletID(),
        clientId.Quote().data(),
        diskId.Quote().data());

    if (needToAcquireOrReleaseDevices) {
        ReleaseDiskFromOldClients(ctx, args.RemovedClientIds);

        // Acquire disk for new client.
        TAcquireReleaseDiskRequest request{
            args.Info.GetClientId(),
            args.Info.GetVolumeAccessMode(),
            args.Info.GetMountSeqNumber(),
            std::make_shared<TClientRequest>(
                args.RequestInfo,
                args.DiskId,
                args.PipeServerActorId,
                args.Info),
            args.ForceTabletRestart};
        AddAcquireReleaseDiskRequest(ctx, std::move(request));
    } else {
        auto response = CreateAddClientResponse(
            args.Error,
            TabletID(),
            clientId,
            args.ForceTabletRestart,
            args.Info.GetInstanceId(),
            *State);

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }

    OnClientListUpdate(ctx);

    const auto mediaKind = State->GetMeta().GetConfig().GetStorageMediaKind();

    if (IsReliableDiskRegistryMediaKind(mediaKind)) {
        const bool shouldResyncDueToInactivity = args.WriterLastActivityTimestamp
            && (ctx.Now() - args.WriterLastActivityTimestamp)
                > Config->GetResyncAfterClientInactivityInterval();

        if (args.WriterChanged || shouldResyncDueToInactivity) {
            State->SetReadWriteError(MakeError(E_REJECTED, "toggling resync"));
            ExecuteTx<TToggleResync>(
                ctx,
                nullptr,   // requestInfo
                true,      // resyncEnabled
                false      // alertResyncChecksumMismatch
            );
        }
    }
}

void TVolumeActor::OnClientListUpdate(const NActors::TActorContext& ctx)
{
    if (State->GetDiskRegistryBasedPartitionActor()) {
        NCloud::Send(
            ctx,
            State->GetDiskRegistryBasedPartitionActor(),
            std::make_unique<TEvVolume::TEvRWClientIdChanged>(
                State->GetReadWriteAccessClientId()));
    }

    if (StartMode != EVolumeStartMode::MOUNTED) {
        auto request = std::make_unique<TEvService::TEvVolumeMountStateChanged>(
            State->GetDiskId(),
            !State->GetLocalMountClientId().empty());
        NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleUpdateReadWriteClientInfo(
    const TEvVolumePrivate::TEvUpdateReadWriteClientInfo::TPtr& ev,
    const TActorContext& ctx)
{
    UpdateReadWriteClientInfoScheduled = false;

    ScheduleRegularUpdates(ctx);

    if (!State) {
        return;
    }

    const auto mediaKind =
        State->GetMeta().GetConfig().GetStorageMediaKind();

    if (!IsReliableDiskRegistryMediaKind(mediaKind)) {
        return;
    }

    const auto* info = State->GetClient(State->GetReadWriteAccessClientId());
    if (!info || info->GetDisconnectTimestamp()) {
        return;
    }

    State->SetLastActivityTimestamp(
        State->GetReadWriteAccessClientId(),
        ctx.Now());

    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TUpdateClientInfo>(
        ctx,
        std::move(requestInfo),
        State->GetReadWriteAccessClientId());
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareUpdateClientInfo(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateClientInfo& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteUpdateClientInfo(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateClientInfo& args)
{
    Y_UNUSED(ctx);

    Y_ABORT_UNLESS(State);

    if (const auto* info = State->GetClient(args.ClientId)) {
        TVolumeDatabase db(tx.DB);
        db.WriteClient(*info);
    }
}

void TVolumeActor::CompleteUpdateClientInfo(
    const TActorContext& ctx,
    TTxVolume::TUpdateClientInfo& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

}   // namespace NCloud::NBlockStore::NStorage
