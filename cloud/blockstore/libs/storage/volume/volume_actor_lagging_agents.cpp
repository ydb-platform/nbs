#include "volume_actor.h"

#include "volume_tx.h"

#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>

#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NCloud::NBlockStore::NStorage::NPartition;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::LaggingDevicesAreAllowed() const
{
    switch (State->GetConfig().GetStorageMediaKind()) {
        case NProto::STORAGE_MEDIA_SSD_MIRROR2: {
            const auto& partConfig = State->GetConfig();
            return Config->GetLaggingDevicesForMirror2DisksEnabled() ||
                   Config->IsLaggingDevicesForMirror2DisksFeatureEnabled(
                       partConfig.GetCloudId(),
                       partConfig.GetFolderId(),
                       partConfig.GetDiskId());
        }

        case NProto::STORAGE_MEDIA_SSD_MIRROR3: {
            const auto& partConfig = State->GetConfig();
            return Config->GetLaggingDevicesForMirror3DisksEnabled() ||
                   Config->IsLaggingDevicesForMirror3DisksFeatureEnabled(
                       partConfig.GetCloudId(),
                       partConfig.GetFolderId(),
                       partConfig.GetDiskId());
        }

        default:
            break;
    }

    return false;
}

void TVolumeActor::HandleReportLaggingDevicesToDR(
    const TEvVolumePrivate::TEvReportOutdatedLaggingDevicesToDR::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReportOutdatedLaggingDevicesToDR(ctx);
}

void TVolumeActor::ReportOutdatedLaggingDevicesToDR(
    const NActors::TActorContext& ctx)
{
    if (!State || State->GetMeta().GetLaggingAgentsInfo().GetAgents().empty()) {
        return;
    }

    auto request = std::make_unique<
        TEvDiskRegistry::TEvAddOutdatedLaggingDevicesRequest>();
    *request->Record.MutableDiskId() = State->GetDiskId();
    for (const auto& laggingAgent:
         State->GetMeta().GetLaggingAgentsInfo().GetAgents())
    {
        for (const auto& laggingDevice: laggingAgent.GetDevices()) {
            *request->Record.AddOutdatedLaggingDevices() = laggingDevice;
        }
    }
    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request),
        0   // cookie
    );
}

void TVolumeActor::HandleAddOutdatedLaggingDevicesResponse(
    const TEvDiskRegistry::TEvAddOutdatedLaggingDevicesResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(State);
    if (State->GetMeta().GetLaggingAgentsInfo().GetAgents().empty()) {
        return;
    }

    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Couldn't add lagging devices to the DR. Error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(msg->GetError()).c_str());

        ctx.Schedule(
            TDuration::Seconds(1),
            new TEvVolumePrivate::TEvReportOutdatedLaggingDevicesToDR());
        return;
    }
}

void TVolumeActor::HandleDeviceTimedOut(
    const TEvVolumePrivate::TEvDeviceTimedOutRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Device %s timed out",
        LogTitle.GetWithTime().c_str(),
        msg->DeviceUUID.Quote().c_str());

    if (!LaggingDevicesAreAllowed()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvVolumePrivate::TEvDeviceTimedOutResponse>(
                MakeError(
                    E_PRECONDITION_FAILED,
                    "Disk can't have lagging devices")));
        return;
    }

    if (UpdateVolumeConfigInProgress) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvVolumePrivate::TEvDeviceTimedOutResponse>(
                MakeError(E_REJECTED, "Volume config update in progress")));
        return;
    }

    const auto& meta = State->GetMeta();
    if (State->IsMirrorResyncNeeded() || meta.GetResyncIndex() > 0) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvVolumePrivate::TEvDeviceTimedOutResponse>(
                MakeError(
                    E_INVALID_STATE,
                    "Resync is in progress, can't have lagging devices")));
        return;
    }

    const NProto::TDeviceConfig* timedOutDeviceConfig =
        FindDeviceConfig(meta, msg->DeviceUUID);
    if (!timedOutDeviceConfig) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Could not find config with device %s",
            LogTitle.GetWithTime().c_str(),
            msg->DeviceUUID.c_str());

        auto response =
            std::make_unique<TEvVolumePrivate::TEvDeviceTimedOutResponse>(
                MakeError(
                    E_NOT_FOUND,
                    TStringBuilder() << "Could not find config with device "
                                     << msg->DeviceUUID));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto timedOutDeviceReplicaIndex =
        FindReplicaIndexByAgentId(meta, timedOutDeviceConfig->GetAgentId());
    Y_DEBUG_ABORT_UNLESS(timedOutDeviceReplicaIndex);

    TVector<NProto::TLaggingDevice> timedOutAgentDevices =
        CollectLaggingDevices(
            meta,
            *timedOutDeviceReplicaIndex,
            timedOutDeviceConfig->GetAgentId());
    Y_DEBUG_ABORT_UNLESS(!timedOutAgentDevices.empty());

    NProto::TLaggingAgent unavailableAgent;
    unavailableAgent.SetAgentId(timedOutDeviceConfig->GetAgentId());
    unavailableAgent.SetReplicaIndex(*timedOutDeviceReplicaIndex);
    unavailableAgent.MutableDevices()->Assign(
        std::make_move_iterator(timedOutAgentDevices.begin()),
        std::make_move_iterator(timedOutAgentDevices.end()));

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    AddTransaction(*requestInfo);
    ExecuteTx<TAddLaggingAgent>(
        ctx,
        std::move(requestInfo),
        std::move(unavailableAgent));
}

void TVolumeActor::HandleUpdateLaggingAgentMigrationState(
    const TEvVolumePrivate::TEvUpdateLaggingAgentMigrationState::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Lagging agent %s migration progress: %lu/%lu blocks",
        LogTitle.GetWithTime().c_str(),
        msg->AgentId.Quote().c_str(),
        msg->CleanBlockCount,
        msg->CleanBlockCount + msg->DirtyBlockCount);

    State->UpdateLaggingAgentMigrationState(
        msg->AgentId,
        msg->CleanBlockCount,
        msg->DirtyBlockCount);
}

void TVolumeActor::HandleLaggingAgentMigrationFinished(
    const TEvVolumePrivate::TEvLaggingAgentMigrationFinished::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Lagging agent %s migration finished",
        LogTitle.GetWithTime().c_str(),
        msg->AgentId.Quote().c_str());

    if (UpdateVolumeConfigInProgress) {
        // When the volume configuration update is in progress, we don't know at
        // which stage it is. By removing the lagging agent from the meta, we
        // have either done it before new meta were created, so our change will
        // take effect. Or we're too late and, upon partition restart, the
        // volume will send all the lagging agents to the DiskRegistry, which
        // will make them fresh and reallocate the volume.
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Lagging agent %s removal may fail because the volume config "
            "update is in progress",
            LogTitle.GetWithTime().c_str(),
            msg->AgentId.Quote().c_str());
        State->RemoveLaggingAgent(msg->AgentId);
        return;
    }

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    AddTransaction(*requestInfo);
    ExecuteTx<TRemoveLaggingAgent>(ctx, std::move(requestInfo), msg->AgentId);
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareAddLaggingAgent(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TAddLaggingAgent& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteAddLaggingAgent(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TAddLaggingAgent& args)
{
    Y_DEBUG_ABORT_UNLESS(!args.Agent.GetDevices().empty());
    const auto& meta = State->GetMeta();
    for (const auto& laggingAgent: meta.GetLaggingAgentsInfo().GetAgents()) {
        // Whether the agent is lagging already.
        if (laggingAgent.GetAgentId() == args.Agent.GetAgentId()) {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s Agent %s is already lagging",
                LogTitle.GetWithTime().c_str(),
                laggingAgent.GetAgentId().c_str());

            STORAGE_CHECK_PRECONDITION(
                laggingAgent.DevicesSize() == args.Agent.DevicesSize());
            args.Error = MakeError(S_ALREADY, "Device is already lagging");
            return;
        }

        // Intersect row indexes of known lagging devices and a new one. We only
        // allow one lagging device per row.
        const bool intersects =
            HaveCommonRows(args.Agent.GetDevices(), laggingAgent.GetDevices());
        if (intersects) {
            // TODO(komarevtsev-d): Allow source and target of the migration to
            // lag at the same time. "TLaggingAgentsReplicaProxyActor" does not
            // support this yet.
            LOG_WARN(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s Will not add a lagging agent %s. Agent's "
                "devices intersect with already lagging %s",
                LogTitle.GetWithTime().c_str(),
                args.Agent.GetAgentId().Quote().c_str(),
                laggingAgent.GetAgentId().Quote().c_str());

            args.Error = MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "There are other lagging devices on agent "
                                 << laggingAgent.GetAgentId());
            return;
        }
    }

    // Check for fresh devices in the same row.
    for (const auto& laggingDevice: args.Agent.GetDevices()) {
        const bool rowHasFreshDevice = RowHasFreshDevices(
            meta,
            laggingDevice.GetRowIndex(),
            args.Agent.GetReplicaIndex());
        if (rowHasFreshDevice) {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s There are other fresh devices on the same row with device "
                "%s",
                LogTitle.GetWithTime().c_str(),
                laggingDevice.GetDeviceUUID().Quote().c_str());

            args.Error = MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "There are other fresh devices on "
                                    "the same row with device "
                                 << laggingDevice.GetDeviceUUID());
            return;
        }
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Add lagging agent: %s, replicaIndex: %u, devices: ( %s )",
        LogTitle.GetWithTime().c_str(),
        args.Agent.GetAgentId().Quote().c_str(),
        args.Agent.GetReplicaIndex(),
        [&laggingDevices = args.Agent.GetDevices()]()
        {
            TStringBuilder ss;
            for (const auto& device: laggingDevices) {
                ss << "[" << device.GetDeviceUUID() << "; "
                   << device.GetRowIndex() << "], ";
            }
            ss.erase(ss.size() - 2);
            return ss;
        }()
            .c_str());

    State->AddLaggingAgent(args.Agent);
    TVolumeDatabase db(tx.DB);
    db.WriteMeta(State->GetMeta());
}

void TVolumeActor::CompleteAddLaggingAgent(
    const TActorContext& ctx,
    TTxVolume::TAddLaggingAgent& args)
{
    RemoveTransaction(*args.RequestInfo);

    if (!HasError(args.Error)) {
        State->ResetLaggingAgentMigrationState(args.Agent.GetAgentId());

        const auto& partActorId = State->GetDiskRegistryBasedPartitionActor();
        Y_DEBUG_ABORT_UNLESS(partActorId);
        NCloud::Send(
            ctx,
            partActorId,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvAddLaggingAgentRequest>(
                args.Agent));
    }

    auto response =
        std::make_unique<TEvVolumePrivate::TEvDeviceTimedOutResponse>(
            std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareRemoveLaggingAgent(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TRemoveLaggingAgent& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteRemoveLaggingAgent(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TRemoveLaggingAgent& args)
{
    auto laggingAgent = State->RemoveLaggingAgent(args.AgentId);
    if (!laggingAgent.has_value()) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Could not find an agent %s in lagging agents list.",
            LogTitle.GetWithTime().c_str(),
            args.AgentId.c_str());
        return;
    }

    TVolumeDatabase db(tx.DB);
    db.WriteMeta(State->GetMeta());
    args.RemovedLaggingAgent = std::move(*laggingAgent);
    args.ShouldStartResync = !State->HasLaggingAgents() &&
                             Config->GetResyncAfterLaggingAgentMigration();
}

void TVolumeActor::CompleteRemoveLaggingAgent(
    const TActorContext& ctx,
    TTxVolume::TRemoveLaggingAgent& args)
{
    if (args.RemovedLaggingAgent.GetAgentId().empty()) {
        return;
    }

    if (args.ShouldStartResync) {
        State->SetReadWriteError(MakeError(
            E_REJECTED,
            "toggling resync after lagging agent migration"));
        ExecuteTx<TToggleResync>(
            ctx,
            nullptr,   // requestInfo
            true,      // resyncEnabled
            true       // alertResyncChecksumMismatch
        );
    }

    RemoveTransaction(*args.RequestInfo);
    NCloud::Send(
        ctx,
        State->GetDiskRegistryBasedPartitionActor(),
        std::make_unique<
            TEvNonreplPartitionPrivate::TEvRemoveLaggingAgentRequest>(
            std::move(args.RemovedLaggingAgent)));
}

}   // namespace NCloud::NBlockStore::NStorage
