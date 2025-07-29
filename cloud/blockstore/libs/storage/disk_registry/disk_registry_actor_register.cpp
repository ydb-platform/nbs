#include "disk_registry_actor.h"
#include "disk_registry_database.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleRegisterAgent(
    const TEvDiskRegistry::TEvRegisterAgentRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(RegisterAgent);

    auto* msg = ev->Get();
    const auto& agentConfig = msg->Record.GetAgentConfig();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto getDeviceDecription = [] (const NProto::TDeviceConfig& config) {
        TStringStream out;
        out << config.GetDeviceUUID()
            << " (" << config.GetDeviceName() << " "
            << config.GetBlocksCount() << " x "  << config.GetBlockSize()
            << " PoolName:'" << config.GetPoolName() << "'"
            << " Rack:'" << config.GetRack() << "'"
            << "); ";
        return out.Str();
    };

    auto* mutableAgentConfig = msg->Record.MutableAgentConfig();
    for (auto& device: *mutableAgentConfig->MutableDevices()) {
        if (!device.GetRack().empty()) {
            continue;
        }

        ReportRegisterAgentWithEmptyRackName(
            "",
            {{"AgentId", agentConfig.GetAgentId()},
             {"NodeId", TStringBuilder() << agentConfig.GetNodeId()}});

        TString fakeRackName = "Rack-" + agentConfig.GetAgentId();
        device.SetRack(fakeRackName);
        LOG_CRIT(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Received RegisterAgent request with empty Rack name, "
            "generated new based on AgentId: NodeId=%u, AgentId=%s"
            ", SeqNo=%lu, Dedicated=%s, Device=%s",
            TabletID(),
            agentConfig.GetNodeId(),
            agentConfig.GetAgentId().c_str(),
            agentConfig.GetSeqNumber(),
            agentConfig.GetDedicatedDiskAgent() ? "true" : "false",
            getDeviceDecription(device).c_str()
        );
    }

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received RegisterAgent request: NodeId=%u, AgentId=%s"
        ", SeqNo=%lu, Dedicated=%s, Devices=[%s]",
        TabletID(),
        agentConfig.GetNodeId(),
        agentConfig.GetAgentId().c_str(),
        agentConfig.GetSeqNumber(),
        agentConfig.GetDedicatedDiskAgent() ? "true" : "false",
        [&agentConfig, &getDeviceDecription] {
            TStringStream out;
            for (const auto& config: agentConfig.GetDevices()) {
                out << getDeviceDecription(config);
            }
            return out.Str();
        }().c_str());

    ExecuteTx<TAddAgent>(
        ctx,
        std::move(requestInfo),
        std::move(*msg->Record.MutableAgentConfig()),
        ctx.Now(),
        ev->Recipient);
}

bool TDiskRegistryActor::PrepareAddAgent(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddAgent& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteAddAgent(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddAgent& args)
{
    TDiskRegistryDatabase db(tx.DB);

    auto [r, error] = State->RegisterAgent(db, args.Config, args.Timestamp);

    args.Error = std::move(error);
    args.AffectedDisks = std::move(r.AffectedDisks);
    args.NotifiedDisks = std::move(r.DisksToReallocate);
    args.DevicesToDisableIO = std::move(r.DevicesToDisableIO);

    if (HasError(args.Error)) {
        return;
    }

    for (auto it = ServerToAgentId.begin(); it != ServerToAgentId.end(); ) {
        const auto& agentId = it->second;

        if (agentId == args.Config.GetAgentId()) {
            const auto& serverId = it->first;
            NCloud::Send<TEvents::TEvPoisonPill>(ctx, serverId);

            ServerToAgentId.erase(it++);
        } else {
            ++it;
        }
    }

    ServerToAgentId[args.RegisterActorId] = args.Config.GetAgentId();

    auto& info = AgentRegInfo[args.Config.GetAgentId()];
    info.Connected = true;
    info.SeqNo += 1;

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Execute register agent: NodeId=%u, AgentId=%s"
        ", SeqNo=%lu",
        TabletID(),
        args.Config.GetNodeId(),
        args.Config.GetAgentId().c_str(),
        info.SeqNo);
}

void TDiskRegistryActor::CompleteAddAgent(
    const TActorContext& ctx,
    TTxDiskRegistry::TAddAgent& args)
{
    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Complete register agent: NodeId=%u, AgentId=%s"
        ", AffectedDisks=%lu, Error=%s",
        TabletID(),
        args.Config.GetNodeId(),
        args.Config.GetAgentId().c_str(),
        args.AffectedDisks.size(),
        FormatError(args.Error).c_str());

    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "AddAgent error: %s",
            FormatError(args.Error).c_str());
    }

    for (const auto& diskId: args.AffectedDisks) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] AffectedDiskID=%s",
            TabletID(),
            diskId.Quote().c_str());
    }

    for (const auto& diskId: args.NotifiedDisks) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] NotifiedDiskID=%s",
            TabletID(),
            diskId.Quote().c_str());
    }

    auto response = std::make_unique<TEvDiskRegistry::TEvRegisterAgentResponse>();
    *response->Record.MutableError() = std::move(args.Error);
    response->Record.MutableDevicesToDisableIO()->Assign(
        args.DevicesToDisableIO.begin(),
        args.DevicesToDisableIO.end());

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    SendCachedAcquireRequestsToAgent(ctx, args.Config);

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);
    SecureErase(ctx);
    StartMigration(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUnregisterAgent(
    const TEvDiskRegistry::TEvUnregisterAgentRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UnregisterAgent);

    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received UnregisterAgent request: NodeId=%u",
        TabletID(),
        msg->Record.GetNodeId());

    ExecuteTx<TRemoveAgent>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetNodeId(),
        ctx.Now());
}

bool TDiskRegistryActor::PrepareRemoveAgent(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemoveAgent& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteRemoveAgent(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemoveAgent& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    // TODO: affected disks
    args.Error = State->UnregisterAgent(db, args.NodeId);
}

void TDiskRegistryActor::CompleteRemoveAgent(
    const TActorContext& ctx,
    TTxDiskRegistry::TRemoveAgent& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "RemoveAgent error: %s",
            FormatError(args.Error).c_str());
    }

    auto response = std::make_unique<TEvDiskRegistry::TEvUnregisterAgentResponse>();

    *response->Record.MutableError() = std::move(args.Error);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
