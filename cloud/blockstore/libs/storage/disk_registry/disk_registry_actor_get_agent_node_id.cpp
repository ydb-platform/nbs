#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleGetAgentNodeId(
    const TEvDiskRegistry::TEvGetAgentNodeIdRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(GetDependentDisks);

    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received GetAgentNodeId request: AgentId=%s",
        TabletID(),
        msg->Record.GetAgentId().c_str());

    Y_DEBUG_ABORT_UNLESS(State);
    auto response =
        std::make_unique<TEvDiskRegistry::TEvGetAgentNodeIdResponse>();
    const NProto::TAgentConfig* agent =
        State->FindAgent(msg->Record.GetAgentId());
    if (!agent) {
        *response->Record.MutableError() = MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Couldn't find agent with id: " << msg->Record.GetAgentId());
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    response->Record.SetNodeId(agent->GetNodeId());
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
