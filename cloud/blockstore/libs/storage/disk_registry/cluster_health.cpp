#include "cluster_health.h"

#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_state.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TClusterHealth::TClusterHealth(TDiskRegistryState* owner) : Owner(owner)
{
    for (const auto& agentConfig : Owner->GetAgents()) {
        Agents.insert(agentConfig.GetAgentId());
        DisconnectedAgents.insert(agentConfig.GetAgentId());
        if (agentConfig.GetState() == NProto::AGENT_STATE_UNAVAILABLE) {
            UnavailableAgents.insert(agentConfig.GetAgentId());
        }
    }
}

TClusterHealth::~TClusterHealth() = default;


void TClusterHealth::OnAgentDisconnected(const TString& agentId) {
    DisconnectedAgents.insert(agentId);
}

void TClusterHealth::OnAgentConnected(const TString& agentId) {
    DisconnectedAgents.erase(agentId);
}

void TClusterHealth::OnAgentStateChanged(const TString& agentId)
{
    const auto* agentConfig = Owner->FindAgent(agentId);
    if (!agentConfig) {
        Agents.erase(agentId);
        UnavailableAgents.erase(agentId);
        DisconnectedAgents.erase(agentId);
        return;
    }

    Agents.insert(agentId);
    if (agentConfig->GetState() == NProto::AGENT_STATE_UNAVAILABLE) {
        UnavailableAgents.insert(agentId);
        DisconnectedAgents.erase(agentId);
    } else {
        UnavailableAgents.erase(agentId);
    }
}

double TClusterHealth::GetDisconnectedAgentsRatio() const
{
    const ui32 agentCount = Agents.size();
    const ui32 disconnectedAgentCount = DisconnectedAgents.size();
    const ui32 unavailableAgentCount = UnavailableAgents.size();
    Y_DEBUG_ABORT_UNLESS(
        agentCount > unavailableAgentCount,
        "%u should be greater than %u",
        agentCount,
        unavailableAgentCount);

    return Max<double>(
        0,
        static_cast<double>(disconnectedAgentCount) /
            (agentCount - unavailableAgentCount));
}

}   // namespace NCloud::NBlockStore::NStorage
