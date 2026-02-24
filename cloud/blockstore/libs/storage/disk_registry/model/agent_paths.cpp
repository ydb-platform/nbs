#include "agent_paths.h"

namespace NCloud::NBlockStore::NStorage {

TAgentsPaths::TAgentsPaths(const TVector<NProto::TAgentConfig>& agents)
{
    for (const auto& agent: agents) {
        for (const auto& [path, state]: agent.GetPathAttachStates()) {
            if (state == NProto::PATH_ATTACH_STATE_ATTACHING) {
                AddPathToAttach(agent.GetAgentId(), path);
            }
        }
    }
}

auto TAgentsPaths::GetPathsToAttach() const -> const TPathsByAgentId&
{
    return PathsToAttach;
}

void TAgentsPaths::AddPathToAttach(const TString& agentId, const TString& path)
{
    auto& pathsForAgent = PathsToAttach[agentId];
    pathsForAgent.insert(path);
}

void TAgentsPaths::DeletePathToAttach(const TString& agentId, const TString& path)
{
    auto it = PathsToAttach.find(agentId);
    if (it == PathsToAttach.end()) {
        return;
    }

    auto& pathsForAgent = it->second;
    pathsForAgent.erase(path);

    if (!pathsForAgent) {
        PathsToAttach.erase(it);
    }
}

void TAgentsPaths::DeleteAgent(const TString& agentId)
{
    PathsToAttach.erase(agentId);
}

}   // namespace NCloud::NBlockStore::NStorage
