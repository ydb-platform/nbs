#include "agent_paths.h"

namespace NCloud::NBlockStore::NStorage {

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

}   // namespace NCloud::NBlockStore::NStorage
