#include "cluster.h"

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

bool TClusterNode::AddClient(const NProto::TClusterClient& client)
{
    auto [it, inserted] =
        Clients.emplace(client.GetClientId(), client.GetOpaque());

    return inserted;
}

bool TClusterNode::RemoveClient(const TString& clientId)
{
    auto it = Clients.find(clientId);
    if (it != Clients.end()) {
        Clients.erase(it);
        return true;
    }

    return false;
}

void TClusterNode::ListClients(
    NProto::TListClusterClientsResponse& response) const
{
    for (const auto& [clientId, opaque]: Clients) {
        auto* record = response.AddClients();
        record->SetClientId(clientId);
        record->SetOpaque(opaque);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TCluster::AddNode(const TString& nodeId)
{
    auto [it, inserted] = Nodes.emplace(nodeId, TClusterNode());
    return inserted;
}

bool TCluster::RemoveNode(const TString& nodeId)
{
    auto it = Nodes.find(nodeId);
    if (it != Nodes.end()) {
        Nodes.erase(it);
        return true;
    }

    return false;
}

TClusterNode* TCluster::FindNode(const TString& nodeId)
{
    auto it = Nodes.find(nodeId);
    if (it != Nodes.end()) {
        return &it->second;
    }

    return nullptr;
}

void TCluster::ListNodes(NProto::TListClusterNodesResponse& response) const
{
    for (const auto& [nodeId, node]: Nodes) {
        auto* record = response.AddNodes();
        record->SetNodeId(nodeId);
        record->SetFlags(node.GetFlags());
        record->SetClients(node.NumClients());
    }
}

}   // namespace NCloud::NFileStore
