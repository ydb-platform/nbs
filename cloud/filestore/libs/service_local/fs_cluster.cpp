#include "fs.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

NProto::TAddClusterNodeResponse TLocalFileSystem::AddClusterNode(
    const NProto::TAddClusterNodeRequest& request)
{
    const auto& nodeId = request.GetNodeId();

    with_lock (ClusterLock) {
        if (!Cluster.AddNode(nodeId)) {
            return TErrorResponse(
                S_FALSE,
                TStringBuilder()
                    << "node is already in the cluster: " << nodeId.Quote());
        }
    }

    return {};
}

NProto::TRemoveClusterNodeResponse TLocalFileSystem::RemoveClusterNode(
    const NProto::TRemoveClusterNodeRequest& request)
{
    const auto& nodeId = request.GetNodeId();

    with_lock (ClusterLock) {
        if (!Cluster.RemoveNode(nodeId)) {
            return TErrorResponse(
                S_FALSE,
                TStringBuilder()
                    << "node is not in the cluster: " << nodeId.Quote());
        }
    }

    return {};
}

NProto::TListClusterNodesResponse TLocalFileSystem::ListClusterNodes(
    const NProto::TListClusterNodesRequest& request)
{
    Y_UNUSED(request);

    NProto::TListClusterNodesResponse response;
    with_lock (ClusterLock) {
        Cluster.ListNodes(response);
    }

    return response;
}

NProto::TAddClusterClientsResponse TLocalFileSystem::AddClusterClients(
    const NProto::TAddClusterClientsRequest& request)
{
    const auto& nodeId = request.GetNodeId();

    with_lock (ClusterLock) {
        auto* node = Cluster.FindNode(nodeId);
        if (!node) {
            return TErrorResponse(
                E_ARGUMENT,
                TStringBuilder()
                    << "invalid node specified: " << nodeId.Quote());
        }

        for (const auto& client: request.GetClients()) {
            node->AddClient(client);
        }
    }

    return {};
}

NProto::TRemoveClusterClientsResponse TLocalFileSystem::RemoveClusterClients(
    const NProto::TRemoveClusterClientsRequest& request)
{
    const auto& nodeId = request.GetNodeId();

    with_lock (ClusterLock) {
        auto* node = Cluster.FindNode(nodeId);
        if (!node) {
            return TErrorResponse(
                E_ARGUMENT,
                TStringBuilder()
                    << "invalid node specified: " << nodeId.Quote());
        }

        for (const auto& clientId: request.GetClientIds()) {
            node->RemoveClient(clientId);
        }
    }

    return {};
}

NProto::TListClusterClientsResponse TLocalFileSystem::ListClusterClients(
    const NProto::TListClusterClientsRequest& request)
{
    const auto& nodeId = request.GetNodeId();

    NProto::TListClusterClientsResponse response;
    with_lock (ClusterLock) {
        auto* node = Cluster.FindNode(nodeId);
        if (!node) {
            return TErrorResponse(
                E_ARGUMENT,
                TStringBuilder()
                    << "invalid node specified: " << nodeId.Quote());
        }

        node->ListClients(response);
    }

    return response;
}

NProto::TUpdateClusterResponse TLocalFileSystem::UpdateCluster(
    const NProto::TUpdateClusterRequest& request)
{
    const auto& nodeId = request.GetNodeId();

    with_lock (ClusterLock) {
        auto* node = Cluster.FindNode(nodeId);
        if (!node) {
            return TErrorResponse(
                E_ARGUMENT,
                TStringBuilder()
                    << "invalid node specified: " << nodeId.Quote());
        }

        switch (request.GetUpdate()) {
            case NProto::TUpdateClusterRequest::E_START_GRACE:
                node->SetFlags(
                    ProtoFlag(NProto::TClusterNode::F_NEED_RECOVERY));
                break;

            case NProto::TUpdateClusterRequest::E_STOP_GRACE:
                node->ClearFlags(
                    ProtoFlag(NProto::TClusterNode::F_NEED_RECOVERY) |
                    ProtoFlag(NProto::TClusterNode::F_GRACE_ENFORCING));
                break;

            case NProto::TUpdateClusterRequest::E_JOIN_GRACE:
                node->SetFlags(
                    ProtoFlag(NProto::TClusterNode::F_GRACE_ENFORCING));
                break;

            default:
                return TErrorResponse(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "invalid update request: " << request.GetUpdate());
        }
    }

    return {};
}

}   // namespace NCloud::NFileStore
