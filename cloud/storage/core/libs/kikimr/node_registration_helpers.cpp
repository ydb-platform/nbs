#include "node_registration_helpers.h"

#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

NKikimrNodeBroker::TNodeInfo CreateNodeInfo(
    const NYdb::NDiscovery::TNodeInfo& info)
{
    NKikimrNodeBroker::TNodeInfo node;
    node.SetNodeId(info.NodeId);
    node.SetAddress(info.Address);
    node.SetExpire(info.Expire);
    node.SetPort(info.Port);
    node.SetHost(info.Host);
    node.SetResolveHost(info.ResolveHost);
    *node.MutableLocation() = CreateNodeLocation(info.Location);
    return node;
}

NActorsInterconnect::TNodeLocation CreateNodeLocation(
    const NYdb::NDiscovery::TNodeLocation& source)
{
    NActorsInterconnect::TNodeLocation location;
    if (source.DataCenter) {
        location.SetDataCenter(source.DataCenter.value());
    }
    if (source.Module) {
        location.SetModule(source.Module.value());
    }
    if (source.Rack) {
        location.SetRack(source.Rack.value());
    }
    if (source.Unit) {
        location.SetUnit(source.Unit.value());
    }
    return location;
}

NKikimrConfig::TStaticNameserviceConfig_TNode CreateStaticNodeInfo(
    const NYdb::NDiscovery::TNodeInfo& info)
{
    NKikimrConfig::TStaticNameserviceConfig_TNode node;
    node.SetNodeId(info.NodeId);
    node.SetAddress(info.Address);
    node.SetPort(info.Port);
    node.SetHost(info.Host);
    node.SetInterconnectHost(info.ResolveHost);
    *node.MutableLocation() = CreateNodeLocation(info.Location);
    return node;
}

NKikimrConfig::TStaticNameserviceConfig_TNode CreateStaticNodeInfo(
    const NKikimrNodeBroker::TNodeInfo& info)
{
    NKikimrConfig::TStaticNameserviceConfig_TNode node;
    node.SetNodeId(info.GetNodeId());
    node.SetAddress(info.GetAddress());
    node.SetPort(info.GetPort());
    node.SetHost(info.GetHost());
    node.SetInterconnectHost(info.GetResolveHost());
    *node.MutableLocation() = info.GetLocation();
    return node;
}

}   // namespace NCloud::NBlockStore::NStorage
