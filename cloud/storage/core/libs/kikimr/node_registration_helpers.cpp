#include "node_registration_helpers.h"

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

NKikimrNodeBroker::TNodeInfo CreateNodeInfo(
    const NYdb::NDiscovery::TNodeInfo& info,
    std::optional<TString> nodeName)
{
    NKikimrNodeBroker::TNodeInfo node;
    node.SetNodeId(info.NodeId);
    node.SetAddress(TString{info.Address});
    node.SetExpire(info.Expire);
    node.SetPort(info.Port);
    node.SetHost(TString{info.Host});
    node.SetResolveHost(TString{info.ResolveHost});
    *node.MutableLocation() = CreateNodeLocation(info.Location);
    if (nodeName.has_value()) {
        node.SetName(std::move(*nodeName));
    }
    return node;
}

NActorsInterconnect::TNodeLocation CreateNodeLocation(
    const NYdb::NDiscovery::TNodeLocation& source)
{
    NActorsInterconnect::TNodeLocation location;
    if (source.DataCenter) {
        location.SetDataCenter(TString{source.DataCenter.value()});
    }
    if (source.Module) {
        location.SetModule(TString{source.Module.value()});
    }
    if (source.Rack) {
        location.SetRack(TString{source.Rack.value()});
    }
    if (source.Unit) {
        location.SetUnit(TString{source.Unit.value()});
    }
    return location;
}

NKikimrConfig::TStaticNameserviceConfig_TNode CreateStaticNodeInfo(
    const NYdb::NDiscovery::TNodeInfo& info)
{
    NKikimrConfig::TStaticNameserviceConfig_TNode node;
    node.SetNodeId(info.NodeId);
    node.SetAddress(TString{info.Address});
    node.SetPort(info.Port);
    node.SetHost(TString{info.Host});
    node.SetInterconnectHost(TString{info.ResolveHost});
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
