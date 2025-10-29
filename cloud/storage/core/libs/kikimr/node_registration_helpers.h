#pragma once

#include "public.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/library/actors/protos/interconnect.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>


namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

NKikimrNodeBroker::TNodeInfo CreateNodeInfo(
    const NYdb::NDiscovery::TNodeInfo& info,
    std::optional<TString> nodeName);

NActorsInterconnect::TNodeLocation CreateNodeLocation(
    const NYdb::NDiscovery::TNodeLocation& source);

NKikimrConfig::TStaticNameserviceConfig_TNode CreateStaticNodeInfo(
    const NYdb::NDiscovery::TNodeInfo& info);

NKikimrConfig::TStaticNameserviceConfig_TNode CreateStaticNodeInfo(
    const NKikimrNodeBroker::TNodeInfo& info);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NStorage
