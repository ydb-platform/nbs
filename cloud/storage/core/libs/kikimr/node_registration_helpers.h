#pragma once

#include "public.h"

#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/protos/node_broker.pb.h>
#include <contrib/ydb/library/actors/protos/interconnect.pb.h>
#include <ydb-cpp-sdk/client/discovery/discovery.h>


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
