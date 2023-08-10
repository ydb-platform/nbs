#pragma once

#include "public.h"

#include "agent_counters.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TAgentListConfig
{
    double TimeoutGrowthFactor = 2;
    TDuration MinRejectAgentTimeout;
    TDuration MaxRejectAgentTimeout;
    TDuration DisconnectRecoveryInterval;
    bool SerialNumberValidationEnabled = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TKnownAgent
{
    THashMap<TString, NProto::TDeviceConfig> Devices;
};

////////////////////////////////////////////////////////////////////////////////

class TAgentList
{
    using TAgentId = TString;
    using TDeviceId = TString;
    using TNodeId = ui32;

private:
    const TAgentListConfig Config;
    const NMonitoring::TDynamicCountersPtr ComponentGroup;

    double RejectTimeoutMultiplier = 1;
    TInstant LastAgentEventTs;

    TVector<NProto::TAgentConfig> Agents;
    TVector<TAgentCounters> AgentCounters;
    TSimpleCounter RejectAgentTimeoutCounter;

    THashMap<TAgentId, size_t> AgentIdToIdx;
    THashMap<TNodeId, size_t> NodeIdToIdx;

    THashMap<TString, NProto::TDiskRegistryAgentParams> DiskRegistryAgentListParams;

    TLog Log;

public:
    TAgentList(
        const TAgentListConfig& config,
        NMonitoring::TDynamicCountersPtr counters,
        TVector<NProto::TAgentConfig> configs,
        THashMap<TString, NProto::TDiskRegistryAgentParams> diskRegistryAgentListParams,
        TLog log);

    const TVector<NProto::TAgentConfig>& GetAgents() const
    {
        return Agents;
    }

    TNodeId FindNodeId(const TAgentId& agentId) const;

    NProto::TAgentConfig* FindAgent(TNodeId nodeId);
    const NProto::TAgentConfig* FindAgent(TNodeId nodeId) const;

    NProto::TAgentConfig* FindAgent(const TAgentId& agentId);
    const NProto::TAgentConfig* FindAgent(const TAgentId& agentId) const;

    NProto::TAgentConfig& RegisterAgent(
        NProto::TAgentConfig config,
        TInstant timestamp,
        const TKnownAgent& knownAgent,
        THashSet<TDeviceId>* newDevices);

    bool RemoveAgent(TNodeId nodeId);
    bool RemoveAgent(const TAgentId& agentId);

    bool RemoveAgentFromNode(TNodeId nodeId);

    void PublishCounters(TInstant now);
    void UpdateCounters(
        const NProto::TAgentStats& stats,
        const NProto::TMeanTimeBetweenFailures& mtbf);

    TDuration GetRejectAgentTimeout(TInstant now, const TString& agentId) const;
    void OnAgentDisconnected(TInstant now);

    const THashMap<TString, NProto::TDiskRegistryAgentParams>& GetDiskRegistryAgentListParams() const
    {
        return DiskRegistryAgentListParams;
    }
    const NProto::TDiskRegistryAgentParams* GetDiskRegistryAgentListParams(
        const TString& agentId) const;
    void SetDiskRegistryAgentListParams(
        const TString& agentId, const NProto::TDiskRegistryAgentParams& params);
    TVector<TString> CleanupExpiredAgentListParams(TInstant now);

private:
    NProto::TAgentConfig& AddAgent(NProto::TAgentConfig config);

    NProto::TAgentConfig& AddNewAgent(
        NProto::TAgentConfig config,
        TInstant timestamp,
        const TKnownAgent& knownAgent,
        THashSet<TDeviceId>* newDevices);

    void TransferAgent(
        NProto::TAgentConfig& agent,
        TNodeId newNodeId);

    void RemoveAgentByIdx(size_t index);

    bool ValidateSerialNumber(
        const TKnownAgent& knownAgent,
        const NProto::TDeviceConfig& config);
};

}   // namespace NCloud::NBlockStore::NStorage
