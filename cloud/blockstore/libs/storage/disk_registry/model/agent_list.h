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
    TInstant LastAgentDisconnectTs;
    THashMap<TString, TInstant> Rack2LastDisconnectTs;

    TVector<NProto::TAgentConfig> Agents;
    TVector<TAgentCounters> AgentCounters;
    TSimpleCounter RejectAgentTimeoutCounter;

    THashMap<TAgentId, size_t> AgentIdToIdx;
    THashMap<TNodeId, size_t> NodeIdToIdx;

    THashSet<TAgentId> DisconnectedAgents;

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

    struct TAgentRegistrationResult
    {
        std::reference_wrapper<NProto::TAgentConfig> Agent;
        THashSet<TDeviceId> NewDevices;
        TNodeId PrevNodeId = 0;
        THashMap<TDeviceId, NProto::TDeviceConfig> OldConfigs;
    };

    TAgentRegistrationResult RegisterAgent(
        NProto::TAgentConfig config,
        TInstant timestamp,
        const TKnownAgent& knownAgent);

    struct TUpdateAgentDevicesResult
    {
        NProto::TAgentConfig* Agent = nullptr;
        TVector<TDeviceId> NewDevices;
    };

    // If agent with agentId exists, split devices between Devices &
    // UnknownDevices according to knownAgent.
    TUpdateAgentDevicesResult TryUpdateAgentDevices(
        const TString& agentId,
        const TKnownAgent& knownAgent);

    void OnAgentDisconnected(const TAgentId& agentId);

    bool RemoveAgent(TNodeId nodeId);
    bool RemoveAgent(const TAgentId& agentId);

    bool RemoveAgentFromNode(TNodeId nodeId);

    void PublishCounters(TInstant now);
    void UpdateCounters(
        const TString& agentId,
        const NProto::TAgentStats& stats,
        const NProto::TMeanTimeBetweenFailures& mtbf);

    TDuration GetRejectAgentTimeout(TInstant now, const TString& agentId) const;
    void OnAgentDisconnected(TInstant now, const TString& agentId);

    const THashMap<TString, NProto::TDiskRegistryAgentParams>& GetDiskRegistryAgentListParams() const
    {
        return DiskRegistryAgentListParams;
    }
    const NProto::TDiskRegistryAgentParams* GetDiskRegistryAgentListParams(
        const TString& agentId) const;
    void SetDiskRegistryAgentListParams(
        const TString& agentId, const NProto::TDiskRegistryAgentParams& params);
    TVector<TString> CleanupExpiredAgentListParams(TInstant now);
    TVector<TString> GetAgentIdsWithOverriddenListParams() const;

private:
    NProto::TAgentConfig& AddAgent(NProto::TAgentConfig config);

    TAgentRegistrationResult AddNewAgent(
        NProto::TAgentConfig config,
        TInstant timestamp,
        const TKnownAgent& knownAgent);

    void TransferAgent(
        NProto::TAgentConfig& agent,
        TNodeId newNodeId);

    void RemoveAgentByIdx(size_t index);

    bool ValidateSerialNumber(
        const TKnownAgent& knownAgent,
        const NProto::TDeviceConfig& config);

    void UpdateDevice(
        NProto::TDeviceConfig& device,
        const NProto::TAgentConfig& agent,
        const TKnownAgent& knownAgent,
        TInstant timestamp,
        const NProto::TDeviceConfig& oldConfig);

    void AddUpdatedDevice(
        NProto::TAgentConfig& agent,
        const TKnownAgent& knownAgent,
        TInstant timestamp,
        const NProto::TDeviceConfig& oldDevice,
        NProto::TDeviceConfig newConfig);

    void AddLostDevice(
        NProto::TAgentConfig& agent,
        TInstant timestamp,
        NProto::TDeviceConfig device);

    void AddNewDevice(
        NProto::TAgentConfig& agent,
        const TKnownAgent& knownAgent,
        TInstant timestamp,
        NProto::TDeviceConfig device);

    TString FindAgentRack(const TString& agentId) const;

    void RegisterCounters(const NProto::TAgentConfig& agent);
};

}   // namespace NCloud::NBlockStore::NStorage
