#include "agent_list.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

double UpdateRejectTimeoutMultiplier(
    double currentMultiplier,
    TInstant lastUpdateTs,
    double timeoutGrowthFactor,
    TDuration disconnectRecoveryInterval,
    TInstant now)
{
    if (!disconnectRecoveryInterval) {
        disconnectRecoveryInterval = TDuration::Minutes(1);
    }
    double exponent = double((now - lastUpdateTs).MicroSeconds())
        / disconnectRecoveryInterval.MicroSeconds();
    currentMultiplier /= pow(timeoutGrowthFactor, exponent);

    if (currentMultiplier < 1) {
        currentMultiplier = 1;
    }

    return currentMultiplier;
}

////////////////////////////////////////////////////////////////////////////////

void SetInvalidSerialNumberError(
    NProto::TDeviceConfig& device,
    TInstant timestamp)
{
    device.SetState(NProto::DEVICE_STATE_ERROR);
    device.SetStateTs(timestamp.MicroSeconds());
    device.SetStateMessage(TStringBuilder()
        << "invalid serial number: " << device.GetSerialNumber());
}

////////////////////////////////////////////////////////////////////////////////

struct TByUUID
{
    template <typename T, typename U>
    bool operator () (const T& lhs, const U& rhs) const
    {
        return lhs.GetDeviceUUID() < rhs.GetDeviceUUID();
    }
};

template <typename T>
T SetDifference(const T& a, const T& b)
{
    T diff;

    std::set_difference(
        a.cbegin(), a.cend(),
        b.cbegin(), b.cend(),
        RepeatedPtrFieldBackInserter(&diff),
        TByUUID());

    return diff;
}

template <typename T>
T SetIntersection(const T& a, const T& b)
{
    T comm;

    std::set_intersection(
        a.cbegin(), a.cend(),
        b.cbegin(), b.cend(),
        RepeatedPtrFieldBackInserter(&comm),
        TByUUID());

    return comm;
}

NProto::TDeviceConfig& EnsureDevice(
    NProto::TAgentConfig& agent,
    const TString& uuid)
{
    auto& devices = *agent.MutableDevices();

    auto it = LowerBoundBy(
        devices.begin(),
        devices.end(),
        uuid,
        [] (const auto& x) -> const TString& {
            return x.GetDeviceUUID();
        });

    Y_VERIFY_DEBUG(it != devices.end());
    Y_VERIFY_DEBUG(it->GetDeviceUUID() == uuid);

    return *it;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TAgentList::TAgentList(
        const TAgentListConfig& config,
        NMonitoring::TDynamicCountersPtr counters,
        TVector<NProto::TAgentConfig> configs,
        THashMap<TString, NProto::TDiskRegistryAgentParams> diskRegistryAgentListParams,
        TLog log)
    : Config(config)
    , ComponentGroup(std::move(counters))
    , DiskRegistryAgentListParams(std::move(diskRegistryAgentListParams))
    , Log(std::move(log))
{
    Agents.reserve(configs.size());

    for (auto& config: configs) {
        AddAgent(std::move(config));
    }

    if (ComponentGroup) {
        RejectAgentTimeoutCounter.Register(ComponentGroup, "RejectAgentTimeout");
    }
}

NProto::TAgentConfig& TAgentList::AddAgent(NProto::TAgentConfig config)
{
    auto& agent = Agents.emplace_back(std::move(config));

    if (ComponentGroup) {
        AgentCounters.emplace_back().Register(agent, ComponentGroup);
    }

    auto& devices = *agent.MutableDevices();

    Sort(devices, TByUUID());

    for (auto& device: devices) {
        device.SetNodeId(agent.GetNodeId());
        device.SetAgentId(agent.GetAgentId());
        if (device.GetUnadjustedBlockCount() == 0) {
            device.SetUnadjustedBlockCount(device.GetBlocksCount());
        }
    }

    const size_t agentIndex = Agents.size() - 1;

    AgentIdToIdx[agent.GetAgentId()] = agentIndex;

    if (const ui32 nodeId = agent.GetNodeId()) {
        NodeIdToIdx[nodeId] = agentIndex;
    }

    return agent;
}

NProto::TAgentConfig& TAgentList::AddNewAgent(
    NProto::TAgentConfig agentConfig,
    TInstant timestamp,
    const TKnownAgent& knownAgent,
    THashSet<TDeviceId>* newDevices)
{
    Y_VERIFY(newDevices);
    Y_VERIFY_DEBUG(
        agentConfig.GetState() == NProto::AGENT_STATE_ONLINE,
        "Trying to add a new agent which is not in online state");

    agentConfig.SetState(NProto::AGENT_STATE_ONLINE);
    agentConfig.SetWorkTs(timestamp.Seconds());
    agentConfig.SetStateTs(timestamp.MicroSeconds());
    agentConfig.SetStateMessage("New agent");

    for (auto& device: *agentConfig.MutableDevices()) {
        device.SetStateTs(timestamp.MicroSeconds());
        device.SetUnadjustedBlockCount(device.GetBlocksCount());

        if (device.GetState() != NProto::DEVICE_STATE_ERROR
                && !ValidateSerialNumber(knownAgent, device))
        {
            SetInvalidSerialNumberError(device, timestamp);
        }

        newDevices->insert(device.GetDeviceUUID());
    }

    return AddAgent(std::move(agentConfig));
}

////////////////////////////////////////////////////////////////////////////////

ui32 TAgentList::FindNodeId(const TAgentId& agentId) const
{
    const auto* agent = FindAgent(agentId);

    return agent
        ? agent->GetNodeId()
        : 0;
}

const NProto::TAgentConfig* TAgentList::FindAgent(TNodeId nodeId) const
{
    return const_cast<TAgentList*>(this)->FindAgent(nodeId);
}

const NProto::TAgentConfig* TAgentList::FindAgent(const TAgentId& agentId) const
{
    return const_cast<TAgentList*>(this)->FindAgent(agentId);
}

NProto::TAgentConfig* TAgentList::FindAgent(TNodeId nodeId)
{
    auto it = NodeIdToIdx.find(nodeId);
    if (it == NodeIdToIdx.end()) {
        return nullptr;
    }

    return &Agents[it->second];
}

NProto::TAgentConfig* TAgentList::FindAgent(const TAgentId& agentId)
{
    auto it = AgentIdToIdx.find(agentId);
    if (it == AgentIdToIdx.end()) {
        return nullptr;
    }

    return &Agents[it->second];
}

////////////////////////////////////////////////////////////////////////////////

void TAgentList::TransferAgent(
    NProto::TAgentConfig& agent,
    TNodeId newNodeId)
{
    Y_VERIFY_DEBUG(newNodeId != 0);
    Y_VERIFY_DEBUG(!FindAgent(newNodeId));

    NodeIdToIdx.erase(agent.GetNodeId());
    NodeIdToIdx[newNodeId] = std::distance(&Agents[0], &agent);

    agent.SetNodeId(newNodeId);
}

bool TAgentList::ValidateSerialNumber(
    const TKnownAgent& knownAgent,
    const NProto::TDeviceConfig& config)
{
    if (!Config.SerialNumberValidationEnabled) {
        return true;
    }

    auto* knownDevice = knownAgent.Devices.FindPtr(config.GetDeviceUUID());
    return knownDevice
        && knownDevice->GetSerialNumber() == config.GetSerialNumber();
}

NProto::TAgentConfig& TAgentList::RegisterAgent(
    NProto::TAgentConfig agentConfig,
    TInstant timestamp,
    const TKnownAgent& knownAgent,
    THashSet<TDeviceId>* newDeviceIds)
{
    Y_VERIFY(newDeviceIds);

    auto* agent = FindAgent(agentConfig.GetAgentId());

    if (!agent) {
        STORAGE_INFO("A brand new agent %s #%d has arrived",
            agentConfig.GetAgentId().c_str(),
            agentConfig.GetNodeId());

        return AddNewAgent(
            std::move(agentConfig),
            timestamp,
            knownAgent,
            newDeviceIds);
    }

    if (agent->GetNodeId() != agentConfig.GetNodeId()) {
        STORAGE_INFO("Agent %s changed his previous node from #%d to #%d",
            agentConfig.GetAgentId().c_str(),
            agent->GetNodeId(),
            agentConfig.GetNodeId());

        TransferAgent(*agent, agentConfig.GetNodeId());
    }

    agent->SetSeqNumber(agentConfig.GetSeqNumber());
    agent->SetDedicatedDiskAgent(agentConfig.GetDedicatedDiskAgent());

    auto& newList = *agentConfig.MutableDevices();
    Sort(newList, TByUUID());

    auto newDevices = SetDifference(newList, agent->GetDevices());
    auto oldDevices = SetIntersection(newList, agent->GetDevices());
    auto removedDevices = SetDifference(agent->GetDevices(), newList);

    for (const auto& config: removedDevices) {
        auto& device = EnsureDevice(*agent, config.GetDeviceUUID());

        device.SetNodeId(agent->GetNodeId());
        device.SetAgentId(agent->GetAgentId());
        if (device.GetState() != NProto::DEVICE_STATE_ERROR) {
            device.SetState(NProto::DEVICE_STATE_ERROR);
            device.SetStateTs(timestamp.MicroSeconds());
            device.SetStateMessage("lost");
        }
    }

    for (auto& config: oldDevices) {
        auto& device = EnsureDevice(*agent, config.GetDeviceUUID());

        if (device.GetUnadjustedBlockCount() == 0) {
            device.SetUnadjustedBlockCount(device.GetBlocksCount());
        }

        // update volatile fields
        device.SetBaseName(config.GetBaseName());
        device.SetTransportId(config.GetTransportId());
        device.SetNodeId(agent->GetNodeId());
        device.SetAgentId(agent->GetAgentId());
        device.SetRack(config.GetRack());
        device.MutableRdmaEndpoint()->CopyFrom(config.GetRdmaEndpoint());

        if (config.GetState() == NProto::DEVICE_STATE_ERROR) {
            device.SetState(config.GetState());
            device.SetStateTs(config.GetStateTs());
            device.SetStateMessage(config.GetStateMessage());

            continue;
        }

        if (!ValidateSerialNumber(knownAgent, config)) {
            SetInvalidSerialNumberError(config, timestamp);

            continue;
        }

        device.SetSerialNumber(config.GetSerialNumber());

        if (device.GetBlockSize() != config.GetBlockSize()
            || device.GetUnadjustedBlockCount() != config.GetBlocksCount())
        {
            device.SetState(NProto::DEVICE_STATE_ERROR);
            device.SetStateTs(timestamp.MicroSeconds());
            device.SetStateMessage(TStringBuilder() <<
                "configuration changed: "
                    << device.GetBlockSize() << "x" << device.GetUnadjustedBlockCount()
                    << " -> "
                    << config.GetBlockSize() << "x" << config.GetBlocksCount()
            );

            continue;
        }
    }

    for (auto& device: newDevices) {
        newDeviceIds->insert(device.GetDeviceUUID());

        device.SetStateTs(timestamp.MicroSeconds());
        device.SetUnadjustedBlockCount(device.GetBlocksCount());

        device.SetNodeId(agent->GetNodeId());
        device.SetAgentId(agent->GetAgentId());

        if (device.GetState() != NProto::DEVICE_STATE_ERROR
                && !ValidateSerialNumber(knownAgent, device))
        {
            SetInvalidSerialNumberError(device, timestamp);
        }

        *agent->MutableDevices()->Add() = std::move(device);
    }

    Sort(*agent->MutableDevices(), TByUUID());

    return *agent;
}

////////////////////////////////////////////////////////////////////////////////

void TAgentList::PublishCounters(TInstant now)
{
    for (auto& counter: AgentCounters) {
        counter.Publish(now);
    }

    RejectAgentTimeoutCounter.Set(GetRejectAgentTimeout(now, "").MilliSeconds());
    RejectAgentTimeoutCounter.Publish(now);
}

void TAgentList::UpdateCounters(
    const NProto::TAgentStats& stats,
    const NProto::TMeanTimeBetweenFailures& mtbf)
{
    // TODO: add AgentId to TAgentStats & use AgentIdToIdx (NBS-3280)
    auto it = NodeIdToIdx.find(stats.GetNodeId());
    if (it != NodeIdToIdx.end()) {
        AgentCounters[it->second].Update(stats, mtbf);
    }
}

////////////////////////////////////////////////////////////////////////////////

TDuration TAgentList::GetRejectAgentTimeout(TInstant now, const TString& agentId) const
{
    const auto m = UpdateRejectTimeoutMultiplier(
        RejectTimeoutMultiplier,
        LastAgentEventTs,
        Config.TimeoutGrowthFactor,
        Config.DisconnectRecoveryInterval,
        now);

    const auto* params = GetDiskRegistryAgentListParams(agentId);

    auto nonReplicatedAgentMinTimeout = params
        ? TDuration::MilliSeconds(params->GetNewNonReplicatedAgentMinTimeoutMs())
        : Config.MinRejectAgentTimeout;
    auto nonReplicatedAgentMaxTimeoutMs = params
        ? TDuration::MilliSeconds(params->GetNewNonReplicatedAgentMaxTimeoutMs())
        : Config.MaxRejectAgentTimeout;

    return Min(nonReplicatedAgentMaxTimeoutMs, m * nonReplicatedAgentMinTimeout);
}

void TAgentList::OnAgentDisconnected(TInstant now)
{
    const double multiplierLimit = Config.TimeoutGrowthFactor *
        Config.MaxRejectAgentTimeout / Config.MinRejectAgentTimeout;

    RejectTimeoutMultiplier = Min(UpdateRejectTimeoutMultiplier(
        RejectTimeoutMultiplier,
        LastAgentEventTs,
        Config.TimeoutGrowthFactor,
        Config.DisconnectRecoveryInterval,
        now) * Config.TimeoutGrowthFactor, multiplierLimit);

    LastAgentEventTs = now;
}

////////////////////////////////////////////////////////////////////////////////

bool TAgentList::RemoveAgent(TNodeId nodeId)
{
    auto it = NodeIdToIdx.find(nodeId);

    if (it == NodeIdToIdx.end()) {
        return false;
    }

    RemoveAgentByIdx(it->second);

    return true;
}

bool TAgentList::RemoveAgent(const TAgentId& agentId)
{
    auto it = AgentIdToIdx.find(agentId);

    if (it == AgentIdToIdx.end()) {
        return false;
    }

    RemoveAgentByIdx(it->second);

    return true;
}

bool TAgentList::RemoveAgentFromNode(TNodeId nodeId)
{
    return NodeIdToIdx.erase(nodeId) != 0;
}

void TAgentList::RemoveAgentByIdx(size_t index)
{
    if (index != Agents.size() - 1) {
        std::swap(Agents[index], Agents.back());

        if (!AgentCounters.empty()) {
            std::swap(AgentCounters[index], AgentCounters.back());
        }

        auto& buddy = Agents[index];
        AgentIdToIdx[buddy.GetAgentId()] = index;

        if (const ui32 nodeId = buddy.GetNodeId()) {
            NodeIdToIdx[nodeId] = index;
        }
    }

    auto& agent = Agents.back();

    AgentIdToIdx.erase(agent.GetAgentId());
    NodeIdToIdx.erase(agent.GetNodeId());

    Agents.pop_back();

    if (!AgentCounters.empty()) {
        AgentCounters.pop_back();
    }
}

const NProto::TDiskRegistryAgentParams* TAgentList::GetDiskRegistryAgentListParams(
    const TString& agentId) const
{
    return DiskRegistryAgentListParams.FindPtr(agentId);
}

TVector<TString> TAgentList::CleanupExpiredAgentListParams(TInstant now) {
    TVector<TString> expiredAgentListParamsAgentIds;
    for (const auto& [agentId, params]: DiskRegistryAgentListParams) {
        if (now.MilliSeconds() > params.GetDeadlineMs()) {
            expiredAgentListParamsAgentIds.push_back(agentId);
        }
    }
    for (const auto& agentId: expiredAgentListParamsAgentIds) {
        DiskRegistryAgentListParams.erase(agentId);
    }
    return expiredAgentListParamsAgentIds;
}

void TAgentList::SetDiskRegistryAgentListParams(
    const TString& agentId,
    const NProto::TDiskRegistryAgentParams& params)
{
    DiskRegistryAgentListParams[agentId] = params;
}

}   // namespace NCloud::NBlockStore::NStorage
