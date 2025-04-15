#include "agent_list.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

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

double LogWithBase(double v, double base)
{
    return log(v) / log(base);
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

void FilterOutUnknownDevices(
    NProto::TAgentConfig& agentConfig,
    const TKnownAgent& knownAgent)
{
    auto& devices = *agentConfig.MutableDevices();
    auto& unknownDevices = *agentConfig.MutableUnknownDevices();

    auto it = std::partition(
        devices.begin(),
        devices.end(),
        [&] (const auto& d) {
            return knownAgent.Devices.contains(d.GetDeviceUUID());
        });

    std::move(it, devices.end(), RepeatedFieldBackInserter(&unknownDevices));

    devices.erase(it, devices.end());
}

bool IsAllowedDevice(
    const NProto::TAgentConfig& agent,
    const NProto::TDeviceConfig device)
{
    return !FindIfPtr(
        agent.GetUnknownDevices(),
        [&] (const auto& d) {
            return d.GetDeviceUUID() == device.GetDeviceUUID();
        });
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

auto TAgentList::AddNewAgent(
    NProto::TAgentConfig agentConfig,
    TInstant timestamp,
    const TKnownAgent& knownAgent) -> TAgentRegistrationResult
{
    Y_DEBUG_ABORT_UNLESS(
        agentConfig.GetState() == NProto::AGENT_STATE_ONLINE,
        "Trying to add a new agent which is not in online state");

    agentConfig.SetState(NProto::AGENT_STATE_ONLINE);
    agentConfig.SetWorkTs(timestamp.Seconds());
    agentConfig.SetStateTs(timestamp.MicroSeconds());
    agentConfig.SetStateMessage("New agent");

    THashSet<TDeviceId> newDevices;

    for (auto& device: *agentConfig.MutableDevices()) {
        UpdateDevice(device, agentConfig, knownAgent, timestamp, {});

        device.SetStateTs(timestamp.MicroSeconds());

        newDevices.insert(device.GetDeviceUUID());
    }

    return {AddAgent(std::move(agentConfig)), std::move(newDevices), 0, {}, {}};
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
    Y_DEBUG_ABORT_UNLESS(newNodeId != 0);
    Y_DEBUG_ABORT_UNLESS(!FindAgent(newNodeId));

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

void TAgentList::UpdateDevice(
    NProto::TDeviceConfig& device,
    const NProto::TAgentConfig& agent,
    const TKnownAgent& knownAgent,
    TInstant timestamp,
    const NProto::TDeviceConfig& oldConfig)
{
    STORAGE_CHECK_PRECONDITION(
        device.GetState() == NProto::DEVICE_STATE_ERROR ||
        device.GetState() == NProto::DEVICE_STATE_ONLINE);

    // If DA reports broken device we should keep the state and state message of
    // the device. At the same time we shouldn't remove the 'error' state
    // automatically.
    if (oldConfig.GetState() != NProto::DEVICE_STATE_ERROR
        && device.GetState() == NProto::DEVICE_STATE_ERROR)
    {
        device.SetStateTs(timestamp.MicroSeconds());
    } else {
        // Otherwise, we should keep the old state.

        device.SetState(oldConfig.GetState());
        device.SetStateTs(oldConfig.GetStateTs());
        device.SetStateMessage(oldConfig.GetStateMessage());
    }

    device.SetCmsTs(oldConfig.GetCmsTs());
    device.SetNodeId(agent.GetNodeId());
    device.SetAgentId(agent.GetAgentId());

    if (!device.GetUnadjustedBlockCount()) {
        device.SetUnadjustedBlockCount(device.GetBlocksCount());
    }

    // Keep the old blocks count. The device may be used.
    // Zero check is for tests only. Since a lot of them create new devices with
    // disks and |TDiskRegistryState::AdjustDeviceIfNeeded| can't restore
    // BlocksCount to a proper value.
    if (oldConfig.GetBlocksCount()) {
        device.SetBlocksCount(oldConfig.GetBlocksCount());
    }

    if (device.GetState() == NProto::DEVICE_STATE_ERROR) {
        return;
    }

    if (!ValidateSerialNumber(knownAgent, device)) {
        device.SetState(NProto::DEVICE_STATE_ERROR);
        device.SetStateTs(timestamp.MicroSeconds());
        device.SetStateMessage(TStringBuilder()
            << "invalid serial number: " << device.GetSerialNumber());

        return;
    }
}

void TAgentList::AddUpdatedDevice(
    NProto::TAgentConfig& agent,
    const TKnownAgent& knownAgent,
    TInstant timestamp,
    const NProto::TDeviceConfig& oldDevice,
    NProto::TDeviceConfig newDevice)
{
    UpdateDevice(newDevice, agent, knownAgent, timestamp, oldDevice);

    agent.MutableDevices()->Add(std::move(newDevice));
}

void TAgentList::AddLostDevice(
    NProto::TAgentConfig& agent,
    TInstant timestamp,
    NProto::TDeviceConfig device)
{
    device.SetNodeId(agent.GetNodeId());
    device.SetAgentId(agent.GetAgentId());

    if (device.GetState() != NProto::DEVICE_STATE_ERROR) {
        device.SetState(NProto::DEVICE_STATE_ERROR);
        device.SetStateTs(timestamp.MicroSeconds());
        device.SetStateMessage("lost");
    }

    agent.MutableDevices()->Add(std::move(device));
}

void TAgentList::AddNewDevice(
    NProto::TAgentConfig& agent,
    const TKnownAgent& knownAgent,
    TInstant timestamp,
    NProto::TDeviceConfig device)
{
    UpdateDevice(device, agent, knownAgent, timestamp, {});

    device.SetStateTs(timestamp.MicroSeconds());

    agent.MutableDevices()->Add(std::move(device));
}

auto TAgentList::TryUpdateAgentDevices(
    const TString& agentId,
    const TKnownAgent& knownAgent) -> TUpdateAgentDevicesResult
{
    auto* agent = FindAgent(agentId);
    if (!agent) {
        return {};
    }

    const auto timestamp = TInstant::MicroSeconds(agent->GetStateTs());

    TVector<TDeviceId> newDevices;

    // move known devices from UnknownDevices to Devices
    {
        auto end = agent->MutableUnknownDevices()->end();
        auto it = std::partition(
            agent->MutableUnknownDevices()->begin(),
            end,
            [&](const auto& device) {
                return !knownAgent.Devices.contains(device.GetDeviceUUID());
            });

        std::for_each(it, end, [&] (auto& newDevice) {
            newDevices.push_back(newDevice.GetDeviceUUID());
            AddNewDevice(*agent, knownAgent, timestamp, std::move(newDevice));
        });

        agent->MutableUnknownDevices()->erase(it, end);
    }

    FilterOutUnknownDevices(*agent, knownAgent);

    Sort(*agent->MutableDevices(), TByUUID());
    Sort(*agent->MutableUnknownDevices(), TByUUID());

    RegisterCounters(*agent);

    return {agent, std::move(newDevices)};
}

void TAgentList::RegisterCounters(const NProto::TAgentConfig& agent)
{
    if (ComponentGroup) {
        auto it = AgentIdToIdx.find(agent.GetAgentId());
        if (it != AgentIdToIdx.end()) {
            AgentCounters[it->second].Register(agent, ComponentGroup);
        }
    }
}

auto TAgentList::RegisterAgent(
    NProto::TAgentConfig agentConfig,
    TInstant timestamp,
    const TKnownAgent& knownAgent) -> TAgentRegistrationResult
{
    FilterOutUnknownDevices(agentConfig, knownAgent);

    auto* agent = FindAgent(agentConfig.GetAgentId());
    if (!agent) {
        STORAGE_INFO("A brand new agent %s #%d has arrived",
            agentConfig.GetAgentId().c_str(),
            agentConfig.GetNodeId());

        return AddNewAgent(
            std::move(agentConfig),
            timestamp,
            knownAgent);
    }

    const TNodeId prevNodeId = agent->GetNodeId();

    if (prevNodeId != agentConfig.GetNodeId()) {
        STORAGE_INFO("Agent %s changed his previous node from #%d to #%d",
            agentConfig.GetAgentId().c_str(),
            prevNodeId,
            agentConfig.GetNodeId());

        TransferAgent(*agent, agentConfig.GetNodeId());
    }

    agent->SetSeqNumber(agentConfig.GetSeqNumber());
    agent->SetDedicatedDiskAgent(agentConfig.GetDedicatedDiskAgent());
    *agent->MutableUnknownDevices()
        = std::move(*agentConfig.MutableUnknownDevices());
    agent->SetTemporaryAgent(agentConfig.GetTemporaryAgent());

    auto& newList = *agentConfig.MutableDevices();
    Sort(newList, TByUUID());

    auto oldList = std::move(*agent->MutableDevices());
    Sort(oldList, TByUUID());

    agent->MutableDevices()->Clear();

    THashSet<TDeviceId> newDeviceIds;
    THashMap<TDeviceId, NProto::TDeviceConfig> oldConfigs;

    int i = 0;
    int j = 0;

    THashSet<TString> lostDeviceUUIDs;
    while (i != newList.size() && j != oldList.size()) {
        auto& newDevice = newList[i];
        auto& oldDevice = oldList[j];

        const int cmp = newDevice.GetDeviceUUID()
            .compare(oldDevice.GetDeviceUUID());

        if (cmp == 0) {
            AddUpdatedDevice(
                *agent,
                knownAgent,
                timestamp,
                oldDevice,
                std::move(newDevice));

            oldConfigs[oldDevice.GetDeviceUUID()] = std::move(oldDevice);

            ++i;
            ++j;

            continue;
        }

        if (cmp < 0) {
            newDeviceIds.insert(newDevice.GetDeviceUUID());

            AddNewDevice(*agent, knownAgent, timestamp, std::move(newDevice));

            ++i;

            continue;
        }

        if (IsAllowedDevice(*agent, oldDevice)) {
            lostDeviceUUIDs.emplace(oldDevice.GetDeviceUUID());
            AddLostDevice(*agent, timestamp, std::move(oldDevice));
        }

        ++j;
    }

    for (; i < newList.size(); ++i) {
        auto& newDevice = newList[i];
        newDeviceIds.insert(newDevice.GetDeviceUUID());

        AddNewDevice(*agent, knownAgent, timestamp, std::move(newDevice));
    }

    for (; j < oldList.size(); ++j) {
        auto& oldDevice = oldList[j];
        if (IsAllowedDevice(*agent, oldDevice)) {
            lostDeviceUUIDs.emplace(oldDevice.GetDeviceUUID());
            AddLostDevice(*agent, timestamp, std::move(oldDevice));
        }
    }

    Sort(*agent->MutableDevices(), TByUUID());
    Sort(*agent->MutableUnknownDevices(), TByUUID());

    RegisterCounters(*agent);

    return {
        .Agent = *agent,
        .NewDevices = std::move(newDeviceIds),
        .PrevNodeId = prevNodeId,
        .OldConfigs = std::move(oldConfigs),
        .LostDeviceUUIDs = std::move(lostDeviceUUIDs)};
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
    const TString& agentId,
    const NProto::TAgentStats& stats,
    const NProto::TMeanTimeBetweenFailures& mtbf)
{
    auto it = AgentIdToIdx.find(agentId);
    if (it != AgentIdToIdx.end()) {
        AgentCounters[it->second].Update(agentId, stats, mtbf);
    }
}

////////////////////////////////////////////////////////////////////////////////

TString TAgentList::FindAgentRack(const TString& agentId) const
{
    if (const auto* agent = FindAgent(agentId)) {
        if (agent->DevicesSize()) {
            return agent->GetDevices(0).GetRack();
        }
    }

    return {};
}

TDuration TAgentList::GetRejectAgentTimeout(TInstant now, const TString& agentId) const
{
    const auto m = UpdateRejectTimeoutMultiplier(
        RejectTimeoutMultiplier,
        LastAgentDisconnectTs,
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

void TAgentList::OnAgentDisconnected(TInstant now, const TString& agentId)
{
    auto& ts = Rack2LastDisconnectTs[FindAgentRack(agentId)];

    // the following formula calculates the time needed to restore
    // MinRejectAgentTimeout after reaching MaxRejectAgentTimeout
    const TDuration disconnectCooldown = Config.DisconnectRecoveryInterval
        * LogWithBase(
            Config.MaxRejectAgentTimeout / Config.MinRejectAgentTimeout,
            Config.TimeoutGrowthFactor);

    if (ts + disconnectCooldown > now) {
        // we have already taken this rack unavailability into account
        return;
    }

    ts = now;

    const double multiplierLimit = Config.TimeoutGrowthFactor *
        Config.MaxRejectAgentTimeout / Config.MinRejectAgentTimeout;

    RejectTimeoutMultiplier = Min(UpdateRejectTimeoutMultiplier(
        RejectTimeoutMultiplier,
        LastAgentDisconnectTs,
        Config.TimeoutGrowthFactor,
        Config.DisconnectRecoveryInterval,
        now) * Config.TimeoutGrowthFactor, multiplierLimit);

    LastAgentDisconnectTs = now;
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

TVector<TString> TAgentList::GetAgentIdsWithOverriddenListParams() const
{
    TVector<TAgentId> agentIds(Reserve(DiskRegistryAgentListParams.size()));
    for (const auto& [agentId, _]: DiskRegistryAgentListParams) {
        agentIds.push_back(agentId);
    }
    return agentIds;
}

}   // namespace NCloud::NBlockStore::NStorage
