#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/diagnostics/solomon_counters.h>
#include <cloud/storage/core/libs/diagnostics/weighted_percentile.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceCounters
{
    TCumulativeCounter ReadCount;
    TCumulativeCounter ReadBytes;

    TCumulativeCounter WriteCount;
    TCumulativeCounter WriteBytes;

    TCumulativeCounter ZeroCount;
    TCumulativeCounter ZeroBytes;

    TCumulativeCounter ErrorCount;

    void Register(NMonitoring::TDynamicCountersPtr counters);
    void Publish(TInstant now);
    void Reset();
    void Update(const NProto::TDeviceStats& stats);
};

struct TDeviceCountersWithPercentiles: TDeviceCounters
{
    TVector<NMonitoring::TDynamicCounters::TCounterPtr> TimePercentileCounters;
    TVector<double> TimePercentiles;

    void Register(NMonitoring::TDynamicCountersPtr counters);
    void Publish(TInstant now);
};

////////////////////////////////////////////////////////////////////////////////

class TAgentCounters
{
public:
    TCumulativeCounter InitErrorsCount;

    TDeviceCounters TotalCounters;

    // AgentId:DeviceName -> Counters
    THashMap<TString, TDeviceCountersWithPercentiles> DeviceCounters;

    NMonitoring::TDynamicCounters::TCounterPtr MeanTimeBetweenFailures;

    void Register(
        const NProto::TAgentConfig& config,
        NMonitoring::TDynamicCountersPtr counters);

    void Update(
        const TString& agentId,
        const NProto::TAgentStats& stats,
        const NProto::TMeanTimeBetweenFailures& mtbf);
    void Publish(TInstant now);

private:
    void ResetDeviceStats(
        const TString& agentId,
        const NProto::TDeviceStats& stats);

    void UpdateDeviceStats(
        const TString& agentId,
        const NProto::TDeviceStats& stats);
};

}   // namespace NCloud::NBlockStore::NStorage
