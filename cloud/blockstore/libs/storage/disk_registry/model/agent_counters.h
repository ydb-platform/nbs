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

    TCumulativeCounter TotalReadCount;
    TCumulativeCounter TotalReadBytes;

    TCumulativeCounter TotalWriteCount;
    TCumulativeCounter TotalWriteBytes;

    TCumulativeCounter TotalZeroCount;
    TCumulativeCounter TotalZeroBytes;

    TCumulativeCounter TotalErrorCount;

    THashMap<TString, TDeviceCounters> DeviceCounters;

    NMonitoring::TDynamicCounters::TCounterPtr MeanTimeBetweenFailures;

    void Register(
        const NProto::TAgentConfig& config,
        NMonitoring::TDynamicCountersPtr counters);

    void Update(
        const NProto::TAgentStats& stats,
        const NProto::TMeanTimeBetweenFailures& mtbf);
    void Publish(TInstant now);

private:
    void UpdateDeviceStats(const NProto::TDeviceStats& stats);

    void UpdateReadCount(TDeviceCounters& counters, ui64 value);
    void UpdateReadBytes(TDeviceCounters& counters, ui64 value);

    void UpdateWriteCount(TDeviceCounters& counters, ui64 value);
    void UpdateWriteBytes(TDeviceCounters& counters, ui64 value);

    void UpdateZeroCount(TDeviceCounters& counters, ui64 value);
    void UpdateZeroBytes(TDeviceCounters& counters, ui64 value);

    void UpdateErrorCount(TDeviceCounters& counters, ui64 value);
};

}   // namespace NCloud::NBlockStore::NStorage
