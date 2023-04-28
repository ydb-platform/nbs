#include "agent_counters.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TDeviceCounters::Register(
    NMonitoring::TDynamicCountersPtr counters)
{
    ReadCount.Register(counters, "ReadCount");
    ReadBytes.Register(counters, "ReadBytes");

    WriteCount.Register(counters, "WriteCount");
    WriteBytes.Register(counters, "WriteBytes");

    ZeroCount.Register(counters, "ZeroCount");
    ZeroBytes.Register(counters, "ZeroBytes");

    ErrorCount.Register(counters, "ZeroCount");

    const auto& names = GetDefaultPercentileNames();
    TimePercentileCounters.reserve(names.size());

    auto percentileGroup = counters->GetSubgroup("percentiles", "Time");

    for (auto& name: names) {
        TimePercentileCounters.emplace_back(percentileGroup->GetCounter(name));
    }
}

void TDeviceCounters::Publish(TInstant now)
{
    ReadCount.Publish(now);
    ReadBytes.Publish(now);

    WriteCount.Publish(now);
    WriteBytes.Publish(now);

    ZeroCount.Publish(now);
    ZeroBytes.Publish(now);

    ErrorCount.Publish(now);

    for (size_t i = 0; i != TimePercentiles.size(); ++i) {
        *TimePercentileCounters[i] = std::lround(TimePercentiles[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAgentCounters::Register(
    const NProto::TAgentConfig& agentConfig,
    NMonitoring::TDynamicCountersPtr counters)
{
    auto agentCounters = counters->GetSubgroup("agent", agentConfig.GetAgentId());

    InitErrorsCount.Register(agentCounters, "Errors/Init");

    TotalReadCount.Register(agentCounters, "ReadCount");
    TotalReadBytes.Register(agentCounters, "ReadBytes");

    TotalWriteCount.Register(agentCounters, "WriteCount");
    TotalWriteBytes.Register(agentCounters, "WriteBytes");

    TotalZeroCount.Register(agentCounters, "ZeroCount");
    TotalZeroBytes.Register(agentCounters, "ZeroBytes");

    TotalErrorCount.Register(agentCounters, "ErrorCount");

    for (auto& deviceConfig: agentConfig.GetDevices()) {
        const auto& uuid = deviceConfig.GetDeviceUUID();

        DeviceCounters[uuid].Register(
            agentCounters->GetSubgroup("device", uuid));
    }

    MeanTimeBetweenFailures =
        agentCounters->GetCounter("MeanTimeBetweenFailures");
}

void TAgentCounters::UpdateReadCount(TDeviceCounters& counters, ui64 value)
{
    TotalReadCount.Increment(value);
    counters.ReadCount.Set(value);
}

void TAgentCounters::UpdateReadBytes(TDeviceCounters& counters, ui64 value)
{
    TotalReadBytes.Increment(value);
    counters.ReadBytes.Set(value);
}

void TAgentCounters::UpdateWriteCount(TDeviceCounters& counters, ui64 value)
{
    TotalWriteCount.Increment(value);
    counters.WriteCount.Set(value);
}

void TAgentCounters::UpdateWriteBytes(TDeviceCounters& counters, ui64 value)
{
    TotalWriteBytes.Increment(value);
    counters.WriteBytes.Set(value);
}

void TAgentCounters::UpdateZeroCount(TDeviceCounters& counters, ui64 value)
{
    TotalZeroCount.Increment(value);
    counters.ZeroCount.Set(value);
}

void TAgentCounters::UpdateZeroBytes(TDeviceCounters& counters, ui64 value)
{
    TotalZeroBytes.Increment(value);
    counters.ZeroBytes.Set(value);
}

void TAgentCounters::UpdateErrorCount(TDeviceCounters& counters, ui64 value)
{
    TotalErrorCount.Increment(value);
    counters.ErrorCount.Set(value);
}

void TAgentCounters::Update(
    const NProto::TAgentStats& stats,
    const NProto::TMeanTimeBetweenFailures& mtbf)
{
    InitErrorsCount.Set(stats.GetInitErrorsCount());

    TotalReadCount.Reset();
    TotalReadBytes.Reset();

    TotalWriteCount.Reset();
    TotalWriteBytes.Reset();

    TotalZeroCount.Reset();
    TotalZeroBytes.Reset();

    TotalErrorCount.Reset();

    for (const auto& device: stats.GetDeviceStats()) {
        UpdateDeviceStats(device);
    }

    MeanTimeBetweenFailures->Set(
        mtbf.GetBrokenCount()
        ? mtbf.GetWorkTime() / mtbf.GetBrokenCount()
        : 0);
}

void TAgentCounters::UpdateDeviceStats(const NProto::TDeviceStats& stats)
{
    auto it = DeviceCounters.find(stats.GetDeviceUUID());

    if (it == DeviceCounters.end()) {
        return;
    }

    auto& counters = it->second;

    UpdateReadCount(counters, stats.GetNumReadOps());
    UpdateReadBytes(counters, stats.GetBytesRead());

    UpdateWriteCount(counters, stats.GetNumWriteOps());
    UpdateWriteBytes(counters, stats.GetBytesWritten());

    UpdateZeroCount(counters, stats.GetNumZeroOps());
    UpdateZeroBytes(counters, stats.GetBytesZeroed());

    UpdateErrorCount(counters, stats.GetErrors());

    TVector<TBucketInfo> buckets(stats.HistogramBucketsSize());

    for (size_t i = 0; i != buckets.size(); ++i) {
        const auto& src = stats.GetHistogramBuckets(i);
        buckets[i] = { src.GetValue(), src.GetCount() };
    }

    counters.TimePercentiles = CalculateWeightedPercentiles(
        buckets,
        GetDefaultPercentiles());
}

void TAgentCounters::Publish(TInstant now)
{
    InitErrorsCount.Publish(now);

    TotalReadCount.Publish(now);
    TotalReadBytes.Publish(now);

    TotalWriteCount.Publish(now);
    TotalWriteBytes.Publish(now);

    TotalZeroCount.Publish(now);
    TotalZeroBytes.Publish(now);

    for (auto& [uuid, counters]: DeviceCounters) {
        counters.Publish(now);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
