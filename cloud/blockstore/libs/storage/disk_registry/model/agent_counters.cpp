#include "agent_counters.h"

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString MakeDeviceCountersKey(const TString& agentId, const TString& deviceName)
{
    return TStringBuilder() << agentId << ":" << deviceName;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDeviceCounters::Register(NMonitoring::TDynamicCountersPtr counters)
{
    ReadCount.Register(counters, "ReadCount");
    ReadBytes.Register(counters, "ReadBytes");

    WriteCount.Register(counters, "WriteCount");
    WriteBytes.Register(counters, "WriteBytes");

    ZeroCount.Register(counters, "ZeroCount");
    ZeroBytes.Register(counters, "ZeroBytes");

    ErrorCount.Register(counters, "ErrorCount");
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
}

void TDeviceCounters::Reset()
{
    ReadCount.Reset();
    ReadBytes.Reset();

    WriteCount.Reset();
    WriteBytes.Reset();

    ZeroCount.Reset();
    ZeroBytes.Reset();

    ErrorCount.Reset();
}

void TDeviceCounters::Update(const NProto::TDeviceStats& stats)
{
    ReadCount.Increment(stats.GetNumReadOps());
    ReadBytes.Increment(stats.GetBytesRead());

    WriteCount.Increment(stats.GetNumWriteOps());
    WriteBytes.Increment(stats.GetBytesWritten());

    ZeroCount.Increment(stats.GetNumZeroOps());
    ZeroBytes.Increment(stats.GetBytesZeroed());

    ErrorCount.Increment(stats.GetErrors());
}

void TDeviceCountersWithPercentiles::Register(
    NMonitoring::TDynamicCountersPtr counters)
{
    TDeviceCounters::Register(counters);

    const auto& names = GetDefaultPercentileNames();
    TimePercentileCounters.reserve(names.size());

    auto percentileGroup = counters->GetSubgroup("percentiles", "Time");

    for (auto& name: names) {
        TimePercentileCounters.emplace_back(percentileGroup->GetCounter(name));
    }
}

void TDeviceCountersWithPercentiles::Publish(TInstant now)
{
    TDeviceCounters::Publish(now);

    for (size_t i = 0; i != TimePercentiles.size(); ++i) {
        *TimePercentileCounters[i] = std::lround(TimePercentiles[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAgentCounters::Register(
    const NProto::TAgentConfig& agentConfig,
    NMonitoring::TDynamicCountersPtr counters)
{
    auto agentCounters =
        counters->GetSubgroup("agent", agentConfig.GetAgentId());

    InitErrorsCount.Register(agentCounters, "Errors/Init");
    TotalCounters.Register(agentCounters);

    for (auto& deviceConfig: agentConfig.GetDevices()) {
        const auto& deviceName = deviceConfig.GetDeviceName();
        const auto key =
            MakeDeviceCountersKey(agentConfig.GetAgentId(), deviceName);

        DeviceCounters[key].Register(agentCounters->GetSubgroup("device", key));
    }

    MeanTimeBetweenFailures =
        agentCounters->GetCounter("MeanTimeBetweenFailures");
}

void TAgentCounters::Update(
    const TString& agentId,
    const NProto::TAgentStats& stats,
    const NProto::TMeanTimeBetweenFailures& mtbf)
{
    InitErrorsCount.Set(stats.GetInitErrorsCount());

    TotalCounters.Reset();

    for (const auto& device: stats.GetDeviceStats()) {
        ResetDeviceStats(agentId, device);
    }

    for (const auto& device: stats.GetDeviceStats()) {
        UpdateDeviceStats(agentId, device);
    }

    MeanTimeBetweenFailures->Set(
        mtbf.GetBrokenCount() ? mtbf.GetWorkTime() / mtbf.GetBrokenCount() : 0);
}

void TAgentCounters::ResetDeviceStats(
    const TString& agentId,
    const NProto::TDeviceStats& stats)
{
    const auto key = MakeDeviceCountersKey(agentId, stats.GetDeviceName());

    auto it = DeviceCounters.find(key);

    if (it == DeviceCounters.end()) {
        return;
    }

    it->second.Reset();
}

void TAgentCounters::UpdateDeviceStats(
    const TString& agentId,
    const NProto::TDeviceStats& stats)
{
    const auto key = MakeDeviceCountersKey(agentId, stats.GetDeviceName());

    auto it = DeviceCounters.find(key);

    if (it == DeviceCounters.end()) {
        return;
    }

    auto& counters = it->second;
    counters.Update(stats);
    TotalCounters.Update(stats);

    TVector<TBucketInfo> buckets(stats.HistogramBucketsSize());

    for (size_t i = 0; i != buckets.size(); ++i) {
        const auto& src = stats.GetHistogramBuckets(i);
        buckets[i] = {src.GetValue(), src.GetCount()};
    }

    counters.TimePercentiles =
        CalculateWeightedPercentiles(buckets, GetDefaultPercentiles());
}

void TAgentCounters::Publish(TInstant now)
{
    InitErrorsCount.Publish(now);
    TotalCounters.Publish(now);

    for (auto& [_, counters]: DeviceCounters) {
        counters.Publish(now);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
