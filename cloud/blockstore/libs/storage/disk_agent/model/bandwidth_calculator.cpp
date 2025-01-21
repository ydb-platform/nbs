#include "bandwidth_calculator.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto WindowDuration = TDuration::Seconds(1);

ui64 GetNetworkBandwidth(const TDiskAgentConfig& config)
{
    const ui64 networkMbitThroughput =
        config.GetNetworkMbitThroughput()
            ? config.GetNetworkMbitThroughput()
            : config.GetThrottlerConfig().GetDefaultNetworkMbitThroughput();
    return (networkMbitThroughput * 1_MB / 8) *
           config.GetThrottlerConfig().GetDirectCopyBandwidthFraction();
}

}   // namespace

TBandwidthCalculator::TBandwidthCalculator(const TDiskAgentConfig& config)
    : MaxTotalBandwidth(GetNetworkBandwidth(config))
    , MaxDeviceBandwidth(
          config.GetThrottlerConfig().GetMaxDeviceBandwidthMiB() * 1_MB)
{}

TBandwidthCalculator::~TBandwidthCalculator() = default;

ui64 TBandwidthCalculator::RegisterRequest(
    const TString& deviceUUID,
    TInstant now)
{
    DeviceLastRequest[deviceUUID] = now;
    ClearHistory(now - WindowDuration);
    return GetRecommendedBandwidth();
}

void TBandwidthCalculator::ClearHistory(TInstant deadline)
{
    for (auto it = DeviceLastRequest.begin(); it != DeviceLastRequest.end();) {
        if (it->second < deadline) {
            DeviceLastRequest.erase(it++);
        } else {
            ++it;
        }
    }
}

ui64 TBandwidthCalculator::GetRecommendedBandwidth() const
{
    ui64 result = MaxTotalBandwidth / DeviceLastRequest.size();
    if (MaxDeviceBandwidth) {
        result = Min(MaxDeviceBandwidth, result);
    }
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
