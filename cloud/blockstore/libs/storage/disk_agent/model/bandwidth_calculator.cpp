#include "bandwidth_calculator.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

constexpr auto WindowDuration = TDuration::Seconds(1);

TBandwidthCalculator::TBandwidthCalculator(ui64 maxTotalBandwidth)
    : MaxTotalBandwidth(maxTotalBandwidth)
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
    return MaxTotalBandwidth / DeviceLastRequest.size();
}

}   // namespace NCloud::NBlockStore::NStorage
