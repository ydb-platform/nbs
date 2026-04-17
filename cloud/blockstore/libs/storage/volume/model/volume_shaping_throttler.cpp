#include "volume_shaping_throttler.h"

#include <cloud/storage/core/libs/throttling/helpers.h>
#include <cloud/storage/core/protos/diagnostics.pb.h>

namespace NCloud::NBlockStore::NStorage {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TPerformanceProfile = NCloud::NProto::TPerformanceProfile;

const NProto::TShapingThrottlerQuota& GetShapingThrottlerQuota(
    const NProto::TShapingThrottlerConfig& shapingThrottlerConfig,
    NCloud::NProto::EStorageMediaKind storageMediaKind)
{
    switch (storageMediaKind) {
        case NCloud::NProto::STORAGE_MEDIA_SSD:
            return shapingThrottlerConfig.GetSsdQuota();
        case NCloud::NProto::STORAGE_MEDIA_HDD:
        case NCloud::NProto::STORAGE_MEDIA_HYBRID:
            return shapingThrottlerConfig.GetHddQuota();
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED:
            return shapingThrottlerConfig.GetNonreplQuota();
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2:
            return shapingThrottlerConfig.GetMirror2Quota();
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3:
            return shapingThrottlerConfig.GetMirror3Quota();
        default:
            return shapingThrottlerConfig.GetHddQuota();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVolumeShapingThrottler::TVolumeShapingThrottler(
    const NProto::TShapingThrottlerConfig& shapingThrottlerConfig,
    NCloud::NProto::EStorageMediaKind storageMediaKind,
    double spentShapingBudgetShare)
    : ShapingThrottlerQuota(
          GetShapingThrottlerQuota(shapingThrottlerConfig, storageMediaKind))
    , UnspentCostBucket(
          TDuration::MilliSeconds(ShapingThrottlerQuota.GetMaxBudget()),
          TDuration::MilliSeconds(ShapingThrottlerQuota.GetBudgetRefillTime()),
          ShapingThrottlerQuota.GetBudgetSpendRate(),
          spentShapingBudgetShare)
    , ReadPerformanceProfile(
          ShapingThrottlerQuota.HasRead() ? &ShapingThrottlerQuota.GetRead()
                                          : nullptr)
    , WritePerformanceProfile(
          ShapingThrottlerQuota.HasWrite() ? &ShapingThrottlerQuota.GetWrite()
                                           : nullptr)
{}

TDuration TVolumeShapingThrottler::SuggestDelay(
    TInstant ts,
    ui64 byteCount,
    EVolumeThrottlingOpType opType,
    TDuration executionTime)
{
    const TPerformanceProfile* performanceProfile = nullptr;
    switch (opType) {
        case EVolumeThrottlingOpType::Read:
            performanceProfile = ReadPerformanceProfile;
            break;
        case EVolumeThrottlingOpType::Write:
            performanceProfile = WritePerformanceProfile;
            break;
        case EVolumeThrottlingOpType::Zero:
        case EVolumeThrottlingOpType::Describe:
            return TDuration::Zero();
    }
    if (!performanceProfile) {
        return TDuration::Zero();
    }
    const TDuration cost = ShapingThrottlerQuota.GetExpectedIoParallelism() *
                           CostPerIO(
                               performanceProfile->GetIops(),
                               performanceProfile->GetBandwidth(),
                               byteCount);
    return UnspentCostBucket.Register(ts, cost, executionTime);
}

double TVolumeShapingThrottler::CalculateCurrentSpentBudgetShare(
    TInstant now) const
{
    return UnspentCostBucket.CalculateCurrentSpentBudgetShare(now);
}

TDuration TVolumeShapingThrottler::GetCurrentBudget() const
{
    return UnspentCostBucket.GetCurrentBudget();
}

}   // namespace NCloud::NBlockStore::NStorage
