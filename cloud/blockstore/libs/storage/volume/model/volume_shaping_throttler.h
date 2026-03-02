#pragma once

#include "public.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/api/public.h>
#include <cloud/blockstore/public/api/protos/volume_throttling.pb.h>

#include <cloud/storage/core/libs/throttling/unspent_cost_bucket.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TVolumeShapingThrottler
{
private:
    NProto::TShapingThrottlerQuota ShapingThrottlerQuota;
    TUnspentCostBucket UnspentCostBucket;

    const NCloud::NProto::TPerformanceProfile* ReadPerformanceProfile;
    const NCloud::NProto::TPerformanceProfile* WritePerformanceProfile;

public:
    TVolumeShapingThrottler(
        const NProto::TShapingThrottlerConfig& shapingThrottlerConfig,
        NCloud::NProto::EStorageMediaKind storageMediaKind,
        double spentShapingBudgetShare);

    TDuration SuggestDelay(
        TInstant ts,
        ui64 byteCount,
        EVolumeThrottlingOpType opType,
        TDuration executionTime);

    [[nodiscard]] TDuration GetCurrentBudget() const;
    [[nodiscard]] double CalculateCurrentSpentBudgetShare(TInstant now) const;
};

}   // namespace NCloud::NBlockStore::NStorage
