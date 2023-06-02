#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/weighted_percentile.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

THistogramPtr CreateHistogram();

TVector<TBucketInfo> CollectBuckets(
    spdk_histogram_data& histogram,
    ui64 ticksRate);

}   // namespace NCloud::NBlockStore::NSpdk
