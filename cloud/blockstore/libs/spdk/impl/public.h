#pragma once

#include <cloud/blockstore/libs/spdk/iface/public.h>

#include <memory>

struct spdk_bdev;
struct spdk_histogram_data;

namespace NCloud::NBlockStore::NSpdk {

using THistogramPtr = std::shared_ptr<spdk_histogram_data>;

}   // namespace NCloud::NBlockStore::NSpdk
