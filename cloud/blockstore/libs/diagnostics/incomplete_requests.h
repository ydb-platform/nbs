#pragma once

#include "public.h"

#include "metric_request.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <array>
#include <functional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

using TIncompleteRequestsCollector = std::function<void(
    TCallContext& callContext,
    const TMetricRequest& metricRequest,
    TRequestTime time)>;

////////////////////////////////////////////////////////////////////////////////

struct IIncompleteRequestProvider
{
    virtual ~IIncompleteRequestProvider() = default;

    virtual size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) = 0;
};

IIncompleteRequestProviderPtr CreateIncompleteRequestProviderStub();

TIncompleteRequestsCollector CreateIncompleteRequestsCollectorStub();

}   // namespace NCloud::NBlockStore
