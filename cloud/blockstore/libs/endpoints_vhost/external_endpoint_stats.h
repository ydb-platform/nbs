#pragma once

#include <cloud/blockstore/libs/diagnostics/latency_sli.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/json/json_value.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TEndpointStats
{
    TString ClientId;
    TString DiskId;

    IServerStatsPtr ServerStats;

    std::shared_ptr<TLatencyBatchTracker> LatencyTracker =
        std::make_shared<TLatencyBatchTracker>();

    void Update(const NJson::TJsonValue& stats);
};

}   // namespace NCloud::NBlockStore::NServer
