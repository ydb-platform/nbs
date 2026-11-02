#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/json/json_value.h>

#include <util/datetime/base.h>

#include <array>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TEndpointStats
{
    TString ClientId;
    TString DiskId;

    IServerStatsPtr ServerStats;

    void Update(const NJson::TJsonValue& stats);
    void Update(const NJson::TJsonValue& stats, TInstant now);

    // This object belongs to one child process / stats pipe. A restart creates
    // a new reader, so its cumulative counters start from zero again.
    TString LatencyEpoch = {};
    ui64 LatencySequence = 0;
    TInstant LatencyReceivedAt = {};
    std::array<std::array<ui64, 3>, 2> LatencyPrevious = {};
    std::array<bool, 2> LatencyNeedsBaseline = {};

    void UpdateLatency(const NJson::TJsonValue& stats, TInstant now);
};

}   // namespace NCloud::NBlockStore::NServer
