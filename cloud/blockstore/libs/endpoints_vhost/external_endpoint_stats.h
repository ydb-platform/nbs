#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <array>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TEndpointStats
{
    TString ClientId;
    TString DiskId;

    IServerStatsPtr ServerStats;

    TString SocketPath = {};

    void Update(const NJson::TJsonValue& stats);
    void RegisterExternalStats();
    void FinishExternalStats();

    // This object belongs to one child process / stats pipe. A restart creates
    // a new reader, so its cumulative counters start from zero again.
    std::array<std::array<ui64, 3>, 2> LatencyPrevious = {};
    std::array<bool, 2> LatencyNeedsBaseline = {};

    IVolumeInfoPtr ExternalVolume = {};
    NMonitoring::TDynamicCounters::TCounterPtr LastFreshSnapshot = {};
    NMonitoring::TDynamicCounters::TCounterPtr IncompleteStreams = {};
    bool CompleteFinalSnapshot = false;

    void UpdateLatency(const NJson::TJsonValue& stats);
};

}   // namespace NCloud::NBlockStore::NServer
