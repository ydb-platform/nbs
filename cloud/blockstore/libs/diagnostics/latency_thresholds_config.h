#pragma once

#include "latency_thresholds.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf LatencyThresholdsConfigV1EnvName =
    "NBS_VHOST_LATENCY_THRESHOLDS_CONFIG_V1";

// The v1 value is either "unconfigured" (feature enabled, selected media kind
// has no ladder) or a comma-separated list of
// min_request_bytes:read_threshold_ms:write_threshold_ms buckets.
TLatencyThresholdLadder ParseLatencyThresholdsConfigV1(TStringBuf value);

TString SerializeLatencyThresholdsConfigV1(const TLatencyThresholdLadder* ladder);

}   // namespace NCloud::NBlockStore::NVHostServer
