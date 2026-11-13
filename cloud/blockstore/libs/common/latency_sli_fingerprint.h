#pragma once

#include "latency_sli.h"

namespace NCloud::NBlockStore {

// Bounded identity of the canonical threshold table for child stats messages.
TString GetLatencySliConfigFingerprint(const TLatencySliConfig& config);

}   // namespace NCloud::NBlockStore
