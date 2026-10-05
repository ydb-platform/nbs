#pragma once
#include <cloud/blockstore/config/diagnostics.pb.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NVHostServer {
inline constexpr TStringBuf LatencyConfigEnvName =
    "NBS_VHOST_LATENCY_CONFIG_V1";

struct TLatencyConfig
{
    NProto::TDiagnosticsConfig Config;
    ui32 MediaKind = 0;
};

TString SerializeLatencyConfig(const NProto::TDiagnosticsConfig& config,
                               ui32 mediaKind);
TLatencyConfig ParseLatencyConfig(TStringBuf value);
// Zero means persistence is unavailable; the receiver rejects that generation.
ui64 NextLatencyGeneration(const TString& socketPath);
}   // namespace NCloud::NBlockStore::NVHostServer
