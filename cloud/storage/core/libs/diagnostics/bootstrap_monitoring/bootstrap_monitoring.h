#pragma once

#include <cloud/storage/core/libs/diagnostics/monitoring.h>

namespace NKikimrConfig {
class TMonitoringConfig;
}

namespace NCloud::NStorage {

// Serves counters independently of node registration and the main actor system.
// Stop it before starting the main monitoring listener and reuse GetCounters().
IMonitoringServicePtr CreateBootstrapMonitoring(
    const NKikimrConfig::TMonitoringConfig& config, const TString& serviceName);

}   // namespace NCloud::NStorage
