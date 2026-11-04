#pragma once

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/kikimr/public.h>

#include <library/cpp/logger/log.h>

namespace NKikimrConfig {
class TMonitoringConfig;
}

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Exposes counters while node registration and CMS fetch block initialization.
// Startup is best effort: setup failures are logged, and the HTTP backend
// retries bind and TLS errors without aborting daemon initialization.
// Stop the HTTP server immediately before KikimrStart(); the normal YDB monitor
// then binds using final configuration and keeps the same counter root.
class TKikimrMonitoring final: public IStartable
{
private:
    struct TImpl;

    const NMonitoring::TDynamicCountersPtr Counters;
    const NMonitoring::TDynamicCountersPtr InitializationCounters;
    const std::unique_ptr<TImpl> Impl;

public:
    TKikimrMonitoring(
        const NKikimrConfig::TMonitoringConfig& config,
        const TString& serviceName, TLog log = {});
    ~TKikimrMonitoring() override;

    void Start() override;
    void Stop() override;

    NMonitoring::TDynamicCountersPtr GetCounters() const;

    // Set Initializing to zero after the main actor system starts. This does
    // not signal daemon readiness: components and endpoint restore follow it.
    void CompleteInitialization();
};

TKikimrMonitoringPtr CreateKikimrMonitoring(
    const NKikimrConfig::TMonitoringConfig& config,
    const TString& serviceName, TLog log = {});

}   // namespace NCloud::NStorage
