#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TModuleStatsRegisterArgs
{
    TString FileSystemId;
    TString ClientId;
    TString CloudId;
    TString FolderId;
    TString SessionId;
    IModuleStatsPtr ModuleStats;
};

////////////////////////////////////////////////////////////////////////////////

struct IModuleStats
{
    virtual ~IModuleStats() = default;

    virtual TStringBuf GetName() const = 0;

    virtual void RegisterCounters(
        NMetrics::IMetricsRegistry& localMetricsRegistry,
        NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) = 0;

    virtual void UpdateStats(TInstant now) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IModuleStatsRegistry: public IStatsHandler
{
    // Registers stats for a module under stats->GetName().
    virtual void Register(TModuleStatsRegisterArgs args) = 0;

    // Unregister all module stats associated with the session
    virtual void Unregister(const TString& sessionId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IModuleStatsRegistryPtr CreateModuleStatsRegistry(
    ITimerPtr timer,
    IFsCountersProviderPtr fsCountersProvider,
    NMonitoring::TDynamicCountersPtr totalCounters);

IModuleStatsRegistryPtr CreateModuleStatsRegistryStub();

}   // namespace NCloud::NFileStore
