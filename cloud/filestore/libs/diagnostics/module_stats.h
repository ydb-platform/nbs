#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IModuleStats
{
    virtual ~IModuleStats() = default;

    virtual TStringBuf GetName() const = 0;
    virtual NMonitoring::TDynamicCountersPtr GetCounters() = 0;

    virtual void UpdateStats() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IModuleStatsRegistry: public IStatsHandler
{
    // Registers stats for (fsId, clientId) under stats->GetName().
    // Attaches stats->GetCounters() to the counter hierarchy.
    virtual void Register(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId,
        IModuleStatsPtr stats) = 0;

    virtual void Unregister(
        const TString& fileSystemId,
        const TString& clientId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IModuleStatsRegistryPtr CreateModuleStatsRegistry(
    IFsCountersProviderPtr fsCountersProvider);

IModuleStatsRegistryPtr CreateModuleStatsRegistryStub();

}   // namespace NCloud::NFileStore
