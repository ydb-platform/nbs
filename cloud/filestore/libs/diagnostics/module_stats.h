#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IModuleStats
{
    virtual ~IModuleStats() = default;

    virtual void UpdateStats() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IModuleStatsRegistry: public IStatsHandler
{
    virtual NMonitoring::TDynamicCountersPtr GetFileSystemModuleCounters(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId,
        const TString& moduleName) = 0;

    virtual void Register(
        const TString& fileSystemId,
        const TString& clientId,
        IModuleStatsPtr stats) = 0;

    virtual void Unregister(
        const TString& fileSystemId,
        const TString& clientId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IModuleStatsRegistryPtr CreateModuleStatsRegistry(
    TString component,
    NMonitoring::TDynamicCountersPtr rootCounters);

IModuleStatsRegistryPtr CreateModuleStatsRegistryStub();

}   // namespace NCloud::NFileStore
