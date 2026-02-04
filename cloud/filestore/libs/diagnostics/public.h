#pragma once

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <memory>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_COUNTERS_ROOT(counters) \
    (counters)->GetSubgroup("counters", "filestore")

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticsConfig;
using TDiagnosticsConfigPtr = std::shared_ptr<TDiagnosticsConfig>;

struct IRequestStats;
using IRequestStatsPtr = std::shared_ptr<IRequestStats>;

struct IIncompleteRequestProvider;
using IIncompleteRequestProviderPtr = std::shared_ptr<IIncompleteRequestProvider>;

struct IRequestStatsRegistry;
using IRequestStatsRegistryPtr = std::shared_ptr<IRequestStatsRegistry>;

struct IProfileLog;
using IProfileLogPtr = std::shared_ptr<IProfileLog>;

struct IStorageCounters;
using IStorageCountersPtr = std::shared_ptr<IStorageCounters>;

class TFileSystemStatCounters;
using TFileSystemStatCountersPtr = std::shared_ptr<TFileSystemStatCounters>;

struct IModuleStats;
using IModuleStatsPtr = std::shared_ptr<IModuleStats>;

struct IModuleStatsRegistry;
using IModuleStatsRegistryPtr = std::shared_ptr<IModuleStatsRegistry>;

}   // namespace NCloud::NFileStore
