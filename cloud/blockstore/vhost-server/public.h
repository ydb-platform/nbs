#pragma once

#include <memory>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct IBackend;
using IBackendPtr = std::shared_ptr<IBackend>;

struct ICompletionStats;
using ICompletionStatsPtr = std::shared_ptr<ICompletionStats>;

struct IThrottler;
using IThrottlerPtr = std::shared_ptr<IThrottler>;

}   // namespace NCloud::NBlockStore::NVHostServer
