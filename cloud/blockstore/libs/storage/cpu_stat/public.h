#pragma once

#include "cloud/storage/core/libs/diagnostics/stats_fetcher.h"

#include <memory>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

using IStatsFetcherPtr = std::shared_ptr<IStatsFetcher>;

}   // namespace NCloud::NStorage
