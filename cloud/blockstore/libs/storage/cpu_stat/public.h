#pragma once

#include <memory>

#include "cloud/storage/core/libs/diagnostics/stats_fetcher.h"

namespace NCloud::NStorage {

using IStatsFetcherPtr = std::shared_ptr<IStatsFetcher>;

} // namespace NCloud::NStorage
