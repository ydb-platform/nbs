#pragma once

#include <memory>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsCommon;
using TOptionsCommonPtr = std::shared_ptr<TOptionsCommon>;

class TConfigInitializerCommon;
using TConfigInitializerCommonPtr = std::shared_ptr<TConfigInitializerCommon>;

} // namespace NCloud::NFileStore::NDaemon
