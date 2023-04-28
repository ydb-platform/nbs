#pragma once

#include <memory>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsCommon;
using TOptionsCommonPtr = std::shared_ptr<TOptionsCommon>;

struct TConfigInitializerCommon;
using TConfigInitializerCommonPtr = std::shared_ptr<TConfigInitializerCommon>;

}   // namespace NCloud::NBlockStore::NServer
