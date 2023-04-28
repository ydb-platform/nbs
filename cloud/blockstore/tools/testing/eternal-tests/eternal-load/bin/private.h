#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

struct ITest;
using ITestPtr = std::shared_ptr<ITest>;

}   // namespace NCloud::NBlockStore
