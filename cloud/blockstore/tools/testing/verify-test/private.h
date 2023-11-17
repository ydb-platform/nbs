#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

struct ITest;
using ITestPtr = std::shared_ptr<ITest>;

struct ITestExecutor;
using ITestExecutorPtr = std::shared_ptr<ITestExecutor>;

struct TTestExecutorConfig;
using TTestExecutorConfigPtr = std::shared_ptr<TTestExecutorConfig>;

}   // namespace NCloud::NBlockStore
