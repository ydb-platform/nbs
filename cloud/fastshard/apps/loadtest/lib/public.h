#pragma once

#include <memory>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TAppContext;
struct TLoadTestConfig;

struct ILoadTest;
using ILoadTestPtr = std::shared_ptr<ILoadTest>;

}   // namespace NCloud::NFastShard::NLoadTest
