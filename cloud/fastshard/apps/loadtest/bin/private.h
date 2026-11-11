#pragma once

#include <memory>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

}   // namespace NCloud::NFastShard::NLoadTest
