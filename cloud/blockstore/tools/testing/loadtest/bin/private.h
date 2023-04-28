#pragma once

#include <memory>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

}   // namespace NCloud::NBlockStore::NLoadTest
