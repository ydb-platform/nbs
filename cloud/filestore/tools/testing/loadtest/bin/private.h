#pragma once

#include <memory>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

}   // namespace NCloud::NFileStore::NLoadTest
