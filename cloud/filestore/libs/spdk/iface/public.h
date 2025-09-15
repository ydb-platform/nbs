#pragma once

#include <memory>

namespace NCloud::NFileStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

class TSpdkEnvConfig;
using TSpdkEnvConfigPtr = std::shared_ptr<TSpdkEnvConfig>;

struct ISpdkEnv;
using ISpdkEnvPtr = std::shared_ptr<ISpdkEnv>;

using TSpdkBuffer = std::shared_ptr<char>;
}   // namespace NCloud::NFileStore::NSpdk
