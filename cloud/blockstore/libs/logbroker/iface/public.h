#pragma once

#include <memory>

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

struct IService;
using IServicePtr = std::shared_ptr<IService>;

class TLogbrokerConfig;
using TLogbrokerConfigPtr = std::shared_ptr<TLogbrokerConfig>;

}   // namespace NCloud::NBlockStore::NLogbroker
