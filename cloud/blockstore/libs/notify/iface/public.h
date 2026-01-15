#pragma once

#include <memory>

namespace NCloud::NBlockStore::NNotify {

////////////////////////////////////////////////////////////////////////////////

struct IService;
using IServicePtr = std::shared_ptr<IService>;

class TNotifyConfig;
using TNotifyConfigPtr = std::shared_ptr<TNotifyConfig>;

}   // namespace NCloud::NBlockStore::NNotify
