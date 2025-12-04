#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ILocalNVMeService;
using ILocalNVMeServicePtr = std::shared_ptr<ILocalNVMeService>;

class TLocalNVMeConfig;
using TLocalNVMeConfigPtr = std::shared_ptr<TLocalNVMeConfig>;

}   // namespace NCloud::NBlockStore
