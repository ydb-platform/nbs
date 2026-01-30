#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeConfig;
using TLocalNVMeConfigPtr = std::shared_ptr<TLocalNVMeConfig>;

struct ILocalNVMeService;
using ILocalNVMeServicePtr = std::shared_ptr<ILocalNVMeService>;

struct ILocalNVMeDeviceProvider;
using ILocalNVMeDeviceProviderPtr = std::shared_ptr<ILocalNVMeDeviceProvider>;

}   // namespace NCloud::NBlockStore
