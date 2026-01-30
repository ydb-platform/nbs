#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ILocalNVMeService;
using ILocalNVMeServicePtr = std::shared_ptr<ILocalNVMeService>;

}   // namespace NCloud::NBlockStore
