#pragma once

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct ILocalNVMeService;
using ILocalNVMeServicePtr = std::shared_ptr<ILocalNVMeService>;

}   // namespace NCloud::NBlockStore::NStorage
