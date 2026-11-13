#pragma once

#include <cloud/blockstore/libs/icv2/iface/public.h>

#include <memory>

namespace NCloud::NBlockStore::NIcV2 {

struct IService;
using IServicePtr = std::shared_ptr<IService>;

}   // namespace NCloud::NBlockStore::NIcV2
