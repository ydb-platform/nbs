#pragma once

#include <cloud/storage/core/libs/rdma/iface/public.h>

#include <memory>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

namespace NVerbs {

struct IVerbs;
using IVerbsPtr = std::shared_ptr<IVerbs>;

}   // namespace NVerbs

}   // namespace NCloud::NStorage::NRdma
