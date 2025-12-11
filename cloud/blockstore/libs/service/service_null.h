#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

namespace NProto {
class TNullServiceConfig;
}

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateNullService(const NProto::TNullServiceConfig& config);

}   // namespace NCloud::NBlockStore
