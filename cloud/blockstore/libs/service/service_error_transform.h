#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/map.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateErrorTransformService(
    IBlockStorePtr service,
    TMap<EErrorKind, EWellKnownResultCodes> errorTransformMap);

}   // namespace NCloud::NBlockStore
