#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

size_t SerializeError(ui32 code, TStringBuf message, TStringBuf buffer);

NProto::TError ParseError(TStringBuf buffer);

}   // namespace NCloud::NBlockStore::NRdma
