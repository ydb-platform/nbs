#pragma once

#include "public.h"

#include <cloud/storage/core/libs/journalled/iface/public.h>

#include <util/system/types.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateInMemoryDevice(ui32 pageSize);

}   // namespace NCloud::NJournalled
