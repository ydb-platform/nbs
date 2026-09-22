#pragma once

#include "public.h"

#include <cloud/fastshard/journal/iface/public.h>

#include <util/system/types.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateInMemoryDevice(ui32 pageSize);

}   // namespace NCloud::NJournalled
