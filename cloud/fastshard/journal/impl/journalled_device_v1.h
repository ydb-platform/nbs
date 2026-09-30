#pragma once

#include "public.h"

#include <cloud/fastshard/journal/iface/public.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDevice(IDevicePtr dataStore);

}   // namespace NCloud::NJournalled
