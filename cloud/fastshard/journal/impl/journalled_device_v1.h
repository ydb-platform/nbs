#pragma once

#include "public.h"

#include <cloud/fastshard/journal/iface/public.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceV1(IDevicePtr dataStore);

}   // namespace NCloud::NJournalled
