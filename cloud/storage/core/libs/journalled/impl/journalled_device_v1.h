#pragma once

#include "public.h"

#include <cloud/storage/core/libs/journalled/iface/public.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDevice(IDevicePtr dataStore);

}   // namespace NCloud::NJournalled
