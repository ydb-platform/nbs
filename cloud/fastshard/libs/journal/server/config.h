#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct TJournalledDeviceConfig
{
    TString DeviceUUID;

    ui64 BlocksCount = 0;
    ui32 BlockSize = 0;

    ui64 LogMetaSize = 0;
    ui64 LogDataSize = 0;
};

}   // namespace NCloud::NJournalled
