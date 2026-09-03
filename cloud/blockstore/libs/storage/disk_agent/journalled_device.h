#pragma once

#include "public.h"

#include <cloud/storage/core/libs/journalled_device/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NJournalled::IJournalledDevicePtr CreateJournalledDevice(
    TString deviceUUID,
    TDeviceClientPtr deviceClient);

}   // namespace NCloud::NBlockStore::NStorage
