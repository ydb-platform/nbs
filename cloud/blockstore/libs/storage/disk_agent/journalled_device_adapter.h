#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/journalled_device/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NJournalled::IDevicePtr CreateDeviceAdapter(
    TString deviceUUID,
    TDeviceClientPtr deviceClient);

}   // namespace NCloud::NBlockStore::NStorage
