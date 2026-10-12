#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/storage/core/libs/common/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// The adapter serves the given region of the device as a device of its own:
// page 0 of the requests is the first block of the region, the requests beyond
// it are rejected.
//
// The adapter does not check the client sessions, that is up to the caller.
NJournalled::IDevicePtr CreateDeviceAdapter(
    ITimerPtr timer,
    TDeviceClientPtr deviceClient,
    TString deviceUUID,
    NJournalled::TPageRangeRef region,
    ui32 blockSize);

}   // namespace NCloud::NBlockStore::NStorage
