#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/journalled_device/public.h>

#include <util/generic/string.h>
#include <util/generic/ylimits.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// A part of a device - the byte offset and size, both multiples of the block
// size the requests use. The default is the whole device.
struct TDeviceRegion
{
    static constexpr ui64 WholeDevice = Max<ui64>();

    ui64 Offset = 0;
    ui64 Size = WholeDevice;
};

// The adapter serves the given region of the device as a device of its own:
// page 0 of the requests is the first page of the region, the requests beyond
// it are rejected.
//
// A request that carries a client id is checked against the device sessions.
// A request without one is the device's own - the journal reading and writing
// its parts - and is served whatever the sessions are.
NJournalled::IDevicePtr CreateDeviceAdapter(
    ITimerPtr timer,
    TString deviceUUID,
    TDeviceClientPtr deviceClient,
    TDeviceRegion region = {});

}   // namespace NCloud::NBlockStore::NStorage
