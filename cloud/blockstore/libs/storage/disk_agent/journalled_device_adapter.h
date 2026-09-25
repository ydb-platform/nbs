#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/fastshard/journal/iface/public.h>

#include <cloud/storage/core/libs/common/public.h>

#include <util/generic/string.h>
#include <util/generic/ylimits.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// A part of a device in the blocks of the block size the adapter serves. The
// default is the whole device.
struct TDeviceRegion
{
    static constexpr ui64 WholeDevice = Max<ui64>();

    ui64 FirstBlockIndex = 0;
    ui64 BlockCount = WholeDevice;
};

// The adapter serves the given region of the device as a device of its own:
// page 0 of the requests is the first block of the region, the requests beyond
// it are rejected.
//
// The adapter does not check the client sessions, that is up to the caller.
NJournalled::IDevicePtr CreateDeviceAdapter(
    ITimerPtr timer,
    TString deviceUUID,
    ui32 blockSize,
    TDeviceClientPtr deviceClient,
    TDeviceRegion region = {});

}   // namespace NCloud::NBlockStore::NStorage
