#pragma once

#include <cloud/fastshard/libs/journal/iface/public.h>
#include <cloud/fastshard/libs/journal/server/public.h>

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NCloud::NFastShard::NJdServer {

////////////////////////////////////////////////////////////////////////////////

/**
 * A device manager that serves one device.
 *
 * The devices handed out are views of page ranges of |device|.
 *
 * A client acquires the device as its writer or as one of its readers,
 * according to the access mode, and holds it until ReleaseDevices. Only the
 * writer may write, only the writer and the readers may read. A writer with a
 * higher generation takes the device over; a writer with a lower generation
 * than the writer's last acquire or release is rejected, unless its
 * generation is zero. Readers neither check nor raise the generation.
 */
NJournalled::IDeviceManagerPtr CreateSingleDeviceManager(
    NJournalled::IDevicePtr device,
    TString deviceUUID);

}   // namespace NCloud::NFastShard::NJdServer
