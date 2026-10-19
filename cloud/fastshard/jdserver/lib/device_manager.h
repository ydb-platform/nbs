#pragma once

#include <cloud/fastshard/journal/server/public.h>
#include <cloud/storage/core/libs/common/public.h>

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NCloud::NFastShard::NJdServer {

////////////////////////////////////////////////////////////////////////////////

/**
 * A device manager that serves one device backed by a file.
 *
 * The file is opened (and grown to |blockCount| * |blockSize| bytes if it is
 * shorter) right away; the devices handed out are views of its page ranges.
 *
 * A client owns the device from AcquireDevices to ReleaseDevices and only the
 * owner may access it. A client with a higher generation takes the device
 * over; one with a lower generation than the last acquire or release is
 * rejected, unless its generation is zero.
 *
 * @throws yexception if the file cannot be opened or resized.
 */
NJournalled::IDeviceManagerPtr CreateFileDeviceManager(
    IFileIOServicePtr fileIO,
    TString deviceUUID,
    const TString& filePath,
    ui64 blockCount,
    ui32 blockSize);

}   // namespace NCloud::NFastShard::NJdServer
