#pragma once

#include "public.h"

#include <cloud/fastshard/journal/iface/public.h>

#include <cloud/storage/core/libs/common/public.h>

#include <util/generic/string.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateFileDevice(
    IFileIOServicePtr fileIO,
    const TString& filePath,
    ui64 pageCount,
    ui32 pageSize);

}   // namespace NCloud::NJournalled
