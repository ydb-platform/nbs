#pragma once

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateAIOService(size_t maxEvents = 1024);

IFileIOServicePtr CreateThreadedAIOService(ui32 threadCount, size_t maxEvents = 1024);

}   // namespace NCloud
