#pragma once

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Create a single-threaded io_uring service: IFileIOService operations should
// not be invoked concurrently.
IFileIOServicePtr CreateIoUringService(
    TString completionThreadName,
    ui32 submissionQueueEntries);

}   // namespace NCloud
