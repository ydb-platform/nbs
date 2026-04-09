#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <memory>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

// Creates a wrapper around `inner` that:
// - On Start(): creates/truncates the file at `fullFilePath`, mmaps it locally,
//   calls Mmap RPC on `shmControl` to register the region with the server, and
//   schedules periodic PingMmapRegion calls to keep the region alive.
// - On WriteData(): copies data from the request buffer into the shared memory
//   region, replaces the buffer with an iovec pointing into the region, and
//   forwards the request to `dataOps` (session-aware transport for headers).
// - On ReadData(): adds an iovec pointing into the shared memory region and
//   forwards the request to `dataOps`; on response, copies data from the region
//   into the response buffer.
// - On Stop(): unmaps the local memory and calls Munmap RPC on `shmControl`.

IFileStoreServicePtr CreateSharedMemoryClient(
    TString fullFilePath,
    TString fileName,
    ui64 shmSize,
    ui64 slotSize,
    IFileStoreServicePtr inner,
    IShmControlPtr shmControl,
    std::shared_ptr<IFileStore> dataOps,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    ILoggingServicePtr logging);

}   // namespace NCloud::NFileStore::NLoadTest
