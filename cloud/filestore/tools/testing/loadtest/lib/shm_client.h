#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <memory>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

// Manages a shared memory region and prepares read/write requests to use it.
// On Start(), registers the region with the server via IShmControl (Mmap RPC)
// and keeps it alive with periodic pings. PrepareWrite/PrepareRead inject an
// iovec descriptor into the request so the server reads/writes through the
// shared region instead of copying over gRPC.
struct IShmDataClient
    : public IStartable
{
    // Copies the buffer into a SHM slot and replaces it with an iovec descriptor.
    // No-op if the SHM region is not yet active.
    virtual void PrepareWrite(NProto::TWriteDataRequest& request) = 0;

    // Allocates a SHM slot for the read response and sets the iovec descriptor.
    // Returns a pointer to the local buffer to read from once the RPC completes.
    // Returns nullptr if the SHM region is not yet active.
    virtual char* PrepareRead(NProto::TReadDataRequest& request) = 0;
};

using IShmDataClientPtr = std::shared_ptr<IShmDataClient>;

////////////////////////////////////////////////////////////////////////////////

IShmDataClientPtr CreateSharedMemoryClient(
    TString fullFilePath,
    ui64 shmSize,
    ui64 slotSize,
    IShmControlPtr shmControl,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    ILoggingServicePtr logging);

}   // namespace NCloud::NFileStore::NLoadTest
