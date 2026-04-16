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

// Minimal interface for SHM mechanics: slot allocation and iovec injection.
// Callers are responsible for forwarding the (modified) request to the session.
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

// Creates a shared-memory data client that:
// - On Start(): starts the gRPC control transport, creates/truncates the file
//   at `fullFilePath`, mmaps it locally, calls Mmap RPC on `shmControl` to
//   register the region with the server, and schedules periodic PingMmapRegion
//   calls to keep it alive.
// - PrepareWrite(): copies the buffer into a SHM slot and sets the iovec descriptor.
// - PrepareRead(): allocates a SHM slot, sets the iovec descriptor, returns local ptr.
// - On Stop(): calls Munmap RPC on `shmControl`, unmaps local memory, and
//   stops the gRPC control transport.

IShmDataClientPtr CreateSharedMemoryClient(
    TString fullFilePath,
    ui64 shmSize,
    ui64 slotSize,
    IShmControlPtr shmControl,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    ILoggingServicePtr logging);

}   // namespace NCloud::NFileStore::NLoadTest
