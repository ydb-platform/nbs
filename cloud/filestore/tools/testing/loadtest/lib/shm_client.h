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
//
// Shared memory file is split into fixed-size slots, which are expected to
// be large enough to hold the biggest request buffer. Slots are allocated from
// a free list, and the offset is returned to the client after the input/output
// operation is complete.
struct IShmDataClient
    : public IStartable
{
    // Copies the buffer into a SHM slot and replaces it with an iovec
    // descriptor. Returns the allocated slot offset. Caller must pass the
    // returned offset to FreeOffset() after the RPC completes.
    virtual ui64 PrepareWrite(NProto::TWriteDataRequest& request) = 0;

    // Allocates a SHM slot for the read response and sets the iovec descriptor.
    // Returns a pointer to the local buffer to read from once the RPC
    // completes, and the slot offset via outOffset. Caller must pass the offset
    // to FreeOffset().
    virtual char* PrepareRead(
        NProto::TReadDataRequest& request,
        ui64& outOffset) = 0;

    // Returns a slot offset back to the free pool after the RPC using it completes.
    virtual void FreeOffset(ui64 offset) = 0;
};

using IShmDataClientPtr = std::shared_ptr<IShmDataClient>;

////////////////////////////////////////////////////////////////////////////////

IShmDataClientPtr CreateSharedMemoryClient(
    TString baseDir,
    TString filePath,
    ui64 shmSize,
    ui64 slotSize,
    IShmControlPtr shmControl,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    ILoggingServicePtr logging);

}   // namespace NCloud::NFileStore::NLoadTest
