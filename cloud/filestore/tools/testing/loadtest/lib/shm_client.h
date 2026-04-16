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

// Minimal interface for SHM-backed data I/O.
struct IShmDataClient
    : public IStartable
{
    virtual NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request) = 0;
};

using IShmDataClientPtr = std::shared_ptr<IShmDataClient>;

////////////////////////////////////////////////////////////////////////////////

// Creates a shared-memory data client that:
// - On Start(): creates/truncates the file at `fullFilePath`, mmaps it locally,
//   calls Mmap RPC on `shmControl` to register the region with the server, and
//   schedules periodic PingMmapRegion calls to keep it alive.
// - On WriteData(): copies data into the shared memory region and forwards the
//   request (with an iovec) to `dataOps`.
// - On ReadData(): adds an iovec into the shared memory region, forwards to
//   `dataOps`, and copies the response data out of the region.
// - On Stop(): calls Munmap RPC on `shmControl` and unmaps local memory.

IShmDataClientPtr CreateSharedMemoryClient(
    TString fullFilePath,
    ui64 shmSize,
    ui64 slotSize,
    IShmControlPtr shmControl,
    std::shared_ptr<IFileStore> dataOps,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    ILoggingServicePtr logging);

}   // namespace NCloud::NFileStore::NLoadTest
