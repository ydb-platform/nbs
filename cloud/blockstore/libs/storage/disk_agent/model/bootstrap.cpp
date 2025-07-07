#include "bootstrap.h"

#include "config.h"

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/io_uring/service.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

std::function<IFileIOServicePtr()> CreateAIOServiceFactory(
    const TDiskAgentConfig& config)
{
    return [events = config.GetMaxAIOContextEvents()]
    {
        return CreateAIOService(events);
    };
}

std::function<IFileIOServicePtr()> CreateIoUringServiceFactory(
    const TDiskAgentConfig& config)
{
    const ui32 events = config.GetMaxAIOContextEvents();
    // io_uring service is not thread-safe, so it should be
    // used with a submission thread anyway
    const bool useSubmissionThread =
        !config.GetUseLocalStorageSubmissionThread();
    const bool isNull =
        config.GetBackend() == NProto::DISK_AGENT_BACKEND_IO_URING_NULL;
    return [events, useSubmissionThread, isNull, nextIndex = ui32()]() mutable
    {
        const ui32 index = nextIndex++;
        TString cqName = TStringBuilder() << "RNG" << index;

        IFileIOServicePtr fileIO =
            isNull ? CreateIoUringServiceNull(std::move(cqName), events)
                   : CreateIoUringService(std::move(cqName), events);

        if (useSubmissionThread) {
            fileIO = CreateConcurrentFileIOService(
                TStringBuilder() << "RNG.SQ" << index,
                std::move(fileIO));
        }

        return fileIO;
    };
}

}   // namespace NCloud::NBlockStore::NStorage
