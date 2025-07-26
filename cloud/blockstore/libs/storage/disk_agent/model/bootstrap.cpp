#include "bootstrap.h"

#include "config.h"

#include <cloud/blockstore/libs/service_local/file_io_service_provider.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/io_uring/service.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceFactoryPtr CreateAIOServiceFactory(const TDiskAgentConfig& config)
{
    return NCloud::CreateAIOServiceFactory(
        {.MaxEvents = config.GetMaxAIOContextEvents()});
}

IFileIOServiceFactoryPtr CreateIoUringServiceFactory(
    const TDiskAgentConfig& config)
{
    TIoUringServiceParams params{
        .SubmissionQueueEntries = config.GetMaxAIOContextEvents()};

    if (config.GetBackend() == NProto::DISK_AGENT_BACKEND_IO_URING_NULL) {
        return NCloud::CreateIoUringServiceNullFactory(std::move(params));
    }

    return NCloud::CreateIoUringServiceFactory(std::move(params));
}

NServer::IFileIOServiceProviderPtr CreateFileIOServiceProvider(
    const TDiskAgentConfig& config,
    IFileIOServiceFactoryPtr factory)
{
    if (config.GetPathsPerFileIOService()) {
        return NServer::CreateFileIOServiceProvider(
            config.GetPathsPerFileIOService(),
            std::move(factory));
    }

    return NServer::CreateSingleFileIOServiceProvider(
        factory->CreateFileIOService());
}

}   // namespace NCloud::NBlockStore::NStorage
