#include "bootstrap.h"

#include "config.h"

#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service_local/file_io_service_provider.h>
#include <cloud/blockstore/libs/service_local/storage_local.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/service_local/storage_nvme.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/io_uring/service.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

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

IFileIOServiceFactoryPtr CreateFileIOServiceFactory(
    const TDiskAgentConfig& config)
{
    switch (config.GetBackend()) {
        case NProto::DISK_AGENT_BACKEND_SPDK:
        case NProto::DISK_AGENT_BACKEND_NULL:
            break;
        case NProto::DISK_AGENT_BACKEND_AIO:
            return CreateAIOServiceFactory(config);
        case NProto::DISK_AGENT_BACKEND_IO_URING:
        case NProto::DISK_AGENT_BACKEND_IO_URING_NULL:
        case NProto::DISK_AGENT_BACKEND_IO_URING_NVME:
            return CreateIoUringServiceFactory(config);
    }

    return nullptr;
}

NServer::IFileIOServiceProviderPtr CreateFileIOServiceProvider(
    const TDiskAgentConfig& config)
{
    IFileIOServiceFactoryPtr factory = CreateFileIOServiceFactory(config);
    if (!factory) {
        return nullptr;
    }

    if (config.GetPathsPerFileIOService()) {
        return NServer::CreateFileIOServiceProvider(
            config.GetPathsPerFileIOService(),
            std::move(factory));
    }

    return NServer::CreateSingleFileIOServiceProvider(
        factory->CreateFileIOService());
}

IStorageProviderPtr CreateStorageProvider(
    ILoggingServicePtr logging,
    const TDiskAgentConfig& config,
    NServer::IFileIOServiceProviderPtr provider,
    NNvme::INvmeManagerPtr nvmeManager)
{
    switch (config.GetBackend()) {
        case NProto::DISK_AGENT_BACKEND_SPDK:
            break;
        case NProto::DISK_AGENT_BACKEND_AIO:
            return CreateLocalStorageProvider(
                std::move(provider),
                std::move(nvmeManager),
                {
                    .DirectIO = !config.GetDirectIoFlagDisabled(),
                    .UseSubmissionThread =
                        config.GetUseLocalStorageSubmissionThread(),
                });
        case NProto::DISK_AGENT_BACKEND_NULL:
            return NServer::CreateNullStorageProvider();
        case NProto::DISK_AGENT_BACKEND_IO_URING:
        case NProto::DISK_AGENT_BACKEND_IO_URING_NULL:
        case NProto::DISK_AGENT_BACKEND_IO_URING_NVME: {
            auto storageProvider = CreateLocalStorageProvider(
                std::move(provider),
                nvmeManager,
                {
                    .DirectIO = !config.GetDirectIoFlagDisabled(),
                    // Each io_uring service already has its own submission
                    // thread, so we don't need one here
                    .UseSubmissionThread = false,
                });

            if (config.GetBackend() == NProto::DISK_AGENT_BACKEND_IO_URING_NVME)
            {
                storageProvider = NServer::CreateNvmeStorageProvider(
                    std::move(logging),
                    std::move(storageProvider),
                    std::move(nvmeManager));
            }

            return storageProvider;
        }
    }

    return nullptr;
}

NNvme::INvmeManagerPtr CreateNvmeManager(const TDiskAgentConfig& config)
{
    switch (config.GetBackend()) {
        case NProto::DISK_AGENT_BACKEND_SPDK:
            break;
        case NProto::DISK_AGENT_BACKEND_AIO:
        case NProto::DISK_AGENT_BACKEND_NULL:
        case NProto::DISK_AGENT_BACKEND_IO_URING:
        case NProto::DISK_AGENT_BACKEND_IO_URING_NULL:
        case NProto::DISK_AGENT_BACKEND_IO_URING_NVME:
            return NNvme::CreateNvmeManager(config.GetSecureEraseTimeout());
    }

    return nullptr;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCreateDiskAgentBackendComponentsResult CreateDiskAgentBackendComponents(
    ILoggingServicePtr logging,
    const TDiskAgentConfig& config)
{
    if (config.GetBackend() == NProto::DISK_AGENT_BACKEND_SPDK) {
        return {};
    }

    auto nvmeManager = CreateNvmeManager(config);
    auto provider = CreateFileIOServiceProvider(config);

    return {
        .NvmeManager = nvmeManager,
        .FileIOServiceProvider = provider,
        .StorageProvider = CreateStorageProvider(
            std::move(logging),
            config,
            provider,
            nvmeManager),
    };
}

}   // namespace NCloud::NBlockStore::NStorage
