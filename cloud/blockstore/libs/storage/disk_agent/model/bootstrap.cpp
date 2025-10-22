#include "bootstrap.h"

#include "chaos_storage_provider.h"
#include "config.h"

#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service_local/file_io_service_provider.h>
#include <cloud/blockstore/libs/service_local/storage_local.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/io_uring/service.h>

#include <util/string/builder.h>

#include <atomic>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TConcurrentAioServiceFactory final: IFileIOServiceFactory
{
    const IFileIOServiceFactoryPtr Factory;
    std::atomic<ui32> Index = 0;

    explicit TConcurrentAioServiceFactory(IFileIOServiceFactoryPtr factory)
        : Factory(std::move(factory))
    {}

    IFileIOServicePtr CreateFileIOService() final
    {
        const ui32 index = Index++;

        return CreateConcurrentFileIOService(
            TStringBuilder() << "AIO.SQ" << index,
            Factory->CreateFileIOService());
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceFactoryPtr CreateAIOServiceFactory(const TDiskAgentConfig& config)
{
    auto factory = NCloud::CreateAIOServiceFactory(
        {.MaxEvents = config.GetMaxAIOContextEvents()});

    if (config.GetUseOneSubmissionThreadPerAIOServiceEnabled()) {
        factory =
            std::make_shared<TConcurrentAioServiceFactory>(std::move(factory));
    }

    return factory;
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
    const TDiskAgentConfig& config,
    NServer::IFileIOServiceProviderPtr provider,
    NNvme::INvmeManagerPtr nvmeManager)
{
    IStorageProviderPtr result;

    switch (config.GetBackend()) {
        case NProto::DISK_AGENT_BACKEND_SPDK: {
            break;
        }
        case NProto::DISK_AGENT_BACKEND_AIO: {
            result = CreateLocalStorageProvider(
                std::move(provider),
                std::move(nvmeManager),
                {
                    .DirectIO = !config.GetDirectIoFlagDisabled(),
                    .UseSubmissionThread =
                        config.GetUseLocalStorageSubmissionThread(),
                    .EnableDataIntegrityValidation =
                        config.GetEnableDataIntegrityValidationForDrBasedDisks(),
                });
            break;
        }
        case NProto::DISK_AGENT_BACKEND_NULL: {
            result = NServer::CreateNullStorageProvider();
            break;
        }
        case NProto::DISK_AGENT_BACKEND_IO_URING:
        case NProto::DISK_AGENT_BACKEND_IO_URING_NULL: {
            result = CreateLocalStorageProvider(
                std::move(provider),
                std::move(nvmeManager),
                {
                    .DirectIO = !config.GetDirectIoFlagDisabled(),
                    // Each io_uring service already has its own submission
                    // thread, so we don't need one here
                    .UseSubmissionThread = false,
                    .EnableDataIntegrityValidation =
                        config.GetEnableDataIntegrityValidationForDrBasedDisks(),
                });
            break;
        }
    }

    if (result && config.HasChaosConfig()) {
        result = NServer::CreateChaosStorageProvider(
            std::move(result),
            config.GetChaosConfig());
    }

    return result;
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
            return NNvme::CreateNvmeManager(config.GetSecureEraseTimeout());
    }

    return nullptr;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCreateDiskAgentBackendComponentsResult CreateDiskAgentBackendComponents(
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
        .StorageProvider = CreateStorageProvider(config, provider, nvmeManager),
    };
}

}   // namespace NCloud::NBlockStore::NStorage
