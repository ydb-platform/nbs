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
    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    std::atomic<ui32> Index = 0;

    TConcurrentAioServiceFactory(
        IFileIOServiceFactoryPtr factory,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : Factory(std::move(factory))
        , Counters(std::move(counters))
    {}

    IFileIOServicePtr CreateFileIOService() final
    {
        const ui32 index = Index++;

        return CreateConcurrentFileIOService(
            TStringBuilder() << "AIO.SQ" << index,
            Factory->CreateFileIOService(), Counters);
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceFactoryPtr CreateAIOServiceFactory(
    const TDiskAgentConfig& config,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
{
    auto factory = NCloud::CreateAIOServiceFactory(
        {.MaxEvents = config.GetMaxAIOContextEvents(), .Counters = counters});

    if (config.GetUseOneSubmissionThreadPerAIOServiceEnabled()) {
        factory = std::make_shared<TConcurrentAioServiceFactory>(
            std::move(factory), std::move(counters));
    }

    return factory;
}

IFileIOServiceFactoryPtr CreateIoUringServiceFactory(
    const TDiskAgentConfig& config,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
{
    TIoUringServiceParams params{
        .SubmissionQueueEntries = config.GetMaxAIOContextEvents(),
        .Counters = std::move(counters)};

    if (config.GetBackend() == NProto::DISK_AGENT_BACKEND_IO_URING_NULL) {
        return NCloud::CreateIoUringServiceNullFactory(std::move(params));
    }

    return NCloud::CreateIoUringServiceFactory(std::move(params));
}

IFileIOServiceFactoryPtr CreateFileIOServiceFactory(
    const TDiskAgentConfig& config,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
{
    switch (config.GetBackend()) {
        case NProto::DISK_AGENT_BACKEND_SPDK:
        case NProto::DISK_AGENT_BACKEND_NULL:
            break;
        case NProto::DISK_AGENT_BACKEND_AIO:
            return CreateAIOServiceFactory(config, std::move(counters));
        case NProto::DISK_AGENT_BACKEND_IO_URING:
        case NProto::DISK_AGENT_BACKEND_IO_URING_NULL:
            return CreateIoUringServiceFactory(config, std::move(counters));
    }

    return nullptr;
}

NServer::IFileIOServiceProviderPtr CreateFileIOServiceProvider(
    const TDiskAgentConfig& config,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
{
    IFileIOServiceFactoryPtr factory =
        CreateFileIOServiceFactory(config, std::move(counters));
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
    NNvme::INvmeManagerPtr nvmeManager,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
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
                NServer::TLocalStorageProviderParams{
                    .DirectIO = !config.GetDirectIoFlagDisabled(),
                    .UseSubmissionThread =
                        config.GetUseLocalStorageSubmissionThread(),
                    .ValidatedBlocksRatio = config.GetValidatedBlocksRatio(),
                    .DataIntegrityValidationPolicy =
                        config
                            .GetDataIntegrityValidationPolicyForDrBasedDisks(),
                }, std::move(counters));
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
                NServer::TLocalStorageProviderParams{
                    .DirectIO = !config.GetDirectIoFlagDisabled(),
                    // Each io_uring service already has its own submission
                    // thread, so we don't need one here
                    .UseSubmissionThread = false,
                    .ValidatedBlocksRatio = config.GetValidatedBlocksRatio(),
                    .DataIntegrityValidationPolicy =
                        config
                            .GetDataIntegrityValidationPolicyForDrBasedDisks(),
                }, std::move(counters));
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

NNvme::INvmeManagerPtr CreateNvmeManager(
    ILoggingServicePtr logging,
    const TDiskAgentConfig& config)
{
    switch (config.GetBackend()) {
        case NProto::DISK_AGENT_BACKEND_SPDK:
            break;
        case NProto::DISK_AGENT_BACKEND_AIO:
        case NProto::DISK_AGENT_BACKEND_NULL:
        case NProto::DISK_AGENT_BACKEND_IO_URING:
        case NProto::DISK_AGENT_BACKEND_IO_URING_NULL:
            return NNvme::CreateNvmeManager(
                std::move(logging),
                config.GetSecureEraseTimeout(),
                config.GetNVMeAdminCmdTimeout());
    }

    return nullptr;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCreateDiskAgentBackendComponentsResult CreateDiskAgentBackendComponents(
    ILoggingServicePtr logging,
    const TDiskAgentConfig& config,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
{
    Y_ABORT_UNLESS(logging);

    if (config.GetBackend() == NProto::DISK_AGENT_BACKEND_SPDK) {
        return {};
    }

    auto nvmeManager = CreateNvmeManager(std::move(logging), config);
    auto provider = CreateFileIOServiceProvider(config, counters);

    return {
        .NvmeManager = nvmeManager,
        .FileIOServiceProvider = provider,
        .StorageProvider = CreateStorageProvider(
            config, provider, nvmeManager, std::move(counters)),
    };
}

}   // namespace NCloud::NBlockStore::NStorage
