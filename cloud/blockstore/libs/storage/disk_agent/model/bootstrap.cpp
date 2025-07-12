#include "bootstrap.h"

#include "config.h"

#include <cloud/blockstore/libs/service_local/file_io_service_provider.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/io_uring/service.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TIoUringServiceFactory
{
private:
    const ui32 Events;
    const bool UseSubmissionThread;
    const bool IsNull;

    ui32 Index = 0;

public:
    explicit TIoUringServiceFactory(const TDiskAgentConfig& config)
        : Events(config.GetMaxAIOContextEvents())
        // io_uring service is not thread-safe, so it should be
        // used with a submission thread anyway
        , UseSubmissionThread(!config.GetUseLocalStorageSubmissionThread())
        , IsNull(
              config.GetBackend() == NProto::DISK_AGENT_BACKEND_IO_URING_NULL)
    {}

    IFileIOServicePtr operator () ()
    {
        return CreateService(Index++);
    }

private:
    [[nodiscard]] IFileIOServicePtr CreateService(ui32 index) const
    {
        TString cqName = TStringBuilder() << "RNG" << index;

        IFileIOServicePtr fileIO =
            IsNull ? CreateIoUringServiceNull(std::move(cqName), Events)
                   : CreateIoUringService(std::move(cqName), Events);

        if (UseSubmissionThread) {
            fileIO = CreateConcurrentFileIOService(
                TStringBuilder() << "RNG.SQ" << index,
                std::move(fileIO));
        }

        return fileIO;
    }
};

}   // namespace

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
    return TIoUringServiceFactory{config};
}

NServer::IFileIOServiceProviderPtr CreateFileIOServiceProvider(
    const TDiskAgentConfig& config,
    std::function<IFileIOServicePtr()> factory)
{
    if (config.GetPathsPerFileIOService()) {
        return NServer::CreateFileIOServiceProvider(
            config.GetPathsPerFileIOService(),
            std::move(factory));
    }

    return NServer::CreateSingleFileIOServiceProvider(factory());
}

}   // namespace NCloud::NBlockStore::NStorage
