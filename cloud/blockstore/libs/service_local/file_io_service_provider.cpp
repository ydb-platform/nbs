#include "file_io_service_provider.h"

#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileIOServiceProvider final
    : public IFileIOServiceProvider
{
private:
    const ui32 PathsPerServices;
    IFileIOServiceFactoryPtr Factory;

    TVector<std::pair<TString, size_t>> PathToFileIOIndex;
    TVector<IFileIOServicePtr> FileIOs;

public:
    explicit TFileIOServiceProvider(
            ui32 pathsPerServices,
            IFileIOServiceFactoryPtr factory)
        : PathsPerServices{Max(pathsPerServices, 1U)}
        , Factory(std::move(factory))
    {}

    void Start() override
    {}

    void Stop() override
    {
        for (auto& fileIO: FileIOs) {
            fileIO->Stop();
        }
    }

    IFileIOServicePtr CreateFileIOService(TStringBuf filePath) override
    {
        if (auto* p = FindIfPtr(
                PathToFileIOIndex,
                [=](const auto& p) { return p.first == filePath; }))
        {
            return FileIOs[p->second];
        }

        if (PathToFileIOIndex.size() + 1 > PathsPerServices * FileIOs.size()) {
            auto service = Factory->CreateFileIOService();
            Y_DEBUG_ABORT_UNLESS(service);
            service->Start();
            FileIOs.push_back(service);
        }

        PathToFileIOIndex.emplace_back(filePath, FileIOs.size() - 1);

        return FileIOs.back();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSingleFileIOServiceProvider final
    : IFileIOServiceProvider
{
    IFileIOServicePtr FileIO;

    explicit TSingleFileIOServiceProvider(
            IFileIOServicePtr fileIO)
        : FileIO{std::move(fileIO)}
    {}

    void Start() override
    {
        FileIO->Start();
    }

    void Stop() override
    {
        FileIO->Stop();
    }

    IFileIOServicePtr CreateFileIOService(TStringBuf filePath) override
    {
        Y_UNUSED(filePath);

        return FileIO;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceProviderPtr CreateSingleFileIOServiceProvider(
    IFileIOServicePtr fileIO)
{
    return std::make_shared<TSingleFileIOServiceProvider>(std::move(fileIO));
}

IFileIOServiceProviderPtr CreateFileIOServiceProvider(
    ui32 filePathsPerServices,
    IFileIOServiceFactoryPtr factory)
{
    return std::make_shared<TFileIOServiceProvider>(
        filePathsPerServices,
        std::move(factory));
}

}   // namespace NCloud::NBlockStore::NServer
