#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/threading/light_rw_lock/lightrwlock.h>

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/system/spinlock.h>

#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TMmapRegionMetadata
{
    TString FilePath;
    void* Address = nullptr;
    size_t Size = 0; // in bytes
    ui64 Id = 0;

    bool operator<(const TMmapRegionMetadata& other) const
    {
        return std::tie(FilePath, Size, Id) <
               std::tie(other.FilePath, other.Size, other.Id);
    }
};

struct TMmapRegion
{
public:
    TMmapRegion(
            TString filePath,
            void* address,
            size_t size,
            ui64 id,
            TFileHandle fd)
        : Metadata{
            .FilePath = std::move(filePath),
            .Address = address,
            .Size = size,
            .Id = id}
        , Fd(std::move(fd))
    {}

    TMmapRegionMetadata ToMetadata() const
    {
        return Metadata;
    }

private:
    TMmapRegionMetadata Metadata;
    TFileHandle Fd;
};

class TServerState
{
public:
    explicit TServerState(const TString& sharedMemoryBasePath);

    ~TServerState();

    TResultOrError<TMmapRegionMetadata> CreateMmapRegion(
        const TString& filePath,
        size_t size /* in bytes */);

    NProto::TError DestroyMmapRegion(ui64 mmapId);

    TVector<TMmapRegionMetadata> ListMmapRegions();

    TResultOrError<TMmapRegionMetadata> GetMmapRegion(ui64 mmapId);

private:
    TLightRWLock StateLock;
    std::unordered_map<ui64, TMmapRegion> MmapRegions;
    TFsPath SharedMemoryBasePath;
};

}   // namespace NCloud::NFileStore::NServer
