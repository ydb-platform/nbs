#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/threading/light_rw_lock/lightrwlock.h>

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
    size_t Size = 0;
    ui64 Id = 0;
};

struct TMmapRegion: public TMmapRegionMetadata
{
    TMmapRegion() = default;

    TMmapRegion(TString filePath, void* address, size_t size, ui64 id)
        : TMmapRegionMetadata{
              .FilePath = std::move(filePath),
              .Address = address,
              .Size = size,
              .Id = id}
    {}

    TFileHandle Fd;

    TMmapRegionMetadata ToMetadata() const
    {
        return TMmapRegionMetadata{
            .FilePath = FilePath,
            .Address = Address,
            .Size = Size,
            .Id = Id};
    }
};

class TServerState
{
public:
    TServerState() = default;

    explicit TServerState(TString sharedMemoryBasePath);

    void Initialize(const TString& sharedMemoryBasePath);

    TResultOrError<TMmapRegionMetadata> CreateMmapRegion(
        const TString& filePath,
        size_t size);

    NProto::TError DestroyMmapRegion(ui64 mmapId);

    TVector<TMmapRegionMetadata> ListMmapRegions();

    TResultOrError<TMmapRegionMetadata> GetMmapRegion(ui64 mmapId);

private:
    TLightRWLock StateLock;
    std::unordered_map<ui64, TMmapRegion> MmapRegions;
    TString SharedMemoryBasePath;
};

}   // namespace NCloud::NFileStore::NServer
