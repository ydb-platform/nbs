#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/threading/light_rw_lock/lightrwlock.h>

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/system/spinlock.h>

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <tuple>
#include <unordered_map>

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TMmapRegionMetadata
{
    TString FilePath;
    void* Address = nullptr;
    size_t Size = 0;   // in bytes
    ui64 Id = 0;
    TInstant LatestActivityTimestamp;
    ui32 PageSize = 0;   // in bytes

    bool operator<(const TMmapRegionMetadata& other) const
    {
        // Exclude timestamp from comparison - it's not part of identity
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
            ui32 pageSize)
        : Metadata{
            .FilePath = std::move(filePath),
            .Address = address,
            .Size = size,
            .Id = id,
            .LatestActivityTimestamp = TInstant::Now(),
            .PageSize = pageSize
        }
        , PageMap(pageSize != 0 ? size / pageSize : 0)
    {
    }

    [[nodiscard]] TMmapRegionMetadata ToMetadata() const
    {
        return Metadata;
    }

    void UpdateActivityTimestamp()
    {
        Metadata.LatestActivityTimestamp = TInstant::Now();
    }

    [[nodiscard]] TInstant GetLatestActivityTimestamp() const
    {
        return Metadata.LatestActivityTimestamp;
    }

    const TMmapRegionMetadata& GetMapRegionMetadata() const
    {
        return Metadata;
    }

    bool LockPage(ui64 index)
    {
        if (index >= PageMap.size()) {
            return false;
        }

        if (PageMap[index]) {
            return false;
        }

        PageMap[index] = true;
        return true;
    }

    bool UnlockPage(ui64 index)
    {
        if (index >= PageMap.size() || !PageMap[index]) {
            return false;
        }

        PageMap[index] = false;
        return true;
    }

    ui32 GetPageSize() const
    {
        return Metadata.PageSize;
    }

    size_t GetAddress() const
    {
        return reinterpret_cast<size_t>(Metadata.Address);
    }

    size_t GetSize() const
    {
        return Metadata.Size;
    }

private:
    TMmapRegionMetadata Metadata;
    TVector<bool> PageMap;
};

struct TServerStateStats
{
    size_t MmapRegionCount = 0;
    ui64 TotalMmapSize = 0;
};

class TServerState
{
public:
    TServerState(
        const TString& sharedMemoryBasePath,
        TDuration regionTimeout);

    ~TServerState();

    TResultOrError<TMmapRegionMetadata> CreateMmapRegion(
        const TString& filePath,
        size_t size /* in bytes */,
        ui32 pageSize /* in bytes */);

    NProto::TError DestroyMmapRegion(ui64 mmapId);

    TVector<TMmapRegionMetadata> ListMmapRegions();

    TResultOrError<TMmapRegionMetadata> GetMmapRegion(ui64 mmapId);

    NProto::TError PingMmapRegion(ui64 mmapId);

    [[nodiscard]] TServerStateStats GetStateStats() const;

    NProto::TError InvalidateTimedOutRegions();

    TResultOrError<google::protobuf::RepeatedPtrField<NProto::TIovec>>
    AdjustAndLockIovecs(
        ui64 mmapId,
        const google::protobuf::RepeatedPtrField<NProto::TIovec>& iovecs);

    NProto::TError UnlockIovecs(
        ui64 mmapId,
        const google::protobuf::RepeatedPtrField<NProto::TIovec>& iovecs);

private:
    TLightRWLock StateLock;
    std::unordered_map<ui64, TMmapRegion> MmapRegions;
    TFsPath SharedMemoryBasePath;
    TDuration RegionTimeout;
};

using TServerStatePtr = std::shared_ptr<TServerState>;

}   // namespace NCloud::NFileStore::NServer
