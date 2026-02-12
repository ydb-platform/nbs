#include "server_memory_state.h"

#include <util/folder/path.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/guard.h>

#include <sys/mman.h>

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

TServerState::TServerState(const TString& sharedMemoryBasePath)
    : SharedMemoryBasePath(sharedMemoryBasePath)
{}

TServerState::~TServerState()
{
    TLightWriteGuard guard(StateLock);
    for (const auto& [id, region]: MmapRegions) {
        auto metadata = region.ToMetadata();
        munmap(metadata.Address, metadata.Size);
    }
    MmapRegions.clear();
}

TResultOrError<TMmapRegionMetadata> TServerState::CreateMmapRegion(
    const TString& filePath,
    size_t size)
{
    if (size == 0) {
        return MakeError(E_ARGUMENT, "Size must be greater than zero");
    }

    TString fullPath;

    try {
        fullPath = SharedMemoryBasePath.Child(filePath).RealPath().GetPath();
    } catch (const TIoException& e) {
        return MakeError(
            E_ARGUMENT,
            Sprintf("Failed to get real path: %s", e.what()));
    }

    TFileHandle file(fullPath, OpenExisting | RdWr);
    if (!file.IsOpen()) {
        return MakeError(
            E_IO,
            Sprintf("Failed to open file %s", fullPath.c_str()));
    }

    auto fileSize = file.GetLength();
    if (fileSize < 0) {
        return MakeError(
            E_IO,
            Sprintf("Failed to get file size: %s", fullPath.c_str()));
    }
    if (static_cast<size_t>(fileSize) < size) {
        return MakeError(
            E_IO,
            Sprintf(
                "File size %ld is less than requested mmap size %zu: %s",
                fileSize,
                size,
                fullPath.c_str()));
    }

    void* addr = mmap(
        nullptr,
        size,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        file,
        0 /* offset */);
    if (addr == MAP_FAILED) {
        return MakeError(
            E_IO,
            Sprintf(
                "Failed to mmap file: %s, %s",
                fullPath.c_str(),
                LastSystemErrorText()));
    }

    ui64 mmapId = ClampVal(RandomNumber<ui64>(), 1ul, Max<ui64>());
    TMmapRegion region(std::move(fullPath), addr, size, mmapId);

    TLightWriteGuard guard(StateLock);

    auto [it, inserted] = MmapRegions.emplace(mmapId, std::move(region));
    if (!inserted) {
        // extremely unlikely case of id collision
        munmap(addr, size);
        return MakeError(E_FAIL, Sprintf("Mmap ID collision: %lu", mmapId));
    };
    return it->second.ToMetadata();
}

NProto::TError TServerState::DestroyMmapRegion(ui64 mmapId)
{
    TLightWriteGuard guard(StateLock);
    auto it = MmapRegions.find(mmapId);
    if (it == MmapRegions.end()) {
        return MakeError(
            E_TRANSPORT_ERROR,
            Sprintf("Mmap region not found: %lu", mmapId));
    }
    auto metadata = it->second.ToMetadata();
    if (munmap(metadata.Address, metadata.Size) != 0) {
        return MakeError(
            E_IO,
            Sprintf(
                "Failed to unmap memory region: %lu, %s",
                mmapId,
                LastSystemErrorText()));
    }
    MmapRegions.erase(it);

    return NProto::TError{};
}

TVector<TMmapRegionMetadata> TServerState::ListMmapRegions()
{
    TVector<TMmapRegionMetadata> regions;

    TLightReadGuard guard(StateLock);
    regions.reserve(MmapRegions.size());
    for (const auto& [id, region]: MmapRegions) {
        regions.emplace_back(region.ToMetadata());
    }
    return regions;
}

TResultOrError<TMmapRegionMetadata> TServerState::GetMmapRegion(ui64 mmapId)
{
    TLightReadGuard guard(StateLock);
    auto it = MmapRegions.find(mmapId);
    if (it == MmapRegions.end()) {
        return MakeError(
            E_TRANSPORT_ERROR,
            Sprintf("Mmap region not found: %lu", mmapId));
    }
    return it->second.ToMetadata();
}

NProto::TError TServerState::PingMmapRegion(ui64 mmapId)
{
    TLightWriteGuard guard(StateLock);
    auto it = MmapRegions.find(mmapId);
    if (it == MmapRegions.end()) {
        return MakeError(
            E_TRANSPORT_ERROR,
            Sprintf("Mmap region not found: %lu", mmapId));
    }

    // update latest activity timestamp
    it->second.UpdateActivityTimestamp();

    return NProto::TError{};
}

TServerStateStats TServerState::GetStateStats() const
{
    TLightReadGuard guard(StateLock);

    TServerStateStats info;
    info.MmapRegionCount = MmapRegions.size();
    for (const auto& [id, region] : MmapRegions) {
        info.TotalMmapSize += region.ToMetadata().Size;
    }
    return info;
}

}   // namespace NCloud::NFileStore::NServer
