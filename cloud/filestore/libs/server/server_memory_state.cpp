#include "server_memory_state.h"

#include <util/folder/path.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/guard.h>

#include <sys/mman.h>

namespace NCloud::NFileStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MinPageSize = 4096;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TServerState::TServerState(
        const TString& sharedMemoryBasePath,
        TDuration regionTimeout)
    : SharedMemoryBasePath(sharedMemoryBasePath)
    , RegionTimeout(regionTimeout)
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
    size_t size,
    ui32 pageSize)
{
    if (size < MinPageSize) {
        return MakeError(E_ARGUMENT, "Size must be greater or equal to 4 KB");
    }

    if (pageSize != 0 && size % pageSize != 0) {
        return MakeError(
            E_ARGUMENT,
            Sprintf(
                "Page size %u is not aligned with region size: %lu",
                pageSize,
                size));
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
    TMmapRegion region(std::move(fullPath), addr, size, mmapId, pageSize);

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

TResultOrError<google::protobuf::RepeatedPtrField<NProto::TIovec>>
TServerState::AdjustAndLockIovecs(
    ui64 mmapId,
    const google::protobuf::RepeatedPtrField<NProto::TIovec>& iovecs)
{
    // The page state may be modified while holding a read lock because the
    // state flag is atomic
    TLightReadGuard guard(StateLock);
    auto it = MmapRegions.find(mmapId);
    if (it == MmapRegions.end()) {
        return MakeError(
            E_TRANSPORT_ERROR,
            Sprintf("Mmap region not found: %lu", mmapId));
    }

    auto& region = it->second;
    const ui64 regionAddress = region.GetAddress();
    const ui64 regionSize = region.GetSize();

    auto adjustedIovecs = iovecs;
    for (auto& iovec: adjustedIovecs) {
        ui64 offset = iovec.GetBase();
        ui64 length = iovec.GetLength();

        if (offset >= regionSize || length > regionSize - offset) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Iovec out of bounds: offset=" << offset
                    << " length=" << length << " region_size=" << regionSize);
        }

        iovec.SetBase(offset + regionAddress);
    }

    const auto pageSize = region.GetPageSize();
    // If page size is not specified, page size restrictions are currently not
    // enforced. We plan to enable page size enforcement by default in the
    // future.
    if (!pageSize) {
        return adjustedIovecs;
    }

    int i = 0;
    NProto::TError err;
    for (; i < adjustedIovecs.size(); ++i) {
        const ui64& base = adjustedIovecs[i].GetBase() - regionAddress;
        const ui64& length = adjustedIovecs[i].GetLength();
        // The last iovec in a request may have a length smaller than PageSize.
        if (base % pageSize != 0 || length > pageSize ||
            (length < pageSize && i != adjustedIovecs.size() - 1))
        {
            err = MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Iovec is not aligned with page size: offset=" << base
                    << " length=" << length << " page size=" << pageSize);
            break;
        }

        ui64 index = base / pageSize;
        if (!region.LockPage(index)) {
            err = MakeError(
                E_TRANSPORT_ERROR,
                Sprintf("Address range is in use: [%lu, %lu]", base, length));
            break;
        }
    }

    if (HasError(err)) {
        // Unlock previously locked pages in case of error
        if (i > 0) {
            for (int j = i - 1; j >= 0; --j) {
                const ui64& base = adjustedIovecs[j].GetBase() - regionAddress;
                ui64 index = base / pageSize;
                auto ret = region.UnlockPage(index);
                Y_DEBUG_ABORT_UNLESS(ret);
            }
        }

        return err;
    }

    return {adjustedIovecs};
}

NProto::TError TServerState::UnlockIovecs(
    ui64 mmapId,
    const google::protobuf::RepeatedPtrField<NProto::TIovec>& iovecs)
{
    TLightReadGuard guard(StateLock);
    auto it = MmapRegions.find(mmapId);
    if (it == MmapRegions.end()) {
        return MakeError(
            E_TRANSPORT_ERROR,
            Sprintf("Mmap region not found: %lu", mmapId));
    }

    auto& region = it->second;
    const ui64 pageSize = region.GetPageSize();
    const ui64 regionAddress = region.GetAddress();
    if (!pageSize) {
        return {};
    }

    NProto::TError err;
    TString failedOffsets;
    for (const auto& iovec: iovecs) {
        bool failed = false;
        if (iovec.GetBase() >= regionAddress) {
            const ui64& base = iovec.GetBase() - regionAddress;
            ui64 index = base / pageSize;
            if (!region.UnlockPage(index)) {
                failed = true;
            }
        } else {
            failed = true;
        }

        if (failed) {
            if (!failedOffsets.empty()) {
                failedOffsets += ", ";
            }
            failedOffsets += std::to_string(iovec.GetBase());
        }
    }

    if (!failedOffsets.empty()) {
        return MakeError(
            E_TRANSPORT_ERROR,
            Sprintf(
                "Failed to unlock pages with offsets: %s",
                failedOffsets.c_str()));
    }

    return {};
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

NProto::TError TServerState::InvalidateTimedOutRegions()
{
    auto now = TInstant::Now();

    TLightWriteGuard guard(StateLock);

    for (auto it = MmapRegions.begin(); it != MmapRegions.end(); ) {
        if (now - it->second.GetLatestActivityTimestamp() > RegionTimeout) {
            auto metadata = it->second.ToMetadata();
            if (munmap(metadata.Address, metadata.Size) != 0) {
                return MakeError(
                    E_IO,
                    Sprintf(
                        "Failed to unmap memory region: %lu, %s",
                        metadata.Id,
                        LastSystemErrorText()));
            }
            it = MmapRegions.erase(it);
        } else {
            ++it;
        }
    }

    return NProto::TError{};
}

}   // namespace NCloud::NFileStore::NServer
