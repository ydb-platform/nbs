#include "persistent_storage.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferStorage: public IPersistentStorage
{
private:
    const IPersistentStorageStatsPtr Stats;
    TFileRingBuffer Storage;
    THashSet<const void*> DeletedEntries;
    const TPersistentStorageConfig Config;

public:
    TFileRingBufferStorage(
        IPersistentStorageStatsPtr stats,
        TPersistentStorageConfig config)
        : Stats(std::move(stats))
        , Storage(config.FilePath, config.DataCapacity, config.MetadataCapacity)
        , Config(std::move(config))
    {
        UpdateStats();
    }

    NProto::TError Init()
    {
        if (Storage.IsCorrupted()) {
            return MakeError(E_FAIL, "Data structure is corrupted");
        }

        if (!Storage.ValidateMetadata()) {
            Storage.SetCorrupted();
            UpdateStats();
            return MakeError(E_FAIL, "Metadata is corrupted");
        }

        if (Config.EnableChecksumValidation && !Storage.Validate().empty()) {
            Storage.SetCorrupted();
            UpdateStats();
            return MakeError(E_FAIL, "Data entries are corrupted");
        }

        return {};
    }

    bool Empty() const override
    {
        return Storage.Empty();
    }

    void Visit(const TVisitor& visitor) override
    {
        Storage.Visit(
            [this, &visitor](ui32 checksum, TStringBuf entry)
            {
                Y_UNUSED(checksum);
                if (!DeletedEntries.contains(entry.data())) {
                    visitor({entry.data(), entry.size()});
                }
            });

        UpdateStats();
    }

    ui64 GetMaxSupportedAllocationByteCount() const override
    {
        return Storage.GetMaxSupportedAllocationByteCount();
    }

    const void* Alloc(const TAllocationWriter& writer, size_t size) override
    {
        Y_ENSURE(size > 0, "Zero-size allocations are not allowed");

        if (Storage.GetAvailableByteCount() < size) {
            return nullptr;
        }

        // TFileRingBuffer does not support in-place allocation yet
        auto buffer = TString::Uninitialized(size);
        writer(buffer.begin(), size);

        Y_ABORT_UNLESS(Storage.PushBack(buffer));

        UpdateStats();

        return Storage.Back().data();
    }

    void Free(const void* ptr) override
    {
        auto [it, inserted] = DeletedEntries.insert(ptr);
        Y_ENSURE(inserted, "Double free detected");

        while (!Storage.Empty()) {
            const char* front = Storage.Front().data();
            if (DeletedEntries.erase(front) != 0) {
                Storage.PopFront();
            } else {
                break;
            }
        }

        if (Storage.Empty()) {
            Y_ENSURE(
                DeletedEntries.empty(),
                "Orphaned deleted entries detected");
        }

        UpdateStats();
    }

    TPersistentStorageStats GetStats() const override
    {
        return {
            .RawCapacityByteCount = Storage.GetRawCapacity(),
            .RawUsedByteCount = Storage.GetRawUsedBytesCount(),
            .EntryCount = Storage.Size() - DeletedEntries.size(),
            .IsCorrupted = Storage.IsCorrupted()};
    }

private:
    void UpdateStats()
    {
        Stats->UpdatePersistentStorageStats(GetStats());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IPersistentStoragePtr> CreateFileRingBufferPersistentStorage(
    IPersistentStorageStatsPtr stats,
    TPersistentStorageConfig config)
{
    auto storage = std::make_shared<TFileRingBufferStorage>(
        std::move(stats),
        std::move(config));

    auto error = storage->Init();
    if (HasError(error)) {
        return error;
    }

    return static_cast<IPersistentStoragePtr>(storage);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
