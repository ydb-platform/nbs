#include "persistent_storage_impl.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <util/generic/hash_set.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferStorage::TImpl
{
private:
    TFileRingBuffer Storage;
    THashSet<const void*> DeletedEntries;
    TPersistentStorageConfig Config;

public:
    TImpl(TFileRingBuffer storage, TPersistentStorageConfig config)
        : Storage(std::move(storage))
        , Config(std::move(config))
    {}

    bool Empty() const
    {
        return Storage.Empty();
    }

    void Visit(const TVisitor& visitor)
    {
        Storage.Visit(
            [this, &visitor](ui32 checksum, TStringBuf entry)
            {
                Y_UNUSED(checksum);
                if (!DeletedEntries.contains(entry.data())) {
                    visitor({entry.data(), entry.size()});
                }
            });
    }

    ui64 GetMaxSupportedAllocationByteCount() const
    {
        // Full capacity minus entry header
        return Storage.GetRawCapacity() - 8;
    }

    const void* Alloc(const TAllocationWriter& writer, size_t size)
    {
        Y_ENSURE(size > 0, "Empty allocations are now allowed");

        if (Storage.GetMaxAllocationBytesCount() < size) {
            Y_ENSURE(
                !Storage.Empty(),
                "The requested allocation size "
                    << size << " exceeds the maximum allocation size "
                    << Storage.GetMaxAllocationBytesCount());

            return nullptr;
        }

        // TFileRingBuffer does not support in-place allocation yet
        auto buffer = TString::Uninitialized(size);
        writer(buffer.begin(), size);

        Y_ABORT_UNLESS(Storage.PushBack(buffer));
        return Storage.Back().data();
    }

    void Free(const void* ptr)
    {
        auto [it, inserted] = DeletedEntries.insert(ptr);
        Y_ENSURE(inserted, "Double free detected");

        while (!Storage.Empty()) {
            const char* front = Storage.Front().data();
            if (!DeletedEntries.erase(front)) {
                return;
            }
            Storage.PopFront();
        }

        if (Storage.Empty()) {
            Y_ENSURE(
                DeletedEntries.empty(),
                "Orphaned deleted entries detected");
        }
    }

    const TPersistentStorageConfig& GetConfig() const
    {
        return Config;
    }

    TPersistentStorageStats GetStats() const
    {
        return {
            .RawCapacityByteCount = Storage.GetRawCapacity(),
            .RawUsedByteCount = Storage.GetRawUsedBytesCount(),
            .EntryCount = Storage.Size()};
    }
};

////////////////////////////////////////////////////////////////////////////////

TFileRingBufferStorage::TFileRingBufferStorage() = default;

TFileRingBufferStorage::TFileRingBufferStorage(
    TFileRingBufferStorage&&) noexcept = default;

TFileRingBufferStorage& TFileRingBufferStorage::operator=(
    TFileRingBufferStorage&&) noexcept = default;

TFileRingBufferStorage::~TFileRingBufferStorage() = default;

NProto::TError TFileRingBufferStorage::Init(TPersistentStorageConfig config)
{
    auto fileRingBuffer = TFileRingBuffer(
        config.FilePath,
        config.DataCapacity,
        config.MetadataCapacity);

    if (fileRingBuffer.IsCorrupted()) {
        return MakeError(E_FAIL, "Data structure is corrupted");
    }

    if (!fileRingBuffer.ValidateMetadata()) {
        return MakeError(E_FAIL, "Metadata is corrupted");
    }

    if (!fileRingBuffer.Validate().empty()) {
        return MakeError(E_FAIL, "Data entries are corrupted");
    }

    Impl =
        std::make_unique<TImpl>(std::move(fileRingBuffer), std::move(config));

    return {};
}

bool TFileRingBufferStorage::Empty() const
{
    return Impl->Empty();
}

void TFileRingBufferStorage::Visit(const TVisitor& visitor)
{
    Impl->Visit(visitor);
}

ui64 TFileRingBufferStorage::GetMaxSupportedAllocationByteCount() const
{
    return Impl->GetMaxSupportedAllocationByteCount();
}

const void* TFileRingBufferStorage::Alloc(
    const TAllocationWriter& writer,
    size_t size)
{
    return Impl->Alloc(writer, size);
}

void TFileRingBufferStorage::Free(const void* ptr)
{
    Impl->Free(ptr);
}

const TPersistentStorageConfig& TFileRingBufferStorage::GetConfig() const
{
    return Impl->GetConfig();
}

TPersistentStorageStats TFileRingBufferStorage::GetStats() const
{
    return Impl->GetStats();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
