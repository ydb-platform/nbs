#include "persistent_storage_impl.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>

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

////////////////////////////////////////////////////////////////////////////////

class TTestStorage::TImpl
{
private:
    struct TChunk: TIntrusiveListItem<TChunk>
    {
        TString Data;
    };

    THashMap<const void*, std::unique_ptr<TChunk>> ChunkMap;
    TIntrusiveList<TChunk> ChunkList;
    size_t Capacity = 0;

public:
    size_t Size() const
    {
        return ChunkMap.size();
    }

    void Visit(const TVisitor& visitor)
    {
        for (const auto& it: ChunkList) {
            visitor(it.Data);
        }
    }

    const void* Alloc(const TAllocationWriter& writer, size_t size)
    {
        if (Capacity > 0 && Size() >= Capacity) {
            return nullptr;
        }

        auto str = TString::Uninitialized(size);
        writer(str.begin(), size);

        // The returned pointer should be different from the pointer
        // passed to the writer
        auto chunk = std::make_unique<TChunk>();
        chunk->Data = TString::Uninitialized(size);
        str.copy(chunk->Data.begin(), size);

        const void* res = chunk->Data.data();

        ChunkList.PushBack(chunk.get());
        ChunkMap[res] = std::move(chunk);

        return res;
    }

    void Free(const void* ptr)
    {
        auto it = ChunkMap.find(ptr);
        Y_ENSURE(it != ChunkMap.end(), "Double free detected");

        ChunkList.Remove(it->second.get());
        ChunkMap.erase(it);
    }

     void SetCapacity(size_t capacity)
     {
        Capacity = capacity;
     }
};

////////////////////////////////////////////////////////////////////////////////

TTestStorage::TTestStorage()
    : Impl(std::make_unique<TImpl>())
{}

TTestStorage::~TTestStorage() = default;

bool TTestStorage::Empty() const
{
    return Impl->Size() == 0;
}

void TTestStorage::Visit(const TVisitor& visitor)
{
    Impl->Visit(visitor);
}

ui64 TTestStorage::GetMaxSupportedAllocationByteCount() const
{
    return Max<ui64>();
}

const void* TTestStorage::Alloc(const TAllocationWriter& writer, size_t size)
{
    return Impl->Alloc(writer, size);
}

void TTestStorage::Free(const void* ptr)
{
    Impl->Free(ptr);
}

TPersistentStorageStats TTestStorage::GetStats() const
{
    return {
        .RawCapacityByteCount = 0,
        .RawUsedByteCount = 0,
        .EntryCount = Impl->Size()};
}

void TTestStorage::SetCapacity(size_t capacity)
{
    Impl->SetCapacity(capacity);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
