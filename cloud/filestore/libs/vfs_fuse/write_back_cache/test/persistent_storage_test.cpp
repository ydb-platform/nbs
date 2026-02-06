#include "persistent_storage_test.h"

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TTestStorage::TImpl
{
private:
    struct TChunk: TIntrusiveListItem<TChunk>
    {
        TString Data;
    };

    IWriteBackCacheStats& Stats;
    THashMap<const void*, std::unique_ptr<TChunk>> ChunkMap;
    TIntrusiveList<TChunk> ChunkList;
    size_t Capacity = 0;

public:
    explicit TImpl(IWriteBackCacheStats& stats)
        : Stats(stats)
    {}

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

        UpdateStats();

        return res;
    }

    void Free(const void* ptr)
    {
        auto it = ChunkMap.find(ptr);
        Y_ENSURE(it != ChunkMap.end(), "Double free detected");

        ChunkList.Remove(it->second.get());
        ChunkMap.erase(it);

        UpdateStats();
    }

    void SetCapacity(size_t capacity)
    {
        Capacity = capacity;
        UpdateStats();
    }

    TPersistentStorageStats GetStats() const
    {
        return {
            .RawCapacityByteCount = Capacity,
            .RawUsedByteCount = Size(),
            .EntryCount = Size(),
            .IsCorrupted = false};
    }

    void UpdateStats()
    {
        Stats.UpdatePersistentStorageStats(GetStats());
    }
};

////////////////////////////////////////////////////////////////////////////////

TTestStorage::TTestStorage(IWriteBackCacheStats& stats)
    : Impl(std::make_unique<TImpl>(stats))
{}

TTestStorage::~TTestStorage() = default;

void TTestStorage::SetCapacity(size_t capacity)
{
    Impl->SetCapacity(capacity);
}

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
    return Impl->GetStats();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
