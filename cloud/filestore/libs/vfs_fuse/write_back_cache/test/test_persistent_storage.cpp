#include "test_persistent_storage.h"

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestStorage: public ITestPersistentStorage
{
private:
    struct TChunk: TIntrusiveListItem<TChunk>
    {
        TString Data;
    };

    const IWriteBackCacheStatsPtr Stats;
    THashMap<const void*, std::unique_ptr<TChunk>> ChunkMap;
    TIntrusiveList<TChunk> ChunkList;
    size_t Capacity = 0;

public:
    explicit TTestStorage(IWriteBackCacheStatsPtr stats)
        : Stats(std::move(stats))
    {}

    bool Empty() const override
    {
        return ChunkMap.empty();
    }

    void Visit(const TVisitor& visitor) override
    {
        for (const auto& it: ChunkList) {
            visitor(it.Data);
        }
    }

    ui64 GetMaxSupportedAllocationByteCount() const override
    {
        return Max<ui64>();
    }

    TResultOrError<char*> Alloc(size_t size) override
    {
        if (Capacity > 0 && ChunkMap.size() >= Capacity) {
            return nullptr;
        }

        auto chunk = std::make_unique<TChunk>();
        chunk->Data = TString::Uninitialized(size);

        char* res = chunk->Data.begin();

        ChunkList.PushBack(chunk.get());
        ChunkMap[res] = std::move(chunk);

        UpdateStats();

        return res;
    }

    bool Commit() override
    {
        return true;
    }

    void Free(const void* ptr) override
    {
        auto it = ChunkMap.find(ptr);
        Y_ENSURE(it != ChunkMap.end(), "Double free detected");

        ChunkList.Remove(it->second.get());
        ChunkMap.erase(it);

        UpdateStats();
    }

    void SetCapacity(size_t capacity) override
    {
        Capacity = capacity;
        UpdateStats();
    }

    TPersistentStorageStats GetStats() const override
    {
        return {
            .RawCapacityByteCount = Capacity,
            .RawUsedByteCount = ChunkMap.size(),
            .EntryCount = ChunkMap.size(),
            .IsCorrupted = false};
    }

    void UpdateStats()
    {
        Stats->UpdatePersistentStorageStats(GetStats());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestPersistentStoragePtr CreateTestPersistentStorage(
    IWriteBackCacheStatsPtr stats)
{
    return std::make_shared<TTestStorage>(std::move(stats));
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
