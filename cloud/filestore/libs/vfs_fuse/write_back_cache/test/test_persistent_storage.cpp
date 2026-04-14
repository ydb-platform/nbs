#include "test_persistent_storage.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

TTestStorage::TTestStorage(IPersistentStorageStatsPtr stats)
    : Stats(std::move(stats))
{}

bool TTestStorage::Empty() const
{
    return Data.empty();
}

void TTestStorage::Visit(const TVisitor& visitor)
{
    for (const auto& it: List) {
        visitor(it.Data);
    }
}

ui64 TTestStorage::GetMaxSupportedAllocationByteCount() const
{
    return Max<ui64>();
}

TResultOrError<char*> TTestStorage::Alloc(size_t size)
{
    if (Capacity > 0 && Data.size() >= Capacity) {
        return nullptr;
    }

    auto item = std::make_unique<TEntry>();
    item->Data = TString::Uninitialized(size);
    List.PushBack(item.get());
    char* res = item->Data.begin();

    Data[res] = std::move(item);

    SetStats();

    return res;
}

bool TTestStorage::Commit()
{
    return true;
}

void TTestStorage::Free(const void* ptr)
{
    auto it = Data.find(ptr);
    Y_ENSURE(it != Data.end(), "Double free detected");

    Data.erase(it);

    SetStats();
}

void TTestStorage::UpdateStats() const
{}

void TTestStorage::SetCapacity(size_t capacity)
{
    Capacity = capacity;
    SetStats();
}

void TTestStorage::SetStats()
{
    Stats->SetPersistentStorageCounters(
        /* rawCapacityBytesCount = */ Capacity,
        /* rawUsedBytesCount = */ Data.size(),
        /* entryCount = */ Data.size(),
        /* isCorrupted = */ false);
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TTestStorage> CreateTestStorage(
    const IWriteBackCacheStatsPtr& stats)
{
    return std::make_shared<TTestStorage>(stats->GetPersistentStorageStats());
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
