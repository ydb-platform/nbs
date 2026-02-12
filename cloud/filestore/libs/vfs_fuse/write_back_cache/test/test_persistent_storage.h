#pragma once

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/persistent_storage.h>

#include <util/generic/hash.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TTestStorage: public IPersistentStorage
{
private:
    const IPersistentStorageStatsPtr Stats;
    THashMap<const void*, std::unique_ptr<TString>> Data;
    size_t Capacity = 0;

public:
    explicit TTestStorage(IPersistentStorageStatsPtr stats);

    bool Empty() const override;
    void Visit(const TVisitor& visitor) override;
    ui64 GetMaxSupportedAllocationByteCount() const override;
    TResultOrError<char*> Alloc(size_t size) override;
    bool Commit() override;
    void Free(const void* ptr) override;
    TPersistentStorageStats GetStats() const override;

    void SetCapacity(size_t capacity);

private:
    void UpdateStats();
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
