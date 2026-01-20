#pragma once

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/persistent_storage.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache_stats.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TTestStorage: public IPersistentStorage
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    explicit TTestStorage(IWriteBackCacheStats& stats);
    ~TTestStorage() override;

    void SetCapacity(size_t capacity);

    bool Empty() const override;
    void Visit(const TVisitor& visitor) override;
    ui64 GetMaxSupportedAllocationByteCount() const override;
    const void* Alloc(const TAllocationWriter& writer, size_t size) override;
    void Free(const void* ptr) override;
    TPersistentStorageStats GetStats() const override;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
