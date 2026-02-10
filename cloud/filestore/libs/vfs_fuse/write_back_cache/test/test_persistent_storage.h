#pragma once

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/persistent_storage.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache_stats.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct ITestPersistentStorage: public IPersistentStorage
{
    virtual void SetCapacity(size_t capacity) = 0;
};

using ITestPersistentStoragePtr = std::shared_ptr<ITestPersistentStorage>;

////////////////////////////////////////////////////////////////////////////////

ITestPersistentStoragePtr CreateTestPersistentStorage(
    IWriteBackCacheStatsPtr stats);

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
