#pragma once

#include <cloud/filestore/config/vfs.pb.h>

#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheLock
{
    virtual ~IWriteBackCacheLock() = default;

    virtual void Acquire() noexcept = 0;
    virtual void Release() noexcept = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWriteBackCacheLock> CreateWriteBackCacheLock(
    NProto::EWriteBackCacheLockMethod method);

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
