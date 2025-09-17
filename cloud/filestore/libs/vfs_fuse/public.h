#pragma once

#include <cloud/filestore/libs/vfs/public.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct IFileSystem;
using IFileSystemPtr = std::shared_ptr<IFileSystem>;

struct IFileSystemFactory;
using IFileSystemFactoryPtr = std::shared_ptr<IFileSystemFactory>;

struct TFileSystemConfig;
using TFileSystemConfigPtr = std::shared_ptr<TFileSystemConfig>;

struct ICompletionQueue;
using ICompletionQueuePtr = std::shared_ptr<ICompletionQueue>;

class THandleOpsQueue;
using THandleOpsQueuePtr = std::unique_ptr<THandleOpsQueue>;

class TDirectoryHandlesStorage;
using TDirectoryHandlesStoragePtr = std::unique_ptr<TDirectoryHandlesStorage>;

}   // namespace NCloud::NFileStore::NFuse
