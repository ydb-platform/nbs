#pragma once

#include <cloud/filestore/libs/vfs/public.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct IFileSystem;
using IFileSystemPtr = std::shared_ptr<IFileSystem>;

struct IFileSystemFactory;
using IFileSystemFactoryPtr = std::shared_ptr<IFileSystemFactory>;

struct ICompletionQueue;
using ICompletionQueuePtr = std::shared_ptr<ICompletionQueue>;

}   // namespace NCloud::NFileStore::NFuse
