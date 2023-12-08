#pragma once

#include <memory>

namespace NCloud::NFileStore::NVFS {

////////////////////////////////////////////////////////////////////////////////

struct TVFSConfig;
using TVFSConfigPtr = std::shared_ptr<TVFSConfig>;

struct IFileSystemLoop;
using IFileSystemLoopPtr = std::shared_ptr<IFileSystemLoop>;

struct IFileSystemLoopFactory;
using IFileSystemLoopFactoryPtr = std::shared_ptr<IFileSystemLoopFactory>;

}   // namespace NCloud::NFileStore::NVFS
