#pragma once

#include <memory>

namespace NCloud::NFileStore::NVFSVhost {

////////////////////////////////////////////////////////////////////////////////

struct TVfsRequest;
using TVfsRequestPtr = std::shared_ptr<TVfsRequest>;

struct TVfsRequestContext;
using TVfsRequestContextPtr = std::shared_ptr<TVfsRequestContext>;

struct IVfsDevice;
using IVfsDevicePtr = std::shared_ptr<IVfsDevice>;

struct IVfsQueue;
using IVfsQueuePtr = std::shared_ptr<IVfsQueue>;

struct IVfsQueueFactory;
using IVfsQueueFactoryPtr = std::shared_ptr<IVfsQueueFactory>;

struct IFileSystem;
using IFileSystemPtr = std::shared_ptr<IFileSystem>;

struct IFileSystemFactory;
using IFileSystemFactoryPtr = std::shared_ptr<IFileSystemFactory>;

}   // namespace NCloud::NFileStore::NVFSVhost
