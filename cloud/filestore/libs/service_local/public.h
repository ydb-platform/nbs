#pragma once

#include <memory>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TIndexNode;
using TIndexNodePtr = std::shared_ptr<TIndexNode>;

class TLocalFileStoreConfig;
using TLocalFileStoreConfigPtr = std::shared_ptr<TLocalFileStoreConfig>;

class TLocalFileSystem;
using TLocalFileSystemPtr = std::shared_ptr<TLocalFileSystem>;

class TSession;
using TSessionPtr = std::shared_ptr<TSession>;

struct INodeLoader;
using INodeLoaderPtr = std::shared_ptr<INodeLoader>;

}   // namespace NCloud::NFileStore
