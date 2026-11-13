#pragma once

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IRdmaTabletRequestHandler;
using IRdmaTabletRequestHandlerPtr = std::shared_ptr<IRdmaTabletRequestHandler>;

struct TFileStoreRdmaServerConfig;
using TFileStoreRdmaServerConfigPtr =
    std::shared_ptr<TFileStoreRdmaServerConfig>;

}   // namespace NCloud::NFileStore::NStorage
