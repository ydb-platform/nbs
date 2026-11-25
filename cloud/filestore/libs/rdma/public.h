#pragma once

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IRdmaTabletRequestHandler;
using IRdmaTabletRequestHandlerPtr = std::shared_ptr<IRdmaTabletRequestHandler>;

}   // namespace NCloud::NFileStore::NStorage
