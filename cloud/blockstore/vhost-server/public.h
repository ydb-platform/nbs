#pragma once

#include <memory>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct IBackend;
using IBackendPtr = std::shared_ptr<IBackend>;

}   // namespace NCloud::NBlockStore::NVHostServer
