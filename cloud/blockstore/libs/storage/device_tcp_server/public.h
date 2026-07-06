#pragma once

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IDeviceServerBackend;
using IDeviceServerBackendPtr = std::shared_ptr<IDeviceServerBackend>;

}   // namespace NCloud::NBlockStore::NStorage
