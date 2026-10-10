#pragma once

#include <memory>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IServerBackend;
using IServerBackendPtr = std::shared_ptr<IServerBackend>;

struct IDeviceManager;
using IDeviceManagerPtr = std::shared_ptr<IDeviceManager>;

}   // namespace NCloud::NJournalled
