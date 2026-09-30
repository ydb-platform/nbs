#pragma once

#include <memory>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IServerBackend;
using IServerBackendPtr = std::shared_ptr<IServerBackend>;

}   // namespace NCloud::NJournalled
