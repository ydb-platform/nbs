#pragma once

#include <memory>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IDevice;
using IDevicePtr = std::shared_ptr<IDevice>;

struct IJournal;
using IJournalPtr = std::shared_ptr<IJournal>;

struct IJournalledDevice;
using IJournalledDevicePtr = std::shared_ptr<IJournalledDevice>;

}   // namespace NCloud::NJournalled
