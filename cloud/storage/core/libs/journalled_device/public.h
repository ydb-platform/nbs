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

struct IKeyBufferStore;
using IKeyBufferStorePtr = std::shared_ptr<IKeyBufferStore>;

struct IPageStore;
using IPageStorePtr = std::shared_ptr<IPageStore>;

}   // namespace NCloud::NJournalled
