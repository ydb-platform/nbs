#pragma once

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IJournalledDeviceServerProvider;
using IJournalledDeviceServerProviderPtr =
    std::shared_ptr<IJournalledDeviceServerProvider>;

}   // namespace NCloud::NBlockStore::NStorage
