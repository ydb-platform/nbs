#pragma once

#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <util/generic/strbuf.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IJournalledDevice;
using IJournalledDevicePtr = std::shared_ptr<IJournalledDevice>;

}   // namespace NCloud::NBlockStore::NStorage
