#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NStorage {

NProto::TError SaveDiskAgentConfig(const TString& path, NProto::TDiskAgentConfig proto);

}   // namespace NCloud::NBlockStore::NStorage
