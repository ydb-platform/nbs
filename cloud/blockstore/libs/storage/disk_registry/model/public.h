#pragma once

#include "cloud/blockstore/public/api/protos/disk.pb.h"
#include "cloud/blockstore/public/api/protos/local_ssd.pb.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NProto::EStoragePoolKind ToStoragePoolKind(NProto::EDevicePoolKind kind);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
