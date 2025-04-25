#pragma once

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

// We process 4 MB of data at a time.
// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui64 MigrationRangeSize = 4_MB;

}   // namespace NCloud::NBlockStore::NStorage
