#pragma once

#include <cloud/blockstore/public/api/protos/io.pb.h>

#include <cloud/storage/core/libs/common/sglist.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] NProto::TChecksum CalculateChecksum(const TSgList& sglist);
[[nodiscard]] NProto::TChecksum CalculateChecksum(
    const NProto::TIOVector& iov,
    ui32 blockSize);

}   // namespace NCloud::NBlockStore
