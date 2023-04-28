#pragma once

#include "public.h"

#include "sglist.h"

#include <cloud/blockstore/public/api/protos/io.pb.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TSgList ResizeIOVector(NProto::TIOVector& iov, ui32 blocksCount, ui32 blockSize);

TSgList GetSgList(const NProto::TWriteBlocksRequest& request);

TResultOrError<TSgList> GetSgList(
    const NProto::TReadBlocksResponse& response,
    ui32 expectedBlockSize);

void CopyToSgList(
    const NProto::TIOVector& iov,
    const TSgList& sglist,
    ui64 offsetInBlocks,
    ui32 blockSize);

}   // namespace NCloud::NBlockStore
