#pragma once

#include "public.h"

#include "block.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <util/generic/strbuf.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct IBlobsIndexVisitor
{
    virtual ~IBlobsIndexVisitor() = default;

    virtual bool Visit(
        ui64 commitId,
        ui64 blobId,
        const NProto::TBlobMeta& blobMeta,
        const TStringBuf blockMask) = 0;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
