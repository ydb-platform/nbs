#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_data_ref.h>

#include <util/generic/maybe.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IBlockDigestGenerator
{
    virtual TMaybe<ui32> ComputeDigest(
        ui64 blockIndex,
        TBlockDataRef blockContent) const = 0;

    virtual bool ShouldProcess(
        ui64 blockIndex,
        ui32 blockCount,
        ui32 blockSize
    ) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IBlockDigestGeneratorPtr CreateExt4BlockDigestGenerator(
    ui32 digestedBlocksPercentage);
IBlockDigestGeneratorPtr CreateTestBlockDigestGenerator();
IBlockDigestGeneratorPtr CreateBlockDigestGeneratorStub();

}   // namespace NCloud::NBlockStore
