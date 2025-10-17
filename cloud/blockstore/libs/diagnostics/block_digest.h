#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/block_data_ref.h>

#include <util/generic/maybe.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IBlockDigestGenerator
{
    virtual TMaybe<ui32> ComputeDigest(
        ui64 blockIndex,
        TBlockDataRef blockContent) const = 0;

    // calculate checksum independently of ShouldProcess result
    virtual TMaybe<ui32> ComputeDigestForce(
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

////////////////////////////////////////////////////////////////////////////////

ui32 ComputeDefaultDigest(TBlockDataRef blockContent);

}   // namespace NCloud::NBlockStore
