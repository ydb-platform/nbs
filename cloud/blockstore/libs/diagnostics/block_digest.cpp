#include "block_digest.h"

#include <cloud/blockstore/libs/common/constants.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

#include <cmath>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

ui32 ComputeDefaultDigest(TBlockDataRef blockContent)
{
    Y_DEBUG_ABORT_UNLESS(blockContent.Data() != nullptr);

    if (blockContent.Data() != nullptr) {
        return Crc32c(blockContent.Data(), blockContent.Size());
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

struct TExt4BlockDigestGenerator final
    : IBlockDigestGenerator
{
    const ui32 DigestedBlocksPercentage;
    TVector<ui32> ZeroBlockDigests;

    TExt4BlockDigestGenerator(ui32 digestedBlocksPercentage)
        : DigestedBlocksPercentage(digestedBlocksPercentage)
        , ZeroBlockDigests(1 + log2(MaxBlockSize / DefaultBlockSize))
    {
        for (ui32 i = 0; i < ZeroBlockDigests.size(); ++i) {
            TVector<char> buf(DefaultBlockSize * pow(2, i));
            ZeroBlockDigests[i] = Crc32c(buf.data(), buf.size());
        }
    }

    TMaybe<ui32> ComputeDigest(
        ui64 blockIndex,
        TBlockDataRef blockContent) const override
    {
        if (!ShouldProcess(blockIndex, blockContent.Size())) {
            return Nothing();
        }

        if (blockContent.Data() != nullptr) {
            return ComputeDefaultDigest(blockContent);
        }

        double idx = log2(static_cast<double>(blockContent.Size()) / DefaultBlockSize);
        auto intIdx = static_cast<int>(idx);
        if (intIdx < 0 || intIdx >= ZeroBlockDigests.ysize() || idx != intIdx) {
            return Nothing();
        }

        return ZeroBlockDigests[intIdx];
    }

    bool ShouldProcess(ui64 blockIndex, ui32 blockSize) const
    {
        Y_DEBUG_ABORT_UNLESS(blockSize);
        if (!blockSize) {
            return false;
        }

        const auto partitionTableSize = 1_MB;
        const auto groupSize = 128_MB;
        const auto blocksInGroup = groupSize / blockSize;
        const auto fsOffset = partitionTableSize / blockSize;

        if (blockIndex > fsOffset) {
            blockIndex -= fsOffset;

            const auto relativeIndex = blockIndex % blocksInGroup;
            const auto threshold =
                (DigestedBlocksPercentage / 100.) * blocksInGroup;

            return relativeIndex <= threshold;
        }

        return true;
    }

    bool ShouldProcess(
        ui64 blockIndex,
        ui32 blockCount,
        ui32 blockSize) const override
    {
        return ShouldProcess(blockIndex, blockSize)
            || ShouldProcess(blockIndex + blockCount - 1, blockSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockDigestGenerator final
    : IBlockDigestGenerator
{
    TMaybe<ui32> ComputeDigest(
        ui64 blockIndex,
        TBlockDataRef blockContent) const override
    {
        Y_UNUSED(blockIndex);

        if (blockContent.Data() == nullptr) {
            return 0;
        }

        Y_DEBUG_ABORT_UNLESS(blockContent.Size() >= 4);
        if (blockContent.Size() < 4) {
            return Nothing();
        }

        return *reinterpret_cast<const ui32*>(blockContent.Data());
    }

    bool ShouldProcess(
        ui64 blockIndex,
        ui32 blockCount,
        ui32 blockSize) const override
    {
        Y_UNUSED(blockIndex);
        Y_UNUSED(blockCount);
        Y_UNUSED(blockSize);

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockDigestGeneratorStub final
    : IBlockDigestGenerator
{
    TMaybe<ui32> ComputeDigest(ui64, TBlockDataRef) const override
    {
        return Nothing();
    }

    bool ShouldProcess(
        ui64 blockIndex,
        ui32 blockCount,
        ui32 blockSize) const override
    {
        Y_UNUSED(blockIndex);
        Y_UNUSED(blockCount);
        Y_UNUSED(blockSize);

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

IBlockDigestGeneratorPtr CreateExt4BlockDigestGenerator(
    ui32 digestedBlocksPercentage)
{
    return std::make_shared<TExt4BlockDigestGenerator>(
        digestedBlocksPercentage
    );
}

IBlockDigestGeneratorPtr CreateTestBlockDigestGenerator()
{
    return std::make_shared<TTestBlockDigestGenerator>();
}

IBlockDigestGeneratorPtr CreateBlockDigestGeneratorStub()
{
    return std::make_shared<TBlockDigestGeneratorStub>();
}

}   // namespace NCloud::NBlockStore
