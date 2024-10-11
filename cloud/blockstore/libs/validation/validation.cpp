#include "validation.h"

#include <library/cpp/digest/crc32c/crc32c.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCrcDigestCalculator final
    : IBlockDigestCalculator
{
    ui64 Calculate(ui64 blockIndex, const TStringBuf block) const override
    {
        Y_UNUSED(blockIndex);

        return Crc32c(block.data(), block.size());
    }
};

}   // namespace

IBlockDigestCalculatorPtr CreateCrcDigestCalculator()
{
    return std::make_shared<TCrcDigestCalculator>();
}

TVector<ui64> CalculateBlocksDigest(
    const TSgList& blocks,
    const IBlockDigestCalculator& digestCalculator,
    ui32 blockSize,
    ui64 blockIndex,
    ui32 blockCount,
    ui64 zeroBlockDigest)
{
    Y_ABORT_UNLESS(zeroBlockDigest != InvalidDigest);

    TVector<ui64> result(Reserve(blockCount));

    size_t n = 0;
    const char* data = nullptr;
    size_t size = 0;

    for (size_t i = 0; i < blockCount; ++i) {
        while (size == 0 && n < blocks.size()) {
            data = blocks[n].Data();
            size = blocks[n].Size();
            ++n;
        }
        if (size == 0) {
            break;
        }

        Y_ABORT_UNLESS(size >= blockSize);
        size -= blockSize;

        if (data) {
            result.push_back(digestCalculator.Calculate(
                blockIndex + i,
                {data, blockSize}
            ));
            data += blockSize;
        } else {
            result.push_back(zeroBlockDigest);
        }
    }

    return result;
}

}   // namespace NCloud::NBlockStore
