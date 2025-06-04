#include "public.h"

#include "cloud/blockstore/libs/common/block_range.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TBlocksInfo
{
    TBlocksInfo() = default;
    TBlocksInfo(ui64 from, ui64 length, ui32 blockSize);
    TBlocksInfo(const TBlocksInfo&) = default;

    [[nodiscard]] size_t BufferSize() const;

    // The data may be misaligned for two reasons: if the start or end of the
    // block do not correspond to the block boundaries, or if the client buffers
    // are not a multiple of the block size.
    [[nodiscard]] bool IsAligned() const;

    // Creates an aligned TBlocksInfo.
    [[nodiscard]] TBlocksInfo MakeAligned() const;

    // Split the unaligned TBlocksInfo into two TBlocksInfo objects.
    // If TBlocksInfo is aligned or contains no more than 2 blocks, then the
    // result contains the same TBlocksInfo as the first value and std::nullopt
    // as the second value.
    [[nodiscard]] std::pair<TBlocksInfo, std::optional<TBlocksInfo>>
    Split() const;

    TBlockRange64 Range;
    // Offset relative to the beginning of the range.
    ui64 BeginOffset = 0;
    // Offset relative to the ending of the range.
    ui64 EndOffset = 0;
    const ui32 BlockSize = 0;
    // The request also unaligned if the sglist buffer sizes are not multiples
    // of the block size
    bool SgListAligned = true;

    friend bool operator==(const TBlocksInfo& lhs, const TBlocksInfo& rhs)
    {
        return lhs.Range == rhs.Range && lhs.BeginOffset == rhs.BeginOffset &&
               lhs.EndOffset == rhs.EndOffset &&
               lhs.BlockSize == rhs.BlockSize &&
               lhs.SgListAligned == rhs.SgListAligned;
    }

    [[nodiscard]] TString Print() const;
};

IOutputStream& operator<<(IOutputStream& out, const TBlocksInfo& rhs);

}   // namespace NCloud::NBlockStore
