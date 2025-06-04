#include "blocks_info.h"

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

TBlocksInfo::TBlocksInfo(ui64 from, ui64 length, ui32 blockSize)
    : BlockSize(blockSize)
{
    ui64 startIndex = from / blockSize;
    ui64 beginOffset = from - startIndex * blockSize;

    auto realLength = beginOffset + length;
    ui64 blocksCount = realLength / blockSize;

    if (blocksCount * blockSize < realLength) {
        ++blocksCount;
    }

    ui64 endOffset = blocksCount * blockSize - realLength;

    Range = TBlockRange64::WithLength(startIndex, blocksCount);
    BeginOffset = beginOffset;
    EndOffset = endOffset;
}

size_t TBlocksInfo::BufferSize() const
{
    return Range.Size() * BlockSize - BeginOffset - EndOffset;
}

bool TBlocksInfo::IsAligned() const
{
    return SgListAligned && BeginOffset == 0 && EndOffset == 0;
}

TBlocksInfo TBlocksInfo::MakeAligned() const
{
    TBlocksInfo result(*this);
    result.BeginOffset = 0;
    result.EndOffset = 0;
    result.SgListAligned = true;
    return result;
}

std::pair<TBlocksInfo, std::optional<TBlocksInfo>> TBlocksInfo::Split() const
{
    if (IsAligned() || Range.Size() <= 2) {
        return {*this, std::nullopt};
    }

    TBlocksInfo firstBlocksInfo = *this, secondBlocksInfo = *this;
    if (BeginOffset != 0) {
        // The first blocksInfo contains one block with an unaligned
        // BeginOffset. The second blocksInfo is aligned.
        firstBlocksInfo.EndOffset = 0;
        firstBlocksInfo.Range.End = firstBlocksInfo.Range.Start;
        secondBlocksInfo.BeginOffset = 0;
        ++secondBlocksInfo.Range.Start;
    } else {
        // The first blocksInfo is aligned.
        // The second blocksInfo contains one block with an unaligned EndOffset.
        --firstBlocksInfo.Range.End;
        firstBlocksInfo.EndOffset = 0;
        secondBlocksInfo.Range.Start = secondBlocksInfo.Range.End;
    }

    return {firstBlocksInfo, secondBlocksInfo};
}

TString TBlocksInfo::Print() const
{
    return TStringBuilder()
           << "[Range: " << Range << " BeginOffset: " << BeginOffset
           << " EndOffset: " << EndOffset << " BlockSize: " << BlockSize
           << " SgListAligned: " << SgListAligned << "]";
}

IOutputStream& operator<<(IOutputStream& out, const TBlocksInfo& rhs)
{
    out << rhs.Print();
    return out;
}

}   // namespace NCloud::NBlockStore
