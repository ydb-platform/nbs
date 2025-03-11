#include "sglist_block_range.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TSgListBlockRange::TSgListBlockRange(const TSgList& sglist, ui32 blockSize)
    : BlockSize(blockSize)
    , End(sglist.end())
    , It(sglist.begin())
{}

[[nodiscard]] bool TSgListBlockRange::HasNext() const {
    return It != End;
}

TSgList TSgListBlockRange::Next(ui64 blockCount)
{
    TSgList sglist;
    while (blockCount) {
        if (!HasNext()) {
            return sglist;
        }

        const auto remains = It->Size() / BlockSize - Offset;
        const auto n = std::min(remains, blockCount);

        sglist.push_back({It->Data() + Offset * BlockSize, n * BlockSize});
        blockCount -= n;
        Offset += n;

        if (n == remains) {
            Offset = 0;
            ++It;
        }
    }

    return sglist;
}

}   // namespace NCloud
