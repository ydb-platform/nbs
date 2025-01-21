#pragma once

#include "sglist.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TSgListBlockRange
{
    const ui32 BlockSize;

    TSgList::const_iterator It;
    TSgList::const_iterator End;
    ui64 Offset = 0;

    TSgListBlockRange(const TSgList& sglist, ui32 blockSize)
        : BlockSize(blockSize)
        , It(sglist.begin())
        , End(sglist.end())
    {}

    TSgList Next(ui64 blockCount)
    {
        TSgList sglist;
        while (blockCount) {
            if (It == End) {
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
};

}   // namespace NCloud
