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

    TSgListBlockRange(const TSgList& sglist, ui32 blockSize);

    TSgList Next(ui64 blockCount);
};

}   // namespace NCloud
