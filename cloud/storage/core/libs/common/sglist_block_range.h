#pragma once

#include "sglist.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TSgListBlockRange
{
    const ui32 BlockSize;
    const TSgList::const_iterator End;

    TSgList::const_iterator It;
    ui64 Offset = 0;

public:
    TSgListBlockRange(const TSgList& sglist, ui32 blockSize);
    ~TSgListBlockRange() = default;

    TSgList Next(ui64 blockCount);
    [[nodiscard]] bool HasNext() const;
};

}   // namespace NCloud
