#pragma once

#include "public.h"

#include "sglist.h"

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TSgList ResizeBlocks(
    TVector<TString>& blocks,
    ui64 blocksCount,
    const TString& blockContent);

}   // namespace NCloud
