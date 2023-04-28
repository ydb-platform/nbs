#include "random.h"

#include <util/generic/guid.h>
#include <util/system/unaligned_mem.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TString SafeCreateGuidAsString()
{
    TGUID guid;
    ui64* dw = reinterpret_cast<ui64*>(guid.dw);

    WriteUnaligned<ui64>(&dw[0], RandInt<ui64>());
    WriteUnaligned<ui64>(&dw[1], RandInt<ui64>());

    return guid.AsGuidString();
}

}   // namespace NCloud
