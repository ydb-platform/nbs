#include "aligned_string.h"

#include <util/system/align.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

std::tuple<TString, ui32> AlignedString(ui32 size, ui32 align)
{
    auto buffer = TString::Uninitialized(size + align);
    ui32 offset = 0;

    if (align != 0) {
        offset = AlignUp(buffer.data(), align) - buffer.data();
        buffer.resize(size + offset);
    }

    return std::make_tuple(std::move(buffer), offset);
}

}   // namespace NCloud
