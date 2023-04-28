#pragma once

#include "alloc.h"

#include <util/generic/vector.h>
#include <util/generic/strbuf.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

using TByteVector = TVector<char, TStlAllocator>;

////////////////////////////////////////////////////////////////////////////////

TByteVector FromStringBuf(TStringBuf source, IAllocator* allocator);

}   // namespace NCloud
