#include "byte_vector.h"

#include "alloc.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TByteVector FromStringBuf(TStringBuf source, IAllocator* allocator)
{
    TByteVector result{{allocator}};
    result.resize(source.size());
    std::memcpy(result.data(), source.data(), source.size());
    return result;
}

}   // namespace NCloud
