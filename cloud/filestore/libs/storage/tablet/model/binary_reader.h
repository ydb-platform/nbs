#pragma once

#include "public.h"

#include "alloc.h"

#include <cloud/storage/core/libs/common/byte_vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBinaryReader
{
private:
    const TByteVector& Buffer;
    TByteVector::const_iterator Ptr = Buffer.begin();

public:
    TBinaryReader(const TByteVector& buffer)
        : Buffer(buffer)
    {}

    size_t Avail() const
    {
        return std::distance(Ptr, Buffer.end());
    }

    const char* Read(size_t count)
    {
        return Consume(count);
    }

    template <typename T>
    const T& Read()
    {
        return *reinterpret_cast<const T*>(Consume(sizeof(T)));
    }

    template <typename T>
    const T* Read(size_t count)
    {
        return reinterpret_cast<const T*>(Consume(count * sizeof(T)));
    }

private:
    const char* Consume(size_t count)
    {
        Y_VERIFY(Avail() >= count, "Invalid encoding");
        const char* p = Ptr;
        Ptr += count;
        return p;
    }
};

}   // namespace NCloud::NFileStore::NStorage
