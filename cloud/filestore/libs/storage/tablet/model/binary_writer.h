#pragma once

#include "public.h"

#include "alloc.h"

#include <cloud/storage/core/libs/common/byte_vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBinaryWriter
{
private:
    TByteVector Buffer;

public:
    TBinaryWriter(IAllocator* alloc)
        : Buffer{{alloc}}
    {}

    size_t Written() const
    {
        return Buffer.size();
    }

    void Write(const char* buf, size_t count)
    {
        memcpy(Grow(count), buf, count);
    }

    template <typename T>
    void Write(const T& val)
    {
        memcpy(Grow(sizeof(val)), &val, sizeof(val));
    }

    TByteVector Finish()
    {
        return std::move(Buffer);
    }

private:
    char* Grow(size_t count)
    {
        size_t offset = Buffer.size();
        Buffer.resize(offset + count);
        return (char*)Buffer.data() + offset;
    }
};

}   // namespace NCloud::NFileStore::NStorage
