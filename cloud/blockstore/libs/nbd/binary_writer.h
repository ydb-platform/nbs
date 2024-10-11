#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/sglist.h>

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <util/stream/output.h>
#include <util/system/byteorder.h>

#include <type_traits>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

class TBinaryWriter
{
protected:
    IOutputStream& Out;

public:
    TBinaryWriter(IOutputStream& out)
        : Out(out)
    {}

    void Flush()
    {
        Out.Flush();
    }

    void Write(const void* buffer, size_t len)
    {
        Out.Write(buffer, len);
    }

    void Write(TStringBuf buffer)
    {
        Write(buffer.data(), buffer.size());
    }

    void Write(const TBuffer& buffer)
    {
        Write(buffer.Data(), buffer.Size());
    }

    size_t Write(const TSgList& sglist);

    template <typename T>
    std::enable_if_t<std::is_integral<T>::value, void> Write(T value)
    {
        T tmp = HostToInet(value);

        Write(&tmp, sizeof(T));
    }
};

}   // namespace NCloud::NBlockStore::NBD
