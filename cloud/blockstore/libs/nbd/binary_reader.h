#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/sglist.h>

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/input.h>
#include <util/system/byteorder.h>

#include <type_traits>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

class TBinaryReader
{
protected:
    IInputStream& In;

public:
    TBinaryReader(IInputStream& in)
        : In(in)
    {}

    bool Read(void* buffer, size_t len)
    {
        if (!len) {
            // nothing to read
            return true;
        }

        size_t read = In.Load(buffer, len);

        if (!read) {
            // end of stream
            return false;
        }

        if (read != len) {
            ythrow yexception()
                << "Failed to read required number of bytes from stream!"
                << " Expected: " << len << ", read: " << read << "!";
        }

        return true;
    }

    bool Read(TBuffer& buffer, size_t len)
    {
        buffer.Resize(len);
        return Read(buffer.Data(), len);
    }

    bool Read(TString& s, size_t len)
    {
        s.resize(len);
        return Read(const_cast<char*>(s.data()), len);
    }

    bool Read(const TSgList& sglist, size_t len);

    template <typename T>
    std::enable_if_t<std::is_integral<T>::value, bool> Read(T& value)
    {
        T tmp;
        if (Read(&tmp, sizeof(T))) {
            value = InetToHost(tmp);
            return true;
        }

        return false;
    }

    template <typename... TArgs>
    void ReadOrFail(TArgs&&... args)
    {
        if (!Read(std::forward<TArgs>(args)...)) {
            ythrow yexception() << "Unexpected end of stream";
        }
    }
};

}   // namespace NCloud::NBlockStore::NBD
