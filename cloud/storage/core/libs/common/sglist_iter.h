#pragma once

#include "public.h"
#include "sglist.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TSgListInputIterator
{
private:
    const TSgList& SgList;
    size_t Idx = 0;
    size_t Offset = 0;

public:
    TSgListInputIterator(const TSgList& sgList)
        : SgList(sgList)
    {}

    size_t Read(char* dst, size_t size)
    {
        char* ptr = dst;
        while (Idx < SgList.size() && size) {
            const TBlockDataRef& block = SgList[Idx];
            size_t len = std::min(size, block.Size() - Offset);

            memcpy(ptr, block.Data() + Offset, len);

            ptr += len;
            size -= len;

            Offset += len;
            if (Offset == block.Size()) {
                ++Idx;
                Offset = 0;
            }
        }

        return ptr - dst;
    }

    size_t Size() const
    {
        size_t length = 0;

        size_t idx = Idx;
        size_t offset = Offset;
        for (; idx < SgList.size(); ++idx, offset = 0) {
            length += SgList[idx].Size() - offset;
        }

        return length;
    }

    template<typename T>
    bool Read(T& t)
    {
        static_assert(std::is_trivial_v<T>);
        ui32 read = Read(reinterpret_cast<char*>(&t), sizeof(t));
        return read == sizeof(t);
    }

    template<>
    bool Read(TString& str)
    {
        // try to read null terminated string

        size_t count = 0;
        bool found = false;

        size_t idx = Idx;
        size_t offset = Offset;
        while (idx < SgList.size()) {
            auto buf = SgList[idx].AsStringBuf();
            auto pos = buf.find_first_of('\0', offset);

            if (pos != TStringBuf::npos) {
                count += pos - offset;
                found = true;
                break;
            }

            count += buf.size() - offset;
            ++idx;
            offset = 0;
        }

        if (!found) {
            return false;
        }

        str.resize(count);
        if (count) {
            // non empty string
            auto read = Read(&str[0], count);
            Y_ABORT_UNLESS(read == count, "unexpected size %lu vs %lu",
                read, count);
        }

        ++Offset; // skip '\0'
        if (Offset == SgList[Idx].Size()) {
            ++Idx;
            Offset = 0;
        }

        return true;
    }

    template <typename... Args>
    bool ReadAll(Args&... args)
    {
        return DoRead(args...);
    }

private:
    bool DoRead()
    {
        return true;
    }

    template <typename Arg, typename... Args>
    bool DoRead(Arg& arg, Args&... args)
    {
        if (!Read(arg)) {
            return false;
        }

        return DoRead(args...);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSgListOutputIterator
{
private:
    const TSgList& SgList;
    size_t Idx = 0;
    size_t Offset = 0;

public:
    TSgListOutputIterator(const TSgList& sgList)
        : SgList(sgList)
    {}

    size_t Write(const char* src, size_t size)
    {
        const char* ptr = src;
        while (Idx < SgList.size() && size) {
            const TBlockDataRef& block = SgList[Idx];
            size_t len = std::min(size, block.Size() - Offset);

            // TODO: fix const_cast
            memcpy(const_cast<char*>(block.Data()) + Offset, ptr, len);

            ptr += len;
            size -= len;

            Offset += len;
            if (Offset == block.Size()) {
                ++Idx;
                Offset = 0;
            }
        }

        return ptr - src;
    }

    size_t Size() const
    {
        size_t length = 0;

        size_t idx = Idx;
        size_t offset = Offset;
        for (; idx < SgList.size(); ++idx, offset = 0) {
            length += SgList[idx].Size() - offset;
        }

        return length;
    }

    template<typename T>
    bool Write(const T& t)
    {
        static_assert(std::is_trivial_v<T>);
        size_t written = Write(reinterpret_cast<const char*>(&t), sizeof(t));
        return written == sizeof(t);
    }

    template<>
    bool Write(const TStringBuf& data)
    {
        size_t written = Write(data.data(), data.size());
        if (written == data.size()) {
            return Write("\0", 1) == 1;
        }

        return false;
    }

    template<>
    bool Write(const TString& data)
    {
        return Write(TStringBuf(data.data(), data.size()));
    }

    template <typename... Args>
    bool WriteAll(const Args&... args)
    {
        return DoWrite(args...);
    }

private:
    bool DoWrite()
    {
        return true;
    }

    template <typename Arg, typename... Args>
    bool DoWrite(const Arg& arg, const Args&... args)
    {
        if (!Write(arg)) {
            return false;
        }

        return DoWrite(args...);
    }
};

}   // namespace NCloud
