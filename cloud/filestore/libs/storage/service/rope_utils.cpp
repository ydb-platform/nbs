#include "rope_utils.h"

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <bool Owner = false>
class TIovecContiguousChunk: public IContiguousChunk
{
private:
    char* Base = 0;
    ui64 Size = 0;

public:
    TIovecContiguousChunk(ui64 base, ui64 size)
        : Base(reinterpret_cast<char*>(base))
        , Size(size)
    {
        static_assert(sizeof(ui64) == sizeof(char*));
    }

    ~TIovecContiguousChunk() {
        if constexpr(Owner) {
            delete[] Base;
        }
    }

    TContiguousSpan GetData() const override
    {
        return TContiguousSpan(Base, Size);
    }

    TMutableContiguousSpan UnsafeGetDataMut() override
    {
        return TMutableContiguousSpan(Base, Size);
    }

    IContiguousChunk::TPtr Clone() override
    {
        auto copy = new char[Size];
        ::memcpy(copy, Base, Size);

        return MakeIntrusive<TIovecContiguousChunk<true>>(
            reinterpret_cast<ui64>(copy),
            Size);
    }

    size_t GetOccupiedMemorySize() const override
    {
        return Size;
    }
};

}   // namespace

TRope CreateRope(
    const ::google::protobuf::RepeatedPtrField<NProto::TIovec>& iovecs)
{
    TRope rope;
    for (const auto& iovec: iovecs) {
        if (iovec.GetLength() == 0) {
            continue;
        }
        rope.Insert(
            rope.End(),
            TRope(TRcBuf(
                MakeIntrusive<TIovecContiguousChunk<>>(
                    iovec.GetBase(),
                    iovec.GetLength()))));
    }

    return rope;
}

TRope CreateRope(void* data, ui64 size)
{
    return TRope(TRcBuf(
        MakeIntrusive<TIovecContiguousChunk<>>(
            reinterpret_cast<ui64>(data),
            size)));
}

}   // namespace NCloud::NFileStore::NStorage
