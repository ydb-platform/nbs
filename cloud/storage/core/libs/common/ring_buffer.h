#pragma once

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <cstddef>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TRingBuffer
{
public:
    TRingBuffer(size_t capacity)
        : Buffer(capacity)
    {}

    void PopFront()
    {
        if (IsEmpty()) {
            return;
        }

        Begin = GetNext(Begin);
        DecreaseSize();
    }

    void PopBack()
    {
        if (IsEmpty()) {
            return;
        }

        End = GetPrevious(End);
        DecreaseSize();
    }

    void PushFront(const T& val)
    {
        Begin = GetPrevious(Begin);
        Buffer[Begin] = std::move(val);

        if (IsFull()) {
            End = GetPrevious(End);
        } else {
            IncreaseSize();
        }
    }

    void PushBack(const T& val)
    {
        Buffer[End] = std::move(val);
        End = GetNext(End);

        if (IsFull()) {
            Begin = GetNext(Begin);
        } else {
            IncreaseSize();
        }
    }

    const T& GetFront() const
    {
        Y_VERIFY(!IsEmpty());
        return Buffer[Begin];
    }

    const T& GetBack() const
    {
        Y_VERIFY(!IsEmpty());
        return Buffer[GetPrevious(End)];
    }

    void Clear()
    {
        Begin = End = CurrentSize = 0;
    }

    bool IsEmpty() const
    {
        return Size() == 0;
    }

    bool IsFull() const
    {
        return Size() == Capacity();
    }

    size_t Size() const
    {
        return CurrentSize;
    }

    size_t Capacity() const
    {
        return Buffer.size();
    }

private:
    size_t GetRealIndex(size_t index) const
    {
        return index % Capacity();
    }

    size_t GetNext(size_t index) const
    {
        return GetRealIndex(index + 1);
    }

    size_t GetPrevious(size_t index) const
    {
        return GetRealIndex(index + Capacity() - 1);
    }

    void DecreaseSize()
    {
        Y_VERIFY(!IsEmpty());
        --CurrentSize;
    }

    void IncreaseSize()
    {
        Y_VERIFY(!IsFull());
        ++CurrentSize;
    }

    TVector<T> Buffer;
    size_t Begin = 0;
    size_t End = 0;
    size_t CurrentSize = 0;
};

}   // namespace NCloud
