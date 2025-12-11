#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/memory/alloc.h>

#include <array>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TProfilingAllocator final: public IAllocator
{
private:
    IAllocator* Allocator;
    TAtomic BytesAllocated = 0;

public:
    TProfilingAllocator(IAllocator* allocator = TDefaultAllocator::Instance())
        : Allocator(allocator)
    {}

    size_t GetBytesAllocated() const
    {
        return AtomicGet(BytesAllocated);
    }

    TBlock Allocate(size_t len) override
    {
        auto block = Allocator->Allocate(len);
        AtomicAdd(BytesAllocated, block.Len);
        return block;
    }

    void Release(const TBlock& block) override
    {
        Allocator->Release(block);
        AtomicSub(BytesAllocated, block.Len);
    }

    static TProfilingAllocator* Instance();
};

template <typename AllocatorTag>
class TProfilingAllocatorRegistry
{
    static_assert(static_cast<size_t>(AllocatorTag::Max) <= 1024);

    using TAllocators =
        std::array<TProfilingAllocator, static_cast<size_t>(AllocatorTag::Max)>;

private:
    mutable TAllocators Allocators;

public:
    TProfilingAllocator* GetAllocator(AllocatorTag tag) const
    {
        return &Allocators[static_cast<ui32>(tag)];
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename TAllocator>
class TStlAllocatorBase
{
public:
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using value_type = T;

    template <typename T1>
    struct rebind
    {
        using other = TStlAllocatorBase<T1, TAllocator>;
    };

    TStlAllocatorBase(TAllocator* allocator)
        : Allocator(allocator)
    {}

    template <typename TOther>
    TStlAllocatorBase(const TStlAllocatorBase<TOther, TAllocator>& other)
        : Allocator(other.GetAllocator())
    {}

    TAllocator* GetAllocator() const
    {
        return Allocator;
    }

    size_t max_size() const noexcept
    {
        return size_t(~0) / sizeof(T);
    }

    T* address(T& x) const noexcept
    {
        return std::addressof(x);
    }

    const T* address(const T& x) const noexcept
    {
        return std::addressof(x);
    }

    T* allocate(size_t n, const void* hint = nullptr)
    {
        Y_UNUSED(hint);
        auto block = Allocator->Allocate(n * sizeof(T));
        return (T*)block.Data;
    }

    void deallocate(T* p, size_t n)
    {
        IAllocator::TBlock block = {p, n * sizeof(T)};
        Allocator->Release(block);
    }

    template <typename T1, typename... Args>
    void construct(T1* p, Args&&... args)
    {
        ::new ((void*)p) T1(std::forward<Args>(args)...);
    }

    template <typename T1>
    void destroy(T1* p)
    {
        p->~T1();
    }

    friend bool operator==(
        const TStlAllocatorBase& l,
        const TStlAllocatorBase& r)
    {
        return l.Allocator == r.Allocator;
    }

    friend bool operator!=(
        const TStlAllocatorBase& l,
        const TStlAllocatorBase& r)
    {
        return !(l == r);
    }

private:
    TAllocator* Allocator;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
using TStlAlloc = TStlAllocatorBase<T, IAllocator>;

template <typename T>
bool operator==(const TStlAlloc<T>&, const TStlAlloc<T>&) noexcept
{
    return true;
}

template <typename T>
bool operator!=(const TStlAlloc<T>&, const TStlAlloc<T>&) noexcept
{
    return false;
}

// Any type since it is supposed to be rebound anyway.
using TStlAllocator = TStlAlloc<int>;

}   // namespace NCloud
