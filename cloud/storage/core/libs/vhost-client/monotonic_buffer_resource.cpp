#include "monotonic_buffer_resource.h"

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

TMonotonicBufferResource::TMonotonicBufferResource(std::span<char> buf)
    : TMonotonicBufferResource {buf.data(), buf.size()}
{}

TMonotonicBufferResource::TMonotonicBufferResource(void* addr, size_t size)
    : Ptr {static_cast<char*>(addr)}
    , Capacity(size)
    , Size(size)
{}

TMonotonicBufferResource::TMonotonicBufferResource(TMonotonicBufferResource&& other)
    : Ptr {std::exchange(other.Ptr, nullptr)}
    , Capacity {std::exchange(other.Capacity, 0)}
    , Size {std::exchange(other.Size, 0)}
{}

TMonotonicBufferResource& TMonotonicBufferResource::operator = (
    TMonotonicBufferResource&& other)
{
    std::swap(Ptr, other.Ptr);
    std::swap(Capacity, other.Capacity);
    std::swap(Size, other.Size);

    return *this;
}

std::span<char> TMonotonicBufferResource::Allocate(size_t bytes, size_t alignment)
{
    const size_t offset = Capacity - Size;
    void* ptr = Ptr + offset;

    const bool isAligned512 = reinterpret_cast<std::uintptr_t>(ptr) % 512 == 0;
    if (alignment == 1 && isAligned512) {
        // If we got alignment to 512 byte boundary, but it was not required,
        // then we allocate an unaligned block.
        ++Size;
        ptr = Ptr + offset + 1;
    }

    if (std::align(alignment, bytes, ptr, Size)) {
        Size -= bytes;

        return { static_cast<char*>(ptr), bytes };
    }

    return {};
}

void TMonotonicBufferResource::Release()
{
    Size = Capacity;
}

}   // namespace NVHost
