#include "monotonic_buffer_resource.h"

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

TMonotonicBufferResource::TMonotonicBufferResource(std::span<char> buf)
    : TMonotonicBufferResource{buf.data(), buf.size()}
{}

TMonotonicBufferResource::TMonotonicBufferResource(void* addr, size_t size)
    : Ptr{static_cast<char*>(addr)}
    , Capacity(size)
    , Size(size)
{}

TMonotonicBufferResource::TMonotonicBufferResource(
    TMonotonicBufferResource&& other)
    : Ptr{std::exchange(other.Ptr, nullptr)}
    , Capacity{std::exchange(other.Capacity, 0)}
    , Size{std::exchange(other.Size, 0)}
{}

TMonotonicBufferResource& TMonotonicBufferResource::operator=(
    TMonotonicBufferResource&& other)
{
    std::swap(Ptr, other.Ptr);
    std::swap(Capacity, other.Capacity);
    std::swap(Size, other.Size);

    return *this;
}

std::span<char> TMonotonicBufferResource::Allocate(
    size_t bytes,
    size_t alignment)
{
    const size_t offset = Capacity - Size;
    void* ptr = Ptr + offset;

    if (std::align(alignment, bytes, ptr, Size)) {
        Size -= bytes;

        return {static_cast<char*>(ptr), bytes};
    }

    return {};
}

void TMonotonicBufferResource::Release()
{
    Size = Capacity;
}

}   // namespace NVHost
