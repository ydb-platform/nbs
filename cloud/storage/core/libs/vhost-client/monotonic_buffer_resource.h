#pragma once

#include <memory>
#include <span>

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

// TODO: replace with std::pmr::monotonic_buffer_resource +
// std::pmr::polymorphic_allocator
struct TMonotonicBufferResource
{
private:
    char* Ptr = nullptr;
    size_t Capacity = 0;
    size_t Size = 0;

public:
    TMonotonicBufferResource() = default;

    explicit TMonotonicBufferResource(std::span<char> buf);
    explicit TMonotonicBufferResource(void* addr, size_t size);

    TMonotonicBufferResource(const TMonotonicBufferResource& other) = delete;
    TMonotonicBufferResource(TMonotonicBufferResource&& other);

    TMonotonicBufferResource& operator=(
        const TMonotonicBufferResource& other) = delete;

    TMonotonicBufferResource& operator=(TMonotonicBufferResource&& other);

    std::span<char> Allocate(size_t bytes, size_t alignment = 1);

    // Releases all allocated memory
    void Release();
};

}   // namespace NVHost
