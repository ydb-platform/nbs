#include "chunked_allocator.h"

#include <util/system/yassert.h>

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

TChunkedAllocator::TChunkedAllocator(std::span<char> memory, size_t chunkSize)
    : Memory(memory)
    , ChunkSize(chunkSize)
{
    for (size_t i = 0; i != memory.size() / ChunkSize; ++i) {
        FreeChunks.push(i);
    }
}

size_t TChunkedAllocator::GetChunkSize() const
{
    return ChunkSize;
}

std::span<char> TChunkedAllocator::Allocate(size_t size)
{
    if (ChunkSize < size) {
        return {};
    }

    std::unique_lock lock{Mutex};

    if (FreeChunks.empty()) {
        return {};
    }

    const size_t chunkIndex = FreeChunks.front();
    FreeChunks.pop();

    return {Memory.data() + chunkIndex * ChunkSize, size};
}

void TChunkedAllocator::Deallocate(std::span<char> buf)
{
    Y_ABORT_UNLESS(Memory.data() <= buf.data() && buf.size() <= ChunkSize);

    const size_t chunkIndex = (buf.data() - Memory.data()) / ChunkSize;

    Y_ABORT_UNLESS(chunkIndex < Memory.size() / ChunkSize);

    std::unique_lock lock{Mutex};

    FreeChunks.push(chunkIndex);
}

}   // namespace NVHost
