#pragma once

#include <mutex>
#include <queue>
#include <span>

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

class TChunkedAllocator
{
private:
    const std::span<char> Memory;
    const size_t ChunkSize = 0;

    std::mutex Mutex;
    std::queue<size_t> FreeChunks;

public:
    explicit TChunkedAllocator(std::span<char> memory, size_t chunkSize = 8192);

    size_t GetChunkSize() const;

    std::span<char> Allocate(size_t minSize);
    void Deallocate(std::span<char> chunk);
};

}   // namespace NVHost
