#include "buffer.h"

#include <cloud/blockstore/libs/common/page_size.h>
#include <util/generic/intrlist.h>
#include <util/system/align.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t PAGE_SIZE = 4*1024;
constexpr size_t CHUNK_SIZE = PAGE_SIZE * 1024;
constexpr size_t MAX_CHUNK_ALLOC = CHUNK_SIZE / 4;
constexpr size_t MAX_FREE_CHUNKS = 10;

////////////////////////////////////////////////////////////////////////////////

TPooledBuffer::operator TStringBuf() const
{
    return {reinterpret_cast<char*>(Address), Length};
}

////////////////////////////////////////////////////////////////////////////////

class TBufferPool::TChunk
    : public TIntrusiveListItem<TChunk>
{
private:
    NVerbs::TMemoryRegionPtr MemoryRegion;

    void* Address;
    ui32 Length;
    ui32 Key;

    bool Custom = false;

    size_t AllocatedBytes = 0;
    size_t FreedBytes = 0;

public:
    TChunk(NVerbs::TMemoryRegionPtr mr, bool custom)
        : MemoryRegion(std::move(mr))
        , Address(MemoryRegion->addr)
        , Length(MemoryRegion->length)
        , Key(MemoryRegion->lkey)
        , Custom(custom)
    {}

    // just for tests
    TChunk(void* addr, ui32 length, ui32 key, bool custom)
        : MemoryRegion(NVerbs::NullPtr)
        , Address(addr)
        , Length(length)
        , Key(key)
        , Custom(custom)
    {}

    ~TChunk()
    {
        free(Address);
    }

    bool IsCustom() const
    {
        return Custom;
    }

    bool IsFree() const
    {
        return AllocatedBytes == FreedBytes;
    }

    bool CanAllocate(size_t allocSize) const
    {
        return allocSize <= Length - AllocatedBytes;
    }

    TBuffer AcquireBuffer(size_t allocSize)
    {
        TBuffer buffer;
        buffer.Chunk = this;

        buffer.Key = Key;
        buffer.Address = reinterpret_cast<uintptr_t>(Address) + AllocatedBytes;
        buffer.Length = allocSize;

        AllocatedBytes += allocSize;
        Y_ABORT_UNLESS(AllocatedBytes <= Length);

        return buffer;
    }

    void ReleaseBuffer(TBuffer& buffer)
    {
        FreedBytes += buffer.Length;
        Zero(buffer);
    }

    void Reset()
    {
        AllocatedBytes = 0;
        FreedBytes = 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBufferPool::TImpl
{
    using TChunkList = TIntrusiveListWithAutoDelete<TChunk, TDelete>;

private:
    NVerbs::IVerbsPtr Verbs;

    ibv_pd* const ProtectionDomain;
    const int Flags;

    TChunkList ActiveChunks;
    TChunkList CustomChunks;
    TChunkList FreeChunks;

    TBufferPoolStats Stats;

public:
    TImpl(NVerbs::IVerbsPtr verbs, ibv_pd* pd, int flags)
        : Verbs(std::move(verbs))
        , ProtectionDomain(pd)
        , Flags(flags)
    {}

    TBuffer AcquireBuffer(size_t bytesCount, bool ignoreCache)
    {
        size_t allocSize = AlignUp(bytesCount, GetPlatformPageSize());

        TChunk* chunk;
        if (!ignoreCache && allocSize <= MAX_CHUNK_ALLOC) {
            chunk = AcquireChunk(allocSize);
        } else {
            // allocate custom chunk
            chunk = AllocateChunk(allocSize, true);

            CustomChunks.PushFront(chunk);
            ++Stats.CustomChunksCount;
        }

        return chunk->AcquireBuffer(allocSize);
    }

    void ReleaseBuffer(TBuffer& buffer)
    {
        if (!buffer.Chunk) {
            return;
        }

        auto* chunk = buffer.Chunk;
        chunk->ReleaseBuffer(buffer);

        if (!chunk->IsCustom()) {
            if (chunk->IsFree() && chunk != ActiveChunks.Front()) {
                ReleaseChunk(chunk);
            }
        } else {
            // free custom chunk
            CustomChunks.Remove(chunk);
            --Stats.CustomChunksCount;

            FreeChunk(chunk);
        }
    }

    const TBufferPoolStats& GetStats() const
    {
        return Stats;
    }

private:
    TChunk* AcquireChunk(size_t allocSize)
    {
        TChunk* chunk;

        if (ActiveChunks) {
            // check current chunk
            chunk = ActiveChunks.Front();
            if (chunk->CanAllocate(allocSize)) {
                return chunk;
            }

            if (chunk->IsFree()) {
                ReleaseChunk(chunk);
            }
        }

        if (FreeChunks) {
            // reuse cached chunk
            chunk = FreeChunks.Front();

            FreeChunks.PopFront();
            --Stats.FreeChunksCount;
        } else {
            // allocate new chunk
            chunk = AllocateChunk(CHUNK_SIZE, false);
        }

        ActiveChunks.PushFront(chunk);
        ++Stats.ActiveChunksCount;

        return chunk;
    }

    void ReleaseChunk(TChunk* chunk)
    {
        ActiveChunks.Remove(chunk);
        --Stats.ActiveChunksCount;

        if (Stats.FreeChunksCount < MAX_FREE_CHUNKS) {
            // keep chunk cached
            chunk->Reset();

            FreeChunks.PushFront(chunk);
            ++Stats.FreeChunksCount;
        } else {
            FreeChunk(chunk);
        }
    }

    TChunk* AllocateChunk(size_t chunkSize, bool custom)
    {
        void* addr = malloc(chunkSize);
        Y_ABORT_UNLESS(addr);

        if (ProtectionDomain) {
            auto mr = Verbs->RegisterMemoryRegion(
                ProtectionDomain,
                addr,
                chunkSize,
                Flags);
            return new TChunk(std::move(mr), custom);
        } else {
            return new TChunk(addr, chunkSize, 0, custom);
        }
    }

    void FreeChunk(TChunk* chunk)
    {
        delete chunk;
    }
};

////////////////////////////////////////////////////////////////////////////////

TBufferPool::TBufferPool()
{}

TBufferPool::~TBufferPool()
{}

void TBufferPool::Init(NVerbs::IVerbsPtr verbs, ibv_pd* pd, int flags)
{
    Impl.reset(new TImpl(std::move(verbs), pd, flags));
}

TBufferPool::TBuffer TBufferPool::AcquireBuffer(size_t bytesCount, bool ignoreCache)
{
    return Impl->AcquireBuffer(bytesCount, ignoreCache);
}

void TBufferPool::ReleaseBuffer(TBuffer& buffer)
{
    return Impl->ReleaseBuffer(buffer);
}

const TBufferPoolStats& TBufferPool::GetStats() const
{
    return Impl->GetStats();
}

bool TBufferPool::Initialized() const
{
    return Impl != nullptr;
}

}   // namespace NCloud::NBlockStore::NRdma
