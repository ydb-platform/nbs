#pragma once

#include "public.h"

#include "verbs.h"

#include <cloud/storage/core/libs/rdma/iface/buffer.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

struct TBufferPoolStats
{
    size_t ActiveChunksCount = 0;
    size_t CustomChunksCount = 0;
    size_t FreeChunksCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBufferPool
{
    class TImpl;
    class TChunk;

private:
    TBufferPoolConfig Config;
    std::unique_ptr<TImpl> Impl;

public:
    explicit TBufferPool(TBufferPoolConfig config);
    ~TBufferPool();

    void Init(NVerbs::IVerbsPtr verbs, ibv_pd* pd, int flags);

    struct TBuffer : TBufferDesc
    {
        TChunk* Chunk;
        ui32 LKey;

        explicit operator TStringBuf() const;
    };

    TBuffer AcquireBuffer(size_t bytesCount, bool ignoreCache = false);
    void ReleaseBuffer(TBuffer& buffer);

    const TBufferPoolStats& GetStats() const;
    bool Initialized() const;
};

using TPooledBuffer = TBufferPool::TBuffer;

}   // namespace NCloud::NBlockStore::NRdma
