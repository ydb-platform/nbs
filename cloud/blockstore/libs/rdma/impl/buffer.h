#pragma once

#include "public.h"

#include "verbs.h"

#include <cloud/blockstore/libs/rdma/iface/protocol.h>

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
    NVerbs::IVerbsPtr Verbs;
    std::unique_ptr<TImpl> Impl;

public:
    TBufferPool();
    ~TBufferPool();

    void Init(NVerbs::IVerbsPtr verbs, ibv_pd* pd, int flags);

    struct TBuffer: TBufferDesc
    {
        TChunk* Chunk;

        explicit operator TStringBuf() const;
    };

    TBuffer AcquireBuffer(size_t bytesCount, bool ignoreCache = false);
    void ReleaseBuffer(TBuffer& buffer);

    const TBufferPoolStats& GetStats() const;
    bool Initialized() const;
};

using TPooledBuffer = TBufferPool::TBuffer;

}   // namespace NCloud::NBlockStore::NRdma
