#pragma once

#include "log.h"

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

struct TBufferPoolConfig
{
    size_t ChunkSize = 4_MB;
    size_t MaxChunkAlloc = ChunkSize / 4;
    size_t MaxFreeChunks = 10;

    void Validate(TLog& log);
};

}   // namespace NCloud::NBlockStore::NRdma
