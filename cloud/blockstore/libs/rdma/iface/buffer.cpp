#include "buffer.h"

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

void TBufferPoolConfig::Validate(TLog& log)
{
    if (MaxChunkAlloc > ChunkSize) {
        RDMA_WARN(
            log,
            "MaxChunkAlloc=" << MaxChunkAlloc
                             << " is greater than ChunkSize=" << ChunkSize
                             << ", set MaxChunkAlloc to " << ChunkSize);

        MaxChunkAlloc = ChunkSize;
    }
}

}   // namespace NCloud::NBlockStore::NRdma
