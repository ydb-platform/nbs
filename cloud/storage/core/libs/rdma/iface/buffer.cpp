#include "buffer.h"

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

void TBufferPoolConfig::Validate(TLog& log)
{
    if (MaxChunkAlloc > ChunkSize) {
        RDMA_WARN(
            log,
            "MaxChunkAlloc=" << MaxChunkAlloc
                             << " is greater than ChunkSize=" << ChunkSize
                             << ", set MaxChunkAlloc=" << ChunkSize);

        MaxChunkAlloc = ChunkSize;
    }
}

}   // namespace NCloud::NStorage::NRdma
