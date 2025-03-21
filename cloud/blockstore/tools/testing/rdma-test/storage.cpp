#include "storage.h"

#include <cloud/blockstore/libs/rdma/impl/page_size.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TStorageBuffer IStorage::AllocateBuffer(size_t len)
{
    void* p = aligned_alloc(NRdma::TPageSize::Value, len);
    return { static_cast<char*>(p), free };
}

////////////////////////////////////////////////////////////////////////////////

ui64 CreateRequestId()
{
    for (;;) {
        auto requestId = RandomNumber<ui64>();
        if (requestId) {
            return requestId;
        }
    }
}

size_t CopyMemory(const TSgList& dst, TStringBuf src)
{
    const char* ptr = src.data();
    size_t bytesLeft = src.length();
    size_t bytesCopied = 0;

    for (auto buffer: dst) {
        size_t len = Min(bytesLeft, buffer.Size());
        if (len) {
            memcpy((char*)buffer.Data(), ptr, len);
            ptr += len;
            bytesLeft -= len;
            bytesCopied += len;
        }
    }

    return bytesCopied;
}

size_t CopyMemory(TStringBuf dst, const TSgList& src)
{
    char* ptr = (char*)dst.data();
    size_t bytesLeft = dst.length();
    size_t bytesCopied = 0;

    for (auto buffer: src) {
        size_t len = Min(bytesLeft, buffer.Size());
        if (len) {
            memcpy(ptr, buffer.Data(), len);
            ptr += len;
            bytesLeft -= len;
            bytesCopied += len;
        }
    }

    return bytesCopied;
}

}   // namespace NCloud::NBlockStore
