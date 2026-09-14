#include "write_data_request.h"

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/stream/mem.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

void TPendingWriteDataRequest::SerializeToAllocation()
{
    Y_ABORT_UNLESS(AllocationPtr != nullptr);

    TMemoryOutput memoryOutput(AllocationPtr, AllocationByteCount);

    TSerializedWriteDataRequestHeader header{
        .NodeId = Request->GetNodeId(),
        .Handle = Request->GetHandle(),
        .Offset = Request->GetOffset()};

    memoryOutput.Write(&header, sizeof(header));

    if (Request->GetIovecs().empty()) {
        memoryOutput.Write(
            TStringBuf(Request->GetBuffer()).Skip(Request->GetBufferOffset()));
    } else {
        for (const auto& iovec: Request->GetIovecs()) {
            memoryOutput.Write(TStringBuf(
                reinterpret_cast<const char*>(iovec.GetBase()),
                iovec.GetLength()));
        }
    }

    Y_ABORT_UNLESS(memoryOutput.Exhausted());

    Checksum = Crc32c(AllocationPtr, AllocationByteCount);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
