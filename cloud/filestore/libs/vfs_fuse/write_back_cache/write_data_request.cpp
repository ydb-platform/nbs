#include "write_data_request.h"

#include <cloud/filestore/libs/service/request.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/stream/mem.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

TPendingWriteDataRequest::TPendingWriteDataRequest(
    ui64 sequenceId,
    TInstant time,
    std::shared_ptr<NProto::TWriteDataRequest> request)
    : TWriteDataRequestBase(sequenceId, time)
    , Request(std::move(request))
{
    const ui64 byteCount = NCloud::NFileStore::CalculateByteCount(*Request) -
                           Request->GetBufferOffset();

    // WriteData request should have been previously validated by
    // TUtils::ValidateWriteDataRequest - any serialization failure is a result
    // of invariant violation and is fatal
    Y_ABORT_UNLESS(
        byteCount > 0,
        "WriteData request payload must not be empty");

    AllocationByteCount = sizeof(TSerializedWriteDataRequestHeader) + byteCount;
}

void TPendingWriteDataRequest::SerializeToAllocation()
{
    Y_ABORT_UNLESS(
        AllocationPtr,
        "TPendingWriteDataRequest::SerializeToAllocation was called for a "
        "request with an empty AllocationPtr");

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

    Y_ABORT_UNLESS(
        memoryOutput.Exhausted(),
        "Buffer is expected to be written completely");

    Checksum = Crc32c(AllocationPtr, AllocationByteCount);
    Serialized.store(true, std::memory_order_release);
}

std::unique_ptr<TCachedWriteDataRequest>
TCachedWriteDataRequest::CreateFromAllocation(
    ui64 sequenceId,
    TInstant time,
    TStringBuf allocation)
{
    if (allocation.size() <= sizeof(TSerializedWriteDataRequestHeader)) {
        return nullptr;
    }

    auto data = TStringBuf(
        allocation.SubStr(sizeof(TSerializedWriteDataRequestHeader)));

    return std::make_unique<TCachedWriteDataRequest>(
        sequenceId,
        time,
        allocation.data(),
        data);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
