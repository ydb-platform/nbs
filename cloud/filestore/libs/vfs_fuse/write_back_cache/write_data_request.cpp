#include "write_data_request.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
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

    AllocationByteCount = sizeof(TSerializedWriteDataRequestHeader) + byteCount;
}

bool TPendingWriteDataRequest::SerializeToAllocation()
{
    try {
        if (!AllocationPtr) {
            ReportWriteBackCacheRequestSerializationError(
                "TPendingWriteDataRequest::SerializeToAllocation was called "
                "for a request with an empty AllocationPtr");
            return false;
        }

        TMemoryOutput memoryOutput(AllocationPtr, AllocationByteCount);

        TSerializedWriteDataRequestHeader header{
            .NodeId = Request->GetNodeId(),
            .Handle = Request->GetHandle(),
            .Offset = Request->GetOffset()};

        memoryOutput.Write(&header, sizeof(header));

        if (Request->GetIovecs().empty()) {
            memoryOutput.Write(TStringBuf(Request->GetBuffer())
                                   .Skip(Request->GetBufferOffset()));
        } else {
            for (const auto& iovec: Request->GetIovecs()) {
                memoryOutput.Write(TStringBuf(
                    reinterpret_cast<const char*>(iovec.GetBase()),
                    iovec.GetLength()));
            }
        }

        if (!memoryOutput.Exhausted()) {
            ReportWriteBackCacheRequestSerializationError(
                "TPendingWriteDataRequest::SerializeToAllocation failed: "
                "buffer is not exhausted after writing the payload");
            return false;
        }

        Checksum = Crc32c(AllocationPtr, AllocationByteCount);
        return true;

    } catch (...) {
        ReportWriteBackCacheRequestSerializationError(
            "TPendingWriteDataRequest::SerializeToAllocation failed: " +
            CurrentExceptionMessage());
        return false;
    }
}

void TPendingWriteDataRequest::SetSerialized()
{
    Serialized = true;
}

std::unique_ptr<TCachedWriteDataRequest> TCachedWriteDataRequest::Deserialize(
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
