#include "cached_write_data_request.h"

#include "persistent_storage.h"

#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/public/api/protos/data.pb.h>

#include <span>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBufferWriter
{
    std::span<char> TargetBuffer;

    explicit TBufferWriter(std::span<char> targetBuffer)
        : TargetBuffer(targetBuffer)
    {}

    void Write(TStringBuf buffer)
    {
        Y_ABORT_UNLESS(
            buffer.size() <= TargetBuffer.size(),
            "Not enough space in the buffer to write %lu bytes, remaining: %lu",
            buffer.size(),
            TargetBuffer.size());

        buffer.copy(TargetBuffer.data(), buffer.size());
        TargetBuffer = TargetBuffer.subspan(buffer.size());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TCachedWriteDataRequest>
TCachedWriteDataRequestSerializer::TrySerialize(
    ui64 sequenceId,
    const NProto::TWriteDataRequest& request,
    IPersistentStorage& storage)
{
    const ui64 byteCount =
        NStorage::CalculateByteCount(request) - request.GetBufferOffset();

    const ui64 allocationSize =
        sizeof(TCachedWriteDataRequest::TData) + byteCount;

    const void* allocationPtr = storage.Alloc(
        [&request](char* ptr, size_t size) { Serialize(request, ptr, size); },
        allocationSize);

    if (!allocationPtr) {
        return nullptr;
    }

    return std::unique_ptr<TCachedWriteDataRequest>(new TCachedWriteDataRequest(
        sequenceId,
        byteCount,
        reinterpret_cast<const TCachedWriteDataRequest::TData*>(
            allocationPtr)));
}

std::unique_ptr<TCachedWriteDataRequest>
TCachedWriteDataRequestSerializer::TryDeserialize(
    ui64 sequenceId,
    TStringBuf allocation)
{
    if (allocation.size() <= sizeof(TCachedWriteDataRequest::TData)) {
        return nullptr;
    }

    const ui64 byteCount =
        allocation.size() - sizeof(TCachedWriteDataRequest::TData);

    return std::unique_ptr<TCachedWriteDataRequest>(new TCachedWriteDataRequest(
        sequenceId,
        byteCount,
        reinterpret_cast<const TCachedWriteDataRequest::TData*>(
            allocation.data())));
}

void TCachedWriteDataRequestSerializer::Serialize(
    const NProto::TWriteDataRequest& request,
    char* allocationPtr,
    size_t size)
{
    Y_ABORT_UNLESS(sizeof(TCachedWriteDataRequest::TData) <= size);

    auto* cachedRequest =
        reinterpret_cast<TCachedWriteDataRequest::TData*>(allocationPtr);

    cachedRequest->NodeId = request.GetNodeId();
    cachedRequest->Handle = request.GetHandle();
    cachedRequest->Offset = request.GetOffset();

    TBufferWriter writer(
        {allocationPtr + sizeof(TCachedWriteDataRequest::TData),
         size - sizeof(TCachedWriteDataRequest::TData)});

    if (request.GetIovecs().empty()) {
        writer.Write(
            TStringBuf(request.GetBuffer()).Skip(request.GetBufferOffset()));
    } else {
        for (const auto& iovec: request.GetIovecs()) {
            writer.Write(TStringBuf(
                reinterpret_cast<const char*>(iovec.GetBase()),
                iovec.GetLength()));
        }
    }

    Y_ABORT_UNLESS(
        writer.TargetBuffer.empty(),
        "Buffer is expected to be written completely");
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
