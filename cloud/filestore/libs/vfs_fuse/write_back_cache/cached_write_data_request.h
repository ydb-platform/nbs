#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <util/generic/intrlist.h>
#include <util/generic/strbuf.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TCachedDataPart
{
    ui64 Offset = 0;
    TStringBuf Data;
};

////////////////////////////////////////////////////////////////////////////////

struct IPersistentStorage;
class TCachedWriteDataRequestSerializer;

class TCachedWriteDataRequest
    : public TIntrusiveListItem<TCachedWriteDataRequest>
{
private:
    struct TData;
    friend class TCachedWriteDataRequestSerializer;

    // Unique identifier for the entry, monotonically increasing
    const ui64 SequenceId = 0;

    // ByteCount is not serialized to the persistent queue as it is calculated
    // implicitly
    const ui64 ByteCount = 0;

    // WriteData request stored in the persistent storage
    const TData* Data;

    TCachedWriteDataRequest(ui64 sequenceId, ui64 byteCount, const TData* data);

public:
    ~TCachedWriteDataRequest();

    const void* GetAllocationPtr() const;
    ui64 GetSequenceId() const;
    ui64 GetNodeId() const;
    ui64 GetHandle() const;
    ui64 GetOffset() const;
    ui64 GetByteCount() const;
    TStringBuf GetCachedData() const;
    TStringBuf GetCachedData(ui64 offset, ui64 byteCount) const;
};

////////////////////////////////////////////////////////////////////////////////

class TCachedWriteDataRequestSerializer
{
public:
    static std::unique_ptr<TCachedWriteDataRequest> TrySerialize(
        ui64 sequenceId,
        const NProto::TWriteDataRequest& request,
        IPersistentStorage& storage);

    static std::unique_ptr<TCachedWriteDataRequest> TryDeserialize(
        ui64 sequenceId,
        TStringBuf allocation);

private:
    static void Serialize(
        const NProto::TWriteDataRequest& request,
        char* allocationPtr,
        size_t size);
};

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TCachedWriteDataRequest::TData
{
    ui64 NodeId = 0;
    ui64 Handle = 0;
    ui64 Offset = 0;

    // Data goes right after the header, |byteCount| bytes
    // The validity is ensured by code logic
    TStringBuf GetBuffer(ui64 byteCount) const
    {
        return {
            reinterpret_cast<const char*>(this) +
                sizeof(TCachedWriteDataRequest::TData),
            byteCount};
    }
};

////////////////////////////////////////////////////////////////////////////////

inline TCachedWriteDataRequest::TCachedWriteDataRequest(
    ui64 sequenceId,
    ui64 byteCount,
    const TData* data)
    : SequenceId(sequenceId)
    , ByteCount(byteCount)
    , Data(data)
{}

inline TCachedWriteDataRequest::~TCachedWriteDataRequest()
{
    //Y_ABORT_UNLESS(TIntrusiveListItem<TCachedWriteDataRequest>::Empty());
}

inline const void* TCachedWriteDataRequest::GetAllocationPtr() const
{
    return Data;
}

inline ui64 TCachedWriteDataRequest::GetSequenceId() const
{
    return SequenceId;
}

inline ui64 TCachedWriteDataRequest::GetNodeId() const
{
    return Data->NodeId;
}

inline ui64 TCachedWriteDataRequest::GetHandle() const
{
    return Data->Handle;
}

inline ui64 TCachedWriteDataRequest::GetOffset() const
{
    return Data->Offset;
}

inline ui64 TCachedWriteDataRequest::GetByteCount() const
{
    return ByteCount;
}

inline TStringBuf TCachedWriteDataRequest::GetCachedData() const
{
     return Data->GetBuffer(ByteCount);
}

inline TStringBuf TCachedWriteDataRequest::GetCachedData(
    ui64 offset,
    ui64 byteCount) const
{
    return Data->GetBuffer(ByteCount).SubStr(offset - Data->Offset, byteCount);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
