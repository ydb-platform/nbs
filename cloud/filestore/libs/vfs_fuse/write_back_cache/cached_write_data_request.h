#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <util/datetime/base.h>
#include <util/generic/intrlist.h>
#include <util/generic/strbuf.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

struct IPersistentStorage;
class TCachedWriteDataRequestSerializer;

////////////////////////////////////////////////////////////////////////////////

struct TCachedDataPart
{
    ui64 RelativeOffset = 0;
    TStringBuf Data;
};

////////////////////////////////////////////////////////////////////////////////

class TCachedWriteDataRequest
    : public TIntrusiveListItem<TCachedWriteDataRequest>
{
private:
    struct TData;
    friend class TCachedWriteDataRequestSerializer;

    // Unique identifier for the entry, monotonically increasing
    const ui64 SequenceId;

    // ByteCount is not serialized to the persistent queue as it is calculated
    // implicitly
    const ui64 ByteCount;

    // WriteData request stored in the persistent storage
    const TData* Data;

    TInstant Time;

    TCachedWriteDataRequest(
        ui64 sequenceId,
        TInstant time,
        ui64 byteCount,
        const TData* data);

public:
    const void* GetAllocationPtr() const;
    ui64 GetSequenceId() const;
    ui64 GetNodeId() const;
    ui64 GetHandle() const;
    ui64 GetOffset() const;
    ui64 GetByteCount() const;
    TStringBuf GetCachedData() const;
    TStringBuf GetCachedData(ui64 offset, ui64 byteCount) const;
    TInstant GetTime() const;
    void SetTime(TInstant time);
};

////////////////////////////////////////////////////////////////////////////////

class TCachedWriteDataRequestSerializer
{
public:
    static std::unique_ptr<TCachedWriteDataRequest> TrySerialize(
        ui64 sequenceId,
        TInstant now,
        const NProto::TWriteDataRequest& request,
        IPersistentStorage& storage);

    static std::unique_ptr<TCachedWriteDataRequest>
    TryDeserialize(ui64 sequenceId, TInstant now, TStringBuf allocation);

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
    TInstant time,
    ui64 byteCount,
    const TData* data)
    : SequenceId(sequenceId)
    , ByteCount(byteCount)
    , Data(data)
    , Time(time)
{}

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

inline TInstant TCachedWriteDataRequest::GetTime() const
{
    return Time;
}

inline void TCachedWriteDataRequest::SetTime(TInstant time)
{
    Time = time;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
