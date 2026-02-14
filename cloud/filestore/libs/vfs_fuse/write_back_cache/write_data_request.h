#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

class TWriteDataRequestManager;

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TWriteDataRequestBase: public TIntrusiveListItem<T>
{
private:
    friend class TWriteDataRequestManager;

    // Unique identifier, monotonically increasing
    const ui64 SequenceId;

    // Used internally by TWriteDataRequestManager
    TInstant Time = TInstant::Zero();

public:
    ui64 GetSequenceId() const
    {
        return SequenceId;
    }

protected:
    explicit TWriteDataRequestBase(ui64 sequenceId, TInstant time)
        : SequenceId(sequenceId)
        , Time(time)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TPendingWriteDataRequest
    : public TWriteDataRequestBase<TPendingWriteDataRequest>
{
private:
    std::shared_ptr<NProto::TWriteDataRequest> Request;

    NThreading::TPromise<NProto::TWriteDataResponse> Promise =
        NThreading::NewPromise<NProto::TWriteDataResponse>();

public:
    TPendingWriteDataRequest(
        ui64 sequenceId,
        TInstant time,
        std::shared_ptr<NProto::TWriteDataRequest> request)
        : TWriteDataRequestBase(sequenceId, time)
        , Request(std::move(request))
    {}

    const NProto::TWriteDataRequest& GetRequest() const
    {
        return *Request;
    }

    NThreading::TPromise<NProto::TWriteDataResponse>* MutablePromise()
    {
        return &Promise;
    }
};

////////////////////////////////////////////////////////////////////////////////

// It is not guaranteed that the allocation is properly aligned
// Y_PACKED is used to generate instructions for unaligned access
struct Y_PACKED TSerializedWriteDataRequest
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
                sizeof(TSerializedWriteDataRequest),
            byteCount};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCachedWriteDataRequest
    : public TWriteDataRequestBase<TCachedWriteDataRequest>
{
private:
    // ByteCount is not serialized to the persistent storage as it is calculated
    // implicitly from the allocation size
    const ui64 ByteCount;

    // WriteData request serialized to the persistent storage
    const TSerializedWriteDataRequest* SerializedData;

public:
    TCachedWriteDataRequest(
        ui64 sequenceId,
        TInstant time,
        ui64 byteCount,
        const TSerializedWriteDataRequest* serializedData)
        : TWriteDataRequestBase(sequenceId, time)
        , ByteCount(byteCount)
        , SerializedData(serializedData)
    {}

    const void* GetAllocationPtr() const
    {
        return SerializedData;
    }

    ui64 GetNodeId() const
    {
        return SerializedData->NodeId;
    }

    ui64 GetHandle() const
    {
        return SerializedData->Handle;
    }

    ui64 GetOffset() const
    {
        return SerializedData->Offset;
    }

    ui64 GetByteCount() const
    {
        return ByteCount;
    }

    ui64 GetEnd() const
    {
        return SerializedData->Offset + ByteCount;
    }

    TStringBuf GetBuffer() const
    {
        return SerializedData->GetBuffer(ByteCount);
    }

    TStringBuf GetBuffer(ui64 offset, ui64 byteCount) const
    {
        return SerializedData->GetBuffer(ByteCount).SubStr(
            offset - SerializedData->Offset,
            byteCount);
    }
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
