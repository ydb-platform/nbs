#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

class TWriteDataRequestManager;
struct THandleStateTag;

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

enum class EPendingWriteDataRequestStatus
{
    // The request has no allocation and is waiting for storage space.
    Pending,

    // The request has an uncommitted allocation and is waiting for
    // serialization to start.
    Allocated,

    // Serialization into the uncommitted allocation is in progress. The
    // allocation must remain valid until serialization finishes.
    Serializing,

    // Serialization is complete. The allocation is still uncommitted and is
    // waiting to be committed in sequence order.
    Serialized,

    // Removal was requested while serialization was in progress. Allocation
    // cancellation is deferred until serialization finishes.
    CancelSerializing,

    // The request was removed from the manager and its allocation, if any, was
    // released. It must no longer be used.
    Removed
};

////////////////////////////////////////////////////////////////////////////////

class TPendingWriteDataRequest
    : public TWriteDataRequestBase<TPendingWriteDataRequest>
    , public TIntrusiveListItem<TPendingWriteDataRequest, THandleStateTag>
{
private:
    friend class TWriteDataRequestManager;

    std::shared_ptr<NProto::TWriteDataRequest> Request;

    NThreading::TPromise<NProto::TWriteDataResponse> Promise =
        NThreading::NewPromise<NProto::TWriteDataResponse>();

    char* AllocationPtr = nullptr;
    size_t AllocationByteCount = 0;
    ui32 Checksum = 0;

    EPendingWriteDataRequestStatus Status =
        EPendingWriteDataRequestStatus::Pending;

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

    NThreading::TPromise<NProto::TWriteDataResponse>& AccessPromise()
    {
        return Promise;
    }

    void SerializeToAllocation();
};

using TPendingWriteDataRequestPtr = std::unique_ptr<TPendingWriteDataRequest>;

////////////////////////////////////////////////////////////////////////////////

// It is not guaranteed that the allocation is properly aligned
// Y_PACKED is used to generate instructions for unaligned access
struct Y_PACKED TSerializedWriteDataRequestHeader
{
    ui64 NodeId = 0;
    ui64 Handle = 0;
    ui64 Offset = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TCachedWriteDataRequest
    : public TWriteDataRequestBase<TCachedWriteDataRequest>
    , public TIntrusiveListItem<TCachedWriteDataRequest, THandleStateTag>
{
private:
    // WriteData request header serialized to the persistent storage
    const TSerializedWriteDataRequestHeader* SerializedHeader;

    // WriteData request body referenced in the persistent storage
    const TStringBuf SerializedData;

public:
    TCachedWriteDataRequest(
        ui64 sequenceId,
        TInstant time,
        const void* allocationPtr,
        TStringBuf serializedData)
        : TWriteDataRequestBase(sequenceId, time)
        , SerializedHeader(
              reinterpret_cast<const TSerializedWriteDataRequestHeader*>(
                  allocationPtr))
        , SerializedData(serializedData)
    {}

    const void* GetAllocationPtr() const
    {
        return SerializedHeader;
    }

    ui64 GetNodeId() const
    {
        return SerializedHeader->NodeId;
    }

    ui64 GetHandle() const
    {
        return SerializedHeader->Handle;
    }

    ui64 GetOffset() const
    {
        return SerializedHeader->Offset;
    }

    ui64 GetByteCount() const
    {
        return SerializedData.size();
    }

    ui64 GetEnd() const
    {
        return SerializedHeader->Offset + SerializedData.size();
    }

    TStringBuf GetBuffer() const
    {
        return SerializedData;
    }

    TStringBuf GetBuffer(ui64 offset, ui64 byteCount) const
    {
        return SerializedData.SubStr(
            offset - SerializedHeader->Offset,
            byteCount);
    }
};

using TCachedWriteDataRequestPtr = std::unique_ptr<TCachedWriteDataRequest>;

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
