#include "write_data_request_manager.h"

#include "cloud/filestore/libs/diagnostics/critical_events.h"
#include <cloud/filestore/libs/service/request.h>

#include <util/stream/mem.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum class ECachedWriteDataRequestTag
{
    // No specific actions should be taken
    Unflushed = 0,

    // Handle associated with the request has been released.
    // Attempts to flush the request should be made using another handle.
    UnflushedHandleReleased = 1,

    // Request has been flushed and should be evicted on restart
    Flushed = 2,

    // Used to validate deserialization
    Max = Flushed
};

////////////////////////////////////////////////////////////////////////////////

struct TLoadedWriteDataRequest
{
    ECachedWriteDataRequestTag Tag = ECachedWriteDataRequestTag::Unflushed;
    std::unique_ptr<TCachedWriteDataRequest> Request;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TCachedWriteDataRequest> DeserializeWriteDataRequest(
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TWriteDataRequestManager::TWriteDataRequestManager(
    ISequenceIdGeneratorPtr sequenceIdGenerator,
    IPersistentStoragePtr persistentStorage,
    ITimerPtr timer,
    IWriteDataRequestManagerStatsPtr stats)
    : SequenceIdGenerator(std::move(sequenceIdGenerator))
    , PersistentStorage(std::move(persistentStorage))
    , Timer(std::move(timer))
    , Stats(std::move(stats))
{}

NProto::TError TWriteDataRequestManager::Init(
    const TCachedRequestVisitor& visitor)
{
    if (PersistentStorage->IsCorrupted()) {
        return MakeError(E_INVALID_STATE, "Persistent storage is corrupted");
    }

    // File ring buffer should be able to store any valid TWriteDataRequest.
    // Inability to store it will cause this and future requests to remain
    // in the pending queue forever (including requests with smaller size).
    // Should fit 1 MiB of data plus some headers (assume 1 KiB is enough).
    const ui64 maxAllocationByteCount = 1024 * 1024 + 1016;

    const ui64 maxSupportedAllocationByteCount =
        PersistentStorage->GetMaxSupportedAllocationByteCount();

    if (maxSupportedAllocationByteCount < maxAllocationByteCount) {
        return MakeError(
            E_ARGUMENT,
            Sprintf(
                "MaxSupportedAllocationByteCount (%lu) is less than the "
                "minimal allowed value (%lu)",
                maxSupportedAllocationByteCount,
                maxAllocationByteCount));
    }

    NProto::TError error = {};

    TVector<TLoadedWriteDataRequest> loadedRequests;

    auto visitResult = PersistentStorage->Visit(
        [this, &error, &loadedRequests](ui32 tag, const TStringBuf allocation)
        {
            if (HasError(error)) {
                return;
            }

            if (tag > static_cast<ui32>(ECachedWriteDataRequestTag::Max)) {
                error = MakeError(
                    E_INVALID_STATE,
                    Sprintf(
                        "Request deserialization error: tag value %u exceeds "
                        "the maximal value %u",
                        tag,
                        static_cast<ui32>(ECachedWriteDataRequestTag::Max)));
                return;
            }

            auto request = DeserializeWriteDataRequest(
                SequenceIdGenerator->GenerateId(),
                Timer->Now(),
                allocation);

            if (!request) {
                error =
                    MakeError(E_INVALID_STATE, "Request deserialization error");
                return;
            }

            loadedRequests.push_back(
                {.Tag = static_cast<ECachedWriteDataRequestTag>(tag),
                 .Request = std::move(request)});
        });

    if (HasError(error)) {
        return error;
    }

    if (HasError(visitResult)) {
        return visitResult;
    }

    for (auto& request: loadedRequests) {
        switch (request.Tag) {
            case ECachedWriteDataRequestTag::Unflushed: {
                UnflushedRequestsPushBack(request.Request.get());
                visitor(
                    std::move(request.Request),
                    /* handleReleased = */ false);
                break;
            }
            case ECachedWriteDataRequestTag::UnflushedHandleReleased: {
                UnflushedRequestsPushBack(request.Request.get());
                visitor(
                    std::move(request.Request),
                    /* handleReleased = */ true);
                break;
            }
            case ECachedWriteDataRequestTag::Flushed: {
                // There could be pins that prevented flushed requests from
                // eviction before restart, but they are erased on restart
                // so nothing prevents flushed requests from being removed
                auto freeResult = PersistentStorage->Free(
                    request.Request->GetAllocationPtr());

                if (HasError(freeResult)) {
                    return freeResult;
                }

                break;
            }
        }
    }

    PendingRequests.Clear();

    return {};
}

bool TWriteDataRequestManager::HasPendingRequests() const
{
    return !PendingRequests.Empty() || !AllocatedRequests.Empty() ||
           !SerializingRequests.Empty();
}

bool TWriteDataRequestManager::HasPendingOrUnflushedRequests() const
{
    return HasPendingRequests() || !UnflushedRequests.Empty();
}

ui64 TWriteDataRequestManager::GetMinPendingOrUnflushedSequenceId() const
{
    if (!UnflushedRequests.Empty()) {
        return UnflushedRequests.Front()->GetSequenceId();
    }
    if (!SerializingRequests.Empty()) {
        return SerializingRequests.Front()->GetSequenceId();
    }
    if (!AllocatedRequests.Empty()) {
        return AllocatedRequests.Front()->GetSequenceId();
    }
    if (!PendingRequests.Empty()) {
        return PendingRequests.Front()->GetSequenceId();
    }
    return Max<ui64>();
}

ui64 TWriteDataRequestManager::GetMaxPendingOrUnflushedSequenceId() const
{
    if (!PendingRequests.Empty()) {
        return PendingRequests.Back()->GetSequenceId();
    }
    if (!AllocatedRequests.Empty()) {
        return AllocatedRequests.Back()->GetSequenceId();
    }
    if (!SerializingRequests.Empty()) {
        return SerializingRequests.Back()->GetSequenceId();
    }
    if (!UnflushedRequests.Empty()) {
        return UnflushedRequests.Back()->GetSequenceId();
    }
    return 0;
}

ui64 TWriteDataRequestManager::GetMaxUnflushedSequenceId() const
{
    return UnflushedRequests.Empty()
               ? 0
               : UnflushedRequests.Back()->GetSequenceId();
}

bool TWriteDataRequestManager::GetStorageIsFull() const
{
    return StorageIsFull;
}

std::unique_ptr<TPendingWriteDataRequest> TWriteDataRequestManager::AddRequest(
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    const ui64 sequenceId = SequenceIdGenerator->GenerateId();
    const auto now = Timer->Now();

    auto pendingRequest = std::make_unique<TPendingWriteDataRequest>(
        sequenceId,
        now,
        std::move(request));

    if (!PendingRequests.Empty()) {
        PendingRequestsPushBack(pendingRequest.get());
        return pendingRequest;
    }

    auto res = TryAllocRequestInPersistentStorage(pendingRequest.get());
    if (res.Failed) {
        return nullptr;
    }

    if (res.Allocated) {
        pendingRequest->Status = EPendingWriteDataRequestStatus::Allocated;
        AllocatedRequestsPushBack(pendingRequest.get());
        return pendingRequest;
    }

    PendingRequestsPushBack(pendingRequest.get());
    return pendingRequest;
}

TPendingWriteDataRequest*
TWriteDataRequestManager::GetNextPendingRequestToSerialize()
{
    if (AllocatedRequests.Empty()) {
        return nullptr;
    }

    auto* pendingRequest = AllocatedRequests.PopFront();
    SerializingRequests.PushBack(pendingRequest);

    pendingRequest->Status = EPendingWriteDataRequestStatus::Serializing;

    return pendingRequest;
}

bool TWriteDataRequestManager::SetPendingRequestSerialized(
    TPendingWriteDataRequest* pendingRequest)
{
    if (pendingRequest->Status == EPendingWriteDataRequestStatus::Serializing) {
        pendingRequest->Status = EPendingWriteDataRequestStatus::Serialized;
        return true;
    }

    if (pendingRequest->Status ==
        EPendingWriteDataRequestStatus::CancelSerializing)
    {
        auto cancelAllocResult =
            PersistentStorage->CancelAlloc(pendingRequest->AllocationPtr);

        if (HasError(cancelAllocResult)) {
            return false;
        }

        pendingRequest->Status = EPendingWriteDataRequestStatus::Removed;

        while (!CancelSerializingRequests.empty() &&
               CancelSerializingRequests.front()->Status ==
                   EPendingWriteDataRequestStatus::Removed)
        {
            CancelSerializingRequests.pop_front();
        }

        return AllocPendingRequestsInPersistentStorage();
    }

    ReportWriteBackCacheImpossibleState(
        TStringBuilder() << "SetPendingRequestSerialized has been requested "
                            "for a request in invalid status "
                         << static_cast<ui32>(pendingRequest->Status));

    return false;
}

TWriteDataRequestManager::TGetNextReadyCachedRequestResult
TWriteDataRequestManager::GetNextReadyCachedRequest()
{
    if (SerializingRequests.Empty()) {
        return {};
    }

    auto* pendingRequest = SerializingRequests.Front();
    if (pendingRequest->Status != EPendingWriteDataRequestStatus::Serialized) {
        return {};
    }

    if (NodesWithBackpressure.contains(
            pendingRequest->GetRequest().GetNodeId()))
    {
        // Known limitation: requests are committed in a single global FIFO
        // order. Although backpressure is tracked per node, requests are not
        // reordered. A front request for a backpressured node may therefore
        // block later requests for unrelated nodes. Per-node commit queues or
        // fair scheduling should be added separately.
        return {};
    }

    auto commitResult = PersistentStorage->Commit(
        pendingRequest->AllocationPtr,
        pendingRequest->Checksum);

    if (HasError(commitResult)) {
        return {.Failed = true};
    }

    auto cachedRequest = DeserializeWriteDataRequest(
        pendingRequest->GetSequenceId(),
        Timer->Now(),
        {pendingRequest->AllocationPtr, pendingRequest->AllocationByteCount});

    Y_ABORT_UNLESS(cachedRequest != nullptr);

    SerializingRequestsRemove(pendingRequest);
    UnflushedRequestsPushBack(cachedRequest.get());

    return {.Request = std::move(cachedRequest)};
}

const TPendingWriteDataRequest*
TWriteDataRequestManager::GetFrontPendingRequest() const
{
    if (!SerializingRequests.Empty()) {
        return SerializingRequests.Front();
    }
    if (!AllocatedRequests.Empty()) {
        return AllocatedRequests.Front();
    }
    if (!PendingRequests.Empty()) {
        return PendingRequests.Front();
    }
    return nullptr;
}

bool TWriteDataRequestManager::Remove(TPendingWriteDataRequestPtr request)
{
    switch (request->Status) {
        case EPendingWriteDataRequestStatus::Pending: {
            request->Status = EPendingWriteDataRequestStatus::Removed;
            PendingRequestsRemove(request.get());
            return true;
        }
        case EPendingWriteDataRequestStatus::Allocated: {
            auto cancelAllocResult =
                PersistentStorage->CancelAlloc(request->AllocationPtr);

            if (HasError(cancelAllocResult)) {
                return false;
            }

            request->Status = EPendingWriteDataRequestStatus::Removed;
            AllocatedRequestsRemove(request.get());
            return AllocPendingRequestsInPersistentStorage();
        }
        case EPendingWriteDataRequestStatus::Serializing: {
            request->Status = EPendingWriteDataRequestStatus::CancelSerializing;
            SerializingRequestsRemove(request.get());
            CancelSerializingRequests.push_back(std::move(request));
            return true;
        }
        case EPendingWriteDataRequestStatus::Serialized: {
            auto cancelAllocResult =
                PersistentStorage->CancelAlloc(request->AllocationPtr);

            if (HasError(cancelAllocResult)) {
                return false;
            }

            request->Status = EPendingWriteDataRequestStatus::Removed;
            SerializingRequestsRemove(request.get());
            return AllocPendingRequestsInPersistentStorage();
        }
        case EPendingWriteDataRequestStatus::CancelSerializing:
        case EPendingWriteDataRequestStatus::Removed:
            return true;
    }

    return false;
}

bool TWriteDataRequestManager::SetFlushed(TCachedWriteDataRequest* request)
{
    auto setTagResult = PersistentStorage->SetTag(
        request->GetAllocationPtr(),
        static_cast<ui32>(ECachedWriteDataRequestTag::Flushed));

    if (!HasError(setTagResult)) {
        UnflushedRequestsRemove(request);
        request->Time = Timer->Now();
        FlushedRequestsPushBack(request);
        return true;
    }

    return false;
}

bool TWriteDataRequestManager::SetHandleReleased(
    TCachedWriteDataRequest* request)
{
    auto setTagResult = PersistentStorage->SetTag(
        request->GetAllocationPtr(),
        static_cast<ui32>(ECachedWriteDataRequestTag::UnflushedHandleReleased));

    return !HasError(setTagResult);
}

bool TWriteDataRequestManager::Evict(
    std::unique_ptr<TCachedWriteDataRequest> request)
{
    FlushedRequestsRemove(request.get());

    auto freeResult = PersistentStorage->Free(request->GetAllocationPtr());
    if (HasError(freeResult)) {
        return false;
    }

    return AllocPendingRequestsInPersistentStorage();
}

bool TWriteDataRequestManager::SetBackpressureStatusForNode(ui64 nodeId)
{
    auto [_, added] = NodesWithBackpressure.insert(nodeId);
    if (added) {
        Stats->AddedNodeWithBackpressure();
        return true;
    }
    return false;
}

bool TWriteDataRequestManager::ClearBackpressureStatusForNode(ui64 nodeId)
{
    auto removed = NodesWithBackpressure.erase(nodeId);
    if (removed) {
        Stats->RemovedNodeWithBackpressure();
        return true;
    }
    return false;
}

void TWriteDataRequestManager::UpdateStats() const
{
    auto now = Timer->Now();

    auto maxPendingRequestDuration = PendingRequests.Empty()
                                         ? TDuration::Zero()
                                         : now - PendingRequests.Front()->Time;

    auto maxAllocatedRequestDuration =
        SerializingRequests.Empty()
            ? AllocatedRequests.Empty() ? TDuration::Zero()
                                        : now - AllocatedRequests.Front()->Time
            : now - SerializingRequests.Front()->Time;

    auto maxUnflushedRequestDuration =
        UnflushedRequests.Empty() ? TDuration::Zero()
                                  : now - UnflushedRequests.Front()->Time;

    Stats->UpdateStats(
        maxPendingRequestDuration,
        maxAllocatedRequestDuration,
        maxUnflushedRequestDuration);

    PersistentStorage->UpdateStats();
}

// Private methods

auto TWriteDataRequestManager::TryAllocRequestInPersistentStorage(
    TPendingWriteDataRequest* pendingRequest)
    -> TWriteDataRequestManager::TAllocRequestResult
{
    const auto& request = pendingRequest->GetRequest();

    const ui64 byteCount = NCloud::NFileStore::CalculateByteCount(request) -
                           request.GetBufferOffset();

    const ui64 allocationSize =
        sizeof(TSerializedWriteDataRequestHeader) + byteCount;

    auto allocationResult = PersistentStorage->Alloc(allocationSize);

    if (HasError(allocationResult)) {
        return {.Failed = true};
    }

    char* allocationPtr = allocationResult.GetResult();
    if (allocationPtr == nullptr) {
        StorageIsFull = true;
        return {};
    }

    StorageIsFull = false;

    pendingRequest->AllocationPtr = allocationPtr;
    pendingRequest->AllocationByteCount = allocationSize;

    return {.Allocated = true};
}

bool TWriteDataRequestManager::AllocPendingRequestsInPersistentStorage()
{
    while (!PendingRequests.Empty()) {
        auto* pendingRequest = PendingRequests.Front();
        auto res = TryAllocRequestInPersistentStorage(pendingRequest);
        if (res.Failed) {
            return false;
        }
        if (!res.Allocated) {
            break;
        }
        PendingRequestsRemove(pendingRequest);
        AllocatedRequestsPushBack(pendingRequest);
        pendingRequest->Status = EPendingWriteDataRequestStatus::Allocated;
        pendingRequest->Time = Timer->Now();
    }
    return true;
}

// Access methods that triggers stats update

void TWriteDataRequestManager::PendingRequestsPushBack(
    TPendingWriteDataRequest* request)
{
    PendingRequests.PushBack(request);
    Stats->AddedPendingRequest();
}

void TWriteDataRequestManager::PendingRequestsRemove(
    TPendingWriteDataRequest* request)
{
    PendingRequests.Remove(request);
    Stats->RemovedPendingRequest(Timer->Now() - request->Time);
}

void TWriteDataRequestManager::PendingRequestsPopFront()
{
    auto* request = PendingRequests.Front();
    PendingRequests.PopFront();
    Stats->RemovedPendingRequest(Timer->Now() - request->Time);
}

void TWriteDataRequestManager::AllocatedRequestsPushBack(
    TPendingWriteDataRequest* request)
{
    AllocatedRequests.PushBack(request);
    Stats->AddedAllocatedRequest();
}

void TWriteDataRequestManager::AllocatedRequestsRemove(
    TPendingWriteDataRequest* request)
{
    AllocatedRequests.Remove(request);
    Stats->RemovedAllocatedRequest(Timer->Now() - request->Time);
}

void TWriteDataRequestManager::SerializingRequestsRemove(
    TPendingWriteDataRequest* request)
{
    SerializingRequests.Remove(request);
    Stats->RemovedAllocatedRequest(Timer->Now() - request->Time);
}

void TWriteDataRequestManager::UnflushedRequestsPushBack(
    TCachedWriteDataRequest* request)
{
    UnflushedRequests.PushBack(request);
    Stats->AddedUnflushedRequest();
}

void TWriteDataRequestManager::UnflushedRequestsRemove(
    TCachedWriteDataRequest* request)
{
    UnflushedRequests.Remove(request);
    Stats->RemovedUnflushedRequest(Timer->Now() - request->Time);
}

void TWriteDataRequestManager::FlushedRequestsPushBack(
    TCachedWriteDataRequest* request)
{
    FlushedRequests.PushBack(request);
    Stats->AddedFlushedRequest();
}

void TWriteDataRequestManager::FlushedRequestsRemove(
    TCachedWriteDataRequest* request)
{
    FlushedRequests.Remove(request);
    Stats->RemovedFlushedRequest();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
