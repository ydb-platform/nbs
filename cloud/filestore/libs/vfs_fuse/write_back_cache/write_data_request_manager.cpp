#include "write_data_request_manager.h"

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

            auto request = TCachedWriteDataRequest::Deserialize(
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

    return {};
}

bool TWriteDataRequestManager::HasPendingRequests() const
{
    return HasUnallocatedPendingRequests() || HasAllocatedPendingRequests();
}

bool TWriteDataRequestManager::HasPendingOrUnflushedRequests() const
{
    return HasPendingRequests() || HasUnflushedRequests();
}

ui64 TWriteDataRequestManager::GetMinPendingOrUnflushedSequenceId() const
{
    if (HasUnflushedRequests()) {
        return UnflushedRequests.Front()->GetSequenceId();
    }
    if (HasAllocatedPendingRequests()) {
        return AllocatedPendingRequests.Front()->GetSequenceId();
    }
    if (HasUnallocatedPendingRequests()) {
        return UnallocatedPendingRequests.Front()->GetSequenceId();
    }
    return Max<ui64>();
}

ui64 TWriteDataRequestManager::GetMaxPendingOrUnflushedSequenceId() const
{
    if (HasUnallocatedPendingRequests()) {
        return UnallocatedPendingRequests.Back()->GetSequenceId();
    }
    if (HasAllocatedPendingRequests()) {
        return AllocatedPendingRequests.Back()->GetSequenceId();
    }
    if (HasUnflushedRequests()) {
        return UnflushedRequests.Back()->GetSequenceId();
    }
    return 0;
}

ui64 TWriteDataRequestManager::GetMaxUnflushedSequenceId() const
{
    return HasUnflushedRequests() ? UnflushedRequests.Back()->GetSequenceId()
                                  : 0;
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

    if (HasUnallocatedPendingRequests()) {
        UnallocatedPendingRequestsPushBack(pendingRequest.get());
        return pendingRequest;
    }

    if (!TryAllocRequestInPersistentStorage(pendingRequest.get())) {
        // PersistentStorage failures
        return nullptr;
    }

    if (pendingRequest->HasAllocation()) {
        AllocatedPendingRequestsPushBack(pendingRequest.get());
        SerializationNeededRequests.PushBack(pendingRequest.get());
    } else {
        UnallocatedPendingRequestsPushBack(pendingRequest.get());
    }

    return pendingRequest;
}

TPendingWriteDataRequest*
TWriteDataRequestManager::GetNextPendingRequestToSerialize()
{
    if (SerializationNeededRequests.Empty()) {
        return nullptr;
    }

    return SerializationNeededRequests.PopFront();
}

TWriteDataRequestManager::TGetNextReadyCachedRequestResult
TWriteDataRequestManager::GetNextReadyCachedRequest()
{
    if (AllocatedPendingRequests.Empty()) {
        return {};
    }

    auto* pendingRequest = AllocatedPendingRequests.Front();
    if (!pendingRequest->Serialized) {
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

    auto cachedRequest = TCachedWriteDataRequest::Deserialize(
        pendingRequest->GetSequenceId(),
        Timer->Now(),
        {pendingRequest->AllocationPtr, pendingRequest->AllocationByteCount});

    Y_ABORT_UNLESS(cachedRequest != nullptr);

    AllocatedPendingRequestsRemove(pendingRequest);
    UnflushedRequestsPushBack(cachedRequest.get());

    return {.Request = std::move(cachedRequest)};
}

const TPendingWriteDataRequest*
TWriteDataRequestManager::GetBackUnallocatedPendingRequest() const
{
    return HasUnallocatedPendingRequests() ? UnallocatedPendingRequests.Back()
                                           : nullptr;
}

void TWriteDataRequestManager::RemoveUnallocated(
    std::unique_ptr<TPendingWriteDataRequest> request)
{
    Y_ABORT_UNLESS(!request->HasAllocation());
    UnallocatedPendingRequestsRemove(request.get());
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

    auto maxPendingRequestDuration =
        HasUnallocatedPendingRequests()
            ? now - UnallocatedPendingRequests.Front()->Time
            : TDuration::Zero();

    auto maxAllocatedRequestDuration =
        HasAllocatedPendingRequests()
            ? now - AllocatedPendingRequests.Front()->Time
            : TDuration::Zero();

    auto maxUnflushedRequestDuration =
        HasUnflushedRequests() ? now - UnflushedRequests.Front()->Time
                               : TDuration::Zero();

    Stats->UpdateStats(
        maxPendingRequestDuration,
        maxAllocatedRequestDuration,
        maxUnflushedRequestDuration);

    PersistentStorage->UpdateStats();
}

// Private methods

bool TWriteDataRequestManager::TryAllocRequestInPersistentStorage(
    TPendingWriteDataRequest* pendingRequest)
{
    auto allocationResult =
        PersistentStorage->Alloc(pendingRequest->AllocationByteCount);

    if (HasError(allocationResult)) {
        return false;
    }

    pendingRequest->AllocationPtr = allocationResult.GetResult();
    StorageIsFull = !pendingRequest->HasAllocation();

    return true;
}

bool TWriteDataRequestManager::AllocPendingRequestsInPersistentStorage()
{
    while (HasUnallocatedPendingRequests()) {
        auto* pendingRequest = UnallocatedPendingRequests.Front();
        if (!TryAllocRequestInPersistentStorage(pendingRequest)) {
            return false;
        }
        if (!pendingRequest->HasAllocation()) {
            break;
        }
        UnallocatedPendingRequestsRemove(pendingRequest);
        AllocatedPendingRequestsPushBack(pendingRequest);
        SerializationNeededRequests.PushBack(pendingRequest);
        pendingRequest->Time = Timer->Now();
    }
    return true;
}

bool TWriteDataRequestManager::HasUnallocatedPendingRequests() const
{
    return !UnallocatedPendingRequests.Empty();
}

bool TWriteDataRequestManager::HasAllocatedPendingRequests() const
{
    return !AllocatedPendingRequests.Empty();
}

bool TWriteDataRequestManager::HasUnflushedRequests() const
{
    return !UnflushedRequests.Empty();
}

// Access methods that triggers stats update

void TWriteDataRequestManager::UnallocatedPendingRequestsPushBack(
    TPendingWriteDataRequest* request)
{
    UnallocatedPendingRequests.PushBack(request);
    Stats->AddedPendingRequest();
}

void TWriteDataRequestManager::UnallocatedPendingRequestsRemove(
    TPendingWriteDataRequest* request)
{
    UnallocatedPendingRequests.Remove(request);
    Stats->RemovedPendingRequest(Timer->Now() - request->Time);
}

void TWriteDataRequestManager::UnallocatedPendingRequestsPopFront()
{
    UnallocatedPendingRequestsRemove(UnallocatedPendingRequests.Front());
}

void TWriteDataRequestManager::AllocatedPendingRequestsPushBack(
    TPendingWriteDataRequest* request)
{
    AllocatedPendingRequests.PushBack(request);
    Stats->AddedAllocatedRequest();
}

void TWriteDataRequestManager::AllocatedPendingRequestsRemove(
    TPendingWriteDataRequest* request)
{
    AllocatedPendingRequests.Remove(request);
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
