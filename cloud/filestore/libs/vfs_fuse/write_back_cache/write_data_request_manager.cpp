#include "write_data_request_manager.h"

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

            auto request = TCachedWriteDataRequest::CreateFromAllocation(
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

    UnallocatedPendingRequests.Clear();
    AllocatedPendingRequests.Clear();

    return {};
}

bool TWriteDataRequestManager::HasPendingRequests() const
{
    return HasUnallocatedPendingRequests() || HasAllocatedPendingRequests();
}

bool TWriteDataRequestManager::HasPendingOrUnflushedRequests() const
{
    return !UnflushedRequests.Empty() || HasPendingRequests();
}

ui64 TWriteDataRequestManager::GetMinPendingOrUnflushedSequenceId() const
{
    if (!UnflushedRequests.Empty()) {
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

std::unique_ptr<TPendingWriteDataRequest> TWriteDataRequestManager::AddRequest(
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    const ui64 sequenceId = SequenceIdGenerator->GenerateId();
    const auto now = Timer->Now();

    auto pendingRequest = std::make_unique<TPendingWriteDataRequest>(
        sequenceId,
        now,
        std::move(request));

    UnallocatedPendingRequestsPushBack(pendingRequest.get());
    return pendingRequest;
}

auto TWriteDataRequestManager::TryAllocPendingRequest()
    -> TAllocPendingRequestResult
{
    if (UnallocatedPendingRequests.Empty()) {
        return {};
    }

    auto* pendingRequest = UnallocatedPendingRequests.Front();

    if (NodesWithBackpressure.contains(
            pendingRequest->GetRequest().GetNodeId()))
    {
        // Known limitation: pending requests are global FIFO.
        // Although backpressure is tracked per node, the pending queue is not
        // reordered. A front request for a backpressured node may therefore
        // block requests for unrelated nodes. This is intentional for the
        // current implementation; per-node pending queues/fair scheduling
        // should be added separately.
        return {};
    }

    auto allocationResult =
        PersistentStorage->Alloc(pendingRequest->AllocationByteCount);

    if (HasError(allocationResult)) {
        return {.Failed = true};
    }

    auto* allocationPtr = allocationResult.GetResult();
    if (!allocationPtr) {
        return {.StorageIsFull = true};
    }

    pendingRequest->AllocationPtr = allocationPtr;

    UnallocatedPendingRequests.Remove(pendingRequest);
    AllocatedPendingRequests.PushBack(pendingRequest);

    return {.Request = pendingRequest};
}

TWriteDataRequestManager::TGetNextReadyCachedRequestResult
TWriteDataRequestManager::GetNextReadyCachedRequest()
{
    if (AllocatedPendingRequests.Empty()) {
        return {};
    }

    auto* pendingRequest = AllocatedPendingRequests.Front();
    if (!pendingRequest->Serialized.load(std::memory_order_acquire)) {
        return {};
    }

    auto commitResult = PersistentStorage->Commit(
        pendingRequest->AllocationPtr,
        pendingRequest->Checksum);

    if (HasError(commitResult)) {
        return {.Failed = true};
    }

    auto cachedRequest = TCachedWriteDataRequest::CreateFromAllocation(
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

    return !HasError(freeResult);
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

    const TPendingWriteDataRequest* front = nullptr;
    if (HasAllocatedPendingRequests()) {
        front = AllocatedPendingRequests.Front();
    } else if (HasUnallocatedPendingRequests()) {
        front = UnallocatedPendingRequests.Front();
    }

    auto maxPendingRequestDuration =
        front ? now - front->Time : TDuration::Zero();

    auto maxUnflushedRequestDuration =
        UnflushedRequests.Empty() ? TDuration::Zero()
                                  : now - UnflushedRequests.Front()->Time;

    Stats->UpdateStats(maxPendingRequestDuration, maxUnflushedRequestDuration);

    PersistentStorage->UpdateStats();
}

bool TWriteDataRequestManager::HasUnallocatedPendingRequests() const
{
    return !UnallocatedPendingRequests.Empty();
}

bool TWriteDataRequestManager::HasAllocatedPendingRequests() const
{
    return !AllocatedPendingRequests.Empty();
}

// Access methods that trigger stats update

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

void TWriteDataRequestManager::AllocatedPendingRequestsRemove(
    TPendingWriteDataRequest* request)
{
    AllocatedPendingRequests.Remove(request);
    Stats->RemovedPendingRequest(Timer->Now() - request->Time);
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
