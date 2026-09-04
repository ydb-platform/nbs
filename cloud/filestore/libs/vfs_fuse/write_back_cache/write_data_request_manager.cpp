#include "write_data_request_manager.h"

#include <cloud/filestore/libs/service/request.h>

#include <util/stream/mem.h>
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

TStringBuf SerializeWriteDataRequest(
    const NProto::TWriteDataRequest& request,
    TMemoryOutput& memoryOutput)
{
    TSerializedWriteDataRequestHeader header{
        .NodeId = request.GetNodeId(),
        .Handle = request.GetHandle(),
        .Offset = request.GetOffset()};

    memoryOutput.Write(&header, sizeof(header));

    auto data = TStringBuf(memoryOutput.Buf(), memoryOutput.Avail());

    if (request.GetIovecs().empty()) {
        memoryOutput.Write(
            TStringBuf(request.GetBuffer()).Skip(request.GetBufferOffset()));
    } else {
        for (const auto& iovec: request.GetIovecs()) {
            memoryOutput.Write(TStringBuf(
                reinterpret_cast<const char*>(iovec.GetBase()),
                iovec.GetLength()));
        }
    }

    return data;
}

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
    return !PendingRequests.Empty();
}

bool TWriteDataRequestManager::HasPendingOrUnflushedRequests() const
{
    return !UnflushedRequests.Empty() || !PendingRequests.Empty();
}

ui64 TWriteDataRequestManager::GetMinPendingOrUnflushedSequenceId() const
{
    if (!UnflushedRequests.Empty()) {
        return UnflushedRequests.Front()->GetSequenceId();
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

auto TWriteDataRequestManager::AddRequest(
    std::shared_ptr<NProto::TWriteDataRequest> request) -> TAddRequestResult
{
    const ui64 sequenceId = SequenceIdGenerator->GenerateId();
    const auto now = Timer->Now();

    if (PendingRequests.Empty()) {
        auto res =
            TryStoreRequestInPersistentStorage(sequenceId, now, *request);

        if (res.Failed) {
            return {.Failed = true};
        }

        if (res.CachedRequest) {
            UnflushedRequestsPushBack(res.CachedRequest.get());
            return {.CachedRequest = std::move(res.CachedRequest)};
        }
    }

    auto pendingRequest = std::make_unique<TPendingWriteDataRequest>(
        sequenceId,
        now,
        std::move(request));

    PendingRequestsPushBack(pendingRequest.get());
    return {.PendingRequest = std::move(pendingRequest)};
}

auto TWriteDataRequestManager::TryProcessPendingRequest()
    -> TProcessPendingRequestResult
{
    if (PendingRequests.Empty()) {
        return {};
    }

    auto* pendingRequest = PendingRequests.Front();

    auto res = TryStoreRequestInPersistentStorage(
        pendingRequest->GetSequenceId(),
        Timer->Now(),
        pendingRequest->GetRequest());

    if (!res.CachedRequest) {
        return {.Failed = res.Failed};
    }

    PendingRequestsPopFront();
    UnflushedRequestsPushBack(res.CachedRequest.get());

    return {.CachedRequest = std::move(res.CachedRequest)};
}

TPendingWriteDataRequest* TWriteDataRequestManager::TryPopFrontPendingRequest()
{
    if (PendingRequests.Empty()) {
        return nullptr;
    }

    auto* pendingRequest = PendingRequests.Front();
    PendingRequestsPopFront();
    return pendingRequest;
}

void TWriteDataRequestManager::Remove(
    std::unique_ptr<TPendingWriteDataRequest> request)
{
    PendingRequestsRemove(request.get());
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

    auto maxPendingRequestDuration = PendingRequests.Empty()
                                         ? TDuration::Zero()
                                         : now - PendingRequests.Front()->Time;

    auto maxUnflushedRequestDuration =
        UnflushedRequests.Empty() ? TDuration::Zero()
                                  : now - UnflushedRequests.Front()->Time;

    Stats->UpdateStats(
        maxPendingRequestDuration,
        maxUnflushedRequestDuration);

    PersistentStorage->UpdateStats();
}

// Private methods

auto TWriteDataRequestManager::TryStoreRequestInPersistentStorage(
    ui64 sequenceId,
    TInstant time,
    const NProto::TWriteDataRequest& request) -> TProcessPendingRequestResult
{
    if (NodesWithBackpressure.contains(request.GetNodeId())) {
        // Known limitation: pending requests are global FIFO.
        // Although backpressure is tracked per node, the pending queue is not
        // reordered. A front request for a backpressured node may therefore
        // block requests for unrelated nodes. This is intentional for the
        // current implementation; per-node pending queues/fair scheduling
        // should be added separately.
        return {};
    }

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
        return {};
    }

    TMemoryOutput memoryOutput(allocationPtr, allocationSize);

    auto data = SerializeWriteDataRequest(request, memoryOutput);

    Y_ABORT_UNLESS(
        memoryOutput.Exhausted(),
        "Buffer is expected to be written completely");

    auto commitResult = PersistentStorage->Commit(allocationPtr);
    if (HasError(commitResult)) {
        return {.Failed = true};
    }

    auto res = std::make_unique<TCachedWriteDataRequest>(
        sequenceId,
        time,
        allocationPtr,
        data);

    return {.CachedRequest = std::move(res)};
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
