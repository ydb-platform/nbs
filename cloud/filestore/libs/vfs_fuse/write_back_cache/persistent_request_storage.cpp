#include "persistent_request_storage.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

TPersistentRequestStorage::TPersistentRequestStorage(
    ISequenceIdGenerator& sequenceIdGenerator,
    IPersistentStorage& persistentStorage)
    : SequenceIdGenerator(sequenceIdGenerator)
    , PersistentStorage(persistentStorage)
{}

bool TPersistentRequestStorage::Init(const TCachedRequestVisitor& visitor)
{
    bool success = true;

    TVector<std::unique_ptr<TCachedWriteDataRequest>> loadedRequests;

    PersistentStorage.Visit(
        [this, &success, &loadedRequests](const TStringBuf allocation)
        {
            auto request = TCachedWriteDataRequestSerializer::TryDeserialize(
                SequenceIdGenerator.Generate(),
                allocation);

            if (!request) {
                success = false;
                return false;
            }

            loadedRequests.push_back(std::move(request));
            return true;
        });

    if (!success) {
        return false;
    }

    for (auto& request: loadedRequests) {
        UnflushedRequests.PushBack(request.get());
        visitor(std::move(request));
    }

    PendingRequests.Clear();

    return true;
}

bool TPersistentRequestStorage::HasPendingRequests() const
{
    return !PendingRequests.Empty();
}

bool TPersistentRequestStorage::HasUnflushedRequests() const
{
    return !UnflushedRequests.Empty() || !PendingRequests.Empty();
}

ui64 TPersistentRequestStorage::GetMinUnflushedSequenceId() const
{
    if (!UnflushedRequests.Empty()) {
        return UnflushedRequests.Front()->GetSequenceId();
    }
    if (!PendingRequests.Empty()) {
        return PendingRequests.Front()->SequenceId;
    }
    return Max<ui64>();
}

ui64 TPersistentRequestStorage::GetMaxUnflushedCachedSequenceId() const
{
    return UnflushedRequests.Empty()
               ? 0
               : UnflushedRequests.Back()->GetSequenceId();
}

auto TPersistentRequestStorage::AddRequest(
    std::shared_ptr<NProto::TWriteDataRequest> request) -> TAddRequestResult
{
    const ui64 sequenceId = SequenceIdGenerator.Generate();

    if (PendingRequests.Empty()) {
        auto cachedRequest = TCachedWriteDataRequestSerializer::TrySerialize(
            sequenceId,
            *request,
            PersistentStorage);

        if (cachedRequest) {
            UnflushedRequests.PushBack(cachedRequest.get());
            return cachedRequest;
        }
    }

    auto pendingRequest = std::make_unique<TPendingWriteDataRequest>(
        sequenceId,
        std::move(request));

    PendingRequests.PushBack(pendingRequest.get());
    return pendingRequest;
}

auto TPersistentRequestStorage::TryProcessPendingRequest()
    -> std::unique_ptr<TCachedWriteDataRequest>
{
    if (PendingRequests.Empty()) {
        return nullptr;
    }

    auto* pendingRequest = PendingRequests.Front();

    auto cachedRequest = TCachedWriteDataRequestSerializer::TrySerialize(
        pendingRequest->SequenceId,
        *pendingRequest->Request,
        PersistentStorage);

    if (cachedRequest) {
        PendingRequests.PopFront();
        UnflushedRequests.PushBack(cachedRequest.get());
    }

    return cachedRequest;
}

void TPersistentRequestStorage::Remove(
    std::unique_ptr<TPendingWriteDataRequest> request)
{
    PendingRequests.Remove(request.get());
}

void TPersistentRequestStorage::SetFlushed(
    TCachedWriteDataRequest* request)
{
    UnflushedRequests.Remove(request);
    FlushedRequests.PushBack(request);
}

void TPersistentRequestStorage::Evict(
    std::unique_ptr<TCachedWriteDataRequest> request)
{
    FlushedRequests.Remove(request.get());
    PersistentStorage.Free(request->GetAllocationPtr());
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
