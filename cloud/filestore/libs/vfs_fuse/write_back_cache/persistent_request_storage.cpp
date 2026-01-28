#include "persistent_request_storage.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

TPersistentRequestStorage::TPersistentRequestStorage(
    ISequenceIdGenerator& sequenceIdGenerator,
    IPersistentStorage& persistentStorage,
    ITimer& timer,
    IWriteBackCacheStats& stats)
    : SequenceIdGenerator(sequenceIdGenerator)
    , PersistentStorage(persistentStorage)
    , Timer(timer)
    , Stats(stats)
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
                Timer.Now(),
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
        UnflushedRequestsPushBack(request.get());
        visitor(std::move(request));
    }

    PendingRequests.Clear();

    return true;
}

bool TPersistentRequestStorage::HasPendingRequests() const
{
    return !PendingRequests.Empty();
}

bool TPersistentRequestStorage::HasPendingOrUnflushedRequests() const
{
    return !UnflushedRequests.Empty() || !PendingRequests.Empty();
}

ui64 TPersistentRequestStorage::GetMinPendingOrUnflushedSequenceId() const
{
    if (!UnflushedRequests.Empty()) {
        return UnflushedRequests.Front()->GetSequenceId();
    }
    if (!PendingRequests.Empty()) {
        return PendingRequests.Front()->SequenceId;
    }
    return Max<ui64>();
}

ui64 TPersistentRequestStorage::GetMaxUnflushedSequenceId() const
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
            Timer.Now(),
            *request,
            PersistentStorage);

        if (cachedRequest) {
            UnflushedRequestsPushBack(cachedRequest.get());
            return cachedRequest;
        }
    }

    auto pendingRequest = std::make_unique<TPendingWriteDataRequest>(
        sequenceId,
        Timer.Now(),
        std::move(request));

    PendingRequestsPushBack(pendingRequest.get());
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
        Timer.Now(),
        *pendingRequest->Request,
        PersistentStorage);

    if (cachedRequest) {
        PendingRequestsPopFront();
        UnflushedRequestsPushBack(cachedRequest.get());
    }

    return cachedRequest;
}

void TPersistentRequestStorage::Remove(
    std::unique_ptr<TPendingWriteDataRequest> request)
{
    PendingRequestsRemove(request.get());
}

void TPersistentRequestStorage::SetFlushed(TCachedWriteDataRequest* request)
{
    UnflushedRequestsRemove(request);
    request->SetTime(Timer.Now());
    FlushedRequestsPushBack(request);
}

void TPersistentRequestStorage::Evict(
    std::unique_ptr<TCachedWriteDataRequest> request)
{
    FlushedRequestsRemove(request.get());
    PersistentStorage.Free(request->GetAllocationPtr());
}

// Access methods that triggers stats update

void TPersistentRequestStorage::PendingRequestsPushBack(
    TPendingWriteDataRequest* request)
{
    if (PendingRequests.Empty()) {
        Stats.WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Pending,
            request->Time);
    }

    PendingRequests.PushBack(request);
    Stats.WriteDataRequestEnteredStatus(EWriteDataRequestStatus::Pending);
}

void TPersistentRequestStorage::PendingRequestsRemove(
    TPendingWriteDataRequest* request)
{
    const auto prevMinTime = PendingRequests.Front()->Time;

    Stats.WriteDataRequestExitedStatus(
        EWriteDataRequestStatus::Pending,
        Timer.Now() - request->Time);

    PendingRequests.Remove(request);

    const auto minTime = PendingRequests.Empty()
                             ? TInstant::Zero()
                             : PendingRequests.Front()->Time;

    if (prevMinTime != minTime) {
        Stats.WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Pending,
            minTime);
    }
}

void TPersistentRequestStorage::PendingRequestsPopFront()
{
    auto* request = PendingRequests.Front();

    Stats.WriteDataRequestExitedStatus(
        EWriteDataRequestStatus::Pending,
        Timer.Now() - request->Time);

    PendingRequests.PopFront();

    if (PendingRequests.Empty()) {
        Stats.WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Pending,
            TInstant::Zero());
    } else {
        Stats.WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Pending,
            PendingRequests.Front()->Time);
    }
}

void TPersistentRequestStorage::UnflushedRequestsPushBack(
    TCachedWriteDataRequest* request)
{
    if (UnflushedRequests.Empty()) {
        Stats.WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Cached,
            request->GetTime());
    }

    UnflushedRequests.PushBack(request);
    Stats.WriteDataRequestEnteredStatus(EWriteDataRequestStatus::Cached);
}

void TPersistentRequestStorage::UnflushedRequestsRemove(
    TCachedWriteDataRequest* request)
{
    const auto prevMinTime = UnflushedRequests.Front()->GetTime();

    Stats.WriteDataRequestExitedStatus(
        EWriteDataRequestStatus::Cached,
        Timer.Now() - request->GetTime());

    UnflushedRequests.Remove(request);

    const auto minTime = UnflushedRequests.Empty()
                             ? TInstant::Zero()
                             : UnflushedRequests.Front()->GetTime();

    if (prevMinTime != minTime) {
        Stats.WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Cached,
            minTime);
    }
}

void TPersistentRequestStorage::FlushedRequestsPushBack(
    TCachedWriteDataRequest* request)
{
    if (FlushedRequests.Empty()) {
        Stats.WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Flushed,
            request->GetTime());
    }

    FlushedRequests.PushBack(request);
    Stats.WriteDataRequestEnteredStatus(EWriteDataRequestStatus::Flushed);
}

void TPersistentRequestStorage::FlushedRequestsRemove(
    TCachedWriteDataRequest* request)
{
    const auto prevMinTime = FlushedRequests.Front()->GetTime();

    Stats.WriteDataRequestExitedStatus(
        EWriteDataRequestStatus::Flushed,
        Timer.Now() - request->GetTime());

    FlushedRequests.Remove(request);

    const auto minTime = FlushedRequests.Empty()
                             ? TInstant::Zero()
                             : FlushedRequests.Front()->GetTime();

    if (prevMinTime != minTime) {
        Stats.WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Flushed,
            minTime);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
