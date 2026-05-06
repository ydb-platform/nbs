#include "write_data_request_manager.h"

#include <cloud/filestore/libs/service/request.h>

#include <util/stream/mem.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

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

std::unique_ptr<TCachedWriteDataRequest> TryStoreRequestInPersistentStorage(
    ui64 sequenceId,
    TInstant time,
    const NProto::TWriteDataRequest& request,
    IPersistentStorage& storage)
{
    const ui64 byteCount = NCloud::NFileStore::CalculateByteCount(request) -
                           request.GetBufferOffset();

    const ui64 allocationSize =
        sizeof(TSerializedWriteDataRequestHeader) + byteCount;

    auto allocationResult = storage.Alloc(allocationSize);

    Y_ABORT_UNLESS(
        !HasError(allocationResult),
        "Allocation failed with error: %s",
        allocationResult.GetError().GetMessage().data());

    char* allocationPtr = allocationResult.GetResult();
    if (allocationPtr == nullptr) {
        return nullptr;
    }

    TMemoryOutput memoryOutput(allocationPtr, allocationSize);

    auto data = SerializeWriteDataRequest(request, memoryOutput);

    Y_ABORT_UNLESS(
        memoryOutput.Exhausted(),
        "Buffer is expected to be written completely");

    storage.Commit();

    return std::make_unique<TCachedWriteDataRequest>(
        sequenceId,
        time,
        allocationPtr,
        data);
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

bool TWriteDataRequestManager::Init(const TCachedRequestVisitor& visitor)
{
    bool success = true;

    TVector<std::unique_ptr<TCachedWriteDataRequest>> loadedRequests;

    PersistentStorage->Visit(
        [this, &success, &loadedRequests](const TStringBuf allocation)
        {
            auto request = DeserializeWriteDataRequest(
                SequenceIdGenerator->GenerateId(),
                Timer->Now(),
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
        auto cachedRequest = TryStoreRequestInPersistentStorage(
            sequenceId,
            now,
            *request,
            *PersistentStorage);

        if (cachedRequest) {
            UnflushedRequestsPushBack(cachedRequest.get());
            return cachedRequest;
        }
    }

    auto pendingRequest = std::make_unique<TPendingWriteDataRequest>(
        sequenceId,
        now,
        std::move(request));

    PendingRequestsPushBack(pendingRequest.get());
    return pendingRequest;
}

auto TWriteDataRequestManager::TryProcessPendingRequest()
    -> std::unique_ptr<TCachedWriteDataRequest>
{
    if (PendingRequests.Empty()) {
        return nullptr;
    }

    auto* pendingRequest = PendingRequests.Front();

    auto cachedRequest = TryStoreRequestInPersistentStorage(
        pendingRequest->GetSequenceId(),
        Timer->Now(),
        pendingRequest->GetRequest(),
        *PersistentStorage);

    if (cachedRequest) {
        PendingRequestsPopFront();
        UnflushedRequestsPushBack(cachedRequest.get());
    }

    return cachedRequest;
}

void TWriteDataRequestManager::Remove(
    std::unique_ptr<TPendingWriteDataRequest> request)
{
    PendingRequestsRemove(request.get());
}

void TWriteDataRequestManager::SetFlushed(TCachedWriteDataRequest* request)
{
    UnflushedRequestsRemove(request);
    request->Time = Timer->Now();
    FlushedRequestsPushBack(request);
}

void TWriteDataRequestManager::Evict(
    std::unique_ptr<TCachedWriteDataRequest> request)
{
    FlushedRequestsRemove(request.get());

    PersistentStorage->Free(request->GetAllocationPtr());
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
