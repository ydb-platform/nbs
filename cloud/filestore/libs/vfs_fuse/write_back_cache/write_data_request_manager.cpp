#include "write_data_request_manager.h"

#include <cloud/filestore/libs/storage/core/helpers.h>

#include <span>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBufferWriter
{
    std::span<char> TargetBuffer;

    explicit TBufferWriter(std::span<char> targetBuffer)
        : TargetBuffer(targetBuffer)
    {}

    void Write(TStringBuf buffer)
    {
        Y_ABORT_UNLESS(
            buffer.size() <= TargetBuffer.size(),
            "Not enough space in the buffer to write %lu bytes, remaining: %lu",
            buffer.size(),
            TargetBuffer.size());

        buffer.copy(TargetBuffer.data(), buffer.size());
        TargetBuffer = TargetBuffer.subspan(buffer.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TCachedWriteDataRequest> TrySerialize(
    ui64 sequenceId,
    TInstant time,
    const NProto::TWriteDataRequest& request,
    IPersistentStorage& storage)
{
    const ui64 byteCount =
        NStorage::CalculateByteCount(request) - request.GetBufferOffset();

    auto allocationResult =
        storage.Alloc(sizeof(TSerializedWriteDataRequest) + byteCount);

    Y_ABORT_UNLESS(
        !HasError(allocationResult),
        "Allocation failed with error: %s",
        allocationResult.GetError().GetMessage().data());

    char* allocationPtr = allocationResult.GetResult();
    if (allocationPtr == nullptr) {
        return nullptr;
    }

    auto* serializedRequest =
        reinterpret_cast<TSerializedWriteDataRequest*>(allocationPtr);

    serializedRequest->NodeId = request.GetNodeId();
    serializedRequest->Handle = request.GetHandle();
    serializedRequest->Offset = request.GetOffset();

    TBufferWriter writer(
        {allocationPtr + sizeof(TSerializedWriteDataRequest), byteCount});

    if (request.GetIovecs().empty()) {
        writer.Write(
            TStringBuf(request.GetBuffer()).Skip(request.GetBufferOffset()));
    } else {
        for (const auto& iovec: request.GetIovecs()) {
            writer.Write(TStringBuf(
                reinterpret_cast<const char*>(iovec.GetBase()),
                iovec.GetLength()));
        }
    }

    Y_ABORT_UNLESS(
        writer.TargetBuffer.empty(),
        "Buffer is expected to be written completely");

    storage.Commit();

    return std::make_unique<TCachedWriteDataRequest>(
        sequenceId,
        time,
        byteCount,
        serializedRequest);
}

std::unique_ptr<TCachedWriteDataRequest>
TryDeserialize(ui64 sequenceId, TInstant time, TStringBuf allocation)
{
    if (allocation.size() <= sizeof(TSerializedWriteDataRequest)) {
        return nullptr;
    }

    const ui64 byteCount =
        allocation.size() - sizeof(TSerializedWriteDataRequest);

    return std::make_unique<TCachedWriteDataRequest>(
        sequenceId,
        time,
        byteCount,
        reinterpret_cast<const TSerializedWriteDataRequest*>(
            allocation.data()));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TWriteDataRequestManager::TWriteDataRequestManager(
    ISequenceIdGeneratorPtr sequenceIdGenerator,
    IPersistentStoragePtr persistentStorage,
    ITimerPtr timer,
    IWriteBackCacheStatsPtr stats)
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
            auto request = TryDeserialize(
                SequenceIdGenerator->Generate(),
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
    const ui64 sequenceId = SequenceIdGenerator->Generate();
    const auto now = Timer->Now();

    if (PendingRequests.Empty()) {
        auto cachedRequest =
            TrySerialize(sequenceId, now, *request, *PersistentStorage);

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

    auto cachedRequest = TrySerialize(
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

// Access methods that triggers stats update

void TWriteDataRequestManager::PendingRequestsPushBack(
    TPendingWriteDataRequest* request)
{
    if (PendingRequests.Empty()) {
        Stats->WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Pending,
            request->Time);
    }

    PendingRequests.PushBack(request);
    Stats->WriteDataRequestEnteredStatus(EWriteDataRequestStatus::Pending);
}

void TWriteDataRequestManager::PendingRequestsRemove(
    TPendingWriteDataRequest* request)
{
    const auto prevMinTime = PendingRequests.Front()->Time;

    Stats->WriteDataRequestExitedStatus(
        EWriteDataRequestStatus::Pending,
        Timer->Now() - request->Time);

    PendingRequests.Remove(request);

    const auto minTime = PendingRequests.Empty()
                             ? TInstant::Zero()
                             : PendingRequests.Front()->Time;

    if (prevMinTime != minTime) {
        Stats->WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Pending,
            minTime);
    }
}

void TWriteDataRequestManager::PendingRequestsPopFront()
{
    auto* request = PendingRequests.Front();

    Stats->WriteDataRequestExitedStatus(
        EWriteDataRequestStatus::Pending,
        Timer->Now() - request->Time);

    PendingRequests.PopFront();

    if (PendingRequests.Empty()) {
        Stats->WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Pending,
            TInstant::Zero());
    } else {
        Stats->WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Pending,
            PendingRequests.Front()->Time);
    }
}

void TWriteDataRequestManager::UnflushedRequestsPushBack(
    TCachedWriteDataRequest* request)
{
    if (UnflushedRequests.Empty()) {
        Stats->WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Unflushed,
            request->Time);
    }

    UnflushedRequests.PushBack(request);
    Stats->WriteDataRequestEnteredStatus(EWriteDataRequestStatus::Unflushed);
}

void TWriteDataRequestManager::UnflushedRequestsRemove(
    TCachedWriteDataRequest* request)
{
    const auto prevMinTime = UnflushedRequests.Front()->Time;

    Stats->WriteDataRequestExitedStatus(
        EWriteDataRequestStatus::Unflushed,
        Timer->Now() - request->Time);

    UnflushedRequests.Remove(request);

    const auto minTime = UnflushedRequests.Empty()
                             ? TInstant::Zero()
                             : UnflushedRequests.Front()->Time;

    if (prevMinTime != minTime) {
        Stats->WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Unflushed,
            minTime);
    }
}

void TWriteDataRequestManager::FlushedRequestsPushBack(
    TCachedWriteDataRequest* request)
{
    if (FlushedRequests.Empty()) {
        Stats->WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Flushed,
            request->Time);
    }

    FlushedRequests.PushBack(request);
    Stats->WriteDataRequestEnteredStatus(EWriteDataRequestStatus::Flushed);
}

void TWriteDataRequestManager::FlushedRequestsRemove(
    TCachedWriteDataRequest* request)
{
    const auto prevMinTime = FlushedRequests.Front()->Time;

    Stats->WriteDataRequestExitedStatus(
        EWriteDataRequestStatus::Flushed,
        Timer->Now() - request->Time);

    FlushedRequests.Remove(request);

    const auto minTime = FlushedRequests.Empty()
                             ? TInstant::Zero()
                             : FlushedRequests.Front()->Time;

    if (prevMinTime != minTime) {
        Stats->WriteDataRequestUpdateMinTime(
            EWriteDataRequestStatus::Flushed,
            minTime);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
