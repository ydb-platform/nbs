#include "handle_ops_queue.h"

#include "handle_ops_queue_stats.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueue::THandleOpsQueue(const TString& filePath, ui32 size)
    : RequestsToProcess(filePath, size, 0, EFileRingBufferVersion::V5)
    , Stats(CreateHandleOpsQueueStats(size))
{
    Stats->SetEntryCount(RequestsToProcess.Size());
}

IModuleStatsPtr THandleOpsQueue::GetModuleStats() const
{
    return Stats;
}

THandleOpsQueue::EResult THandleOpsQueue::AddCreateRequest(
    ui64 nodeId,
    ui64 handle,
    ui32 flags,
    ui64 originalRequestId)
{
    NProto::TQueueEntry request;
    auto* queued = request.MutableQueuedCreateHandleRequest();
    queued->SetNodeId(nodeId);
    queued->SetHandle(handle);
    queued->SetFlags(flags);
    queued->SetOriginalRequestId(originalRequestId);

    TString result;
    if (!request.SerializeToString(&result)) {
        Stats->IncrementSerializationErrorCount();
        return THandleOpsQueue::EResult::SerializationError;
    }

    // TODO(#1751): Implement handling errors in
    // https://github.com/ydb-platform/nbs/pull/6867
    if (!RequestsToProcess.PushBack(result).Pushed) {
        Stats->IncrementOverflowErrorCount();
        return THandleOpsQueue::EResult::QueueOverflow;
    }

    Stats->SetEntryCount(RequestsToProcess.Size());
    return THandleOpsQueue::EResult::Ok;
}

THandleOpsQueue::EResult THandleOpsQueue::AddDestroyRequest(
    ui64 nodeId,
    ui64 handle)
{
    if (RequestsToProcess.IsCorrupted()) {
        return THandleOpsQueue::EResult::QueueIsCorrupted;
    }

    NProto::TQueueEntry request;
    request.MutableDestroyHandleRequest()->SetHandle(handle);
    request.MutableDestroyHandleRequest()->SetNodeId(nodeId);

    TString result;
    if (!request.SerializeToString(&result)) {
        Stats->IncrementSerializationErrorCount();
        return THandleOpsQueue::EResult::SerializationError;
    }

    // TODO(#1751): Implement handling errors in
    // https://github.com/ydb-platform/nbs/pull/6867
    if (!RequestsToProcess.PushBack(result).Pushed) {
        Stats->IncrementOverflowErrorCount();
        return THandleOpsQueue::EResult::QueueOverflow;
    }

    Stats->SetEntryCount(RequestsToProcess.Size());
    return THandleOpsQueue::EResult::Ok;
}

std::optional<NProto::TQueueEntry> THandleOpsQueue::Front()
{
    const auto req = RequestsToProcess.Front();

    NProto::TQueueEntry entry;

    // TODO(#1751): Implement handling errors in
    // https://github.com/ydb-platform/nbs/pull/6867
    if (!entry.ParseFromArray(req.Data.data(), req.Data.size())) {
        Stats->IncrementParseErrorCount();
        return std::nullopt;
    }

    return entry;
}

bool THandleOpsQueue::Empty() const
{
    return RequestsToProcess.Empty();
}

bool THandleOpsQueue::IsCorrupted() const
{
    return RequestsToProcess.IsCorrupted();
}

void THandleOpsQueue::PopFront()
{
    auto popFrontResult = RequestsToProcess.PopFront();

    // TODO(#1751): To be resolved in
    // https://github.com/ydb-platform/nbs/pull/6666
    Y_UNUSED(popFrontResult);

    Stats->SetEntryCount(RequestsToProcess.Size());
}

ui64 THandleOpsQueue::Size() const
{
    return RequestsToProcess.Size();
}

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueuePtr CreateHandleOpsQueue(
    const TString& filePath,
    ui32 size)
{
    return std::make_unique<THandleOpsQueue>(filePath, size);
}

}   // namespace NCloud::NFileStore::NFuse
