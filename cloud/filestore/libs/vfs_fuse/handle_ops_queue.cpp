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

IModuleStatsPtr THandleOpsQueue::CreateModuleStats() const
{
    return Stats;
}

THandleOpsQueue::EResult THandleOpsQueue::AddDestroyRequest(
    ui64 nodeId,
    ui64 handle)
{
    NProto::TQueueEntry request;
    request.MutableDestroyHandleRequest()->SetHandle(handle);
    request.MutableDestroyHandleRequest()->SetNodeId(nodeId);

    TString result;
    if (!request.SerializeToString(&result)) {
        Stats->IncrementSerializationErrorCount();
        return THandleOpsQueue::EResult::SerializationError;
    }

    if (!RequestsToProcess.PushBack(result)) {
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
    if (!entry.ParseFromArray(req.data(), req.size())) {
        Stats->IncrementParseErrorCount();
        return std::nullopt;
    }

    return entry;
}

bool THandleOpsQueue::Empty() const
{
    return RequestsToProcess.Empty();
}

void THandleOpsQueue::PopFront()
{
    RequestsToProcess.PopFront();
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
