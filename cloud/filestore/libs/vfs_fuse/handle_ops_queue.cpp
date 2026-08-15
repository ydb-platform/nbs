#include "handle_ops_queue.h"

#include "handle_ops_queue_stats.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueue::THandleOpsQueue(const TString& filePath, ui32 size)
    : RequestsToProcess(filePath, size, 0, EFileRingBufferVersion::V5)
    , Stats(CreateHandleOpsQueueStats(size))
{
    // the queue outlives the process, restore the index from the file
    RequestsToProcess.Visit(
        [this](ui32 /*checksum*/, ui32 /*tag*/, TStringBuf data)
        {
            NProto::TQueueEntry entry;
            if (entry.ParseFromArray(data.data(), data.size()) &&
                entry.HasQueuedCreateHandleRequest())
            {
                UnconfirmedCreates.insert(
                    entry.GetQueuedCreateHandleRequest().GetHandle());
            }
        });

    Stats->SetEntryCount(RequestsToProcess.Size());
}

IModuleStatsPtr THandleOpsQueue::GetModuleStats() const
{
    return Stats;
}

THandleOpsQueue::EResult THandleOpsQueue::AddCreateRequest(
    const NProto::TCreateHandleRequest& createHandleRequest,
    ui64 nodeId,
    ui64 handle,
    ui64 originalRequestId)
{
    NProto::TQueueEntry request;
    auto* queued = request.MutableQueuedCreateHandleRequest();
    *queued->MutableRequest() = createHandleRequest;
    queued->SetHandle(handle);
    queued->SetNodeId(nodeId);
    queued->SetOriginalRequestId(originalRequestId);

    TString result;
    if (!request.SerializeToString(&result)) {
        Stats->IncrementSerializationErrorCount();
        return THandleOpsQueue::EResult::SerializationError;
    }

    if (!RequestsToProcess.PushBack(result)) {
        Stats->IncrementOverflowErrorCount();
        return THandleOpsQueue::EResult::QueueOverflow;
    }

    UnconfirmedCreates.insert(handle);
    Stats->SetEntryCount(RequestsToProcess.Size());
    return THandleOpsQueue::EResult::Ok;
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

std::optional<ui64> THandleOpsQueue::PopFront()
{
    std::optional<ui64> confirmedHandle;

    const auto data = RequestsToProcess.Front();
    NProto::TQueueEntry entry;
    if (entry.ParseFromArray(data.data(), data.size()) &&
        entry.HasQueuedCreateHandleRequest())
    {
        confirmedHandle = entry.GetQueuedCreateHandleRequest().GetHandle();
        UnconfirmedCreates.erase(*confirmedHandle);
    }

    RequestsToProcess.PopFront();
    Stats->SetEntryCount(RequestsToProcess.Size());

    return confirmedHandle;
}

bool THandleOpsQueue::HasUnconfirmedCreate(ui64 handle) const
{
    return UnconfirmedCreates.contains(handle);
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
