#include "handle_ops_queue.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueue::THandleOpsQueue(const TString& filePath, ui32 size)
    : RequestsToProcess(filePath, size)
{}

THandleOpsQueue::EResult THandleOpsQueue::AddDestroyRequest(
    ui64 nodeId,
    ui64 handle)
{
    NProto::TQueueEntry request;
    request.MutableDestroyHandleRequest()->SetHandle(handle);
    request.MutableDestroyHandleRequest()->SetNodeId(nodeId);

    TString result;
    if (!request.SerializeToString(&result)) {
        return THandleOpsQueue::EResult::SerializationError;
    }

    if (!RequestsToProcess.PushBack(result)) {
        return THandleOpsQueue::EResult::QueueOverflow;
    }

    return THandleOpsQueue::EResult::Ok;
}

std::optional<NProto::TQueueEntry> THandleOpsQueue::Front()
{
    const auto req = RequestsToProcess.Front();

    NProto::TQueueEntry entry;
    if (!entry.ParseFromArray(req.data(), req.size())) {
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
