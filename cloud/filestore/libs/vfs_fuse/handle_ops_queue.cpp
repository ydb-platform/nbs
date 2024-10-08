#include "handle_ops_queue.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueue::THandleOpsQueue(const TString& filePath, ui32 size)
    : RequestsToProcess(filePath, size)
{
}

bool THandleOpsQueue::AddDestroyRequest(ui64 nodeId, ui64 handle)
{
    NProto::TQueueEntry request;
    request.MutableDestroyHandleRequest()->SetHandle(handle);
    request.MutableDestroyHandleRequest()->SetNodeId(nodeId);

    TString result;
    Y_UNUSED(request.SerializeToString(&result));

    return RequestsToProcess.Push(result);
}

NProto::TQueueEntry THandleOpsQueue::Front()
{
    const auto req = RequestsToProcess.Front();

    NProto::TQueueEntry entry;
    Y_UNUSED(entry.ParseFromArray(req.data(), req.size()));

    return entry;
}

bool THandleOpsQueue::Empty() const
{
    return RequestsToProcess.Empty();
}

void THandleOpsQueue::Pop()
{
    RequestsToProcess.Pop();
}

ui64 THandleOpsQueue::Size() const
{
    return RequestsToProcess.Size();
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<THandleOpsQueue> CreateHandleOpsQueue(
    const TString& filePath,
    ui32 size)
{
    return std::make_unique<THandleOpsQueue>(filePath, size);
}

}   // namespace NCloud::NFileStore::NFuse
