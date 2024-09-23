#include "handle_ops_queue.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

void THandleOpsQueue::AddDestroyRequest(ui64 nodeId, ui64 handle)
{
    NProto::TQueueEntry request;
    request.MutableDestroyHandleRequest()->SetHandle(handle);
    request.MutableDestroyHandleRequest()->SetNodeId(nodeId);
    Requests.push(request);
}

const NProto::TQueueEntry& THandleOpsQueue::Front()
{
    return Requests.front();
}

bool THandleOpsQueue::Empty() const
{
    return Requests.empty();
}

void THandleOpsQueue::Pop()
{
    Requests.pop();
}

ui64 THandleOpsQueue::Size() const
{
    return Requests.size();
}

}   // namespace NCloud::NFileStore::NFuse
