#include "create_destroy_queue.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

void TCreateDestroyQueue::AddDestroyRequest(ui64 nodeId, ui64 handle)
{
     NProto::TQueueEntry request;
     request.MutableDestroyHandleRequest()->SetHandle(handle);
     request.MutableDestroyHandleRequest()->SetNodeId(nodeId);
     Requests.push(request);
}

NProto::TQueueEntry TCreateDestroyQueue::GetNext()
{
     return Requests.front();
}

bool TCreateDestroyQueue::Empty() const
{
     return Requests.empty();
}

void TCreateDestroyQueue::Remove()
{
     Requests.pop();
}

ui64 TCreateDestroyQueue::Size()
{
     return Requests.size();
}

}   // namespace NCloud::NFileStore::NFuse
