#include "handle_ops_queue.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MAX_ENTRY_SIZE = 8;

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueue::THandleOpsQueue(const TString& filePath, ui32 size)
    :RequestsToProcess(filePath, size, MAX_ENTRY_SIZE)
{
}


bool THandleOpsQueue::AddDestroyRequest(ui64 nodeId, ui64 handle)
{
    NProto::TQueueEntry request;
    request.MutableDestroyHandleRequest()->SetHandle(handle);
    request.MutableDestroyHandleRequest()->SetNodeId(nodeId);

    TString result;
    Y_PROTOBUF_SUPPRESS_NODISCARD request.SerializeToString(&result);

    return RequestsToProcess.Push(result);
}

NProto::TQueueEntry THandleOpsQueue::Front()
{
    NProto::TQueueEntry entry;
    if (!entry.ParseFromString(TString(RequestsToProcess.Front()))) {
        // TODO: corruption?
    }

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
