#pragma once

#include "public.h"

#include "file_ring_buffer.h"

#include <cloud/filestore/libs/vfs_fuse/protos/queue_entry.pb.h>

#include <util/generic/queue.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class THandleOpsQueue
{
private:
    TFileRingBuffer RequestsToProcess;

public:
    enum class EResult
    {
        Ok,
        QueueOverflow,
        SerializationError,
    };

    explicit THandleOpsQueue(const TString& filePath, ui32 size);

    EResult AddDestroyRequest(ui64 nodeId, ui64 handle);
    std::optional<NProto::TQueueEntry> Front();
    void Pop();
    ui64 Size() const;
    bool Empty() const;
};

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueuePtr CreateHandleOpsQueue(
    const TString& filePath,
    ui32 size);

}   // namespace NCloud::NFileStore::NFuse
