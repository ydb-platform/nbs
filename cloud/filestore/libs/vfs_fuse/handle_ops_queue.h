#pragma once

#include "public.h"

#include <cloud/filestore/libs/vfs_fuse/protos/queue_entry.pb.h>

#include <cloud/storage/core/libs/common/file_ring_buffer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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

    THandleOpsQueue(
        const TString& filePath,
        ui32 size,
        TLog log,
        TString logTags);

    EResult AddDestroyRequest(ui64 nodeId, ui64 handle);
    std::optional<NProto::TQueueEntry> Front();
    void PopFront();
    ui64 Size() const;
    bool Empty() const;
};

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueuePtr CreateHandleOpsQueue(
    const TString& filePath,
    ui32 size,
    TLog log,
    TString logTag);

}   // namespace NCloud::NFileStore::NFuse
