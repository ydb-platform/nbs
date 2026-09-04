#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/vfs_fuse/protos/queue_entry.pb.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class THandleOpsQueue
{
private:
    TFileRingBuffer RequestsToProcess;
    std::shared_ptr<class THandleOpsQueueStats> Stats;

public:
    enum class EResult
    {
        Ok,
        QueueOverflow,
        SerializationError,
    };

    explicit THandleOpsQueue(const TString& filePath, ui32 size);

    IModuleStatsPtr GetModuleStats() const;
    EResult AddCreateRequest(
        ui64 nodeId,
        ui64 handle,
        ui32 flags,
        ui64 originalRequestId);
    EResult AddDestroyRequest(ui64 nodeId, ui64 handle);
    std::optional<NProto::TQueueEntry> Front();
    TVector<std::optional<NProto::TQueueEntry>> Front(ui32 count);
    void PopFront();
    void PopFront(ui32 count);
    ui64 Size() const;
    bool Empty() const;
};

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueuePtr CreateHandleOpsQueue(const TString& filePath, ui32 size);

}   // namespace NCloud::NFileStore::NFuse
