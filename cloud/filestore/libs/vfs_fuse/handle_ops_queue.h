#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/vfs_fuse/protos/queue_entry.pb.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer.h>

#include <util/generic/vector.h>

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

    struct TFrontResult
    {
        TVector<std::optional<NProto::TQueueEntry>> Entries;
        // Set if the underlying ring buffer is corrupted. Entries collected
        // before the corruption was detected are still returned.
        NCloud::NProto::TError Error;
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
    TFrontResult Front(ui32 count);
    void PopFront();
    void PopFront(ui32 count);
    ui64 Size() const;
    bool Empty() const;
};

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueuePtr CreateHandleOpsQueue(const TString& filePath, ui32 size);

}   // namespace NCloud::NFileStore::NFuse
