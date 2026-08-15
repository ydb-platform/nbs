#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/vfs_fuse/protos/queue_entry.pb.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer.h>

#include <util/generic/hash_set.h>

#include <memory>
#include <optional>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

// Not thread safe: the caller serializes all access (TFileSystem does it with
// HandleOpsQueueLock).
class THandleOpsQueue
{
private:
    TFileRingBuffer RequestsToProcess;
    // Handles of queued create entries whose ConfirmCreateHandle has not run
    // yet. Mirrors the ring buffer contents; rebuilt from the file on restart.
    THashSet<ui64> UnconfirmedCreates;
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
        const NProto::TCreateHandleRequest& request,
        ui64 nodeId,
        ui64 handle,
        ui64 originalRequestId);
    EResult AddDestroyRequest(ui64 nodeId, ui64 handle);
    std::optional<NProto::TQueueEntry> Front();

    // Returns the handle confirmed by the popped entry, if it was a create.
    std::optional<ui64> PopFront();

    ui64 Size() const;
    bool Empty() const;

    // Whether the queue still holds an unprocessed create confirmation for
    // the handle.
    bool HasUnconfirmedCreate(ui64 handle) const;
};

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueuePtr CreateHandleOpsQueue(const TString& filePath, ui32 size);

}   // namespace NCloud::NFileStore::NFuse
