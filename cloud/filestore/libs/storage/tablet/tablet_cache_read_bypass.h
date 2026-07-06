#pragma once

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief Tracks cache reads that must go to the database while unconfirmed data
 * is not fully reflected in in-memory caches.
 */
class TCacheReadBypass
{
private:
    struct TActiveWrite
    {
        ui64 CommitId = 0;
        // Affected byte range, End is exclusive. Writes that may change the
        // file size are registered with End == Max<ui64>(), because cached
        // file sizes become stale for all offsets starting from the old file
        // size.
        ui64 Begin = 0;
        ui64 End = 0;
    };

public:
    void UpdateLogTag(TString logTag);

    void Activate(ui64 nodeId, ui64 commitId, ui64 begin, ui64 end);

    void Deactivate(ui64 nodeId, ui64 commitId);

    void SetUnconfirmedRecoveryReady(bool unconfirmedRecoveryReady);

    // Checks reads that are not bound to a byte range (e.g. node attrs
    // containing file size). Such reads bypass the cache if any active write
    // is visible to them.
    bool ShouldBypassRead(ui64 nodeId, ui64 commitId) const;

    // Checks reads bound to the [begin, end) byte range. Such reads bypass
    // the cache only if some visible active write intersects the range.
    bool ShouldBypassRead(ui64 nodeId, ui64 commitId, ui64 begin, ui64 end)
        const;

    ui64 GetBypassedNodeReadCount() const;
    ui64 GetBypassedRangeReadCount() const;

private:
    bool ShouldBypassReadImpl(ui64 nodeId, ui64 commitId, ui64 begin, ui64 end)
        const;

private:
    TString LogTag;
    bool UnconfirmedRecoveryReady = false;
    THashMap<ui64, TDeque<TActiveWrite>> ActiveWritesByNodeId;
    mutable ui64 BypassedNodeReadCount = 0;
    mutable ui64 BypassedRangeReadCount = 0;
};

}   // namespace NCloud::NFileStore::NStorage
