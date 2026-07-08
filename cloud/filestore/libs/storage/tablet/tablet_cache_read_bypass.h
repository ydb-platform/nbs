#pragma once

#include <cloud/storage/core/libs/common/byte_range.h>

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
        // Affected byte range. Writes that may change the file size are
        // registered with Range.End() == Max<ui64>(), because cached file
        // sizes become stale for all offsets starting from the old file size.
        TByteRange Range;
    };

public:
    void UpdateLogTag(TString logTag);

    void Activate(ui64 nodeId, ui64 commitId, const TByteRange& range);

    void Deactivate(ui64 nodeId, ui64 commitId);

    void SetUnconfirmedRecoveryReady(bool unconfirmedRecoveryReady);

    // Checks reads that are not bound to a byte range (e.g. node attrs
    // containing file size). Such reads bypass the cache if any active write
    // is visible to them.
    bool ShouldBypassRead(ui64 nodeId, ui64 commitId) const;

    // Checks reads bound to a byte range. Such reads bypass the cache only if
    // some visible active write intersects the range.
    bool ShouldBypassRead(ui64 nodeId, ui64 commitId, const TByteRange& range)
        const;

    ui64 GetBypassedNodeReadCount() const;
    ui64 GetBypassedRangeReadCount() const;

private:
    // range == nullptr checks reads of the whole node.
    bool ShouldBypassReadImpl(
        ui64 nodeId,
        ui64 commitId,
        const TByteRange* range) const;

private:
    TString LogTag;
    bool UnconfirmedRecoveryReady = false;
    THashMap<ui64, TDeque<TActiveWrite>> ActiveWritesByNodeId;
    mutable ui64 BypassedNodeReadCount = 0;
    mutable ui64 BypassedRangeReadCount = 0;
};

}   // namespace NCloud::NFileStore::NStorage
