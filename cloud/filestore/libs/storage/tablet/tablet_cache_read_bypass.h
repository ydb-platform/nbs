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
public:
    void UpdateLogTag(TString logTag);

    void Activate(ui64 nodeId, ui64 commitId);

    void Deactivate(ui64 nodeId, ui64 commitId);

    void SetUnconfirmedRecoveryReady(bool unconfirmedRecoveryReady);

    bool ShouldBypassRead(ui64 nodeId, ui64 commitId) const;

private:
    TString LogTag;
    bool UnconfirmedRecoveryReady = false;
    THashMap<ui64, TDeque<ui64>> CommitIdsByNodeId;
};

}   // namespace NCloud::NFileStore::NStorage
