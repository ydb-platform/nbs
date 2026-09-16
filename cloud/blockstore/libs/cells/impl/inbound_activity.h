#pragma once

#include <cloud/blockstore/libs/service/request.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Last-seen table of inter-cell control requests that skipped authorization,
// keyed by who they came from. Deliberately not a strict open/close counter:
// an Unmount is not guaranteed (a client can crash, a connection can move),
// so a paired counter would leak. A row is dropped once it has not been seen
// for the TTL, so the table shows who is active now, not who ever was.
class TCellInboundActivity
{
public:
    struct TRow
    {
        TString CellId;
        TString Peer;
        TString DiskId;
        TString ClientId;
        TInstant LastSeen;
        ui64 Mounts = 0;
        ui64 Unmounts = 0;
        ui64 Describes = 0;
    };

    // rows unseen for this long are pruned from a snapshot
    static constexpr TDuration Ttl = TDuration::Minutes(5);

    void Record(
        const TString& cellId,
        const TString& peer,
        const TString& diskId,
        const TString& clientId,
        EBlockStoreRequest request,
        TInstant now);

    // the rows still within the TTL at `now`, most-recently-seen first;
    // prunes the expired ones as a side effect
    [[nodiscard]] TVector<TRow> Snapshot(TInstant now);

private:
    TAdaptiveLock Lock;
    THashMap<TString, TRow> Rows;
};

}   // namespace NCloud::NBlockStore::NCells
