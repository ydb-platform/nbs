#pragma once

#include <cloud/blockstore/libs/service/request.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>
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
// for the TTL - pruned both on a snapshot and, amortized, on record, so the
// table stays bounded even if the mon page is never opened.
class TCellInboundActivity
{
public:
    // who a request came from; the identity a row is keyed by
    struct TKey
    {
        TString CellId;
        TString Peer;
        TString DiskId;
        TString ClientId;

        bool operator==(const TKey& other) const = default;
    };

    struct TKeyHash
    {
        size_t operator()(const TKey& key) const
        {
            return MultiHash(key.CellId, key.Peer, key.DiskId, key.ClientId);
        }
    };

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

    // rows unseen for this long are pruned
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
    THashMap<TKey, TRow, TKeyHash> Rows;
    TInstant LastPruned;

    void PruneLocked(TInstant now);
};

}   // namespace NCloud::NBlockStore::NCells
