#include "inbound_activity.h"

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

void TCellInboundActivity::PruneLocked(TInstant now)
{
    EraseNodesIf(
        Rows,
        [&](const auto& kv) { return now - kv.second.LastSeen > Ttl; });
}

void TCellInboundActivity::Record(
    const TString& cellId,
    const TString& peer,
    const TString& diskId,
    const TString& clientId,
    EBlockStoreRequest request,
    TInstant now)
{
    with_lock (Lock) {
        // amortized, at most once per TTL, so the map stays bounded even if
        // the mon page is never opened. A row can linger up to about two TTLs
        // (its own TTL, plus up to a TTL until the next prune runs), which is
        // fine - the point is a bound, not a precise deadline
        if (now - LastPruned >= Ttl) {
            PruneLocked(now);
            LastPruned = now;
        }

        auto& row = Rows[TKey{cellId, peer, diskId, clientId}];
        row.CellId = cellId;
        row.Peer = peer;
        row.DiskId = diskId;
        row.ClientId = clientId;
        row.LastSeen = now;

        switch (request) {
            case EBlockStoreRequest::MountVolume:
                ++row.Mounts;
                break;
            case EBlockStoreRequest::UnmountVolume:
                ++row.Unmounts;
                break;
            case EBlockStoreRequest::DescribeVolume:
                ++row.Describes;
                break;
            default:
                break;
        }
    }
}

TVector<TCellInboundActivity::TRow> TCellInboundActivity::Snapshot(TInstant now)
{
    TVector<TRow> rows;

    with_lock (Lock) {
        PruneLocked(now);

        rows.reserve(Rows.size());
        for (const auto& [key, row]: Rows) {
            Y_UNUSED(key);
            rows.push_back(row);
        }
    }

    SortBy(
        rows,
        [](const TRow& row) { return TInstant::Max() - row.LastSeen; });
    return rows;
}

}   // namespace NCloud::NBlockStore::NCells
