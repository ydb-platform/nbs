#include "inbound_activity.h"

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

void TCellInboundActivity::Record(
    const TString& cellId,
    const TString& peer,
    const TString& diskId,
    const TString& clientId,
    EBlockStoreRequest request,
    TInstant now)
{
    auto key = TStringBuilder()
        << cellId << "|" << peer << "|" << diskId << "|" << clientId;

    with_lock (Lock) {
        auto& row = Rows[key];
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
        EraseNodesIf(
            Rows,
            [&](const auto& kv) { return now - kv.second.LastSeen > Ttl; });

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
