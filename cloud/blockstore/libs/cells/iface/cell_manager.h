#pragma once

#include "public.h"

#include "connection.h"
#include "inbound_activity.h"
#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/rdma/iface/client.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

using TDescribeVolumeFuture =
    NThreading::TFuture<NProto::TDescribeVolumeResponse>;

// Listed from the most to the least authoritative answer: when a cell's hosts
// disagree, the smaller value wins (a concrete answer beats an inconclusive
// one).
enum class ECellDescribeStatus
{
    Found,          // the cell holds the disk (Fqdn is the host that answered)
    MigrationDestination,   // the cell holds only the migration destination
                            // copy, not a client-usable volume (Fqdn answered)
    NotFound,       // the cell answered, the disk is not in it
    Failed,         // timed out or a transport/other error - result unknown
    Unavailable,    // no connected host to ask the cell
};

// Where a disk was (or was not) found in one cell during a monitoring search.
struct TCellDescribeResult
{
    TMaybe<TString> CellId;   // empty for the local service row
    ECellDescribeStatus Status;
    TString Fqdn;             // the host that answered, when Found
    NProto::TError Error;     // detail behind Failed
};

// A cell host as shown on the outbound-status table.
struct TCellHostStatus
{
    TString Fqdn;
    bool Alive = false;
    bool Warm = false;
    ui32 Connections = 0;
};

// A plain snapshot of the cell manager's live state for the mon page - no
// pools, actors or html.
struct TCellsSnapshot
{
    THashMap<TString, TVector<TCellHostStatus>> HostStatuses;   // by cell id
    TVector<TCellInboundActivity::TRow> InboundActivity;
};

struct ICellManager: public IStartable
{
    TCellsConfigPtr Config;

    explicit ICellManager(TCellsConfigPtr config)
        : Config(std::move(config))
    {}

    [[nodiscard]] virtual TCellConnectionFuture CreateConnection(
        const TString& cellId,
        const TString& fqdn,
        const NClient::TClientAppConfigPtr& clientConfig,
        ICellConnectionObserverPtr observer) = 0;

    [[nodiscard]] virtual TDescribeVolumeFuture DescribeVolume(
        TCallContextPtr callContext,
        const TString& diskId,
        const NProto::THeaders& headers,
        IBlockStorePtr service,
        const NProto::TClientConfig& clientConfig) = 0;

    [[nodiscard]] virtual std::shared_ptr<TCellInboundActivity>
        GetInboundActivity() = 0;

    [[nodiscard]] virtual TCellsSnapshot GetSnapshot() = 0;

    [[nodiscard]] virtual NThreading::TFuture<TVector<TCellDescribeResult>>
        SearchVolume(
            TString diskId,
            IBlockStorePtr localService,
            TDuration timeout) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICellManagerPtr CreateCellManagerStub();

}   // namespace NCloud::NBlockStore::NCells
