#include "describe_volume.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/wait/wait.h>

#include <util/datetime/base.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString LocalDescribeLabel{"local"};

////////////////////////////////////////////////////////////////////////////////

struct TCellHostInfo
{
    TString Fqdn;
    IBlockStorePtr Client;
};

struct TCellInfo
{
    const TString CellId;
    TVector<TCellHostInfo> Hosts;

    // TODO: align to avoid false sharing
    TVector<NProto::TError> DescribeResults;

    TCellInfo(
            TString cellId,
            ui32 clientCount)
        : CellId(std::move(cellId))
        , DescribeResults(clientCount)
    {
        Hosts.reserve(clientCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

// A describe carrying a CellId reaches one cell; an empty CellId means the
// local service. The inter-cell request never inherits the caller's Internal.
std::shared_ptr<NProto::TDescribeVolumeRequest> PrepareCellDescribeRequest(
    const NProto::TDescribeVolumeRequest& request,
    const TString& cellId)
{
    auto req = std::make_shared<NProto::TDescribeVolumeRequest>(request);
    auto& headers = *req->MutableHeaders();
    if (cellId) {
        headers.ClearInternal();
        headers.SetCellId(cellId);
    } else {
        headers.SetCellId(LocalDescribeLabel);
    }
    return req;
}

// During a cross-cell migration the destination copy also answers a describe;
// it carries the source disk id tag and is not the volume a client should use.
bool IsMigrationDestinationResponse(
    const NProto::TDescribeVolumeResponse& response)
{
    return !HasError(response) &&
           response.GetVolume().GetTags().contains(SourceDiskIdTagName);
}

////////////////////////////////////////////////////////////////////////////////

struct TMultiCellDescribeHandler;

class TDescribeResponseHandler
    : public std::enable_shared_from_this<TDescribeResponseHandler>
{
    const std::weak_ptr<TMultiCellDescribeHandler> Owner;
    const TCellHostInfo HostInfo;
    const ui32 CellResultIndex;
    TCellInfo& Cell;
    NProto::TDescribeVolumeRequest Request;
    TLog Log;

    TFuture<NProto::TDescribeVolumeResponse> Future;

public:
    TDescribeResponseHandler(
        TLog log,
        std::weak_ptr<TMultiCellDescribeHandler> owner,
        TCellHostInfo hostInfo,
        ui32 cellResultIndex,
        TCellInfo& cell,
        NProto::TDescribeVolumeRequest request);

    void Start();

private:
    void HandleResponse(const auto& future);
};

////////////////////////////////////////////////////////////////////////////////

struct TMultiCellDescribeHandler
    : public std::enable_shared_from_this<TMultiCellDescribeHandler>
{
    const ISchedulerPtr Scheduler;
    TLog Log;
    std::atomic<ui64> RequestCount{0};
    TVector<TCellInfo> Cells;
    NProto::TDescribeVolumeRequest Request;
    bool HasUnavailableCells;

    TPromise<NProto::TDescribeVolumeResponse> Promise;
    TVector<std::shared_ptr<TDescribeResponseHandler>> Handlers;

public:
    TMultiCellDescribeHandler(
        ISchedulerPtr scheduler,
        TLog log,
        TVector<TCellInfo> cells,
        NProto::TDescribeVolumeRequest request,
        bool hasUnavailableCells);

    TFuture<NProto::TDescribeVolumeResponse> Start(TDuration describeTimeout);
    void HandleResponse(NProto::TDescribeVolumeResponse response);
    void Reply(NProto::TDescribeVolumeResponse response);

private:
    void HandleTimeout();
};

////////////////////////////////////////////////////////////////////////////////

TMultiCellDescribeHandler::TMultiCellDescribeHandler(
        ISchedulerPtr scheduler,
        TLog log,
        TVector<TCellInfo> cells,
        NProto::TDescribeVolumeRequest request,
        bool hasUnavailableCells)
    : Scheduler(std::move(scheduler))
    , Log(std::move(log))
    , Cells(std::move(cells))
    , Request(std::move(request))
    , HasUnavailableCells(hasUnavailableCells)
    , Promise(NewPromise<NProto::TDescribeVolumeResponse>())
{
    for (const auto& cell: Cells) {
        RequestCount += cell.Hosts.size();
    }
}

TFuture<NProto::TDescribeVolumeResponse> TMultiCellDescribeHandler::Start(
    TDuration describeTimeout)
{
    auto weak = weak_from_this();
    for (auto& cell: Cells) {
        ui32 hostIndex = 0;
        for (auto& host: cell.Hosts) {
            if (!cell.CellId.empty()) {
                STORAGE_DEBUG(
                    TStringBuilder()
                    << "Send remote Describe Request to " << host.Fqdn
                    << " for volume " << Request.GetDiskId());
            } else {
                STORAGE_DEBUG(
                    TStringBuilder()
                    << "Send local Describe Request for volume "
                    << Request.GetDiskId());
            }

            auto handler = std::make_shared<TDescribeResponseHandler>(
                Log,
                weak,
                host,
                hostIndex,
                cell,
                Request);

            ++hostIndex;

            handler->Start();
            Handlers.push_back(std::move(handler));
        }
    }

    Scheduler->Schedule(
        TInstant::Now() + describeTimeout,
        [weak = std::move(weak)]() {
            if (auto self = weak.lock(); self) {
                self->HandleTimeout();
            }
        });

    Promise.GetFuture().Subscribe(
        [handler = shared_from_this()](const auto&) mutable
        { handler.reset(); });

    return Promise.GetFuture();
}

void TMultiCellDescribeHandler::Reply(NProto::TDescribeVolumeResponse response)
{
    Promise.TrySetValue(std::move(response));
}

void TMultiCellDescribeHandler::HandleTimeout()
{
    NProto::TDescribeVolumeResponse response;
    *response.MutableError() =
        std::move(MakeError(E_REJECTED, "Describe timeout"));
    Reply(std::move(response));
}

void TMultiCellDescribeHandler::HandleResponse(
    NProto::TDescribeVolumeResponse response)
{
    auto now = RequestCount.fetch_sub(1, std::memory_order_acq_rel) - 1;
    if (now == 0) {
        // If we ended up in that place, then all cells have responded
        // with either fatal or retriable errors. If there is at least one
        // cell that responded with a retriable errors only, it’s possible that
        // the volume exists but is currently unavailable. In that case, we
        // return a retriable error so that the user can retry the
        // endpoint start operation. Otherwise, the volume is not present in
        // any cell, and we can return any non-retriable error.
        for (auto& cell: Cells) {
            const bool allRetriable = std::all_of(
                cell.DescribeResults.begin(),
                cell.DescribeResults.end(),
                [] (const auto& result) {
                    return EErrorKind::ErrorRetriable == GetErrorKind(result);
                });
            if (allRetriable) {
                HasUnavailableCells = true;
                break;
            }
        }
        if (HasUnavailableCells) {
            *response.MutableError() =
                std::move(MakeError(E_REJECTED, "Not all cells available"));
            Reply(std::move(response));
            return;
        }

        *response.MutableError() =
            std::move(MakeError(
                E_NOT_FOUND,
                TStringBuilder()
                    << "Volume "
                    << Request.GetDiskId().Quote()
                    << " not found in cells"));
        Reply(std::move(response));
    }
}

////////////////////////////////////////////////////////////////////////////////

TDescribeResponseHandler::TDescribeResponseHandler(
        TLog log,
        std::weak_ptr<TMultiCellDescribeHandler> owner,
        TCellHostInfo hostInfo,
        ui32 cellResultIndex,
        TCellInfo& cell,
        NProto::TDescribeVolumeRequest request)
    : Owner(std::move(owner))
    , HostInfo(std::move(hostInfo))
    , CellResultIndex(cellResultIndex)
    , Cell(cell)
    , Request(std::move(request))
    , Log(std::move(log))
{}

void TDescribeResponseHandler::Start()
{
    auto callContext = MakeIntrusive<TCallContext>();
    auto req = PrepareCellDescribeRequest(Request, Cell.CellId);

    auto weak = weak_from_this();
    Future = HostInfo.Client->DescribeVolume(callContext, std::move(req));
    Future.Subscribe(
        [weak = std::move(weak)](const auto& future)
        {
            if (auto self = weak.lock(); self != nullptr) {
                self->HandleResponse(future);
            }
        });
}

void TDescribeResponseHandler::HandleResponse(const auto& future)
{
    auto owner = Owner.lock();
    if (!owner) {
        return;
    }

    if (owner->Promise.HasValue()) {
        return;
    }
    auto response = future.GetValue();
    if (IsMigrationDestinationResponse(response)) {
        STORAGE_DEBUG(
            TStringBuilder() << "DescribeVolume: got response for disk "
                             << Request.GetDiskId().Quote()
                             << " with source disk id "
                             << response.GetVolume()
                                    .GetTags()
                                    .at(SourceDiskIdTagName)
                                    .Quote()
                             << " from " << HostInfo.Fqdn
                             << " but it will be ignored since volume has "
                                "source disk id tag");

        const auto* msg =
            "DescribeVolume response ignored since volume has source disk "
            "id tag";
        *response.MutableError() = MakeError(E_NOT_FOUND, msg);
    } else if (!HasError(response)) {
        STORAGE_DEBUG(
            TStringBuilder() << "DescribeVolume: got success for disk "
                             << Request.GetDiskId().Quote() << " from "
                             << HostInfo.Fqdn);
        response.SetCellId(Cell.CellId);
        owner->Reply(std::move(response));
        return;
    }

    STORAGE_DEBUG(
        TStringBuilder() << "DescribeVolume: got error "
                         << response.GetError().GetMessage().Quote() << " from "
                         << HostInfo.Fqdn);

    if (EErrorKind::ErrorRetriable != GetErrorKind(response.GetError())) {
        auto code = response.GetError().GetCode();
        const bool volumeNotFoundError =
            code == E_NOT_FOUND ||
            code ==
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist);
        Y_DEBUG_ABORT_UNLESS(volumeNotFoundError);
    }
    Cell.DescribeResults[CellResultIndex] = std::move(response.GetError());

    owner->HandleResponse(std::move(response));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDescribeVolumeFuture DescribeVolume(
    const TCellsConfig& config,
    NProto::TDescribeVolumeRequest request,
    IBlockStorePtr service,
    const TCellHostEndpointsByCellId& endpoints,
    bool hasUnavailableCells,
    TBootstrap bootstrap)
{
    TVector<TCellInfo> cells;

    for (const auto& [cellId, clients]: endpoints) {
        const auto cellIt = config.GetCells().find(cellId);
        if (cellIt == config.GetCells().end()) {
            NProto::TDescribeVolumeResponse response;
            *response.MutableError() = MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Cell " << cellId << " is not found in config");
            return MakeFuture(response);
        }

        TCellInfo cell(
            cellId,
            clients.size());
        for (const auto& client: clients) {
            cell.Hosts.emplace_back(client.GetLogTag(), client.GetService());
        }
        cells.emplace_back(std::move(cell));
    }

    TCellInfo localCell("", 1);
    localCell.Hosts.emplace_back(LocalDescribeLabel, service);
    cells.emplace_back(std::move(localCell));

    auto describeHandler = std::make_shared<TMultiCellDescribeHandler>(
        bootstrap.Scheduler,
        bootstrap.Logging->CreateLog("BLOCKSTORE_CELLS"),
        std::move(cells),
        std::move(request),
        hasUnavailableCells);
    return describeHandler->Start(config.GetDescribeVolumeTimeout());
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsMoreAuthoritative(ECellDescribeStatus candidate, ECellDescribeStatus current)
{
    // ECellDescribeStatus is declared from the most to the least authoritative
    return candidate < current;
}

ECellDescribeStatus ClassifyResponse(
    const NProto::TDescribeVolumeResponse& response)
{
    if (IsMigrationDestinationResponse(response)) {
        return ECellDescribeStatus::NotFound;
    }
    if (!HasError(response)) {
        return ECellDescribeStatus::Found;
    }

    const auto code = response.GetError().GetCode();
    if (code == E_NOT_FOUND ||
        code == MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist))
    {
        return ECellDescribeStatus::NotFound;
    }
    return ECellDescribeStatus::Failed;
}

}   // namespace

TVector<TCellDescribeResult> DescribeVolumeForMonitoring(
    NProto::TDescribeVolumeRequest request,
    const TVector<TString>& cellIds,
    const TCellHostEndpointsByCellId& endpoints,
    IBlockStorePtr localService,
    TDuration timeout)
{
    struct TPending
    {
        ui32 ResultIndex;
        TString Fqdn;
        TFuture<NProto::TDescribeVolumeResponse> Future;
    };

    // give the fired RPCs the same deadline as our wait, so they do not
    // outlive the mon response and pile up during a degradation; every copy
    // made by PrepareCellDescribeRequest inherits it
    request.MutableHeaders()->SetRequestTimeout(timeout.MilliSeconds());

    TVector<TCellDescribeResult> results;
    TVector<TPending> pending;
    TVector<TFuture<void>> inflight;

    auto fire = [&] (ui32 resultIndex,
                     const TString& cellId,
                     const TString& fqdn,
                     const IBlockStorePtr& client)
    {
        auto future = client->DescribeVolume(
            MakeIntrusive<TCallContext>(),
            PrepareCellDescribeRequest(request, cellId));
        inflight.push_back(future.IgnoreResult());
        pending.push_back({resultIndex, fqdn, std::move(future)});
    };

    // every cell starts Unavailable; the first real answer from any of its
    // hosts is always more authoritative and takes over
    for (const auto& cellId: cellIds) {
        ui32 index = results.size();
        auto& result = results.emplace_back();
        result.CellId = cellId;
        result.Status = ECellDescribeStatus::Unavailable;

        auto it = endpoints.find(cellId);
        if (it == endpoints.end()) {
            continue;
        }
        for (const auto& client: it->second) {
            fire(index, cellId, client.GetFqdn(), client.GetService());
        }
    }

    if (localService) {
        ui32 index = results.size();
        auto& result = results.emplace_back();
        // CellId left empty to mark the local row
        result.Status = ECellDescribeStatus::Unavailable;
        fire(index, {}, FQDNHostName(), localService);
    }

    WaitAll(inflight).Wait(timeout);

    for (auto& p: pending) {
        auto& result = results[p.ResultIndex];

        ECellDescribeStatus status;
        TString fqdn;
        NProto::TError error;
        if (!p.Future.HasValue()) {
            status = ECellDescribeStatus::Failed;
            error = MakeError(E_TIMEOUT, "describe timed out");
        } else {
            auto response = p.Future.GetValue();
            status = ClassifyResponse(response);
            if (status == ECellDescribeStatus::Found) {
                fqdn = p.Fqdn;
            } else {
                error = response.GetError();
            }
        }

        if (IsMoreAuthoritative(status, result.Status)) {
            result.Status = status;
            result.Fqdn = std::move(fqdn);
            result.Error = std::move(error);
        }
    }

    return results;
}

}   // namespace NCloud::NBlockStore::NCells
