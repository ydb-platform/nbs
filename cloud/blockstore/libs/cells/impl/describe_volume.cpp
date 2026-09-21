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

#include <util/system/spinlock.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
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

// During a cross-cell migration the destination copy also answers a describe;
// it carries the source disk id tag and is not the volume a client should use.
bool IsMigrationDestinationResponse(
    const NProto::TDescribeVolumeResponse& response)
{
    return !HasError(response) &&
           response.GetVolume().GetTags().contains(SourceDiskIdTagName);
}

bool IsMoreAuthoritative(
    ECellDescribeStatus candidate,
    ECellDescribeStatus current)
{
    // ECellDescribeStatus is declared from the most to the least authoritative
    return candidate < current;
}

ECellDescribeStatus ClassifyResponse(
    const NProto::TDescribeVolumeResponse& response)
{
    if (IsMigrationDestinationResponse(response)) {
        return ECellDescribeStatus::MigrationDestination;
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

////////////////////////////////////////////////////////////////////////////////

// Collects one describe answer per cell (no first-success short-circuit) and
// completes a promise once all arrive or the deadline hits.
class TCellsSearchHandler
    : public std::enable_shared_from_this<TCellsSearchHandler>
{
    TVector<TCellDescribeResult> Results;
    TVector<ui32> PendingPerResult;
    ui32 Pending = 0;
    bool Completed = false;
    TAdaptiveLock Lock;
    TPromise<TVector<TCellDescribeResult>> Promise =
        NewPromise<TVector<TCellDescribeResult>>();

public:
    explicit TCellsSearchHandler(TVector<TCellDescribeResult> results)
        : Results(std::move(results))
        , PendingPerResult(Results.size(), 0)
    {}

    TFuture<TVector<TCellDescribeResult>> GetFuture()
    {
        return Promise.GetFuture();
    }

    void AddTarget(ui32 resultIndex)
    {
        ++PendingPerResult[resultIndex];
        ++Pending;
    }

    void OnResponse(
        ui32 resultIndex,
        const TString& fqdn,
        const NProto::TDescribeVolumeResponse& response)
    {
        TVector<TCellDescribeResult> results;
        with_lock (Lock) {
            if (Completed) {
                return;
            }
            --PendingPerResult[resultIndex];
            ApplyDescribeResponse(Results[resultIndex], fqdn, response);
            if (--Pending != 0) {
                return;
            }
            Completed = true;
            results = std::move(Results);
        }

        // set the value outside the lock: the future's subscribers run inline
        // here and could otherwise re-enter this handler under the same lock
        Promise.SetValue(std::move(results));
    }

    void OnTimeout()
    {
        TVector<TCellDescribeResult> results;
        with_lock (Lock) {
            if (Completed) {
                return;
            }
            // only cells still awaited time out; unqueried ones stay Unavailable
            for (ui32 i = 0; i < Results.size(); ++i) {
                if (PendingPerResult[i] > 0) {
                    ApplyDescribeTimeout(Results[i]);
                }
            }
            Completed = true;
            results = std::move(Results);
        }

        Promise.SetValue(std::move(results));
    }
};

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

TMonitoringDescribePlan PrepareMonitoringDescribe(
    const TVector<TString>& cellIds,
    const TCellHostEndpointsByCellId& endpoints,
    const IBlockStorePtr& localService)
{
    TMonitoringDescribePlan plan;

    // every cell starts Unavailable; the first real answer from any of its
    // hosts is always more authoritative and takes over
    for (const auto& cellId: cellIds) {
        ui32 index = plan.Results.size();
        auto& result = plan.Results.emplace_back();
        result.CellId = cellId;
        result.Status = ECellDescribeStatus::Unavailable;

        auto it = endpoints.find(cellId);
        if (it == endpoints.end()) {
            continue;
        }
        for (const auto& client: it->second) {
            plan.Targets.push_back(
                {index, cellId, client.GetFqdn(), client.GetService()});
        }
    }

    if (localService) {
        ui32 index = plan.Results.size();
        auto& result = plan.Results.emplace_back();
        // CellId left empty to mark the local row
        result.Status = ECellDescribeStatus::Unavailable;
        plan.Targets.push_back({index, {}, FQDNHostName(), localService});
    }

    return plan;
}

void ApplyDescribeResponse(
    TCellDescribeResult& result,
    const TString& fqdn,
    const NProto::TDescribeVolumeResponse& response)
{
    const auto status = ClassifyResponse(response);
    if (!IsMoreAuthoritative(status, result.Status)) {
        return;
    }

    result.Status = status;
    if (status == ECellDescribeStatus::Found ||
        status == ECellDescribeStatus::MigrationDestination)
    {
        result.Fqdn = fqdn;
        result.Error.Clear();
    } else {
        result.Fqdn.clear();
        result.Error = response.GetError();
    }
}

void ApplyDescribeTimeout(TCellDescribeResult& result)
{
    if (!IsMoreAuthoritative(ECellDescribeStatus::Failed, result.Status)) {
        return;
    }

    result.Status = ECellDescribeStatus::Failed;
    result.Fqdn.clear();
    result.Error = MakeError(E_TIMEOUT, "describe timed out");
}

TFuture<TVector<TCellDescribeResult>> SearchVolumeAcrossCells(
    NProto::TDescribeVolumeRequest request,
    const TVector<TString>& cellIds,
    const TCellHostEndpointsByCellId& endpoints,
    IBlockStorePtr localService,
    TDuration timeout,
    ISchedulerPtr scheduler)
{
    // bound the fired RPCs to the same deadline as our wait
    request.MutableHeaders()->SetRequestTimeout(timeout.MilliSeconds());

    auto plan = PrepareMonitoringDescribe(
        cellIds, endpoints, std::move(localService));

    auto handler =
        std::make_shared<TCellsSearchHandler>(std::move(plan.Results));

    if (plan.Targets.empty()) {
        // nothing to ask (no configured cells / no local service): the results
        // are already final
        handler->OnTimeout();
        return handler->GetFuture();
    }

    for (const auto& target: plan.Targets) {
        handler->AddTarget(target.ResultIndex);
    }

    auto future = handler->GetFuture();
    // keep the handler alive until the promise is set; the callbacks below hold
    // only a weak ref, so a hung describe cannot leak it past the deadline
    future.Subscribe([handler] (const auto&) {});

    auto weak = handler->weak_from_this();
    for (const auto& target: plan.Targets) {
        // a describe can throw on launch (bad endpoint) or hand back a future
        // that carries an exception; either way turn it into one Failed answer
        // for this target so the search still completes and keeps its deadline
        try {
            auto describeFuture = target.Service->DescribeVolume(
                MakeIntrusive<TCallContext>(),
                PrepareCellDescribeRequest(request, target.CellId));

            describeFuture.Subscribe(
                [weak, resultIndex = target.ResultIndex, fqdn = target.Fqdn]
                (const auto& f)
                {
                    auto self = weak.lock();
                    if (!self) {
                        return;
                    }
                    NProto::TDescribeVolumeResponse response;
                    try {
                        response = f.GetValue();
                    } catch (...) {
                        *response.MutableError() =
                            MakeError(E_FAIL, CurrentExceptionMessage());
                    }
                    self->OnResponse(resultIndex, fqdn, response);
                });
        } catch (...) {
            NProto::TDescribeVolumeResponse response;
            *response.MutableError() =
                MakeError(E_FAIL, CurrentExceptionMessage());
            handler->OnResponse(target.ResultIndex, target.Fqdn, response);
        }
    }

    scheduler->Schedule(
        TInstant::Now() + timeout,
        [weak = std::move(weak)]
        {
            if (auto self = weak.lock()) {
                self->OnTimeout();
            }
        });

    return future;
}

}   // namespace NCloud::NBlockStore::NCells
