#include "describe_volume.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
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

#include <util/datetime/base.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCellHostInfo
{
    TString Fqdn;
    IBlockStorePtr Client;
};

struct TCellInfo
{
    const TString CellId;
    const bool StrictCellIdCheckInDescribe = false;
    TVector<TCellHostInfo> Hosts;

    // TODO: align to avoid false sharing
    TVector<NProto::TError> DescribeResults;

    TCellInfo(
            TString cellId,
            bool strictCellIdCheckInDescribe,
            ui32 clientCount)
        : CellId(std::move(cellId))
        , StrictCellIdCheckInDescribe(strictCellIdCheckInDescribe)
        , DescribeResults(clientCount)
    {
        Hosts.reserve(clientCount);
    }
};

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

    TAdaptiveLock Lock;
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

    auto req = std::make_shared<NProto::TDescribeVolumeRequest>();
    req->CopyFrom(Request);
    if (Cell.CellId) {
        auto& headers = *req->MutableHeaders();
        headers.ClearInternal();
        headers.SetCellId(Cell.CellId);
    }

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
    if (!HasError(response)) {
        if (!Cell.StrictCellIdCheckInDescribe ||
            Cell.CellId == response.GetCellId())
        {
            response.SetCellId(Cell.CellId);
            owner->Reply(std::move(response));
            STORAGE_DEBUG(
                TStringBuilder()
                    << "DescribeVolume: got success for disk "
                    << Request.GetDiskId().Quote() << " from " << HostInfo.Fqdn);
            return;
        }

        const auto* msg = "DescribeVolume response cell id mismatch";

        ReportWrongCellIdInDescribeVolume(
            msg,
            {{"expected", Cell.CellId}, {"actual", response.GetCellId()}});

        *response.MutableError() = MakeError(E_REJECTED, msg);
    }

    STORAGE_DEBUG(
        TStringBuilder() << "DescribeVolume: got error "
            << response.GetError().GetMessage().Quote()
            << " from "
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
            cellIt->second->GetStrictCellIdCheckInDescribeVolume(),
            clients.size());
        for (const auto& client: clients) {
            cell.Hosts.emplace_back(client.GetLogTag(), client.GetService());
        }
        cells.emplace_back(std::move(cell));
    }

    TCellInfo localCell("", false, 1);
    localCell.Hosts.emplace_back("local", service);
    cells.emplace_back(std::move(localCell));

    auto describeHandler = std::make_shared<TMultiCellDescribeHandler>(
        bootstrap.Scheduler,
        bootstrap.Logging->CreateLog("BLOCKSTORE_CELLS"),
        std::move(cells),
        std::move(request),
        hasUnavailableCells);
    return describeHandler->Start(config.GetDescribeVolumeTimeout());
}

}   // namespace NCloud::NBlockStore::NCells
