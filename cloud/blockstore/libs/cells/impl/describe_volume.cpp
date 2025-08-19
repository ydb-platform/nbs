#include "describe_volume.h"

#include <cloud/blockstore/libs/client/config.h>
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
    TAdaptiveLock Lock;
    NProto::TError VolumeNotFoundError;
    NProto::TError RetriableError;
    TVector<TCellHostInfo> Hosts;
};

using TCellByCellId = THashMap<TString, TCellInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TMultiCellDescribeHandler;

class TDescribeResponseHandler
    : public std::enable_shared_from_this<TDescribeResponseHandler>
{
    const std::weak_ptr<TMultiCellDescribeHandler> Owner;
    const TString CellId;
    const TCellHostInfo HostInfo;
    NProto::TDescribeVolumeRequest Request;
    TCellInfo& Cell;

    TLog Log;
    TFuture<NProto::TDescribeVolumeResponse> Future;

public:
    TDescribeResponseHandler(
        TLog log,
        std::weak_ptr<TMultiCellDescribeHandler> owner,
        TString cellId,
        TCellHostInfo hostInfo,
        NProto::TDescribeVolumeRequest request,
        TCellInfo& cell);

    void Start();

private:
    void HandleResponse(const auto& future);
};

////////////////////////////////////////////////////////////////////////////////

struct TMultiCellDescribeHandler
    : public std::enable_shared_from_this<TMultiCellDescribeHandler>
{
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    TLog Log;
    std::atomic<ui64> RequestCount{0};
    TCellByCellId Cells;
    NProto::TDescribeVolumeRequest Request;
    bool HasUnavailableCells;

    TAdaptiveLock Lock;
    TPromise<NProto::TDescribeVolumeResponse> Promise;
    TVector<std::shared_ptr<TDescribeResponseHandler>> Handlers;

public:
    TMultiCellDescribeHandler(
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        TCellByCellId cells,
        NProto::TDescribeVolumeRequest request,
        bool hasUnavailableCells);

    void Start(TDuration describeTimeout);
    void HandleResponse(NProto::TDescribeVolumeResponse response);
    void Reply(NProto::TDescribeVolumeResponse response);

private:
    void HandleTimeout();
};

////////////////////////////////////////////////////////////////////////////////

TMultiCellDescribeHandler::TMultiCellDescribeHandler(
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        TCellByCellId cells,
        NProto::TDescribeVolumeRequest request,
        bool hasUnavailableCells)
    : Scheduler(std::move(scheduler))
    , Logging(std::move(logging))
    , Log(Logging->CreateLog("BLOCKSTORE_CELLS"))
    , Cells(std::move(cells))
    , Request(std::move(request))
    , HasUnavailableCells(hasUnavailableCells)
    , Promise(NewPromise<NProto::TDescribeVolumeResponse>())
{
    for (const auto& cell: Cells) {
        RequestCount += cell.second.Hosts.size();
    }
}

void TMultiCellDescribeHandler::Start(TDuration describeTimeout)
{
    auto weak = weak_from_this();
    for (auto& [cellId, cell]: Cells) {
        for (auto& host: cell.Hosts) {
            if (!cellId.empty()) {
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
                Logging->CreateLog("BLOCKSTORE_CELLS"),
                weak,
                cellId,
                host,
                Request,
                cell);

            handler->Start();

            Handlers.push_back(std::move(handler));
        }
    }

    auto self = shared_from_this();
    Scheduler->Schedule(
        TInstant::Now() + describeTimeout,
        [self = std::move(self)]() { self->HandleTimeout(); });
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
        // cell that responded with a retriable error, it’s possible that
        // the volume exists but is currently unavailable. In that case, we
        // return a retriable error so that the user can retry the
        // endpoint start operation. Otherwise, the volume is not present in
        // any cell, and we can return any non-retriable error.
        for (auto& cell: Cells) {
            auto& s = cell.second;
            if (!HasError(s.VolumeNotFoundError)) {
                *response.MutableError() = std::move(s.RetriableError);
                Reply(std::move(response));
                return;
            }
        }
        if (HasUnavailableCells) {
            *response.MutableError() =
                std::move(MakeError(E_REJECTED, "Not all cells available"));
            Reply(std::move(response));
            return;
        }
        Reply(std::move(response));
    }
}

////////////////////////////////////////////////////////////////////////////////

TDescribeResponseHandler::TDescribeResponseHandler(
        TLog log,
        std::weak_ptr<TMultiCellDescribeHandler> owner,
        TString cellId,
        TCellHostInfo hostInfo,
        NProto::TDescribeVolumeRequest request,
        TCellInfo& cell)
    : Owner(std::move(owner))
    , CellId(std::move(cellId))
    , HostInfo(std::move(hostInfo))
    , Request(std::move(request))
    , Cell(cell)
    , Log(std::move(log))
{}

void TDescribeResponseHandler::Start()
{
    auto callContext = MakeIntrusive<TCallContext>();

    auto req = std::make_shared<NProto::TDescribeVolumeRequest>();
    req->CopyFrom(Request);
    if (CellId) {
        req->MutableHeaders()->ClearInternal();
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

    auto& Log = owner->Log;

    if (owner->Promise.HasValue()) {
        return;
    }
    auto response = future.GetValue();
    if (!HasError(response)) {
        response.SetCellId(CellId);
        owner->Reply(std::move(response));
        STORAGE_DEBUG(
            TStringBuilder()
            << "DescribeVolume: got success for disk " << Request.GetDiskId()
            << " from " << HostInfo.Fqdn);
        return;
    }

    STORAGE_DEBUG(
        TStringBuilder() << "DescribeVolume: got error "
                         << response.GetError().GetMessage().Quote() << " from "
                         << HostInfo.Fqdn);

    with_lock (Cell.Lock) {
        if (EErrorKind::ErrorRetriable == GetErrorKind(response.GetError())) {
            Cell.RetriableError = std::move(response.GetError());
        } else {
            auto code = response.GetError().GetCode();
            const bool volumeNotFoundError =
                code == E_NOT_FOUND ||
                code == MAKE_SCHEMESHARD_ERROR(
                            NKikimrScheme::StatusPathDoesNotExist);
            Y_DEBUG_ABORT_UNLESS(volumeNotFoundError);
            Cell.VolumeNotFoundError = std::move(response.GetError());
        }
    }

    owner->HandleResponse(std::move(response));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDescribeVolumeFuture DescribeVolume(
    NProto::TDescribeVolumeRequest request,
    IBlockStorePtr service,
    const TCellHostEndpointsByCellId& endpoints,
    bool hasUnavailableCells,
    TDuration timeout,
    TBootstrap bootstrap)
{
    TCellByCellId cells;

    for (const auto& cell: endpoints) {
        TCellInfo s;
        for (const auto& client: cell.second) {
            s.Hosts.emplace_back(client.GetLogTag(), client.GetService());
        }
        cells.emplace(cell.first, std::move(s));
    }

    TCellInfo localCell;
    localCell.Hosts.emplace_back("local", service);
    cells.emplace("", std::move(localCell));

    auto describeResult = std::make_shared<TMultiCellDescribeHandler>(
        bootstrap.Scheduler,
        bootstrap.Logging,
        std::move(cells),
        std::move(request),
        hasUnavailableCells);
    describeResult->Start(timeout);

    return describeResult->Promise.GetFuture();
}

}   // namespace NCloud::NBlockStore::NCells
