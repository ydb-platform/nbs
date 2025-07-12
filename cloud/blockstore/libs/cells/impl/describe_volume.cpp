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

struct THostInfo {
    TString LogTag;
    IBlockStorePtr Client;
};

struct TCell {
    TAdaptiveLock Lock;
    NProto::TError FatalError;
    NProto::TError RetriableError;
    TVector<THostInfo> Hosts;
};

using TCells = THashMap<TString, TCell>;

////////////////////////////////////////////////////////////////////////////////

struct TMultiCellDescribeHandler;

struct TDescribeResponseHandler
{
    const std::weak_ptr<TMultiCellDescribeHandler> Owner;
    const TString CellId;
    const TString LogTag;
    const TString DiskId;
    const TFuture<NProto::TDescribeVolumeResponse> Future;
    TCell& Cell;

    TDescribeResponseHandler(
            std::weak_ptr<TMultiCellDescribeHandler> owner,
            TString cellId,
            TString logTag,
            TString diskId,
            TFuture<NProto::TDescribeVolumeResponse> future,
            TCell& cell);

    void operator ()(const auto& future);

    void Start()
    {
        Future.Subscribe(*this);
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TDescribeVolumeResponse> Describe(
    const IBlockStorePtr& service,
    const NProto::TDescribeVolumeRequest& request,
    bool local)
{
    auto callContext = MakeIntrusive<TCallContext>();

    auto req = std::make_shared<NProto::TDescribeVolumeRequest>();
    req->CopyFrom(request);
    if (!local) {
        req->MutableHeaders()->ClearInternal();
    }

    auto future = service->DescribeVolume(
        callContext,
        std::move(req));
    return future;
}

////////////////////////////////////////////////////////////////////////////////

struct TMultiCellDescribeHandler
    : public std::enable_shared_from_this<TMultiCellDescribeHandler>
{
    const ISchedulerPtr Scheduler;
    TLog Log;
    std::atomic<ui64> Counter{0};
    TCells Cells;
    NProto::TDescribeVolumeRequest Request;
    bool IncompleteCells;

    TAdaptiveLock Lock;
    bool HasValue = false;
    TPromise<NProto::TDescribeVolumeResponse> Promise;
    TVector<TDescribeResponseHandler> Handlers;

    TMultiCellDescribeHandler(
            ISchedulerPtr scheduler,
            TLog log,
            TCells cells,
            NProto::TDescribeVolumeRequest request,
            bool incompleteCells)
        : Scheduler(std::move(scheduler))
        , Log(std::move(log))
        , Cells(std::move(cells))
        , Request(std::move(request))
        , IncompleteCells(incompleteCells)
        , Promise(NewPromise<NProto::TDescribeVolumeResponse>())
    {
        for (const auto& cell: Cells) {
            Counter += cell.second.Hosts.size();
        }
    }

    void DoDescribe(TDuration describeTimeout)
    {
        auto weak = weak_from_this();
        for (auto& cell: Cells) {
            for (auto& host: cell.second.Hosts) {

                if (!cell.first.empty()) {
                    STORAGE_DEBUG(
                        TStringBuilder()
                            << "Send remote Describe Request to " << host.LogTag
                            << " for volume " << Request.GetDiskId());
                } else {
                    STORAGE_DEBUG(
                        TStringBuilder()
                            << "Send local Describe Request for volume "
                            << Request.GetDiskId());
                }

                auto future = Describe(
                    host.Client,
                    Request,
                    cell.first.empty());

                TDescribeResponseHandler handler(
                    weak,
                    cell.first,
                    host.LogTag,
                    Request.GetDiskId(),
                    std::move(future),
                    cell.second);

                handler.Start();
                Handlers.push_back(std::move(handler));
            }
        }

        auto self = shared_from_this();
        Scheduler->Schedule(
            TInstant::Now() + describeTimeout,
            [self=std::move(self)]() {
                self->HandleTimeout();
            });
    }

    void SetResponse(NProto::TDescribeVolumeResponse response)
    {
        Promise.TrySetValue(std::move(response));
    }

    void HandleTimeout()
    {
        NProto::TDescribeVolumeResponse response;
            *response.MutableError() =
                std::move(MakeError(E_REJECTED, "Describe timeout"));
        SetResponse(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

TDescribeResponseHandler::TDescribeResponseHandler(
        std::weak_ptr<TMultiCellDescribeHandler> owner,
        TString cellId,
        TString logTag,
        TString diskId,
        TFuture<NProto::TDescribeVolumeResponse> future,
        TCell& cell)
    : Owner(std::move(owner))
    , CellId(std::move(cellId))
    , LogTag(std::move(logTag))
    , DiskId(std::move(diskId))
    , Future(std::move(future))
    , Cell(cell)
{}

void TDescribeResponseHandler::operator ()(const auto& future)
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
        owner->SetResponse(std::move(response));
        STORAGE_DEBUG(
            TStringBuilder()
                << "Got success for disk " << DiskId
                << " from " << LogTag);
        return;
    }

    STORAGE_DEBUG(
        TStringBuilder()
            << "Got error '" << response.GetError().GetMessage()
            << "' from " << LogTag);

    with_lock(Cell.Lock) {
        if (EErrorKind::ErrorFatal ==
            GetErrorKind(response.GetError()))
        {
            Cell.FatalError = response.GetError();
        } else {
            Cell.RetriableError = response.GetError();
        }
    }

    auto now = owner->Counter.fetch_sub(1, std::memory_order_acq_rel) - 1;
    if (now == 0) {
        TString retryCell;
        // all the cells has replied with errors.
        // try to find cell replied with retriable errors only,
        // if such cell exists the compute should retry kick
        // endpoint, otherwise volume does not exist.
        for (const auto& cell: owner->Cells) {
            const auto& s = cell.second;
            if (!HasError(s.FatalError)) {
                *response.MutableError() =
                    std::move(s.RetriableError);
                owner->SetResponse(std::move(response));
                return;
            }
        }
        if (owner->IncompleteCells) {
            *response.MutableError() =
                    std::move(MakeError(E_REJECTED, "Not all cells available"));
            owner->SetResponse(std::move(response));
            return;
        }
        owner->SetResponse(std::move(response));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::optional<TDescribeFuture> DescribeVolume(
    const NProto::TDescribeVolumeRequest& request,
    const IBlockStorePtr& localService,
    const TCellsEndpoints& endpoints,
    bool hasUnavailableCells,
    TDuration timeout,
    TArguments args)
{
    TCells cells;

    for (const auto& cell: endpoints) {
        TCell s;
        for (const auto& client: cell.second) {
            s.Hosts.emplace_back(
                client.GetLogTag(),
                client.GetService());
        }
        cells.emplace(cell.first, std::move(s));
    }

    TCell localCell;
    localCell.Hosts.emplace_back("local", localService);
    cells.emplace("", std::move(localCell));

    auto describeResult = std::make_shared<TMultiCellDescribeHandler>(
        args.Scheduler,
        args.Logging->CreateLog("BLOCKSTORE_CELLS"),
        std::move(cells),
        request,
        hasUnavailableCells);
    describeResult->DoDescribe(timeout);

    return describeResult->Promise.GetFuture();
}

}   // namespace NCloud::NBlockStore::NCells
