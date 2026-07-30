#include "endpoint.h"

#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/folder/path.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>

namespace NCloud::NBlockStore::NVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TReadBlocksLocalMethod
{
    static TFuture<NProto::TReadBlocksLocalResponse> Execute(
        IDeviceHandler& deviceHandler,
        TCallContextPtr ctx,
        TVhostRequest& vhostRequest)
    {
        TString checkpointId;
        return deviceHandler.Read(
            std::move(ctx),
            vhostRequest.From,
            vhostRequest.Length,
            vhostRequest.SgList,
            checkpointId);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBlocksLocalMethod
{
    static TFuture<NProto::TWriteBlocksLocalResponse> Execute(
        IDeviceHandler& deviceHandler,
        TCallContextPtr ctx,
        TVhostRequest& vhostRequest)
    {
        return deviceHandler.Write(
            std::move(ctx),
            vhostRequest.From,
            vhostRequest.Length,
            vhostRequest.SgList);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TZeroBlocksMethod
{
    static TFuture<NProto::TZeroBlocksResponse> Execute(
        IDeviceHandler& deviceHandler,
        TCallContextPtr ctx,
        TVhostRequest& vhostRequest)
    {
        return deviceHandler.Zero(
            std::move(ctx),
            vhostRequest.From,
            vhostRequest.Length);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEndpoint::TEndpoint(
    TAppContext& appCtx,
    IDeviceHandlerPtr deviceHandler,
    TString socketPath,
    TStorageOptions options,
    ui32 socketAccessMode,
    TExecutor* executor)
    : AppCtx(appCtx)
    , DeviceHandler(std::move(deviceHandler))
    , SocketPath(std::move(socketPath))
    , Options(std::move(options))
    , SocketAccessMode(socketAccessMode)
    , Executor(executor)
{
    Y_ABORT_UNLESS(DeviceHandler);
    Y_ABORT_UNLESS(Executor);
}

void TEndpoint::SetVhostDevice(IVhostDevicePtr vhostDevice)
{
    Y_ABORT_UNLESS(VhostDevice == nullptr);
    VhostDevice = std::move(vhostDevice);
}

NProto::TError TEndpoint::Start()
{
    TFsPath(SocketPath).DeleteIfExists();

    bool started = VhostDevice->Start();

    if (!started) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage(
            TStringBuilder()
            << "could not register block device " << SocketPath.Quote());
        return error;
    }

    auto err = Chmod(SocketPath.c_str(), SocketAccessMode);

    if (err != 0) {
        NProto::TError error;
        error.SetCode(MAKE_SYSTEM_ERROR(err));
        error.SetMessage(
            TStringBuilder()
            << "failed to chmod socket " << SocketPath.Quote());
        return error;
    }

    return NProto::TError();
}

TFuture<NProto::TError> TEndpoint::Stop(bool deleteSocket)
{
    if (Stopped.test_and_set()) {
        return MakeFuture(MakeError(S_ALREADY));
    }

    auto future = VhostDevice->Stop();

    auto cancelError = MakeError(E_CANCELLED, "Vhost endpoint is stopping");
    with_lock (RequestsLock) {
        TLog& Log = AppCtx.Log;
        STORAGE_INFO(
            "Stop endpoint " << SocketPath.Quote() << " with "
                             << RequestsInFlight.Size()
                             << " inflight requests");

        RequestsInFlight.ForEach(
            [&](TRequest* request)
            {
                CompleteRequest(*request, cancelError);
                request->Unlink();
            });
    }

    if (deleteSocket) {
        TLog& Log = AppCtx.Log;
        future = future.Apply(
            [socketPath = SocketPath, Log](const auto& f)
            {
                STORAGE_INFO(
                    "Deletion socket while stopping endpoint "
                    << socketPath.Quote());
                TFsPath(socketPath).DeleteIfExists();
                return f.GetValue();
            });
    }

    return future;
}

void TEndpoint::Update(ui64 blocksCount)
{
    TLog& Log = AppCtx.Log;
    STORAGE_INFO(
        "Update vhost endpoint " << SocketPath.Quote()
                                 << " with blocks count = " << blocksCount);
    VhostDevice->Update(blocksCount);
}

size_t TEndpoint::CollectRequests(const TIncompleteRequestsCollector& collector)
{
    ui64 now = GetCycleCount();
    size_t count = 0;

    with_lock (RequestsLock) {
        for (auto& request: RequestsInFlight) {
            ++count;
            auto requestTime = request.CallContext->CalcRequestTime(now);
            if (requestTime) {
                collector(
                    *request.CallContext,
                    request.MetricRequest.VolumeInfo,
                    request.MetricRequest.MediaKind,
                    request.MetricRequest.RequestType,
                    requestTime);
            }
        }
    }
    return count;
}

void TEndpoint::ProcessRequest(TVhostRequestPtr vhostRequest)
{
    const auto requestType = vhostRequest->Type;
    auto request = RegisterRequest(std::move(vhostRequest));
    if (!request) {
        return;
    }

    switch (requestType) {
        case EBlockStoreRequest::WriteBlocks:
            ProcessRequest<TWriteBlocksLocalMethod>(std::move(request));
            break;
        case EBlockStoreRequest::ReadBlocks:
            ProcessRequest<TReadBlocksLocalMethod>(std::move(request));
            break;
        case EBlockStoreRequest::ZeroBlocks:
            ProcessRequest<TZeroBlocksMethod>(std::move(request));
            break;
        default:
            Y_ABORT(
                "Unexpected request type: %d",
                static_cast<int>(requestType));
            break;
    }
}

template <typename TMethod>
void TEndpoint::ProcessRequest(TRequestPtr request)
{
    auto future = TMethod::Execute(
        *DeviceHandler,
        request->CallContext,
        *request->VhostRequest);

    auto weakPtr = weak_from_this();
    future.Apply(
        [weakPtr, req = std::move(request)](const auto& f)
        {
            const auto& response = f.GetValue();
            if (auto p = weakPtr.lock()) {
                p->CompleteRequest(*req, response.GetError());
                p->UnregisterRequest(*req);
            }
            return f.GetValue();
        });
}

TRequestPtr TEndpoint::RegisterRequest(TVhostRequestPtr vhostRequest)
{
    auto startIndex = vhostRequest->From / Options.BlockSize;
    auto endIndex =
        (vhostRequest->From + vhostRequest->Length) / Options.BlockSize;
    if (endIndex * Options.BlockSize <
        vhostRequest->From + vhostRequest->Length)
    {
        ++endIndex;
    }
    bool unaligned = startIndex * Options.BlockSize != vhostRequest->From ||
                     endIndex * Options.BlockSize !=
                         vhostRequest->From + vhostRequest->Length;
    bool shouldDrop =
        Options.DropDiscardRequests && vhostRequest->IsDiscardRequest;

    auto request =
        MakeIntrusive<TRequest>(CreateRequestId(), std::move(vhostRequest));

    const ui32 blockSize = AppCtx.ServerStats->GetBlockSize(Options.DiskId);

    AppCtx.ServerStats->PrepareMetricRequest(
        request->MetricRequest,
        Options.ClientId,
        Options.DiskId,
        startIndex,
        blockSize * (endIndex - startIndex),
        unaligned);

    AppCtx.ServerStats->RequestStarted(
        AppCtx.Log,
        request->MetricRequest,
        *request->CallContext);

    if (shouldDrop) {
        CompleteRequest(*request, NProto::TError{});
        return nullptr;
    }

    with_lock (RequestsLock) {
        if (!Stopped.test()) {
            RequestsInFlight.PushBack(request.Get());
            return request;
        }
    }

    auto error = MakeError(E_CANCELLED, "Vhost endpoint was stopped");
    CompleteRequest(*request, error);
    return nullptr;
}

void TEndpoint::CompleteRequest(TRequest& request, const NProto::TError& error)
{
    if (request.Completed.test_and_set()) {
        return;
    }

    auto statsError = error;
    auto vhostResult = GetResult(statsError);

    AppCtx.ServerStats->RequestCompleted(
        AppCtx.Log,
        request.MetricRequest,
        *request.CallContext,
        statsError);

    request.VhostRequest->Complete(vhostResult);
}

void TEndpoint::UnregisterRequest(TRequest& request)
{
    with_lock (RequestsLock) {
        request.Unlink();
    }
}

TVhostRequest::EResult TEndpoint::GetResult(NProto::TError& error)
{
    if (!HasError(error)) {
        return TVhostRequest::SUCCESS;
    }

    // Keep the logic synchronized with
    // TAlignedDeviceHandler::ReportCriticalError().
    bool cancelError = error.GetCode() == E_CANCELLED ||
                       GetErrorKind(error) == EErrorKind::ErrorRetriable;

    bool stopEndpoint = AppCtx.ShouldStop.test() || Stopped.test();

    if (stopEndpoint && cancelError) {
        auto flags = error.GetFlags();
        SetProtoFlag(flags, NProto::EF_SILENT);
        error.SetFlags(flags);
        return TVhostRequest::CANCELLED;
    }

    return TVhostRequest::IOERR;
}

}   // namespace NCloud::NBlockStore::NVhost
