#include "endpoint.h"

#include "request_tracker.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/common/future_helper.h>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

using NCloud::NIcV2::IRequestChannelPtr;
using NCloud::NIcV2::TResponseFuture;

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
NThreading::TFuture<TResponse> Fail(const NProto::TError& error)
{
    return NThreading::MakeFuture<TResponse>(TErrorResponse(error));
}

template <typename TResponse>
void ProcessThrottleTime(const TCallContextPtr& context, TResponse& response)
{
    auto& info = *response.MutableHeaders()->MutableThrottler();
    context->AddTime(
        EProcessingStage::Postponed,
        TDuration::MicroSeconds(info.GetDelay()));
    context->AddTime(
        EProcessingStage::Shaping,
        TDuration::MicroSeconds(info.GetShapingDelay()));
    context->SetPossiblePostponeDuration(TDuration::Zero());
    info.SetDelay(0);
    info.SetShapingDelay(0);
}

TSgList GetPayloadSgList(const TRope& payload)
{
    TSgList result;
    for (auto it = payload.Begin(); it != payload.End();
         it.AdvanceToNextContiguousBlock())
    {
        result.emplace_back(it.ContiguousData(), it.ContiguousSize());
    }
    return result;
}

TResultOrError<std::unique_ptr<NStorage::TEvService::TEvReadBlocksRequest>>
CreateRequest(const NProto::TReadBlocksRequest& request)
{
    if (!request.GetBlockSize()) {
        return MakeError(E_ARGUMENT, "empty IC V2 block size");
    }
    auto event = std::make_unique<NStorage::TEvService::TEvReadBlocksRequest>();
    event->Record.CopyFrom(request);
    return event;
}

TResultOrError<std::unique_ptr<NStorage::TEvService::TEvZeroBlocksRequest>>
CreateRequest(const NProto::TZeroBlocksRequest& request)
{
    auto event = std::make_unique<NStorage::TEvService::TEvZeroBlocksRequest>();
    event->Record.CopyFrom(request);
    return event;
}

TRope CopyWritePayload(const TSgList& sglist)
{
    auto payload = TRope::Uninitialized(SgListGetSize(sglist));
    auto dst = payload.GetContiguousSpanMut();
    SgListCopy(sglist, TBlockDataRef(dst.GetData(), dst.GetSize()));
    return payload;
}

TResultOrError<std::unique_ptr<NStorage::TEvService::TEvWriteBlocksRequest>>
CreateRequest(const NProto::TWriteBlocksRequest& request)
{
    if (!request.GetBlockSize()) {
        return MakeError(E_ARGUMENT, "empty IC V2 block size");
    }

    auto event =
        std::make_unique<NStorage::TEvService::TEvWriteBlocksRequest>();
    event->Record.CopyFrom(request);
    event->Record.ClearBlocks();
    event->AddPayload(CopyWritePayload(GetSgList(request)));
    return event;
}

TResultOrError<std::unique_ptr<NStorage::TEvService::TEvWriteBlocksRequest>>
CreateRequest(const NProto::TWriteBlocksLocalRequest& request)
{
    if (!request.GetBlockSize()) {
        return MakeError(E_ARGUMENT, "empty IC V2 block size");
    }

    auto guard = request.Sglist.Acquire();
    if (!guard) {
        return MakeError(E_CANCELLED, "IC V2 write sglist is closed");
    }
    if (SgListGetSize(guard.Get()) !=
        CalculateBytesCount(request, request.GetBlockSize()))
    {
        return MakeError(E_ARGUMENT, "invalid IC V2 write sglist size");
    }

    auto event =
        std::make_unique<NStorage::TEvService::TEvWriteBlocksRequest>();
    const NProto::TWriteBlocksLocalRequest metadata(
        request,
        NProto::TWriteBlocksLocalRequest::TDependentTag{});
    event->Record.CopyFrom(metadata);
    event->AddPayload(CopyWritePayload(guard.Get()));
    return event;
}

NProto::TError CopyReadPayload(
    const NProto::TReadBlocksRequest& request,
    const TRope& payload,
    NProto::TReadBlocksResponse& response)
{
    auto dst = ResizeIOVector(
        *response.MutableBlocks(),
        GetBlocksCount(request),
        request.GetBlockSize());
    SgListCopy(GetPayloadSgList(payload), dst);
    return {};
}

NProto::TError CopyReadPayload(
    const NProto::TReadBlocksLocalRequest& request,
    const TRope& payload,
    NProto::TReadBlocksResponse& response)
{
    Y_UNUSED(response);
    auto guard = request.Sglist.Acquire();
    if (!guard) {
        return MakeError(E_CANCELLED, "IC V2 read sglist is closed");
    }
    if (SgListGetSize(guard.Get()) != payload.GetSize()) {
        return MakeError(E_ARGUMENT, "invalid IC V2 read sglist size");
    }
    SgListCopy(GetPayloadSgList(payload), guard.Get());
    return {};
}

template <typename TResponse>
TResponse DecodeResponse(
    const NProto::TWriteBlocksRequest& request,
    NActors::IEventHandle& event)
{
    Y_UNUSED(request);
    return TResponse(
        std::move(
            event.Get<NStorage::TEvService::TEvWriteBlocksResponse>()->Record));
}

template <typename TResponse>
TResponse DecodeResponse(
    const NProto::TZeroBlocksRequest& request,
    NActors::IEventHandle& event)
{
    Y_UNUSED(request);
    return TResponse(
        std::move(
            event.Get<NStorage::TEvService::TEvZeroBlocksResponse>()->Record));
}

template <typename TResponse, typename TRequest>
    requires std::is_base_of_v<NProto::TReadBlocksRequest, TRequest>
TResponse DecodeResponse(const TRequest& request, NActors::IEventHandle& handle)
{
    auto& event = *handle.Get<NStorage::TEvService::TEvReadBlocksResponse>();
    TResponse response(std::move(event.Record));
    const ui64 bytes = CalculateBytesCount(request, request.GetBlockSize());
    const bool failed = HasError(response.GetError());
    if (response.GetBlocks().BuffersSize() ||
        event.GetPayloadCount() != (failed ? 0 : 1) ||
        (!failed && event.GetPayload(0).GetSize() != bytes))
    {
        return TResponse(
            TErrorResponse(E_BADMSG, "invalid IC V2 read payload"));
    }
    if (!failed) {
        if (auto error =
                CopyReadPayload(request, event.GetPayload(0), response);
            HasError(error))
        {
            return TResponse(TErrorResponse(error));
        }
    }
    return response;
}

////////////////////////////////////////////////////////////////////////////////

class TDataEndpoint final: public TBlockStoreImpl<TDataEndpoint, IDataEndpoint>
{
    const IRequestChannelPtr Channel;
    const std::shared_ptr<TRequestTracker> Tracker;

public:
    TDataEndpoint(IRequestChannelPtr channel, TDataConfig config)
        : Channel(std::move(channel))
        , Tracker(std::make_shared<TRequestTracker>(config))
    {}

    ~TDataEndpoint() override
    {
        RequestStop();
    }

    void Start() override
    {}

    void RequestStop() override
    {
        Tracker->RequestStop();
        Channel->Close();
    }

    void Stop() override
    {
        RequestStop();
        Tracker->Stop();
        Channel->WhenClosed().GetValueSync();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);

        return nullptr;
    }

    NThreading::TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TCallContextPtr context,
        std::shared_ptr<NProto::TReadBlocksRequest> request) override
    {
        return ExecuteDataRequest<NProto::TReadBlocksResponse>(
            std::move(context),
            std::move(request),
            0);
    }

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr context,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return ExecuteDataRequest<NProto::TReadBlocksLocalResponse>(
            std::move(context),
            std::move(request),
            0);
    }

    NThreading::TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr context,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override
    {
        const auto bytes =
            CalculateBytesCount(*request, request->GetBlockSize());
        return ExecuteDataRequest<NProto::TWriteBlocksResponse>(
            std::move(context),
            std::move(request),
            bytes);
    }

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr context,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        const auto bytes =
            CalculateBytesCount(*request, request->GetBlockSize());
        return ExecuteDataRequest<NProto::TWriteBlocksLocalResponse>(
            std::move(context),
            std::move(request),
            bytes);
    }

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr context,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return ExecuteDataRequest<NProto::TZeroBlocksResponse>(
            std::move(context),
            std::move(request),
            0);
    }

    template <typename TMethod>
    static NThreading::TFuture<typename TMethod::TResponse> Execute(
        const TCallContextPtr& context,
        const std::shared_ptr<typename TMethod::TRequest>& request)
    {
        Y_UNUSED(context);
        Y_UNUSED(request);
        return Fail<typename TMethod::TResponse>(
            MakeError(E_NOT_IMPLEMENTED, "unsupported IC V2 data operation"));
    }

private:
    template <typename TResponse, typename TRequest>
    NThreading::TFuture<TResponse> ExecuteDataRequest(
        TCallContextPtr context,
        std::shared_ptr<TRequest> request,
        ui64 payloadBytes)
    {
        Y_DEBUG_ABORT_UNLESS(request && context);

        auto [reservation, error] = Tracker->Acquire(payloadBytes);
        if (HasError(error)) {
            return Fail<TResponse>(error);
        }
        auto [event, preparationError] = CreateRequest(*request);
        if (HasError(preparationError)) {
            return Fail<TResponse>(preparationError);
        }
        const auto timeout =
            TDuration::MilliSeconds(request->GetHeaders().GetRequestTimeout());
        auto promise = NThreading::NewPromise<TResponse>();
        Channel->Request(std::move(event), timeout)
            .Subscribe(
                [promise,
                 reservation = std::move(reservation),
                 request = std::move(request),
                 context = std::move(context)]   //
                (const TResponseFuture& future) mutable
                {
                    auto response = SafeExecute<TResponse>(
                        [&]() -> TResponse
                        {
                            auto result = UnsafeExtractValue(future);
                            if (HasError(result.GetError())) {
                                return TErrorResponse(result.GetError());
                            }
                            auto response = DecodeResponse<TResponse>(
                                *request,
                                *result.GetResult());
                            ProcessThrottleTime(context, response);
                            return response;
                        });
                    promise.SetValue(std::move(response));
                    reservation.reset();
                });
        return promise.GetFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IDataEndpointPtr> CreateDataEndpoint(
    IRequestChannelPtr channel,
    TDataConfig config)
{
    if (!channel || !config.IsValid()) {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 data endpoint configuration");
    }
    return IDataEndpointPtr(
        std::make_shared<TDataEndpoint>(std::move(channel), config));
}

}   // namespace NCloud::NBlockStore::NIcV2
