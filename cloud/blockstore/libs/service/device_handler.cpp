#include "device_handler.h"

#include "aligned_device_handler.h"
#include "context.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/cputimer.h>
#include <util/generic/list.h>
#include <util/generic/vector.h>

#include <atomic>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TReadBlocksResponseFuture = TFuture<NProto::TReadBlocksLocalResponse>;
using TWriteBlocksResponseFuture = TFuture<NProto::TWriteBlocksLocalResponse>;
using TZeroBlocksResponseFuture = TFuture<NProto::TZeroBlocksResponse>;

////////////////////////////////////////////////////////////////////////////////


class TDeviceHandler final
    : public IDeviceHandler
    , public std::enable_shared_from_this<TDeviceHandler>
{
    class TModifyRequest;
    using TModifyRequestPtr = std::shared_ptr<TModifyRequest>;

    template <typename TResponse>
    class TModifyRequestImpl;
    using TWriteRequest = TModifyRequestImpl<NProto::TWriteBlocksLocalResponse>;
    using TZeroRequest = TModifyRequestImpl<NProto::TZeroBlocksResponse>;

private:
    static constexpr ui32 MaxUnalignedRequestSize = 32_MB;

    const IStoragePtr Storage;
    const TString ClientId;
    const ui32 BlockSize;
    const ui32 ZeroBlocksCountLimit;

    TList<TModifyRequestPtr> AlignedRequests;
    TList<TModifyRequestPtr> UnalignedRequests;
    TAdaptiveLock RequestsLock;

public:
    TDeviceHandler(
        IStoragePtr storage,
        TString clientId,
        ui32 blockSize,
        ui32 zeroBlocksCountLimit);

    TReadBlocksResponseFuture Read(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList,
        const TString& checkpointId) override;

    TWriteBlocksResponseFuture Write(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList) override;

    TZeroBlocksResponseFuture Zero(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length) override;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return AllocateStorageBuffer(*Storage, bytesCount);
    }

private:
    TReadBlocksResponseFuture ExecuteAlignedReadRequest(
        TCallContextPtr ctx,
        const TBlocksInfo& blocksInfo,
        TGuardedSgList sgList,
        const TString& checkpointId);

    TReadBlocksResponseFuture ExecuteUnalignedReadRequest(
        TCallContextPtr ctx,
        const TBlocksInfo& blocksInfo,
        TGuardedSgList guardedSgList,
        const TString& checkpointId);

    template <typename TResponse>
    TFuture<TResponse> Modify(
        TCallContextPtr ctx,
        const TBlocksInfo& blocksInfo,
        bool aligned,
        TGuardedSgList sgList);

    template <typename TResponse, typename TRequest>
    TFuture<TResponse> ExecuteModifyRequest(std::shared_ptr<TRequest> request);

    void HandleExecutedModifyRequest(TModifyRequestPtr request);

    template <typename TResponse, typename TRequest>
    TFuture<TResponse> ExecuteAlignedModifyRequest(TRequest& request);

    TFuture<NProto::TError> ExecuteUnalignedModifyRequest(
        TModifyRequestPtr request);

    static TFuture<NProto::TError> HandleRmwReadResponse(
        IStoragePtr storage,
        TModifyRequestPtr request,
        TStorageBuffer buffer,
        size_t bufferSize);

    template <typename TResponse>
    static TFuture<TResponse> CreateResponseFuture(
        const TFuture<NProto::TError>& future);

    static void RemoveRequest(
        TList<TModifyRequestPtr>& requests,
        const TModifyRequestPtr& request);

    static void PrepareRequests(
        const TList<TModifyRequestPtr>& requests,
        TVector<TModifyRequestPtr>& result);
};

////////////////////////////////////////////////////////////////////////////////

class TDeviceHandler::TModifyRequest
{
protected:
    enum EStatus
    {
        Waiting,
        InFlight,
        Completed,
    };

public:
    const TString ClientId;
    const ui32 BlockSize;
    const TCallContextPtr CallContext;
    const TBlocksInfo BlocksInfo;
    const bool Aligned;
    const TGuardedSgList SgList;

protected:
    std::atomic<EStatus> Status = Waiting;
    TVector<TModifyRequestPtr> Dependencies;

public:
    TModifyRequest(
            TString clientId,
            ui32 blockSize,
            TCallContextPtr callContext,
            const TBlocksInfo& blocksInfo,
            bool aligned,
            TGuardedSgList sgList)
        : ClientId(std::move(clientId))
        , BlockSize(blockSize)
        , CallContext(std::move(callContext))
        , BlocksInfo(blocksInfo)
        , Aligned(aligned)
        , SgList(std::move(sgList))
    {}

    virtual ~TModifyRequest() = default;

    bool IsAligned() const
    {
        return Aligned;
    }

    void AddDependencies(const TList<TModifyRequestPtr>& requests)
    {
        for (const auto& request: requests) {
            if (request->Status.load() != Completed &&
                BlocksInfo.Range.Overlaps(request->BlocksInfo.Range))
            {
                Dependencies.push_back(request);
            }
        }
    }

    bool Prepare()
    {
        if (Status.load() != Waiting) {
            return false;
        }

        for (const auto& dependency: Dependencies) {
            if (dependency->Status.load() != Completed) {
                return false;
            }
        }

        Dependencies.clear();

        Status.store(InFlight);
        return true;
    }

    virtual TFuture<NProto::TError> BaseExecute(const IStoragePtr& storage) = 0;

    virtual void Complete(const NProto::TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
class TDeviceHandler::TModifyRequestImpl
    : public TModifyRequest
{
private:
    TPromise<TResponse> Promise;

public:
    using TModifyRequest::TModifyRequest;

    TFuture<NProto::TError> BaseExecute(const IStoragePtr& storage) override
    {
        return Execute(storage).Apply([=] (const auto& f) {
            return f.GetValue().GetError();
        });
    }

    void Complete(const NProto::TError& error) override
    {
        Complete(static_cast<TResponse>(TErrorResponse(error)));
    }

    TFuture<TResponse> Execute(const IStoragePtr& storage);

    TFuture<TResponse> GetFuture()
    {
        Y_ABORT_UNLESS(!Promise.Initialized());
        Promise = NewPromise<TResponse>();
        return Promise.GetFuture();
    }

    void Complete(const TResponse& response)
    {
        Status.store(Completed);

        if (Promise.Initialized()) {
            Promise.SetValue(response);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
TZeroBlocksResponseFuture TDeviceHandler::TZeroRequest::Execute(
    const IStoragePtr& storage)
{
    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->MutableHeaders()->SetRequestId(CallContext->RequestId);
    request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    request->MutableHeaders()->SetClientId(ClientId);
    request->SetStartIndex(BlocksInfo.Range.Start);
    request->SetBlocksCount(BlocksInfo.Range.Size());

    return storage->ZeroBlocks(CallContext, std::move(request));
}

template <>
TWriteBlocksResponseFuture TDeviceHandler::TWriteRequest::Execute(
    const IStoragePtr& storage)
{
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->MutableHeaders()->SetRequestId(CallContext->RequestId);
    request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    request->MutableHeaders()->SetClientId(ClientId);
    request->SetStartIndex(BlocksInfo.Range.Start);
    request->BlocksCount = BlocksInfo.Range.Size();
    request->BlockSize = BlockSize;
    request->Sglist = SgList;

    return storage->WriteBlocksLocal(CallContext, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

TDeviceHandler::TDeviceHandler(
        IStoragePtr storage,
        TString clientId,
        ui32 blockSize,
        ui32 zeroBlocksCountLimit)
    : Storage(std::move(storage))
    , ClientId(std::move(clientId))
    , BlockSize(blockSize)
    , ZeroBlocksCountLimit(zeroBlocksCountLimit)
{
    Y_ABORT_UNLESS(ZeroBlocksCountLimit > 0);
}

TReadBlocksResponseFuture TDeviceHandler::Read(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList,
    const TString& checkpointId)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);

    auto aligned = TryToNormalize(sgList, blocksInfo, length, BlockSize);
    if (HasError(aligned)) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            TErrorResponse(aligned.GetError()));
    }

    if (!aligned.GetResult()) {
        return ExecuteUnalignedReadRequest(
            std::move(ctx),
            blocksInfo,
            std::move(sgList),
            checkpointId);
    }

    return ExecuteAlignedReadRequest(
        std::move(ctx),
        blocksInfo,
        std::move(sgList),
        checkpointId);
}

TWriteBlocksResponseFuture TDeviceHandler::Write(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);

    auto aligned = TryToNormalize(sgList, blocksInfo, length, BlockSize);
    if (HasError(aligned)) {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            TErrorResponse(aligned.GetError()));
    }

    if (!aligned.GetResult() &&
        blocksInfo.Range.Size() > MaxUnalignedRequestSize / BlockSize)
    {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            TErrorResponse(E_ARGUMENT, TStringBuilder()
                << "Unaligned write request is too big. BlockCount="
                << blocksInfo.Range.Size()));
    }

    return Modify<NProto::TWriteBlocksLocalResponse>(
        std::move(ctx),
        blocksInfo,
        aligned.GetResult(),
        std::move(sgList));
}

TZeroBlocksResponseFuture TDeviceHandler::Zero(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length)
{
    if (length == 0) {
        return MakeFuture<NProto::TZeroBlocksResponse>(
            TErrorResponse(E_ARGUMENT, "Local request has zero length"));
    }

    ui64 startIndex = from / BlockSize;
    ui64 beginOffset = from - startIndex * BlockSize;
    ui64 lengthLimit = static_cast<ui64>(ZeroBlocksCountLimit) * BlockSize - beginOffset;

    auto requestLength = std::min(length, lengthLimit);
    auto blocksInfo = TBlocksInfo(from, requestLength, BlockSize);
    bool aligned = (blocksInfo.BeginOffset == 0 && blocksInfo.EndOffset == 0);

    auto result = Modify<NProto::TZeroBlocksResponse>(
        ctx,
        blocksInfo,
        aligned,
        TGuardedSgList({TBlockDataRef::CreateZeroBlock(requestLength)}));

    return result.Apply([=] (const auto& future) mutable {
        auto response = future.GetValue();

        if (length <= requestLength || HasError(response)) {
            return MakeFuture(response);
        }

        return Zero(std::move(ctx), from + requestLength, length - requestLength);
    });
}

template <typename TResponse>
TFuture<TResponse> TDeviceHandler::Modify(
    TCallContextPtr ctx,
    const TBlocksInfo& blocksInfo,
    bool aligned,
    TGuardedSgList sgList)
{
    auto request = std::make_shared<TModifyRequestImpl<TResponse>>(
        ClientId,
        BlockSize,
        std::move(ctx),
        blocksInfo,
        aligned,
        std::move(sgList));

    with_lock (RequestsLock) {
        if (request->IsAligned()) {
            request->AddDependencies(UnalignedRequests);
            AlignedRequests.push_back(request);
        } else {
            request->AddDependencies(UnalignedRequests);
            request->AddDependencies(AlignedRequests);
            UnalignedRequests.push_back(request);
        }

        if (!request->Prepare()) {
            request->CallContext->Postpone(GetCycleCount());
            return request->GetFuture();
        }
    }

    return ExecuteModifyRequest<TResponse>(std::move(request));
}

template <typename TResponse, typename TRequest>
TFuture<TResponse> TDeviceHandler::ExecuteAlignedModifyRequest(
    TRequest& request)
{
    return request.Execute(Storage);
}

template <>
TFuture<NProto::TError> TDeviceHandler::ExecuteAlignedModifyRequest(
    TModifyRequest& request)
{
    return request.BaseExecute(Storage);
}

template <typename TResponse, typename TRequest>
TFuture<TResponse> TDeviceHandler::ExecuteModifyRequest(
    std::shared_ptr<TRequest> request)
{
    TFuture<TResponse> future;

    if (!request->IsAligned()) {
        auto error = ExecuteUnalignedModifyRequest(request);
        future = CreateResponseFuture<TResponse>(error);
    } else {
        future = ExecuteAlignedModifyRequest<TResponse>(*request);
    }

    auto weakPtr = weak_from_this();

    return future.Apply(
        [request = std::move(request), weakPtr = std::move(weakPtr)] (const auto& f) {
        request->Complete(f.GetValue());

        if (auto p = weakPtr.lock()) {
            p->HandleExecutedModifyRequest(std::move(request));
        }

        return f.GetValue();
    });
}

void TDeviceHandler::HandleExecutedModifyRequest(TModifyRequestPtr request)
{
    TVector<TModifyRequestPtr> preparedRequests;

    with_lock (RequestsLock) {
        if (request->IsAligned()) {
            RemoveRequest(AlignedRequests, request);
            PrepareRequests(UnalignedRequests, preparedRequests);
        } else {
            RemoveRequest(UnalignedRequests, request);
            PrepareRequests(AlignedRequests, preparedRequests);
            PrepareRequests(UnalignedRequests, preparedRequests);
        }
    }

    for (auto& preparedRequest: preparedRequests) {
        preparedRequest->CallContext->Advance(GetCycleCount());
        ExecuteModifyRequest<NProto::TError>(std::move(preparedRequest));
    }
}

void TDeviceHandler::RemoveRequest(
    TList<TModifyRequestPtr>& requests,
    const TModifyRequestPtr& request)
{
    for (auto it = requests.begin(); it != requests.end(); ) {
        if (*it == request) {
            it = requests.erase(it);
        } else {
            ++it;
        }
    }
}

void TDeviceHandler::PrepareRequests(
    const TList<TModifyRequestPtr>& requests,
    TVector<TModifyRequestPtr>& result)
{
    for (const auto& request: requests) {
        if (request->Prepare()) {
            result.push_back(request);
        }
    }
}

TReadBlocksResponseFuture TDeviceHandler::ExecuteAlignedReadRequest(
    TCallContextPtr ctx,
    const TBlocksInfo& blocksInfo,
    TGuardedSgList sgList,
    const TString& checkpointId)
{
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->MutableHeaders()->SetRequestId(ctx->RequestId);
    request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    request->MutableHeaders()->SetClientId(ClientId);
    request->SetCheckpointId(checkpointId);
    request->SetStartIndex(blocksInfo.Range.Start);
    request->SetBlocksCount(blocksInfo.Range.Size());
    request->BlockSize = BlockSize;
    request->Sglist = std::move(sgList);

    return Storage->ReadBlocksLocal(std::move(ctx), std::move(request));
}

TReadBlocksResponseFuture TDeviceHandler::ExecuteUnalignedReadRequest(
    TCallContextPtr ctx,
    const TBlocksInfo& blocksInfo,
    TGuardedSgList guardedSgList,
    const TString& checkpointId)
{
    if (blocksInfo.Range.Size() > MaxUnalignedRequestSize / BlockSize) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            TErrorResponse(E_ARGUMENT, TStringBuilder()
                << "Unaligned read request is too big. BlockCount="
                << blocksInfo.Range.Size()));
    }

    auto bufferSize = blocksInfo.Range.Size() * BlockSize;
    auto buffer = AllocateBuffer(bufferSize);

    auto sgListOrError = SgListNormalize(
        TBlockDataRef(buffer.get(), bufferSize),
        BlockSize);

    if (HasError(sgListOrError)) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            TErrorResponse(sgListOrError.GetError()));
    }

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->MutableHeaders()->SetRequestId(ctx->RequestId);
    request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    request->MutableHeaders()->SetClientId(ClientId);
    request->SetCheckpointId(checkpointId);
    request->SetStartIndex(blocksInfo.Range.Start);
    request->SetBlocksCount(blocksInfo.Range.Size());
    request->BlockSize = BlockSize;
    request->Sglist = guardedSgList.Create(sgListOrError.ExtractResult());

    return Storage->ReadBlocksLocal(std::move(ctx), std::move(request))
        .Apply([=, buf = std::move(buffer)] (const auto& future) {
            const auto& response = future.GetValue();
            if (HasError(response)) {
                return response;
            }

            auto guard = guardedSgList.Acquire();
            if (!guard) {
                return static_cast<NProto::TReadBlocksLocalResponse>(
                    TErrorResponse(
                        E_CANCELLED,
                        "failed to acquire sglist in DeviceHandler"));
            }

            const auto& dstSgList = guard.Get();
            auto size = SgListGetSize(dstSgList);
            TBlockDataRef srcBuf(buf.get() + blocksInfo.BeginOffset, size);
            auto cpSize = SgListCopy({srcBuf}, dstSgList);
            Y_ABORT_UNLESS(cpSize == size);

            return response;
        });
}

TFuture<NProto::TError> TDeviceHandler::ExecuteUnalignedModifyRequest(
    TModifyRequestPtr request)
{
    const auto& ctx = request->CallContext;
    const auto& blocksInfo = request->BlocksInfo;

    auto bufferSize = blocksInfo.Range.Size() * BlockSize;
    auto buffer = AllocateBuffer(bufferSize);

    auto sgListOrError = SgListNormalize(
        TBlockDataRef(buffer.get(), bufferSize),
        BlockSize);

    if (HasError(sgListOrError)) {
        return MakeFuture(sgListOrError.GetError());
    }

    TGuardedSgList readSgList(sgListOrError.ExtractResult());

    auto readRequest = std::make_shared<NProto::TReadBlocksLocalRequest>();
    readRequest->MutableHeaders()->SetRequestId(ctx->RequestId);
    readRequest->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    readRequest->MutableHeaders()->SetClientId(ClientId);
    readRequest->SetStartIndex(blocksInfo.Range.Start);
    readRequest->SetBlocksCount(blocksInfo.Range.Size());
    readRequest->BlockSize = BlockSize;
    readRequest->Sglist = readSgList;

    auto future = Storage->ReadBlocksLocal(ctx, std::move(readRequest));

    return future.Apply([=, req = std::move(request), buf = std::move(buffer)]
        (const auto& f) mutable
    {
        readSgList.Close();

        const auto& readResponse = f.GetValue();
        if (HasError(readResponse)) {
            return MakeFuture(readResponse.GetError());
        }

        return HandleRmwReadResponse(
            Storage,
            std::move(req),
            std::move(buf),
            bufferSize);
    });
}

TFuture<NProto::TError> TDeviceHandler::HandleRmwReadResponse(
    IStoragePtr storage,
    TModifyRequestPtr request,
    TStorageBuffer buffer,
    size_t bufferSize)
{
    const auto& ctx = request->CallContext;
    const auto& blocksInfo = request->BlocksInfo;

    {
        auto guard = request->SgList.Acquire();
        if (!guard) {
            return MakeFuture<NProto::TError>(TErrorResponse(
                E_CANCELLED,
                "failed to acquire sglist in DeviceHandler"));
        }

        const auto& srcSgList = guard.Get();
        auto size = SgListGetSize(srcSgList);
        TBlockDataRef dstBuf(buffer.get() + blocksInfo.BeginOffset, size);
        auto cpSize = SgListCopy(srcSgList, {dstBuf});
        Y_ABORT_UNLESS(cpSize == size);
    }

    auto sgListOrError = SgListNormalize(
        TBlockDataRef(buffer.get(), bufferSize),
        request->BlockSize);

    if (HasError(sgListOrError)) {
        return MakeFuture(sgListOrError.GetError());
    }

    TGuardedSgList writeSgList(sgListOrError.ExtractResult());

    auto writeRequest = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    writeRequest->MutableHeaders()->SetRequestId(ctx->RequestId);
    writeRequest->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    writeRequest->MutableHeaders()->SetClientId(request->ClientId);
    writeRequest->SetStartIndex(blocksInfo.Range.Start);
    writeRequest->BlocksCount = blocksInfo.Range.Size();
    writeRequest->BlockSize = request->BlockSize;
    writeRequest->Sglist = writeSgList;

    return storage->WriteBlocksLocal(ctx, std::move(writeRequest))
        .Apply([=, buf = std::move(buffer)] (const auto& future) mutable {
            writeSgList.Close();

            Y_UNUSED(buf);
            return future.GetValue().GetError();
        });
}

template <typename TResponse>
TFuture<TResponse> TDeviceHandler::CreateResponseFuture(
    const TFuture<NProto::TError>& future)
{
    return future.Apply([] (const auto& f) {
        TResponse response;
        response.MutableError()->CopyFrom(f.GetValue());
        return response;
    });
}

template <>
TFuture<NProto::TError> TDeviceHandler::CreateResponseFuture(
    const TFuture<NProto::TError>& future)
{
    return future;
}

////////////////////////////////////////////////////////////////////////////////

struct TDefaultDeviceHandlerFactory final
    : public IDeviceHandlerFactory
{
    IDeviceHandlerPtr CreateDeviceHandler(
        IStoragePtr storage,
        TString clientId,
        ui32 blockSize,
        ui32 maxBlockCount,
        bool unalignedRequestsDisabled) override
    {
        if (unalignedRequestsDisabled) {
            return std::make_shared<TAlignedDeviceHandler>(
                std::move(storage),
                std::move(clientId),
                blockSize,
                maxBlockCount);
        }

        return std::make_shared<TDeviceHandler>(
            std::move(storage),
            std::move(clientId),
            blockSize,
            maxBlockCount);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDeviceHandlerFactoryPtr CreateDefaultDeviceHandlerFactory()
{
    return std::make_shared<TDefaultDeviceHandlerFactory>();
}

}   // namespace NCloud::NBlockStore
