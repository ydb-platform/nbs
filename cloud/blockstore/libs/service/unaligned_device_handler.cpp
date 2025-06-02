
#include "unaligned_device_handler.h"

#include <cloud/blockstore/libs/service/context.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TErrorResponse CreateErrorAcquireResponse()
{
    return {E_CANCELLED, "failed to acquire sglist in DeviceHandler"};
}

TErrorResponse CreateRequestDestroyedResponse()
{
    return {E_CANCELLED, "request destroyed"};
}

TErrorResponse CreateBackendDestroyedResponse()
{
    return {E_CANCELLED, "backend destroyed"};
}

TErrorResponse CreateUnalignedTooBigResponse(ui32 blockCount)
{
    return {
        E_ARGUMENT,
        TStringBuilder() << "Unaligned request is too big. BlockCount="
                         << blockCount};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// The base class for wrapping over a write or zero request.
class TModifyRequest: public std::enable_shared_from_this<TModifyRequest>
{
private:
    enum class EStatus
    {
        Created,
        Postponed,
        InFlight,
        Completed,
    };
    // The current status of the request execution can be read and written
    // only under the RequestsLock.
    EStatus Status = EStatus::Created;

    // A list of requests that prevent the execution.
    TVector<TModifyRequestWeakPtr> Dependencies;

    // A list iterator that points to an object in the list of running requests.
    // Makes it easier to delete an object after the request is completed.
    TModifyRequestIt It;

protected:
    const std::weak_ptr<TAlignedDeviceHandler> Backend;
    const TBlocksInfo BlocksInfo;
    TCallContextPtr CallContext;

    // The buffer that was allocated to execute the unaligned request. Memory is
    // allocated only if the request is not aligned.
    TStorageBuffer RMWBuffer;
    TSgList RMWBufferSgList;

public:
    TModifyRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo);

    virtual ~TModifyRequest() = default;

    void SetIt(TModifyRequestIt it);
    TModifyRequestIt GetIt() const;

    bool IsAligned() const;

    // Adds a list of overlapped requests that this request depends on. The
    // execution of this request will not start until all this requests have
    // been completed.
    void AddDependencies(const TList<TModifyRequestPtr>& requests);

    // Returns a flag indicating whether the request can be executed. The method
    // modifies the internal state and gives permission to execute only once. It
    // is necessary to call the method under RequestsLock.
    [[nodiscard]] bool PrepareToRun();

    // Starts the execution of the postponed request. The method can only be
    // called if the request has been postponed.
    void ExecutePostponed();

    // It is necessary to call the method under RequestsLock.
    void SetCompleted();

protected:
    void AllocateRMWBuffer(TAlignedDeviceHandler& backend);

    virtual void DoPostpone() = 0;
    virtual void DoExecutePostponed() = 0;
};

// Wrapper for a write request.
class TWriteRequest final: public TModifyRequest
{
public:
    using TResponsePromise = TPromise<NProto::TWriteBlocksLocalResponse>;
    using TResponseFuture = TFuture<NProto::TWriteBlocksLocalResponse>;

private:
    TResponsePromise Promise;
    TGuardedSgList SgList;

public:
    TWriteRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo,
        TGuardedSgList sgList);

    TResponseFuture ExecuteOrPostpone(bool readyToRun);

protected:
    void DoPostpone() override;
    void DoExecutePostponed() override;

private:
    TResponseFuture DoExecute();
    TResponseFuture ReadModifyWrite(TAlignedDeviceHandler& backend);
    TResponseFuture ModifyAndWrite();
};

// Wrapper for a zero request.
class TZeroRequest final: public TModifyRequest
{
public:
    using TResponsePromise = TPromise<NProto::TZeroBlocksResponse>;
    using TResponseFuture = TFuture<NProto::TZeroBlocksResponse>;

private:
    TResponsePromise Promise;
    TGuardedSgList SgList;

public:
    TZeroRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo);
    ~TZeroRequest() override;

    TResponseFuture ExecuteOrPostpone(bool readyToRun);

protected:
    void DoPostpone() override;
    void DoExecutePostponed() override;

private:
    TResponseFuture DoExecute();
    TResponseFuture ReadModifyWrite(TAlignedDeviceHandler& backend);
    TResponseFuture ModifyAndWrite();
};

////////////////////////////////////////////////////////////////////////////////

TModifyRequest::TModifyRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo)
    : Backend(std::move(backend))
    , BlocksInfo(blocksInfo)
    , CallContext(std::move(callContext))
{}

void TModifyRequest::AddDependencies(const TList<TModifyRequestPtr>& requests)
{
    for (const auto& request: requests) {
        if (BlocksInfo.Range.Overlaps(request->BlocksInfo.Range)) {
            Dependencies.push_back(request);
        }
    }
}

void TModifyRequest::SetIt(TModifyRequestIt it)
{
    It = it;
}

TModifyRequestIt TModifyRequest::GetIt() const
{
    return It;
}

bool TModifyRequest::IsAligned() const
{
    return BlocksInfo.IsAligned();
}

bool TModifyRequest::PrepareToRun()
{
    if (Status == EStatus::InFlight) {
        // can't run a request that is already running
        return false;
    }

    bool allDependenciesCompleted = AllOf(
        Dependencies,
        [](const TModifyRequestWeakPtr& weakRequest)
        {
            if (auto request = weakRequest.lock()) {
                return request->Status == EStatus::Completed;
            }
            return true;
        });
    if (!allDependenciesCompleted) {
        if (Status == EStatus::Created) {
            // Postponing the execution of the newly created request.
            Status = EStatus::Postponed;
            CallContext->Postpone(GetCycleCount());
            DoPostpone();
        }
        return false;
    }

    // The request can be executed. We update status and give a sign to caller
    // that the request needs to be run for execution. We don't run here because
    // we want to minimize the code executed under RequestsLock.
    Status = EStatus::InFlight;
    return true;
}

void TModifyRequest::ExecutePostponed()
{
    CallContext->Advance(GetCycleCount());
    DoExecutePostponed();
}

void TModifyRequest::SetCompleted()
{
    Status = EStatus::Completed;
}

void TModifyRequest::AllocateRMWBuffer(TAlignedDeviceHandler& backend)
{
    auto bufferSize = BlocksInfo.MakeAligned().BufferSize();
    RMWBuffer = backend.AllocateBuffer(bufferSize);

    auto sgListOrError = SgListNormalize(
        TBlockDataRef(RMWBuffer.get(), bufferSize),
        BlocksInfo.BlockSize);
    Y_DEBUG_ABORT_UNLESS(!HasError(sgListOrError));

    RMWBufferSgList = sgListOrError.ExtractResult();
}

////////////////////////////////////////////////////////////////////////////////

TWriteRequest::TWriteRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo,
        TGuardedSgList sgList)
    : TModifyRequest(
          std::move(backend),
          std::move(callContext),
          blocksInfo)
    , SgList(std::move(sgList))
{}

TWriteRequest::TResponseFuture TWriteRequest::ExecuteOrPostpone(bool readyToRun)
{
    return readyToRun ? DoExecute() : Promise.GetFuture();
}

void TWriteRequest::DoPostpone()
{
    Y_ABORT_UNLESS(!Promise.Initialized());
    Promise = NewPromise<NProto::TWriteBlocksLocalResponse>();
}

void TWriteRequest::DoExecutePostponed()
{
    Y_ABORT_UNLESS(Promise.Initialized());
    auto future = DoExecute();
    future.Subscribe([promise = Promise](const TResponseFuture& f) mutable
                     { promise.SetValue(f.GetValue()); });
}

TWriteRequest::TResponseFuture TWriteRequest::DoExecute()
{
    if (auto backend = Backend.lock()) {
        return IsAligned() ? backend->ExecuteWriteRequest(
                                 std::move(CallContext),
                                 BlocksInfo,
                                 std::move(SgList))
                           : ReadModifyWrite(*backend);
    }

    return MakeFuture<NProto::TWriteBlocksLocalResponse>(
        CreateBackendDestroyedResponse());
}

TWriteRequest::TResponseFuture TWriteRequest::ReadModifyWrite(
    TAlignedDeviceHandler& backend)
{
    AllocateRMWBuffer(backend);

    auto read = backend.ExecuteReadRequest(
        CallContext,
        BlocksInfo.MakeAligned(),
        SgList.Create(RMWBufferSgList),
        {});

    return read.Apply(
        [weakPtr = weak_from_this()](
            const TFuture<NProto::TReadBlocksResponse>& future) mutable
        {
            const auto& response = future.GetValue();
            if (HasError(response)) {
                return MakeFuture<NProto::TWriteBlocksResponse>(
                    TErrorResponse(response.GetError()));
            }

            if (auto p = weakPtr.lock()) {
                return static_cast<TWriteRequest*>(p.get())->ModifyAndWrite();
            }

            return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                CreateRequestDestroyedResponse());
        });
}

TWriteRequest::TResponseFuture TWriteRequest::ModifyAndWrite()
{
    auto backend = Backend.lock();
    if (!backend) {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            CreateBackendDestroyedResponse());
    }

    if (auto guard = SgList.Acquire()) {
        const auto& srcSgList = guard.Get();
        auto size = SgListGetSize(srcSgList);
        TBlockDataRef dstBuf(RMWBuffer.get() + BlocksInfo.BeginOffset, size);
        auto cpSize = SgListCopy(srcSgList, {dstBuf});
        Y_ABORT_UNLESS(cpSize == size);
    } else {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            CreateErrorAcquireResponse());
    }

    return backend->ExecuteWriteRequest(
        CallContext,
        BlocksInfo.MakeAligned(),
        SgList.Create(RMWBufferSgList));
}

////////////////////////////////////////////////////////////////////////////////

TZeroRequest::TZeroRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo)
    : TModifyRequest(
          std::move(backend),
          std::move(callContext),
          blocksInfo)
{}

TZeroRequest::~TZeroRequest()
{
    SgList.Close();
}

TZeroRequest::TResponseFuture TZeroRequest::ExecuteOrPostpone(bool readyToRun)
{
    return readyToRun ? DoExecute() : Promise.GetFuture();
}

void TZeroRequest::DoPostpone()
{
    Y_ABORT_UNLESS(!Promise.Initialized());
    Promise = NewPromise<NProto::TZeroBlocksResponse>();
}

void TZeroRequest::DoExecutePostponed()
{
    Y_ABORT_UNLESS(Promise.Initialized());
    auto future = DoExecute();
    future.Subscribe([promise = Promise](const TResponseFuture& f) mutable
                     { promise.SetValue(f.GetValue()); });
}

TZeroRequest::TResponseFuture TZeroRequest::DoExecute()
{
    if (auto backend = Backend.lock()) {
        return IsAligned() ? backend->ExecuteZeroRequest(
                                 std::move(CallContext),
                                 BlocksInfo)
                           : ReadModifyWrite(*backend);
    }

    return MakeFuture<NProto::TZeroBlocksResponse>(
        CreateBackendDestroyedResponse());
}

TZeroRequest::TResponseFuture TZeroRequest::ReadModifyWrite(
    TAlignedDeviceHandler& backend)
{
    AllocateRMWBuffer(backend);
    SgList.SetSgList(RMWBufferSgList);

    auto read = backend.ExecuteReadRequest(
        CallContext,
        BlocksInfo.MakeAligned(),
        SgList,
        {});

    return read.Apply(
        [weakPtr = weak_from_this()](
            const TFuture<NProto::TReadBlocksResponse>& future) mutable
        {
            const auto& response = future.GetValue();
            if (HasError(response)) {
                return MakeFuture<NProto::TZeroBlocksResponse>(
                    TErrorResponse(response.GetError()));
            }

            if (auto p = weakPtr.lock()) {
                return static_cast<TZeroRequest*>(p.get())->ModifyAndWrite();
            }

            return MakeFuture<NProto::TZeroBlocksResponse>(
                CreateRequestDestroyedResponse());
        });
}

TZeroRequest::TResponseFuture TZeroRequest::ModifyAndWrite()
{
    auto backend = Backend.lock();
    if (!backend) {
        return MakeFuture<NProto::TZeroBlocksResponse>(
            CreateBackendDestroyedResponse());
    }

    std::memset(
        RMWBuffer.get() + BlocksInfo.BeginOffset,
        0,
        BlocksInfo.BufferSize());

    auto result =
        backend->ExecuteWriteRequest(CallContext, BlocksInfo.MakeAligned(), SgList);
    return result.Apply(
        [](const TFuture<NProto::TWriteBlocksResponse>& future)
        {
            return MakeFuture<NProto::TZeroBlocksResponse>(
                TErrorResponse(future.GetValue().GetError()));
        });
}

////////////////////////////////////////////////////////////////////////////////

TUnalignedDeviceHandler::TUnalignedDeviceHandler(
        IStoragePtr storage,
        TString diskId,
        TString clientId,
        ui32 blockSize,
        ui32 maxSubRequestSize,
        ui32 maxZeroBlocksSubRequestSize,
        ui32 maxUnalignedRequestSize,
        bool checkBufferModificationDuringWriting,
        bool isReliableMediaKind)
    : Backend(std::make_shared<TAlignedDeviceHandler>(
          std::move(storage),
          std::move(diskId),
          std::move(clientId),
          blockSize,
          maxSubRequestSize,
          maxZeroBlocksSubRequestSize,
          checkBufferModificationDuringWriting,
          isReliableMediaKind))
    , BlockSize(blockSize)
    , MaxUnalignedBlockCount(maxUnalignedRequestSize / BlockSize)
{}

TUnalignedDeviceHandler::~TUnalignedDeviceHandler()
{
    with_lock (RequestsLock) {
        AlignedRequests.clear();
        UnalignedRequests.clear();
    }
}

TFuture<NProto::TReadBlocksLocalResponse> TUnalignedDeviceHandler::Read(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList,
    const TString& checkpointId)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);
    auto normalizeError = TryToNormalize(sgList, blocksInfo);
    if (HasError(normalizeError)) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            TErrorResponse(normalizeError));
    }
    return blocksInfo.IsAligned() ? Backend->ExecuteReadRequest(
                                        std::move(ctx),
                                        blocksInfo,
                                        std::move(sgList),
                                        checkpointId)
                                  : ExecuteUnalignedReadRequest(
                                        std::move(ctx),
                                        blocksInfo,
                                        std::move(sgList),
                                        checkpointId);
}

TFuture<NProto::TWriteBlocksLocalResponse> TUnalignedDeviceHandler::Write(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);
    auto normalizeError = TryToNormalize(sgList, blocksInfo);
    if (HasError(normalizeError)) {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            TErrorResponse(normalizeError));
    }

    if (!blocksInfo.IsAligned() &&
        blocksInfo.Range.Size() > MaxUnalignedBlockCount)
    {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            CreateUnalignedTooBigResponse(blocksInfo.Range.Size()));
    }

    auto request = std::make_shared<TWriteRequest>(
        Backend,
        ctx,
        blocksInfo,
        std::move(sgList));
    auto weakRequest = request->weak_from_this();
    auto* rawRequest = request.get();

    const bool readyToRun = RegisterRequest(std::move(request));
    auto result = rawRequest->ExecuteOrPostpone(readyToRun);
    return result.Apply(
        [weakDeviceHandler = weak_from_this(),
         weakRequest = std::move(weakRequest)](
            const TFuture<NProto::TWriteBlocksLocalResponse>& f) mutable
        {
            if (auto p = weakDeviceHandler.lock()) {
                p->OnRequestFinished(std::move(weakRequest));
            }
            return f.GetValue();
        });
}

TFuture<NProto::TZeroBlocksResponse>
TUnalignedDeviceHandler::Zero(TCallContextPtr ctx, ui64 from, ui64 length)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);
    if (blocksInfo.IsAligned() || blocksInfo.Range.Size() <= 2) {
        return ExecuteZeroBlocksRequest(ctx, blocksInfo);
    }

    ui64 subRequestLength = 0;
    if (blocksInfo.BeginOffset != 0) {
        // handle only first block with unaligned offset
        Y_ABORT_UNLESS(blocksInfo.BlockSize > blocksInfo.BeginOffset);
        subRequestLength = blocksInfo.BlockSize - blocksInfo.BeginOffset;
        blocksInfo.EndOffset = 0;
        blocksInfo.Range.End = blocksInfo.Range.Start;
    } else {
        // handle whole request without last block with unaligned offset
        subRequestLength = (blocksInfo.Range.Size() - 1) * BlockSize;
        blocksInfo.Range.End--;
        blocksInfo.EndOffset = 0;
    }
    from += subRequestLength;
    length -= subRequestLength;

    auto result = ExecuteZeroBlocksRequest(ctx, blocksInfo);
    if (length == 0) {
        return result;
    }

    return result.Apply(
        [weakPtr = weak_from_this(), ctx = std::move(ctx), from, length](
            const TFuture<NProto::TZeroBlocksResponse>& f)
        {
            const auto& response = f.GetValue();
            if (HasError(response)) {
                return f;
            }

            if (auto self = weakPtr.lock()) {
                return self->Zero(std::move(ctx), from, length);
            }
            return MakeFuture<NProto::TZeroBlocksResponse>(
                TErrorResponse(E_CANCELLED));
        });
}

NThreading::TFuture<NProto::TZeroBlocksResponse>
TUnalignedDeviceHandler::ExecuteZeroBlocksRequest(
    TCallContextPtr ctx,
    const TBlocksInfo& blocksInfo)
{
    auto request = std::make_shared<TZeroRequest>(Backend, ctx, blocksInfo);
    auto weakRequest = request->weak_from_this();
    auto* rawRequest = request.get();

    const bool readyToRun = RegisterRequest(std::move(request));
    auto result = rawRequest->ExecuteOrPostpone(readyToRun);
    return result.Apply(
        [weakDeviceHandler = weak_from_this(),
         weakRequest = std::move(weakRequest)](
            const TFuture<NProto::TZeroBlocksResponse>& f) mutable
        {
            if (auto p = weakDeviceHandler.lock()) {
                p->OnRequestFinished(std::move(weakRequest));
            }
            return f.GetValue();
        });
}

TStorageBuffer TUnalignedDeviceHandler::AllocateBuffer(size_t bytesCount)
{
    return Backend->AllocateBuffer(bytesCount);
}

bool TUnalignedDeviceHandler::RegisterRequest(TModifyRequestPtr request)
{
    with_lock (RequestsLock) {
        request->AddDependencies(UnalignedRequests);
        if (request->IsAligned()) {
            AlignedRequests.push_front(request);
            request->SetIt(AlignedRequests.begin());
        } else {
            request->AddDependencies(AlignedRequests);
            UnalignedRequests.push_front(request);
            request->SetIt(UnalignedRequests.begin());
        }
        return request->PrepareToRun();
    }
}

TFuture<NProto::TReadBlocksLocalResponse>
TUnalignedDeviceHandler::ExecuteUnalignedReadRequest(
    TCallContextPtr ctx,
    TBlocksInfo blocksInfo,
    TGuardedSgList sgList,
    TString checkpointId) const
{
    if (blocksInfo.Range.Size() > MaxUnalignedBlockCount) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            CreateUnalignedTooBigResponse(blocksInfo.Range.Size()));
    }

    auto bufferSize = blocksInfo.MakeAligned().BufferSize();
    auto buffer = Backend->AllocateBuffer(bufferSize);

    auto sgListOrError =
        SgListNormalize(TBlockDataRef(buffer.get(), bufferSize), BlockSize);

    if (HasError(sgListOrError)) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            TErrorResponse(sgListOrError.GetError()));
    }

    auto alignedRequest = Backend->ExecuteReadRequest(
        std::move(ctx),
        blocksInfo.MakeAligned(),
        sgList.Create(sgListOrError.ExtractResult()),
        std::move(checkpointId));

    return alignedRequest.Apply(
        [sgList = std::move(sgList),
         buffer = std::move(buffer),
         beginOffset = blocksInfo.BeginOffset](
            const TFuture<NProto::TReadBlocksResponse>& future)
        {
            const auto& response = future.GetValue();
            if (HasError(response)) {
                return response;
            }

            if (auto guard = sgList.Acquire()) {
                const auto& dstSgList = guard.Get();
                auto size = SgListGetSize(dstSgList);
                TBlockDataRef srcBuf(buffer.get() + beginOffset, size);
                auto cpSize = SgListCopy({srcBuf}, dstSgList);
                Y_ABORT_UNLESS(cpSize == size);
                return response;
            }

            return static_cast<NProto::TReadBlocksLocalResponse>(
                CreateErrorAcquireResponse());
        });
}

void TUnalignedDeviceHandler::OnRequestFinished(
    TModifyRequestWeakPtr weakRequest)
{
    // When the request is complete, it must be unregistered. We
    // also need to find and run postponed requests that are waiting for
    // a completetion this one.

    auto request = weakRequest.lock();
    if (!request) {
        return;
    }

    TVector<TModifyRequest*> readyToRunPostponedRequests;

    auto collectReadyToRunRequests =
        [&](const TList<TModifyRequestPtr>& requests)
    {
        for (const auto& request: requests) {
            if (request->PrepareToRun()) {
                readyToRunPostponedRequests.push_back(request.get());
            }
        }
    };

    with_lock (RequestsLock) {
        request->SetCompleted();
        const bool isAligned = request->IsAligned();
        if (isAligned) {
            AlignedRequests.erase(request->GetIt());
        } else {
            UnalignedRequests.erase(request->GetIt());
        }
        // Here we reset the last reference to the request.
        request.reset();
        Y_DEBUG_ABORT_UNLESS(weakRequest.expired());

        collectReadyToRunRequests(UnalignedRequests);
        if (!isAligned) {
            collectReadyToRunRequests(AlignedRequests);
        }
    }

    for (auto* readyToRunRequest: readyToRunPostponedRequests) {
        readyToRunRequest->ExecutePostponed();
    }
}

}   // namespace NCloud::NBlockStore
