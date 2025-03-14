#include "compound_storage.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist_block_range.h>

#include <util/generic/algorithm.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
TFuture<TResponse> FutureErrorResponse(ui32 code, TString message)
{
    return MakeFuture(ErrorResponse<TResponse>(code, std::move(message)));
}

////////////////////////////////////////////////////////////////////////////////

struct TStorageBlockRange
{
    ui32 Storage = 0;
    TBlockRange64 BlockRange;

    TStorageBlockRange() = default;

    TStorageBlockRange(ui32 storage, TBlockRange64 blockRange)
        : Storage(storage)
        , BlockRange(blockRange)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TStorageIterator
{
private:
    const TVector<ui64>& Offsets;
    const ui64 EndBlock;
    ui32 CurrentStorage;
    ui32 LastStorage;
    ui64 StorageBlockOffset;

public:
    TStorageIterator(
            const TVector<ui64>& offsets,
            ui64 startIndex,
            ui32 blocks)
        : Offsets(offsets)
        , EndBlock(startIndex + blocks)
    {
        auto b = UpperBound(Offsets.begin(), Offsets.end(), startIndex);
        CurrentStorage = std::distance(Offsets.begin(), b);

        auto e = UpperBound(Offsets.begin(), Offsets.end(), EndBlock);
        LastStorage = std::distance(Offsets.begin(), e);
        LastStorage += StartOffset(LastStorage) < EndBlock;

        StorageBlockOffset = startIndex - StartOffset(CurrentStorage);

        if (CurrentStorage >= offsets.size() || LastStorage > offsets.size()) {
            CurrentStorage = 0;
            LastStorage = 0;
        }
    }

public:
    ui32 Count() const
    {
        return LastStorage - CurrentStorage;
    }

    bool Next(TStorageBlockRange& storageBlockRange)
    {
        if (CurrentStorage == LastStorage) {
            return false;
        }

        storageBlockRange.Storage = CurrentStorage;
        storageBlockRange.BlockRange =
            TBlockRange64::MakeClosedIntervalWithLimit(
                StorageBlockOffset,
                Blocks(CurrentStorage) - 1,
                EndBlock - StartOffset(CurrentStorage) - 1);

        ++CurrentStorage;
        StorageBlockOffset = 0;

        return true;
    }

private:
    ui64 StartOffset(ui32 storage) const
    {
        return storage ? Offsets[storage - 1] : 0;
    }

    ui64 Blocks(ui32 storage) const
    {
        return Offsets[storage] - StartOffset(storage);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TReadBlocksCtx
{
    TPromise<NProto::TReadBlocksResponse> Promise;

    TVector<NProto::TReadBlocksResponse> SubResponses;
    TAtomic RemainingRequests;
    const ui32 BlockCount;

    TReadBlocksCtx(ui32 count, ui32 blockCount)
        : Promise(NewPromise<NProto::TReadBlocksResponse>())
        , SubResponses(count)
        , RemainingRequests(count)
        , BlockCount(blockCount)
    {}

    void OnResponse(NProto::TReadBlocksResponse subResponse, ui32 index)
    {
        SubResponses[index] = std::move(subResponse);

        if (AtomicDecrement(RemainingRequests) == 0) {
            OnReady();
        }
    }

    void OnReady()
    {
        NProto::TReadBlocksResponse response;
        auto* buffers = response.MutableBlocks()->MutableBuffers();
        buffers->Reserve(BlockCount);

        auto dst = RepeatedFieldBackInserter(buffers);

        for (auto& subResponse: SubResponses) {
            if (HasError(subResponse)) {
                Promise.SetValue(std::move(subResponse));
                return;
            }

            auto& src = *subResponse.MutableBlocks()->MutableBuffers();

            std::copy(
                std::make_move_iterator(src.begin()),
                std::make_move_iterator(src.end()),
                dst);
        }

        Promise.SetValue(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename R>
struct TGenericBlocksCtx
{
    TPromise<R> Promise;
    TAtomic RemainingRequests;
    TAdaptiveLock Lock;
    NProto::TError LastError;

    explicit TGenericBlocksCtx(ui32 count)
        : Promise(NewPromise<R>())
        , RemainingRequests(count)
    {}

    void OnResponse(const NProto::TError& error)
    {
        if (HasError(error)) {
            with_lock (Lock) {
                LastError.CopyFrom(error);
            }
        }

        if (AtomicDecrement(RemainingRequests) == 0) {
            R response;
            response.MutableError()->Swap(&LastError);
            Promise.SetValue(std::move(response));
        }
    }
};

using TWriteBlocksCtx = TGenericBlocksCtx<NProto::TWriteBlocksResponse>;
using TZeroBlocksCtx = TGenericBlocksCtx<NProto::TZeroBlocksResponse>;

////////////////////////////////////////////////////////////////////////////////

template <typename R>
struct TLocalBlocksCtx : TGenericBlocksCtx<R>
{
    TGuardedSgList::TGuard Guard;

    TLocalBlocksCtx(ui32 count, TGuardedSgList::TGuard guard)
        : TGenericBlocksCtx<R>(count)
        , Guard(std::move(guard))
    {}
};

using TReadBlocksLocalCtx = TLocalBlocksCtx<NProto::TReadBlocksLocalResponse>;
using TWriteBlocksLocalCtx = TLocalBlocksCtx<NProto::TWriteBlocksLocalResponse>;

////////////////////////////////////////////////////////////////////////////////

struct TEraseDeviceCtx
{
    TPromise<NProto::TError> Promise;
    TAtomic RemainingRequests;
    TAdaptiveLock Lock;
    NProto::TError LastError;

    explicit TEraseDeviceCtx(ui32 count)
        : Promise(NewPromise<NProto::TError>())
        , RemainingRequests(count)
    {}

    void OnResponse(const NProto::TError& error)
    {
        if (HasError(error)) {
            with_lock (Lock) {
                LastError.CopyFrom(error);
            }
        }

        if (AtomicDecrement(RemainingRequests) == 0) {
            Promise.SetValue(std::move(LastError));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompoundStorage final
    : public IStorage
{
private:
    const TVector<IStoragePtr> Storages;
    const TVector<ui64> Offsets;
    const ui32 BlockSize;
    const TString DiskId;
    const TString ClientId;
    const IServerStatsPtr ServerStats;

public:
    TCompoundStorage(
            TVector<IStoragePtr> storages,
            TVector<ui64> offsets,
            ui32 blockSize,
            TString diskId,
            TString clientId,
            IServerStatsPtr serverStats)
        : Storages(std::move(storages))
        , Offsets(std::move(offsets))
        , BlockSize(blockSize)
        , DiskId(std::move(diskId))
        , ClientId(std::move(clientId))
        , ServerStats(std::move(serverStats))
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override;

    void ReportIOError() override;

private:
    void OnReadRequestFastPathHits()
    {
        ServerStats->RequestFastPathHit(
            DiskId,
            ClientId,
            EBlockStoreRequest::ReadBlocks);
    }

    void OnWriteRequestFastPathHits()
    {
        ServerStats->RequestFastPathHit(
            DiskId,
            ClientId,
            EBlockStoreRequest::WriteBlocks);
    }

    void OnZeroRequestFastPathHits()
    {
        ServerStats->RequestFastPathHit(
            DiskId,
            ClientId,
            EBlockStoreRequest::ZeroBlocks);
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TZeroBlocksResponse> TCompoundStorage::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    const ui32 totalBlockCount = request->GetBlocksCount();

    if (totalBlockCount == 0) {
        return MakeFuture(NProto::TZeroBlocksResponse());
    }

    TStorageIterator it(
        Offsets,
        request->GetStartIndex(),
        totalBlockCount);

    const ui32 count = it.Count();

    if (count == 0) {
        return FutureErrorResponse<NProto::TZeroBlocksResponse>(
            E_ARGUMENT,
            "Invalid block range");
    }

    if (count == 1) {
        OnZeroRequestFastPathHits();

        TStorageBlockRange storageBlockRange;
        it.Next(storageBlockRange);

        request->SetStartIndex(storageBlockRange.BlockRange.Start);

        Y_ABORT_UNLESS(storageBlockRange.BlockRange.Size() == totalBlockCount);

        return Storages[storageBlockRange.Storage]->ZeroBlocks(
            std::move(callContext),
            std::move(request));
    }

    auto requestContext = std::make_shared<TZeroBlocksCtx>(count);

    TStorageBlockRange storageBlockRange;
    while (it.Next(storageBlockRange)) {
        auto subRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        subRequest->MutableHeaders()->CopyFrom(request->GetHeaders());
        subRequest->SetStartIndex(storageBlockRange.BlockRange.Start);
        subRequest->SetBlocksCount(storageBlockRange.BlockRange.Size());

        Storages[storageBlockRange.Storage]->ZeroBlocks(
            callContext,
            std::move(subRequest)
        ).Subscribe([=] (const auto& future) {
            requestContext->OnResponse(future.GetValue().GetError());
        });
    }

    return requestContext->Promise;
}

TFuture<NProto::TReadBlocksLocalResponse> TCompoundStorage::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    const ui32 totalBlockCount = request->GetBlocksCount();

    if (totalBlockCount == 0) {
        return MakeFuture(NProto::TReadBlocksLocalResponse());
    }

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return FutureErrorResponse<NProto::TReadBlocksLocalResponse>(
            E_CANCELLED,
            "failed to acquire sglist in CompoundStorage");
    }

    TStorageIterator it(
        Offsets,
        request->GetStartIndex(),
        totalBlockCount);

    const ui32 count = it.Count();

    if (count == 0) {
        return FutureErrorResponse<NProto::TReadBlocksLocalResponse>(
            E_ARGUMENT,
            "Invalid block range");
    }

    if (count == 1) {
        OnReadRequestFastPathHits();

        TStorageBlockRange storageBlockRange;
        it.Next(storageBlockRange);

        request->SetStartIndex(storageBlockRange.BlockRange.Start);

        Y_ABORT_UNLESS(storageBlockRange.BlockRange.Size() == totalBlockCount);

        return Storages[storageBlockRange.Storage]->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TSgListBlockRange src(guard.Get(), BlockSize);

    auto requestContext = std::make_shared<TReadBlocksLocalCtx>(
        count,
        std::move(guard));

    TStorageBlockRange storageBlockRange;
    while (it.Next(storageBlockRange)) {
        ui64 startIndex = storageBlockRange.BlockRange.Start;
        ui32 blockCount = storageBlockRange.BlockRange.Size();

        auto subRequest = std::make_shared<NProto::TReadBlocksLocalRequest>();
        subRequest->MutableHeaders()->CopyFrom(request->GetHeaders());
        subRequest->SetStartIndex(startIndex);
        subRequest->SetBlocksCount(blockCount);
        subRequest->BlockSize = request->BlockSize;
        subRequest->Sglist.SetSgList(src.Next(blockCount));

        Storages[storageBlockRange.Storage]->ReadBlocksLocal(
            callContext,
            std::move(subRequest)
        ).Subscribe([=] (const auto& future) {
            requestContext->OnResponse(future.GetValue().GetError());
        });
    }

    return requestContext->Promise;
}

TFuture<NProto::TWriteBlocksLocalResponse> TCompoundStorage::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    const ui32 totalBlockCount = request->BlocksCount;

    if (totalBlockCount == 0) {
        return MakeFuture(NProto::TWriteBlocksLocalResponse());
    }

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return FutureErrorResponse<NProto::TWriteBlocksLocalResponse>(
            E_CANCELLED,
            "failed to acquire sglist in CompoundStorage");
    }

    TStorageIterator it(
        Offsets,
        request->GetStartIndex(),
        totalBlockCount);

    const ui32 count = it.Count();

    if (count == 0) {
        return FutureErrorResponse<NProto::TWriteBlocksLocalResponse>(
            E_ARGUMENT,
            "Invalid block range");
    }

    if (count == 1) {
        OnWriteRequestFastPathHits();

        TStorageBlockRange storageBlockRange;
        it.Next(storageBlockRange);

        request->SetStartIndex(storageBlockRange.BlockRange.Start);

        Y_ABORT_UNLESS(storageBlockRange.BlockRange.Size() == totalBlockCount);

        return Storages[storageBlockRange.Storage]->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TSgListBlockRange dst(guard.Get(), BlockSize);

    auto requestContext = std::make_shared<TWriteBlocksLocalCtx>(
        count,
        std::move(guard));

    TStorageBlockRange storageBlockRange;
    while (it.Next(storageBlockRange)) {
        ui64 startIndex = storageBlockRange.BlockRange.Start;
        ui32 blockCount = storageBlockRange.BlockRange.Size();

        auto subRequest = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        subRequest->MutableHeaders()->CopyFrom(request->GetHeaders());
        subRequest->SetStartIndex(startIndex);
        subRequest->BlocksCount = blockCount;
        subRequest->BlockSize = request->BlockSize;
        subRequest->Sglist.SetSgList(dst.Next(blockCount));

        Storages[storageBlockRange.Storage]->WriteBlocksLocal(
            callContext,
            std::move(subRequest)
        ).Subscribe([=] (const auto& future) {
            requestContext->OnResponse(future.GetValue().GetError());
        });
    }

    return requestContext->Promise;
}

TFuture<NProto::TError> TCompoundStorage::EraseDevice(
    NProto::EDeviceEraseMethod method)
{
    auto context = std::make_shared<TEraseDeviceCtx>(Storages.size());

    for (auto& storage: Storages) {
        storage->EraseDevice(method).Subscribe([=] (auto& future) {
            context->OnResponse(future.GetValue());
        });
    }

    return context->Promise;
}

TStorageBuffer TCompoundStorage::AllocateBuffer(size_t bytesCount)
{
    Y_ABORT_UNLESS(!Storages.empty());

    return Storages.front()->AllocateBuffer(bytesCount);
}

void TCompoundStorage::ReportIOError()
{
    for (const auto& storage: Storages) {
        storage->ReportIOError();
    }
}


}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateCompoundStorage(
    TVector<IStoragePtr> storages,
    TVector<ui64> offsets,
    ui32 blockSize,
    TString diskId,
    TString clientId,
    IServerStatsPtr serverStats)
{
    return std::make_shared<TCompoundStorage>(
        std::move(storages),
        std::move(offsets),
        blockSize,
        std::move(diskId),
        std::move(clientId),
        std::move(serverStats));
}

}   // namespace NCloud::NBlockStore::NServer
