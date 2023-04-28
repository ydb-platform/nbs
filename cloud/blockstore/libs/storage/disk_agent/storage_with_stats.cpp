#include "storage_with_stats.h"

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
bool HasExceptionOrError(const TFuture<T>& future)
{
    return future.HasException() || HasError(future.GetValue().GetError());
}

////////////////////////////////////////////////////////////////////////////////

struct TStorageWithIoStats final
    : public IStorage
{
    IStoragePtr Storage;
    TStorageIoStatsPtr Stats;
    ui32 BlockSize;

    TStorageWithIoStats(
            IStoragePtr storage,
            TStorageIoStatsPtr stats,
            ui32 blockSize)
        : Storage(std::move(storage))
        , Stats(std::move(stats))
        , BlockSize(blockSize)
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Stats->OnZeroStart();

        const ui64 requestBytes = static_cast<ui64>(request->GetBlocksCount()) * BlockSize;

        const auto started = Now();
        auto stats = Stats;

        auto result = Storage->ZeroBlocks(
            std::move(callContext),
            std::move(request));

        return result.Subscribe([=] (const auto& future) {
            if (HasExceptionOrError(future)) {
                stats->OnError();
            } else {
                stats->OnZeroComplete(Now() - started, requestBytes);
            }
        });
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Stats->OnReadStart();

        const ui64 requestBytes = static_cast<ui64>(request->GetBlocksCount()) * BlockSize;
        const auto started = Now();
        auto stats = Stats;

        auto result = Storage->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));

        return result.Subscribe([=] (const auto& future) {
            if (HasExceptionOrError(future)) {
                stats->OnError();
            } else {
                stats->OnReadComplete(Now() - started, requestBytes);
            }
        });
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Stats->OnWriteStart();

        const ui64 requestBytes = static_cast<ui64>(request->BlocksCount) * BlockSize;
        const auto started = Now();
        auto stats = Stats;

        auto result = Storage->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));

        return result.Subscribe([=] (const auto& future) {
            if (HasExceptionOrError(future)) {
                stats->OnError();
            } else {
                stats->OnWriteComplete(Now() - started, requestBytes);
            }
        });
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Stats->OnEraseStart();

        const auto started = Now();
        auto stats = Stats;

        auto result = Storage->EraseDevice(method);

        return result.Subscribe([=] (const auto& future) {
            if (future.HasException() || HasError(future.GetValue())) {
                stats->OnError();
            } else {
                stats->OnEraseComplete(Now() - started);
            }
        });
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Storage->AllocateBuffer(bytesCount);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageIoStats::OnReadStart()
{
    AtomicIncrement(NumReadOps);
}

void TStorageIoStats::OnReadComplete(TDuration duration, ui64 requestBytes)
{
    IncrementBucket(duration);
    AtomicAdd(BytesRead, requestBytes);
}

void TStorageIoStats::OnWriteStart()
{
    AtomicIncrement(NumWriteOps);
}

void TStorageIoStats::OnWriteComplete(TDuration duration, ui64 requestBytes)
{
    AtomicAdd(BytesWritten, requestBytes);
    IncrementBucket(duration);
}

void TStorageIoStats::OnZeroStart()
{
    AtomicIncrement(NumZeroOps);
}

void TStorageIoStats::OnZeroComplete(TDuration duration, ui64 requestBytes)
{
    AtomicAdd(BytesZeroed, requestBytes);
    IncrementBucket(duration);
}

void TStorageIoStats::OnError()
{
    AtomicIncrement(Errors);
}

void TStorageIoStats::OnEraseStart()
{
    AtomicIncrement(NumEraseOps);
}

void TStorageIoStats::OnEraseComplete(TDuration duration)
{
    IncrementBucket(duration);
}

void TStorageIoStats::IncrementBucket(TDuration duration)
{
    auto it = std::lower_bound(
        std::begin(Limits),
        std::end(Limits),
        duration);

    Y_VERIFY_DEBUG(it != std::end(Limits));

    if (it != std::end(Limits)) {
        auto idx = std::distance(std::begin(Limits), it);
        AtomicIncrement(Buckets[idx]);
    }
}

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateStorageWithIoStats(
    IStoragePtr storage,
    TStorageIoStatsPtr stats,
    ui32 blockSize)
{
    return std::make_shared<TStorageWithIoStats>(
        std::move(storage),
        std::move(stats),
        blockSize);
}

}   // namespace NCloud::NBlockStore::NStorage
