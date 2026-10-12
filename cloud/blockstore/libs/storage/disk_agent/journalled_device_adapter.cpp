#include "journalled_device_adapter.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NJournalled;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto CreateWriteBlocksRequest(
    const TPageRange& range,
    ui32 blockSize,
    ui64 firstBlockIndex) -> std::shared_ptr<NProto::TWriteBlocksRequest>
{
    auto request = std::make_shared<NProto::TWriteBlocksRequest>();

    request->SetStartIndex(firstBlockIndex + range.FirstPageNo);
    request->SetBlockSize(blockSize);

    auto& buffers = *request->MutableBlocks()->MutableBuffers();
    buffers.Reserve(range.Pages.size());

    for (const auto& page: range.Pages) {
        buffers.Add()->assign(page.Data(), page.Size());
    }

    return request;
}

auto CreateReadBlocksRequest(
    const TPageRangeRef& rangeRef,
    ui32 blockSize,
    ui64 firstBlockIndex) -> std::shared_ptr<NProto::TReadBlocksRequest>
{
    auto request = std::make_shared<NProto::TReadBlocksRequest>();

    request->SetStartIndex(firstBlockIndex + rangeRef.FirstPageNo);
    request->SetBlocksCount(rangeRef.PageCount);
    request->SetBlockSize(blockSize);

    return request;
}

auto CreateZeroBlocksRequest(
    const TPageRangeRef& rangeRef,
    ui64 firstBlockIndex) -> std::shared_ptr<NProto::TZeroBlocksRequest>
{
    auto request = std::make_shared<NProto::TZeroBlocksRequest>();

    request->SetStartIndex(firstBlockIndex + rangeRef.FirstPageNo);
    request->SetBlocksCount(rangeRef.PageCount);

    return request;
}

// Checks that the pages fit in the region. An unbounded region leaves the
// bounds to the device itself.
NProto::TError ValidatePagesInRegion(
    const TPageRangeRef& region,
    ui64 firstPageNo,
    ui64 pageCount)
{
    if (firstPageNo >= region.PageCount ||
        pageCount > region.PageCount - firstPageNo)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "pages " << firstPageNo << "x" << pageCount
                << " are beyond the device: " << region.PageCount
                << " pages");
    }

    return {};
}

TResultOrError<ui32> ValidateWritePagesRequest(
    const TVector<TPageRange>& ranges,
    const TPageRangeRef& region)
{
    ui32 blockSize = 0;

    if (ranges.empty()) {
        return MakeError(E_ARGUMENT, "nothing to write");
    }

    for (const auto& range: ranges) {
        if (range.Pages.empty()) {
            return MakeError(E_ARGUMENT, "empty page group");
        }

        for (const TBuffer& block: range.Pages) {
            if (block.Size() == 0) {
                return MakeError(
                    E_ARGUMENT,
                    "invalid page data: block must not be empty");
            }

            if (blockSize == 0) {
                blockSize = block.Size();
                continue;
            }

            if (blockSize != block.Size()) {
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "invalid page data: block size mismatch: expected "
                    << blockSize << ", got " << block.Size());
            }
        }

        if (auto error = ValidatePagesInRegion(
                region,
                range.FirstPageNo,
                range.Pages.size());
            HasError(error))
        {
            return error;
        }
    }

    return blockSize;
}

NProto::TError ValidateReadPagesRequest(
    const TVector<TPageRangeRef>& rangeRefs,
    const TPageRangeRef& region)
{
    if (rangeRefs.empty()) {
        return MakeError(E_ARGUMENT, "nothing to read");
    }

    for (const auto& rangeRef: rangeRefs) {
        if (rangeRef.PageCount == 0) {
            return MakeError(
                E_ARGUMENT,
                "page group ref must contain at least one page");
        }

        if (auto error = ValidatePagesInRegion(
                region,
                rangeRef.FirstPageNo,
                rangeRef.PageCount);
            HasError(error))
        {
            return error;
        }
    }

    return {};
}

NProto::TError ValidateZeroPagesRequest(
    const TVector<TPageRangeRef>& rangeRefs,
    const TPageRangeRef& region)
{
    if (rangeRefs.empty()) {
        return MakeError(E_ARGUMENT, "nothing to zero");
    }

    for (const auto& rangeRef: rangeRefs) {
        if (rangeRef.PageCount == 0) {
            return MakeError(
                E_ARGUMENT,
                "page group ref must contain at least one page");
        }

        if (auto error = ValidatePagesInRegion(
                region,
                rangeRef.FirstPageNo,
                rangeRef.PageCount);
            HasError(error))
        {
            return error;
        }
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TDeviceAdapter final: public IDevice
{
private:
    const ITimerPtr Timer;
    const TDeviceClientPtr DeviceClient;
    const TString DeviceUUID;
    const TPageRangeRef Region;
    const ui32 BlockSize;

public:
    TDeviceAdapter(
            ITimerPtr timer,
            TDeviceClientPtr deviceClient,
            TString deviceUUID,
            TPageRangeRef region,
            ui32 blockSize)
        : Timer(std::move(timer))
        , DeviceClient(std::move(deviceClient))
        , DeviceUUID(std::move(deviceUUID))
        , Region(region)
        , BlockSize(blockSize)
    {}

    // NJournalled::IDevice

    [[nodiscard]] auto ReadPages(
        TVector<TPageRangeRef> rangeRefs)
        -> TFuture<TResultOrError<TVector<TBuffer>>> final
    {
        using TResult = TResultOrError<TVector<TBuffer>>;

        if (auto error = ValidateReadPagesRequest(rangeRefs, Region);
            HasError(error))
        {
            return MakeFuture<TResult>(std::move(error));
        }

        auto [storageAdapter, error] = DeviceClient->AccessDevice(DeviceUUID);
        if (HasError(error)) {
            return MakeFuture<TResult>(std::move(error));
        }

        TVector<TFuture<NProto::TReadBlocksResponse>> futures;
        futures.reserve(rangeRefs.size());

        auto now = Timer->Now();
        for (const auto& rangeRef: rangeRefs) {
            futures.push_back(storageAdapter->ReadBlocks(
                now,
                CreateCallContext(),
                CreateReadBlocksRequest(
                    rangeRef,
                    BlockSize,
                    Region.FirstPageNo),
                BlockSize,
                TStringBuf()   // dataBuffer
                ));
        }

        auto all = WaitAll(futures);

        return all.Apply([futures](const TFuture<void>& future) mutable
            -> TResult
            {
                if (future.HasException()) {
                    return ResultOrError(future).GetError();
                }

                TVector<TBuffer> pages;

                for (auto& future: futures) {
                    NProto::TReadBlocksResponse sub = future.ExtractValue();
                    if (HasError(sub)) {
                        return sub.GetError();
                    }

                    for (const auto& block: sub.GetBlocks().GetBuffers()) {
                        pages.emplace_back(block.data(), block.size());
                    }
                }

                return std::move(pages);
            });
    }

    [[nodiscard]] auto WritePages(TVector<TPageRange> ranges)
        -> TFuture<NProto::TError> final
    {
        ui32 requestBlockSize = 0;
        if (auto [bs, error] = ValidateWritePagesRequest(ranges, Region);
            HasError(error))
        {
            return MakeFuture(std::move(error));
        } else {
            requestBlockSize = bs;
        }

        auto [storageAdapter, error] = DeviceClient->AccessDevice(DeviceUUID);
        if (HasError(error)) {
            return MakeFuture(std::move(error));
        }

        TVector<TFuture<NProto::TWriteBlocksResponse>> futures;
        futures.reserve(ranges.size());

        auto now = Timer->Now();
        for (const auto& range: ranges) {
            futures.push_back(storageAdapter->WriteBlocks(
                now,
                CreateCallContext(),
                CreateWriteBlocksRequest(
                    range,
                    requestBlockSize,
                    Region.FirstPageNo),
                requestBlockSize,
                TStringBuf()   // dataBuffer
                ));
        }

        auto all = WaitAll(futures);

        return all.Apply([futures](const TFuture<void>& future) mutable
            -> NProto::TError
            {
                if (future.HasException()) {
                    return ResultOrError(future).GetError();
                }

                for (const auto& future: futures) {
                    const auto& sub = future.GetValue();
                    if (HasError(sub)) {
                        return sub.GetError();
                    }
                }

                return {};
            });
    }

    [[nodiscard]] auto ZeroPages(TVector<TPageRangeRef> ranges)
        -> TFuture<NProto::TError> final
    {
        if (auto error = ValidateZeroPagesRequest(ranges, Region);
            HasError(error))
        {
            return MakeFuture(std::move(error));
        }

        auto [storageAdapter, error] = DeviceClient->AccessDevice(DeviceUUID);
        if (HasError(error)) {
            return MakeFuture(std::move(error));
        }

        TVector<TFuture<NProto::TZeroBlocksResponse>> futures;
        futures.reserve(ranges.size());

        auto now = Timer->Now();
        for (const auto& range: ranges) {
            futures.push_back(storageAdapter->ZeroBlocks(
                now,
                CreateCallContext(),
                CreateZeroBlocksRequest(range, Region.FirstPageNo),
                BlockSize));
        }

        auto all = WaitAll(futures);

        return all.Apply([futures](const TFuture<void>& future) mutable
            -> NProto::TError
            {
                if (future.HasException()) {
                    return ResultOrError(future).GetError();
                }

                for (const auto& future: futures) {
                    const auto& sub = future.GetValue();
                    if (HasError(sub)) {
                        return sub.GetError();
                    }
                }

                return {};
            });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateDeviceAdapter(
    ITimerPtr timer,
    TDeviceClientPtr deviceClient,
    TString deviceUUID,
    TPageRangeRef region,
    ui32 blockSize)
{
    return std::make_shared<TDeviceAdapter>(
        std::move(timer),
        std::move(deviceClient),
        std::move(deviceUUID),
        region,
        blockSize);
}

}   // namespace NCloud::NBlockStore::NStorage
