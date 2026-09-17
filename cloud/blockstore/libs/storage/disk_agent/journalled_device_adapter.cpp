#include "journalled_device_adapter.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/journalled_device/device.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr NProto::EVolumeAccessMode DefaultAccessMode =
    NProto::VOLUME_ACCESS_READ_WRITE;
constexpr ui64 DefaultMountSeqNumber = 0;
constexpr ui64 DefaultVolumeGeneration = 0;

////////////////////////////////////////////////////////////////////////////////

auto CreateWriteBlocksRequest(
    const NJournalled::TPageRange& range,
    ui32 blockSize) -> std::shared_ptr<NProto::TWriteBlocksRequest>
{
    auto request = std::make_shared<NProto::TWriteBlocksRequest>();

    request->SetStartIndex(range.FirstPageNo);
    request->SetBlockSize(blockSize);

    auto& buffers = *request->MutableBlocks()->MutableBuffers();
    buffers.Reserve(range.Pages.size());

    for (const auto& page: range.Pages) {
        buffers.Add()->assign(page.Data(), page.Size());
    }

    return request;
}

auto CreateReadBlocksRequest(
    const NJournalled::TPageRangeRef& rangeRef,
    ui32 blockSize) -> std::shared_ptr<NProto::TReadBlocksRequest>
{
    auto request = std::make_shared<NProto::TReadBlocksRequest>();

    request->SetStartIndex(rangeRef.FirstPageNo);
    request->SetBlocksCount(rangeRef.PageCount);
    request->SetBlockSize(blockSize);

    return request;
}

TResultOrError<ui32> ValidateWritePagesRequest(
    const TVector<NJournalled::TPageRange>& ranges)
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
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "invalid page data: block size mismatch: expected "
                        << blockSize << ", got " << block.Size());
            }
        }
    }

    return blockSize;
}

NProto::TError ValidateReadPagesRequest(
    const TVector<NJournalled::TPageRangeRef>& rangeRefs)
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
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TDeviceAdapter final: public NJournalled::IDevice
{
private:
    const ITimerPtr Timer;
    const TString DeviceUUID;
    const TString ClientId;
    const ui32 BlockSize;
    const TDeviceClientPtr DeviceClient;

public:
    TDeviceAdapter(
        ITimerPtr timer,
        TString deviceUUID,
        TString clientId,
        ui32 blockSize,
        TDeviceClientPtr deviceClient)
        : Timer(std::move(timer))
        , DeviceUUID(std::move(deviceUUID))
        , ClientId(std::move(clientId))
        , BlockSize(blockSize)
        , DeviceClient(std::move(deviceClient))
    {}

    // IStartable

    void Start() override
    {
        auto [updated, error] = DeviceClient->AcquireDevices(
            {DeviceUUID},
            ClientId,
            Timer->Now(),
            DefaultAccessMode,
            DefaultMountSeqNumber,
            ClientId,   // diskId
            DefaultVolumeGeneration);

        Y_UNUSED(updated);

        CheckError(error);
    }

    void Stop() override
    {
        // there is nothing we can do about the error here, the session will
        // expire on its own after ReleaseInactiveSessionsTimeout
        Y_UNUSED(DeviceClient->ReleaseDevices(
            {DeviceUUID},
            ClientId,
            ClientId,   // diskId
            DefaultVolumeGeneration));
    }

    // IDevice

    [[nodiscard]] auto ReadPages(TVector<NJournalled::TPageRangeRef> rangeRefs)
        -> TFuture<TResultOrError<TVector<TBuffer>>> final
    {
        using TResult = TResultOrError<TVector<TBuffer>>;

        if (auto error = ValidateReadPagesRequest(rangeRefs); HasError(error)) {
            return MakeFuture<TResult>(std::move(error));
        }

        auto [storageAdapter, error] =
            DeviceClient->AccessDevice(DeviceUUID, ClientId, DefaultAccessMode);

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
                CreateReadBlocksRequest(rangeRef, BlockSize),
                BlockSize,
                TStringBuf()   // dataBuffer
                ));
        }

        auto all = WaitAll(futures);

        return all.Apply(
            [futures](const TFuture<void>& future) mutable -> TResult
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

    [[nodiscard]] auto WritePages(TVector<NJournalled::TPageRange> ranges)
        -> TFuture<NProto::TError> final
    {
        ui32 requestBlockSize = 0;
        if (auto [bs, error] = ValidateWritePagesRequest(ranges);
            HasError(error))
        {
            return MakeFuture(std::move(error));
        } else {
            requestBlockSize = bs;
        }

        auto [storageAdapter, error] =
            DeviceClient->AccessDevice(DeviceUUID, ClientId, DefaultAccessMode);

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
                CreateWriteBlocksRequest(range, requestBlockSize),
                requestBlockSize,
                TStringBuf()   // dataBuffer
                ));
        }

        auto all = WaitAll(futures);

        return all.Apply(
            [futures](const TFuture<void>& future) mutable -> NProto::TError
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

NJournalled::IDevicePtr CreateDeviceAdapter(
    ITimerPtr timer,
    TString deviceUUID,
    TString clientId,
    ui32 blockSize,
    TDeviceClientPtr deviceClient)
{
    return std::make_shared<TDeviceAdapter>(
        std::move(timer),
        std::move(deviceUUID),
        std::move(clientId),
        blockSize,
        std::move(deviceClient));
}

}   // namespace NCloud::NBlockStore::NStorage
