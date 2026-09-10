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

////////////////////////////////////////////////////////////////////////////////

auto CreateWriteBlocksRequest(
    NCloud::NProto::TDevicePageGroup&& group,
    ui32 blockSize,
    ui64 firstBlockIndex) -> std::shared_ptr<NProto::TWriteBlocksRequest>
{
    auto request = std::make_shared<NProto::TWriteBlocksRequest>();

    request->SetStartIndex(firstBlockIndex + group.GetFirstPageNo());
    request->SetBlockSize(blockSize);

    NProto::TIOVector& blocks = *request->MutableBlocks();
    *blocks.MutableBuffers() = std::move(*group.MutableContent());

    return request;
}

auto CreateReadBlocksRequest(
    const NCloud::NProto::TDevicePageGroupRef& group,
    ui64 firstBlockIndex) -> std::shared_ptr<NProto::TReadBlocksRequest>
{
    auto request = std::make_shared<NProto::TReadBlocksRequest>();

    request->SetStartIndex(firstBlockIndex + group.GetFirstPageNo());
    request->SetBlocksCount(group.GetPageCount());
    request->SetBlockSize(group.GetPageSize());

    return request;
}

TResultOrError<ui32> ValidateWriteLogRecordRequest(
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    ui32 blockSize = 0;

    if (request.PageGroupsSize() == 0) {
        return MakeError(E_ARGUMENT, "nothing to write");
    }

    for (const auto& group: request.GetPageGroups()) {
        if (group.ContentSize() == 0) {
            return MakeError(E_ARGUMENT, "empty page group");
        }

        for (TStringBuf block: group.GetContent()) {
            if (block.empty()) {
                return MakeError(
                    E_ARGUMENT,
                    "invalid page data: block must not be empty");
            }

            if (blockSize == 0) {
                blockSize = block.size();
                continue;
            }

            if (blockSize != block.size()) {
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "invalid page data: block size mismatch: expected "
                    << blockSize << ", got " << block.size());
            }
        }
    }

    return blockSize;
}

// Checks that the region is made of whole pages and the pages fit in it.
// Returns the index of the region's first block in the pages of this size.
TResultOrError<ui64> ValidateRegion(
    const TDeviceRegion& region,
    ui32 pageSize,
    ui64 firstPageNo,
    ui64 pageCount)
{
    const bool bounded = region.Size != TDeviceRegion::WholeDevice;

    if (region.Offset % pageSize || (bounded && region.Size % pageSize)) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "the device region " << region.Offset << "+"
                             << region.Size << " is not made of pages of "
                             << pageSize << " bytes");
    }

    // an unbounded region leaves the bounds to the device itself
    const ui64 regionPageCount = region.Size / pageSize;
    if (bounded && (firstPageNo >= regionPageCount ||
                    pageCount > regionPageCount - firstPageNo))
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "pages " << firstPageNo << "x" << pageCount
                << " are beyond the device: " << regionPageCount << " pages");
    }

    return region.Offset / pageSize;
}

NProto::TError ValidateReadPagesRequest(
    const NCloud::NProto::TReadPagesRequest& request)
{
    if (request.PageGroupRefsSize() == 0) {
        return MakeError(E_ARGUMENT, "nothing to read");
    }

    for (const auto& group: request.GetPageGroupRefs()) {
        if (group.GetPageCount() == 0) {
            return MakeError(
                E_ARGUMENT,
                "page group ref must contain at least one page");
        }

        if (group.GetPageSize() == 0) {
            return MakeError(E_ARGUMENT, "page size must be greater than zero");
        }
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TDeviceAdapter final
    : public NJournalled::IDevice
{
private:
    const ITimerPtr Timer;
    const TString DeviceUUID;
    const TDeviceClientPtr DeviceClient;
    const TDeviceRegion Region;

public:
    TDeviceAdapter(
            ITimerPtr timer,
            TString deviceUUID,
            TDeviceClientPtr deviceClient,
            TDeviceRegion region)
        : Timer(std::move(timer))
        , DeviceUUID(std::move(deviceUUID))
        , DeviceClient(std::move(deviceClient))
        , Region(region)
    {}

private:
    // A request without a client id is the device's own - the journal reading
    // and writing its parts - and is served whatever the sessions are. The
    // requests of the clients carry their client id and are checked against
    // the sessions, the TCP server does not let a request without one through.
    template <typename TRequest>
    static NProto::TError ValidateClientRequest(const TRequest& request)
    {
        if (!request.GetHeaders().GetClientId().empty() &&
            request.GetDeviceUUID().empty())
        {
            return MakeError(E_ARGUMENT, "empty device UUID");
        }

        return {};
    }

    template <typename TRequest>
    TResultOrError<TStorageAdapterPtr> AccessDevice(const TRequest& request)
    {
        const auto& clientId = request.GetHeaders().GetClientId();

        if (clientId.empty()) {
            return DeviceClient->AccessDevice(DeviceUUID);
        }

        return DeviceClient->AccessDevice(
            DeviceUUID,
            clientId,
            DefaultAccessMode);
    }

public:
    // NJournalled::IDevice

    [[nodiscard]] auto ReadPages(
        NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> final
    {
        if (auto error = ValidateClientRequest(request); HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        if (auto error = ValidateReadPagesRequest(request); HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        auto [storageAdapter, error] = AccessDevice(request);
        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        TVector<TFuture<NProto::TReadBlocksResponse>> futures;
        futures.reserve(request.PageGroupRefsSize());

        auto now = Timer->Now();
        for (const auto& group: request.GetPageGroupRefs()) {
            auto [firstBlockIndex, regionError] = ValidateRegion(
                Region,
                group.GetPageSize(),
                group.GetFirstPageNo(),
                group.GetPageCount());

            if (HasError(regionError)) {
                return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                    TErrorResponse(regionError));
            }

            futures.push_back(storageAdapter->ReadBlocks(
                now,
                CreateCallContext(),
                CreateReadBlocksRequest(group, firstBlockIndex),
                group.GetPageSize(),
                TStringBuf()   // dataBuffer
                ));
        }

        auto all = WaitAll(futures);

        return all.Apply([futures, request = std::move(request)]
            (const TFuture<void>& future) mutable
                -> NCloud::NProto::TReadPagesResponse
            {
                if (future.HasException()) {
                    return TErrorResponse(ResultOrError(future).GetError());
                }

                NCloud::NProto::TReadPagesResponse response;
                auto& groups = *response.MutablePageGroups();
                groups.Reserve(futures.size());

                for (size_t i = 0; i != futures.size(); ++i) {
                    NProto::TReadBlocksResponse sub = futures[i].ExtractValue();
                    if (HasError(sub)) {
                        return TErrorResponse(sub.GetError());
                    }

                    auto& group = *groups.Add();

                    group.SetFirstPageNo(
                        request.GetPageGroupRefs(i).GetFirstPageNo());

                    *group.MutableContent() =
                        std::move(*sub.MutableBlocks()->MutableBuffers());
                }

                return response;
            });
    }

    [[nodiscard]] auto WritePages(
        NCloud::NProto::TWriteLogRecordRequest request)
        -> TFuture<NCloud::NProto::TWriteLogRecordResponse> final
    {
        if (auto error = ValidateClientRequest(request); HasError(error)) {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        }

        ui32 requestBlockSize = 0;
        if (auto [bs, error] = ValidateWriteLogRecordRequest(request);
            HasError(error))
        {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        } else {
            requestBlockSize = bs;
        }

        auto [storageAdapter, error] = AccessDevice(request);
        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        }

        for (const auto& group: request.GetPageGroups()) {
            auto [firstBlockIndex, regionError] = ValidateRegion(
                Region,
                requestBlockSize,
                group.GetFirstPageNo(),
                group.ContentSize());

            if (HasError(regionError)) {
                return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                    TErrorResponse(regionError));
            }
        }

        const ui64 firstBlockIndex = Region.Offset / requestBlockSize;

        TVector<TFuture<NProto::TWriteBlocksResponse>> futures;
        futures.reserve(request.PageGroupsSize());

        auto now = Timer->Now();
        for (auto& group: *request.MutablePageGroups()) {
            futures.push_back(storageAdapter->WriteBlocks(
                now,
                CreateCallContext(),
                CreateWriteBlocksRequest(
                    std::move(group),
                    requestBlockSize,
                    firstBlockIndex),
                requestBlockSize,
                TStringBuf()   // dataBuffer
                ));
        }

        auto all = WaitAll(futures);

        return all.Apply([futures](const TFuture<void>& future) mutable
            -> NCloud::NProto::TWriteLogRecordResponse
            {
                if (future.HasException()) {
                    return TErrorResponse(ResultOrError(future).GetError());
                }

                for (const auto& future: futures) {
                    const auto& sub = future.GetValue();
                    if (HasError(sub)) {
                        return TErrorResponse(sub.GetError());
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
    TDeviceClientPtr deviceClient,
    TDeviceRegion region)
{
    return std::make_shared<TDeviceAdapter>(
        std::move(timer),
        std::move(deviceUUID),
        std::move(deviceClient),
        region);
}

}   // namespace NCloud::NBlockStore::NStorage
