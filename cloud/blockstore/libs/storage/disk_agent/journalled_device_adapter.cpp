#include "journalled_device_adapter.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>

#include <cloud/storage/core/libs/common/error.h>
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
    ui32 blockSize) -> std::shared_ptr<NProto::TWriteBlocksRequest>
{
    auto request = std::make_shared<NProto::TWriteBlocksRequest>();

    request->SetStartIndex(group.GetFirstPageNo());
    request->SetBlockSize(blockSize);

    NProto::TIOVector& blocks = *request->MutableBlocks();
    *blocks.MutableBuffers() = std::move(*group.MutableContent());

    return request;
}

auto CreateReadBlocksRequest(const NCloud::NProto::TDevicePageGroupRef& group)
    -> std::shared_ptr<NProto::TReadBlocksRequest>
{
    auto request = std::make_shared<NProto::TReadBlocksRequest>();

    request->SetStartIndex(group.GetFirstPageNo());
    request->SetBlocksCount(group.GetPageCount());
    request->SetBlockSize(group.GetPageSize());

    return request;
}

TResultOrError<ui32> ValidateWriteLogRecordRequest(
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    ui32 blockSize = 0;

    if (request.GetDeviceUUID().empty()) {
        return MakeError(E_ARGUMENT, "empty device UUID");
    }

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

NProto::TError ValidateReadPagesRequest(
    const NCloud::NProto::TReadPagesRequest& request)
{
    if (request.GetDeviceUUID().empty()) {
        return MakeError(E_ARGUMENT, "empty device UUID");
    }

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
    const TString DeviceUUID;
    const TDeviceClientPtr DeviceClient;

public:
    TDeviceAdapter(TString deviceUUID, TDeviceClientPtr deviceClient)
        : DeviceUUID(std::move(deviceUUID))
        , DeviceClient(std::move(deviceClient))
    {}

    // NJournalled::IDevice

    [[nodiscard]] auto ReadPages(
        NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> final
    {
        if (auto error = ValidateReadPagesRequest(request); HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        auto [storageAdapter, error] = DeviceClient->AccessDevice(
            DeviceUUID,
            request.GetHeaders().GetClientId(),
            DefaultAccessMode);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        TVector<TFuture<NProto::TReadBlocksResponse>> futures;
        futures.reserve(request.PageGroupRefsSize());

        auto now = TInstant::Now();
        for (const auto& group: request.GetPageGroupRefs()) {
            futures.push_back(storageAdapter->ReadBlocks(
                now,
                CreateCallContext(),
                CreateReadBlocksRequest(group),
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
        ui32 requestBlockSize = 0;
        if (auto [bs, error] = ValidateWriteLogRecordRequest(request);
            HasError(error))
        {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        } else {
            requestBlockSize = bs;
        }

        auto [storageAdapter, error] = DeviceClient->AccessDevice(
            DeviceUUID,
            request.GetHeaders().GetClientId(),
            DefaultAccessMode);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        }

        TVector<TFuture<NProto::TWriteBlocksResponse>> futures;
        futures.reserve(request.PageGroupsSize());

        auto now = TInstant::Now();
        for (auto& group: *request.MutablePageGroups()) {
            futures.push_back(storageAdapter->WriteBlocks(
                now,
                CreateCallContext(),
                CreateWriteBlocksRequest(std::move(group), requestBlockSize),
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
    TString deviceUUID,
    TDeviceClientPtr deviceClient)
{
    return std::make_shared<TDeviceAdapter>(
        std::move(deviceUUID),
        std::move(deviceClient));
}

}   // namespace NCloud::NBlockStore::NStorage
