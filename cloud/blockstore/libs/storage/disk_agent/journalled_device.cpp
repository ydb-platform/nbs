#include "journalled_device.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/journalled_device/journalled_device.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

#include <atomic>

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

    if (request.GetLogSequenceNumber() <= request.GetPrevLogSequenceNumber()) {
        return MakeError(E_ARGUMENT, TStringBuilder()
                << "invalid lsn: " << request.GetLogSequenceNumber()
                << ", must be greater than the prev one: "
                << request.GetPrevLogSequenceNumber());
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

class TJournalledDevice final
    : public NJournalled::IJournalledDevice
    , public std::enable_shared_from_this<TJournalledDevice>
{
private:
    const TString DeviceUUID;
    const TDeviceClientPtr DeviceClient;

    std::atomic<ui64> LastLsn = 0;

public:
    TJournalledDevice(TString deviceUUID, TDeviceClientPtr deviceClient)
        : DeviceUUID(std::move(deviceUUID))
        , DeviceClient(std::move(deviceClient))
    {}

    void Start() override {}
    void Stop() override {}

    // IJournalledDevice

    [[nodiscard]] auto ReadPages(
        TInstant now,
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

        return all.Apply(
            [futures,
             request = std::move(request)](const TFuture<void>& future) mutable
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

    [[nodiscard]] auto WriteLogRecord(
        TInstant now,
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

        const ui64 lastLsn = LastLsn.load(std::memory_order_relaxed);
        const ui64 lsn = request.GetLogSequenceNumber();
        const ui64 prevLsn = request.GetPrevLogSequenceNumber();

        // TODO(#6956): allow to handle request with wrong lsn order
        if (lastLsn != 0 && prevLsn != lastLsn) {
            const auto code = prevLsn > lastLsn ? E_REJECTED : E_INVALID_STATE;

            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(code, TStringBuilder()
                    << "Wrong lsn: " << prevLsn << ", expected " << lastLsn));
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

        return all.Apply(
            [futures, self = shared_from_this(), lsn](
                const TFuture<void>& future) mutable
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

                self->LastLsn.store(lsn, std::memory_order_relaxed);

                return {};
            });
    }

    [[nodiscard]] auto ReadJournalTail(
        TInstant now,
        NCloud::NProto::TReadJournalTailRequest request)
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> final
    {
        // TODO(#6956): implement journal tail reading
        Y_UNUSED(now, request);

        return MakeFuture<NCloud::NProto::TReadJournalTailResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED, "ReadJournalTail"));
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        TInstant now,
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        // TODO(#6956): implement lsn low watermark advancing
        Y_UNUSED(now, request);

        return MakeFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED, "AdvanceLsnLowWatermark"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NJournalled::IJournalledDevicePtr CreateJournalledDevice(
    TString deviceUUID,
    TDeviceClientPtr deviceClient)
{
    return std::make_shared<TJournalledDevice>(
        std::move(deviceUUID),
        std::move(deviceClient));
}

}   // namespace NCloud::NBlockStore::NStorage
