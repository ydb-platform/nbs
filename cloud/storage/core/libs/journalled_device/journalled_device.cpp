#include "journalled_device.h"

#include "device.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

#include <atomic>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJournalledDevice final
    : public IJournalledDevice
    , public std::enable_shared_from_this<TJournalledDevice>
{
private:
    const IDevicePtr DataStore;

    std::atomic<ui64> LastLsn = 0;

public:
    TJournalledDevice(IDevicePtr dataStore)
        : DataStore(std::move(dataStore))
    {}

    // IJournalledDevice

    [[nodiscard]] auto ReadPages(
        NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> final
    {
        return DataStore->ReadPages(std::move(request));
    }

    [[nodiscard]] auto WriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request)
        -> TFuture<NCloud::NProto::TWriteLogRecordResponse> final
    {
        if (request.GetLogSequenceNumber() <= request.GetPrevLogSequenceNumber()) {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(E_ARGUMENT, TStringBuilder()
                    << "invalid lsn: " << request.GetLogSequenceNumber()
                    << ", must be greater than the prev one: "
                    << request.GetPrevLogSequenceNumber()));
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

        return DataStore->WritePages(std::move(request)).Apply(
            [self = shared_from_this(), lsn](const auto& future) mutable
                -> NCloud::NProto::TWriteLogRecordResponse
            {
                if (future.HasException()) {
                    return TErrorResponse(ResultOrError(future).GetError());
                }

                const auto& response = future.GetValue();
                if (HasError(response)) {
                    return response;
                }

                self->LastLsn.store(lsn, std::memory_order_relaxed);
                return {};
            });
    }

    [[nodiscard]] auto ReadJournalTail(
        NCloud::NProto::TReadJournalTailRequest request)
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> final
    {
        // TODO(#6956): implement journal tail reading
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TReadJournalTailResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED, "ReadJournalTail"));
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        // TODO(#6956): implement lsn low watermark advancing
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED, "AdvanceLsnLowWatermark"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDevice(IDevicePtr dataStore)
{
    return std::make_shared<TJournalledDevice>(std::move(dataStore));
}

}   // namespace NCloud::NJournalled
