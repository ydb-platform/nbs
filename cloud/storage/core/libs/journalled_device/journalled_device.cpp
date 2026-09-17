#include "journalled_device.h"

#include "device.h"
#include "device_helpers.h"

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
    std::atomic<ui64> LastAckedLsn = 0;

public:
    explicit TJournalledDevice(IDevicePtr dataStore)
        : DataStore(std::move(dataStore))
    {}

    // IJournalledDevice

    void Start() override
    {}

    void Stop() override
    {}

    [[nodiscard]] auto ReadPages(
        NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> final
    {
        auto rangeRefs = MakePageRangeRefs(request);

        auto future = DataStore->ReadPages(rangeRefs);

        return future.Apply(
            [rangeRefs = std::move(rangeRefs)](const auto& future)
                -> NCloud::NProto::TReadPagesResponse
            {
                const auto& result = future.GetValue();
                if (HasError(result)) {
                    return TErrorResponse(result.GetError());
                }

                return MakeReadPagesResponse(rangeRefs, result.GetResult());
            });
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

        const ui64 lsn = request.GetLogSequenceNumber();

        // TODO(#6956): now PrevLogSequenceNumber is always zero
        // const ui64 prevLsn = request.GetPrevLogSequenceNumber();
        // const ui64 lastLsn = LastLsn.load(std::memory_order_relaxed);

        // if (lastLsn != 0 && prevLsn != lastLsn) {
        //     const auto code = prevLsn > lastLsn ? E_REJECTED : E_INVALID_STATE;

        //     return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
        //         TErrorResponse(code, TStringBuilder()
        //             << "Wrong lsn: " << prevLsn << ", expected " << lastLsn));
        // }

        auto future = DataStore->WritePages(MakePageRanges(request));

        return future.Apply(
            [self = shared_from_this(), lsn](const auto& future) mutable
                -> NCloud::NProto::TWriteLogRecordResponse
            {
                if (future.HasException()) {
                    return TErrorResponse(
                        ResultOrError(future.IgnoreResult()).GetError());
                }

                const auto& error = future.GetValue();
                if (HasError(error)) {
                    return TErrorResponse(error);
                }

                self->LastLsn.store(lsn, std::memory_order_relaxed);
                return {};
            });
    }

    [[nodiscard]] auto ReadJournalTail(
        NCloud::NProto::TReadJournalTailRequest request)
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> final
    {
        Y_UNUSED(request);

        NCloud::NProto::TReadJournalTailResponse response;
        response.SetLastAckedLogSequenceNumber(
            LastAckedLsn.load(std::memory_order_relaxed));

        return MakeFuture(response);
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        LastAckedLsn.store(
            request.GetLsnLowWatermark(),
            std::memory_order_relaxed);

        NCloud::NProto::TAdvanceLsnLowWatermarkResponse response;
        return MakeFuture(response);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDevice(IDevicePtr dataStore)
{
    return std::make_shared<TJournalledDevice>(std::move(dataStore));
}

}   // namespace NCloud::NJournalled
