#include "journalled_device.h"

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceStub final: public IJournalledDevice
{
public:
    // IJournalledDevice

    TFuture<NProto::TError> Start() noexcept override
    {
        return MakeFuture<NProto::TError>();
    }

    TFuture<void> Stop() noexcept override
    {
        return MakeFuture();
    }

    [[nodiscard]] auto ReadPages(NProto::TReadPagesRequest request)
        -> TFuture<NProto::TReadPagesResponse> final
    {
        Y_UNUSED(request);
        return MakeFuture<NProto::TReadPagesResponse>({});
    }

    [[nodiscard]] auto WriteLogRecord(NProto::TWriteLogRecordRequest request)
        -> TFuture<NProto::TWriteLogRecordResponse> final
    {
        Y_UNUSED(request);
        return MakeFuture<NProto::TWriteLogRecordResponse>({});
    }

    [[nodiscard]] auto ReadJournalTail(NProto::TReadJournalTailRequest request)
        -> TFuture<NProto::TReadJournalTailResponse> final
    {
        Y_UNUSED(request);
        return MakeFuture<NProto::TReadJournalTailResponse>({});
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        Y_UNUSED(request);
        return MakeFuture<NProto::TAdvanceLsnLowWatermarkResponse>({});
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceStub()
{
    return std::make_shared<TJournalledDeviceStub>();
}

}   // namespace NCloud::NJournalled
