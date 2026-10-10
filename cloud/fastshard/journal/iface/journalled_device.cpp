#include "journalled_device.h"

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceStub final: public IJournalledDevice
{
public:
    // IJournalledDevice

    TFuture<NProto::TError> Start() override
    {
        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> Stop() override
    {
        return MakeFuture<NProto::TError>();
    }

    [[nodiscard]] auto ReadPages(NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> final
    {
        Y_UNUSED(request);
        return MakeFuture<NCloud::NProto::TReadPagesResponse>({});
    }

    [[nodiscard]] auto WriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request)
        -> TFuture<NCloud::NProto::TWriteLogRecordResponse> final
    {
        Y_UNUSED(request);
        return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>({});
    }

    [[nodiscard]] auto ReadJournalTail(
        NCloud::NProto::TReadJournalTailRequest request)
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> final
    {
        Y_UNUSED(request);
        return MakeFuture<NCloud::NProto::TReadJournalTailResponse>({});
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        Y_UNUSED(request);
        return MakeFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>({});
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceStub()
{
    return std::make_shared<TJournalledDeviceStub>();
}

}   // namespace NCloud::NJournalled
