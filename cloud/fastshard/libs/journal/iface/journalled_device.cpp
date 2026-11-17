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

    TFuture<TResultOrError<TReadPagesResult>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) final
    {
        Y_UNUSED(rangeRefs);
        return MakeFuture<TResultOrError<TReadPagesResult>>(TReadPagesResult());
    }

    TFuture<NProto::TError> WriteLogRecord(TJournalRecord logRecord) final
    {
        Y_UNUSED(logRecord);
        return MakeFuture<NProto::TError>();
    }

    TFuture<TResultOrError<TJournalTail>> ReadJournalTail(
        ui64 afterLsn,
        ui32 maxRecordCount) final
    {
        Y_UNUSED(afterLsn, maxRecordCount);
        return MakeFuture<TResultOrError<TJournalTail>>(TJournalTail());
    }

    TFuture<NProto::TError> AdvanceWatermark(ui64 lsnLowWatermark) final
    {
        Y_UNUSED(lsnLowWatermark);
        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> WritePages(TVector<TPageRange> ranges) final
    {
        Y_UNUSED(ranges);
        return MakeFuture<NProto::TError>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceStub()
{
    return std::make_shared<TJournalledDeviceStub>();
}

}   // namespace NCloud::NJournalled
