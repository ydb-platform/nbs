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
        // One buffer per requested page: the callers index the pages by
        // their position in the request.
        ui64 pageCount = 0;
        for (const auto& ref: rangeRefs) {
            pageCount += ref.PageCount;
        }

        TReadPagesResult result;
        result.Pages.resize(pageCount);
        return MakeFuture<TResultOrError<TReadPagesResult>>(std::move(result));
    }

    TFuture<NProto::TError> WriteLogRecord(
        TVector<TPageRange> ranges,
        ui64 lsn,
        ui64 prevLsn) final
    {
        Y_UNUSED(ranges, lsn, prevLsn);
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
