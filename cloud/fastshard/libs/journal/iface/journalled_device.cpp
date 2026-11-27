#include "journalled_device.h"

#include <cstring>

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

    TFuture<TResultOrError<ui64>> ReadPages(
        TVector<TPageRangeView> rangeViews) final
    {
        // the stub holds no data, every page reads as a zeroed one
        for (const auto& view: rangeViews) {
            for (const auto& page: view.Pages) {
                std::memset(page.data(), 0, page.size());
            }
        }

        return MakeFuture<TResultOrError<ui64>>(0);
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
