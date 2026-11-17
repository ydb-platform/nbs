#include "journalled_device_v1.h"

#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/libs/journal/iface/journalled_device.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

#include <atomic>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceV1 final
    : public IJournalledDevice
    , public std::enable_shared_from_this<TJournalledDeviceV1>
{
private:
    const IDevicePtr DataStore;

    std::atomic<ui64> LastLsn = 0;
    std::atomic<ui64> LastAckedLsn = 0;

public:
    explicit TJournalledDeviceV1(IDevicePtr dataStore)
        : DataStore(std::move(dataStore))
    {}

    // IJournalledDevice

    TFuture<NProto::TError> Start() noexcept final
    {
        return MakeFuture<NProto::TError>();
    }

    TFuture<void> Stop() noexcept final
    {
        return MakeFuture();
    }

    TFuture<TResultOrError<TReadPagesResult>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) final
    {
        return DataStore->ReadPages(std::move(rangeRefs))
            .Apply(
                [](auto future) -> TResultOrError<TReadPagesResult>
                {
                    auto result = future.ExtractValue();
                    if (HasError(result)) {
                        return result.GetError();
                    }

                    return TReadPagesResult{.Pages = result.ExtractResult()};
                });
    }

    TFuture<NProto::TError> WriteLogRecord(
        TVector<TPageRange> ranges,
        ui64 lsn,
        ui64 prevLsn) final
    {
        if (lsn <= prevLsn) {
            return MakeFuture(MakeError(
                E_ARGUMENT,
                TStringBuilder() << "invalid lsn: " << lsn
                                 << ", must be greater than the prev one: "
                                 << prevLsn));
        }

        // TODO(#6956): check PrevLsn against LastLsn; now PrevLsn is always 0

        auto future = DataStore->WritePages(std::move(ranges));

        return future.Apply(
            [self = shared_from_this(), lsn](const auto& future)
            {
                auto error = ExtractResponse(future);
                if (!HasError(error)) {
                    self->LastLsn.store(lsn, std::memory_order_relaxed);
                }

                return error;
            });
    }

    TFuture<TResultOrError<TJournalTail>> ReadJournalTail(
        ui64 afterLsn,
        ui32 maxRecordCount) final
    {
        Y_UNUSED(afterLsn, maxRecordCount);

        TJournalTail tail{
            .LsnLowWatermark = LastAckedLsn.load(std::memory_order_relaxed)};

        return MakeFuture<TResultOrError<TJournalTail>>(std::move(tail));
    }

    TFuture<NProto::TError> AdvanceWatermark(ui64 lsnLowWatermark) final
    {
        LastAckedLsn.store(lsnLowWatermark, std::memory_order_relaxed);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> WritePages(TVector<TPageRange> ranges) final
    {
        return DataStore->WritePages(std::move(ranges))
            .Apply([](const auto& future) { return ExtractResponse(future); });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceV1(IDevicePtr dataStore)
{
    return std::make_shared<TJournalledDeviceV1>(std::move(dataStore));
}

}   // namespace NCloud::NJournalled
