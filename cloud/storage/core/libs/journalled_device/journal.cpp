#include "journal.h"

#include "device_page_store.h"
#include "key_buffer_store.h"

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJournal final: public IJournal
{
private:
    IKeyBufferStorePtr MetaStore;
    IDevicePageStorePtr DataStore;

public:
    TJournal(
        IKeyBufferStorePtr metaStore,
        IDevicePageStorePtr dataStore)
        : MetaStore(std::move(metaStore))
        , DataStore(std::move(dataStore))
    {}

    TFuture<TResultOrError<ui64>> Restore() override
    {
        return MakeFuture<TResultOrError<ui64>>(
            MakeError(E_NOT_IMPLEMENTED, "Restore"));
    }

    TFuture<NCloud::NProto::TWriteLogRecordResponse> Write(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED, "Write"));
    }

    TFuture<NCloud::NProto::TReadPagesResponse> Read(
        NCloud::NProto::TReadPagesRequest request) const override
    {
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TReadPagesResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED, "Read"));
    }

    auto ReadTail(NCloud::NProto::TReadJournalTailRequest request) const
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> override
    {
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TReadJournalTailResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED, "ReadTail"));
    }

    auto AdvanceLastAckedLsn(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> override
    {
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED, "AdvanceLastAckedLsn"));
    }

    auto GetRecordToFlush(ui64 maxAllowedLsn) const
        -> TFuture<TResultOrError<NCloud::NProto::TJournalRecord>> override
    {
        Y_UNUSED(maxAllowedLsn);

        return MakeFuture<TResultOrError<NCloud::NProto::TJournalRecord>>(
            MakeError(E_NOT_IMPLEMENTED, "GetRecordToFlush"));
    }

    void MarkRecordAsFlushed(ui64 lsn) override
    {
        Y_UNUSED(lsn);
    }

    TFuture<NCloud::NProto::TError> CleanupFlushedRecords() override
    {
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED, "CleanupFlushedRecords"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalPtr CreateJournal(
    IKeyBufferStorePtr metaStore,
    IDevicePageStorePtr dataStore)
{
    return std::make_shared<TJournal>(
        std::move(metaStore),
        std::move(dataStore));
}

}   // namespace NCloud::NJournalled
