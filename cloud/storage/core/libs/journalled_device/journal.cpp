#include "journal.h"

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJournal final: public IJournal
{
public:
    TFuture<NCloud::NProto::TError> Restore() override
    {
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED, "Restore"));
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

    auto GetFirstRecordToFlush() const
        -> TFuture<TResultOrError<NCloud::NProto::TJournalRecord>> override
    {
        return MakeFuture<TResultOrError<NCloud::NProto::TJournalRecord>>(
            MakeError(E_NOT_IMPLEMENTED, "GetFirstRecordToFlush"));
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

IJournalPtr CreateJournal()
{
    return std::make_shared<TJournal>();
}

}   // namespace NCloud::NJournalled
