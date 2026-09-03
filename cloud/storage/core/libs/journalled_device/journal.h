#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

class TJournal
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TJournal(IKeyBufferStorePtr logMetaStore, IPageStorePtr logDataStore);
    ~TJournal();

    // Restoring

    [[nodiscard]] NThreading::TFuture<NCloud::NProto::TError> Restore();

    // Device API

    [[nodiscard]] auto Write(
        const NCloud::NProto::TWriteLogRecordRequest& request)
        -> NThreading::TFuture<NCloud::NProto::TError>;

    [[nodiscard]] auto Read(
        const NCloud::NProto::TReadPagesRequest& request) const
        -> NThreading::TFuture<NCloud::NProto::TReadPagesResponse>;

    [[nodiscard]] auto ReadTail(ui64 afterLsn, ui64 maxRecordCnt) const
        -> NThreading::TFuture<NCloud::NProto::TReadJournalTailResponse>;

    [[nodiscard]] auto AdvanceLastAckedLsn(ui64 lastAckedLsn)
        -> NThreading::TFuture<NCloud::NProto::TError>;

    // Background cleanup

    [[nodiscard]] auto GetFirstRecordToFlush() const
        -> TFutureResultOrError<NCloud::NProto::TJournalRecord>;

    void MarkRecordAsFlushed(ui64 lsn);

    [[nodiscard]] auto CleanupFlushedRecords()
        -> NThreading::TFuture<NCloud::NProto::TError>;
};

}   // namespace NCloud::NJournalled
